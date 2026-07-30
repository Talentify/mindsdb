from __future__ import annotations

import itertools
import json
import time
from datetime import datetime, timedelta, timezone
from typing import Any

import pandas as pd
from mindsdb_sql_parser import ast

from mindsdb.integrations.handlers.meta_ads_handler.errors import MetaAdsAPIError, to_int
from mindsdb.integrations.libs.api_handler import APIResource
from mindsdb.integrations.utilities.sql_utils import FilterOperator
from mindsdb.utilities import log

from .utils import _collect_identifiers, _get_condition_value, _get_condition_values, _to_numeric

logger = log.getLogger(__name__)


class InsightsTable(APIResource):
    """Meta Ads performance insights. GET /{account_path}/insights.

    Pattern B: the selected columns determine the Graph `fields` param (see select()
    override below). Derived columns (link_clicks, purchases, roas, ...) are computed
    locally from the raw `actions`/`action_values` payloads and force those raw fields
    into the request even when the derived column itself is selected instead.

    WHERE params (all handler-consumed, all marked applied):
      - level: 'account' | 'campaign' | 'adset' | 'ad'. Defaults to 'campaign'.
      - start_date / end_date ('YYYY-MM-DD'): maps to Graph time_range={"since","until"}.
      - date_preset: mutually exclusive with start_date/end_date -- if both are given,
        the explicit range wins and a warning is logged.
      - neither start_date/end_date nor date_preset given: defaults to the last 30 days
        ending today (UTC).
      - time_increment: int-like or 'all_days' / 'monthly'. When not given explicitly,
        defaults to 1 (daily rows) if date_start or date_stop is among the resolved
        fields, otherwise 'all_days' (one aggregated row for the range) -- daily is the
        correct raw grain for time-series queries, and the SubSelectStep/DuckDB layer
        handles any further GROUP BY/SUM on top.
      - breakdowns: comma-separated string or IN (...) list, each value validated
        against BREAKDOWN_COLUMNS (all 37 doc-enumerated values -- a value outside that
        list is a typo, so this still raises). Any selected column that is itself a
        breakdown column (e.g. `SELECT age, impressions ...`) is auto-added here too,
        so it isn't silently returned as all-None. The *combination* of breakdowns is
        NOT validated the same way: Meta separately publishes an allow-list of valid
        permutations (DOCUMENTED_BREAKDOWN_PERMUTATIONS) that is demonstrably out of
        sync with the value list, so an undocumented combination only logs a warning
        and is still sent -- the API is the authority on whether a combination works.
      - action_breakdowns: comma-separated string or IN (...) list, passed through
        unvalidated -- the docs never enumerate this parameter exhaustively (see
        ACTION_BREAKDOWN_COLUMNS below).
      - campaign_id / adset_id / ad_id: '=' -> Graph filtering EQUAL entry; IN (...) ->
        Graph filtering IN entry.

    LIMIT pushdown: query_planner.py's plan_api_db_select clears query.limit before
    building the SubSelectStep, so for GROUP BY / aggregate / any non-Identifier target
    (SUM(...), CASE, ...) the LIMIT reaching list() would be the only limit applied
    anywhere in the plan. Pushing it to Graph as a raw-row page size would truncate the
    rows DuckDB aggregates over, silently corrupting SUM/AVG/etc. select() detects this
    shape and sets self._push_limit = False so list() fetches everything instead.

    Large-request async fallback: a synchronous call can fail with error.code == 1 and
    a message mentioning "reduce the amount of data" (also seen as error_subcode 99).
    On exactly that failure this table falls back to POST .../insights (report_run_id),
    polls GET /{report_run_id} every 5s (cap 300s), then paginates GET /{report_run_id}/insights.
    """

    DIMENSION_COLUMNS = [
        "date_start",
        "date_stop",
        "account_id",
        "account_name",
        "campaign_id",
        "campaign_name",
        "adset_id",
        "adset_name",
        "ad_id",
        "ad_name",
        "objective",
        "buying_type",
    ]

    METRIC_COLUMNS = [
        "impressions",
        "reach",
        "frequency",
        "clicks",
        "unique_clicks",
        "spend",
        "cpc",
        "cpm",
        "cpp",
        "ctr",
        "unique_ctr",
        "inline_link_clicks",
        "inline_link_click_ctr",
        "cost_per_inline_link_click",
    ]

    RAW_NESTED_COLUMNS = ["actions", "action_values", "cost_per_action_type", "purchase_roas"]

    DERIVED_COLUMNS = [
        "link_clicks",
        "landing_page_views",
        "leads",
        "post_engagements",
        "video_views",
        "purchases",
        "purchase_value",
        "roas",
    ]

    # `breakdowns` request param -- the doc-enumerated 37 values from the "Generic
    # breakdowns" table (tasks/meta-ads-api-research/breakdowns.md). Copied exactly;
    # per-value validation stays in place because this list is doc-enumerated -- a
    # value outside it is a typo, not an under-documented gap.
    BREAKDOWN_COLUMNS = [
        "age",
        "gender",
        "country",
        "region",
        "publisher_platform",
        "platform_position",
        "impression_device",
        "device_platform",
        # `dma` is still in Meta's published value table but the live API now rejects it
        # outright: "(#100) dma breakdown is no longer supported; ... please instead use
        # comscore_market breakdown." It is kept here so the user gets that explicit,
        # actionable API message naming the replacement, rather than a client-side
        # "invalid breakdown" that hides it. `comscore_market` is NOT in the published
        # table -- it was named by the API itself and verified to be accepted live, which
        # is stronger evidence than the (demonstrably stale) doc.
        "dma",
        "comscore_market",
        "hourly_stats_aggregated_by_advertiser_time_zone",
        "hourly_stats_aggregated_by_audience_time_zone",
        "frequency_value",
        "product_id",
        "app_id",
        "skan_campaign_id",
        "skan_conversion_id",
        "is_conversion_id_modeled",
        "user_segment_key",
        "place_page_id",
        "ad_format_asset",
        "body_asset",
        "call_to_action_asset",
        "description_asset",
        "image_asset",
        "link_url_asset",
        "title_asset",
        "video_asset",
        "action_device",
        "action_destination",
        "action_target_id",
        "action_type",
        "action_reaction",
        "action_carousel_card_id",
        "action_carousel_card_name",
        "action_canvas_component_name",
        "action_video_sound",
        "action_video_type",
    ]

    # `action_breakdowns` request param. `action_converted_product_id` appears ONLY in
    # the "Combining Breakdowns" permutations table, never in the "Generic breakdowns"
    # value table -- so it is kept separate from BREAKDOWN_COLUMNS rather than added to
    # it. This constant exists for documentation/reference only: action_breakdowns
    # stays pass-through/unvalidated (see list()), because the docs never exhaustively
    # enumerate that parameter's legal values.
    ACTION_BREAKDOWN_COLUMNS = ["action_converted_product_id"]

    # Breakdowns for which reach/frequency/unique_* metrics come back silently zeroed
    # instead of erroring (documented "Interactions and caveats" -- a data-correctness
    # risk worth warning about, not a hard rejection).
    HOURLY_BREAKDOWNS = {
        "hourly_stats_aggregated_by_advertiser_time_zone",
        "hourly_stats_aggregated_by_audience_time_zone",
    }

    # ---- Documented valid `breakdowns` permutations ("Combining Breakdowns" table) ----
    # Quoted from tasks/meta-ads-api-research/breakdowns.md. Two bits of notation
    # required a deliberate interpretation, spelled out here rather than silently
    # dropped:
    #   - `a / b` (e.g. "action_carousel_card_id / action_carousel_card_name") is
    #     documented as a *joint* pair ("Documented jointly as ..."), so it expands
    #     into ONE set containing both names together -- not two independent options.
    #   - A trailing `*` marks a row "additionally joinable with action_type,
    #     action_target_id, and action_destination". Read literally, that allows any
    #     combination of those three layered on top of the base row, so each starred
    #     base is expanded into the full powerset of {action_type, action_target_id,
    #     action_destination} added to it (8 variants, including the bare base). This
    #     feeds a warn-only check (see Defect 2 spec / list() below), so over-generating
    #     here is the safe direction: the worst case is a genuinely-invalid combination
    #     that never gets a warning, not a valid one wrongly flagged.
    _ACTION_JOINABLE = ("action_type", "action_target_id", "action_destination")

    _BASE_BREAKDOWN_PERMUTATIONS: list[tuple[frozenset, bool]] = [
        (frozenset({"action_converted_product_id"}), False),
        (frozenset({"action_type"}), True),
        (frozenset({"action_type", "action_converted_product_id"}), False),
        (frozenset({"action_target_id"}), True),
        (frozenset({"action_device"}), True),
        (frozenset({"action_device", "impression_device"}), True),
        (frozenset({"action_device", "publisher_platform"}), True),
        (frozenset({"action_device", "publisher_platform", "impression_device"}), True),
        (frozenset({"action_device", "publisher_platform", "platform_position"}), True),
        (
            frozenset(
                {"action_device", "publisher_platform", "platform_position", "impression_device"}
            ),
            True,
        ),
        (frozenset({"action_reaction"}), False),
        (frozenset({"action_type", "action_reaction"}), False),
        (frozenset({"age"}), True),
        (frozenset({"gender"}), True),
        (frozenset({"age", "gender"}), True),
        (frozenset({"app_id", "skan_conversion_id"}), False),
        (frozenset({"country"}), True),
        (frozenset({"region"}), True),
        (frozenset({"publisher_platform"}), True),
        (frozenset({"publisher_platform", "impression_device"}), True),
        (frozenset({"publisher_platform", "platform_position"}), True),
        (frozenset({"publisher_platform", "platform_position", "impression_device"}), True),
        (frozenset({"product_id"}), True),
        (frozenset({"hourly_stats_aggregated_by_advertiser_time_zone"}), True),
        (frozenset({"hourly_stats_aggregated_by_audience_time_zone"}), True),
        (frozenset({"action_carousel_card_id", "action_carousel_card_name"}), False),
        (
            frozenset({"action_carousel_card_id", "action_carousel_card_name", "impression_device"}),
            False,
        ),
        (frozenset({"action_carousel_card_id", "action_carousel_card_name", "country"}), False),
        (frozenset({"action_carousel_card_id", "action_carousel_card_name", "age"}), False),
        (frozenset({"action_carousel_card_id", "action_carousel_card_name", "gender"}), False),
        (
            frozenset({"action_carousel_card_id", "action_carousel_card_name", "age", "gender"}),
            False,
        ),
    ]

    # DOCUMENTED_BREAKDOWN_PERMUTATIONS (the powerset expansion of the starred rows
    # above) is assigned right after the class body -- a comprehension inside a class
    # body can't see sibling class attributes from its nested `for`/`if` clauses (only
    # the outermost iterable resolves in class scope), so the expansion has to happen
    # as a module-level step instead.

    DEFAULT_FIELDS = [
        "campaign_id",
        "campaign_name",
        "impressions",
        "clicks",
        "spend",
        "reach",
        "ctr",
        "cpc",
        "date_start",
        "date_stop",
    ]

    VALID_LEVELS = {"account", "campaign", "adset", "ad"}

    # Fields that can be requested from Graph directly (dimensions/metrics/raw nested).
    GRAPH_SENDABLE_FIELDS = set(DIMENSION_COLUMNS) | set(METRIC_COLUMNS) | set(RAW_NESTED_COLUMNS)

    # Derived columns are never sent to Graph as fields -- they expand to these raw deps.
    DERIVED_FIELD_DEPENDENCIES = {
        "link_clicks": {"actions"},
        "landing_page_views": {"actions"},
        "leads": {"actions"},
        "post_engagements": {"actions"},
        "video_views": {"actions"},
        "purchases": {"actions"},
        "purchase_value": {"action_values"},
        "roas": {"action_values", "spend"},
    }

    ASYNC_REPORT_TIMEOUT_SECONDS = 300
    ASYNC_REPORT_POLL_SECONDS = 5

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._requested_fields: list[str] | None = None
        # Whether list() may push the SQL LIMIT down to Graph's `limit` param. False
        # for aggregate/complex-target queries -- see select() for why.
        self._push_limit: bool = True

    def get_columns(self) -> list[str]:
        return (
            self.DIMENSION_COLUMNS
            + self.METRIC_COLUMNS
            + self.RAW_NESTED_COLUMNS
            + self.DERIVED_COLUMNS
            + self.BREAKDOWN_COLUMNS
        )

    def select(self, query) -> pd.DataFrame:
        # Reset first so a stale value from a previous query can never leak.
        self._requested_fields = None
        self._push_limit = True

        targets = query.targets or []
        has_star = any(isinstance(target, ast.Star) for target in targets)

        if targets and not has_star:
            seen = set()
            collected: list[str] = []
            for target in targets:
                for name in _collect_identifiers(target):
                    if name not in seen:
                        seen.add(name)
                        collected.append(name)
            if collected:
                self._requested_fields = collected

        # query_planner.py's plan_api_db_select clears query.limit before the
        # SubSelectStep is built, so for aggregate/complex-target queries the LIMIT we
        # receive here in list() is the ONLY limit applied anywhere in the plan --
        # DuckDB does not re-apply it after GROUP BY/SUM. If we still pushed it down
        # to Graph as a raw-row page size, we'd fetch a truncated set of (e.g.) daily
        # rows and DuckDB would aggregate over that truncated set -- silently wrong
        # SUM/AVG/etc. So for GROUP BY or any non-Identifier/non-Star target (CASE,
        # SUM(...), etc.) we must not push the limit down; fetch everything so the
        # aggregation is correct, even though that means the SQL LIMIT effectively
        # goes unenforced for these query shapes (a planner-level limitation, not
        # something this handler can fix on its own).
        is_complex = bool(query.group_by) or any(
            not isinstance(target, (ast.Identifier, ast.Star)) for target in targets
        )
        if is_complex:
            self._push_limit = False

        return super().select(query)

    def _resolve_graph_fields(self, requested_fields: list[str]) -> list[str]:
        resolved: list[str] = []
        seen = set()

        def _add(name: str) -> None:
            if name not in seen:
                seen.add(name)
                resolved.append(name)

        for name in requested_fields:
            if name in self.DERIVED_FIELD_DEPENDENCIES:
                for dep in self.DERIVED_FIELD_DEPENDENCIES[name]:
                    _add(dep)
            elif name in self.GRAPH_SENDABLE_FIELDS:
                _add(name)
            # Breakdown columns and unrecognised names are never sent as fields --
            # breakdowns arrive via the `breakdowns` param instead.

        if not resolved:
            for name in self.DEFAULT_FIELDS:
                _add(name)

        return resolved

    @staticmethod
    def _normalize_list_values(values: list[Any]) -> list[Any]:
        result: list[Any] = []
        for value in values:
            if isinstance(value, str) and "," in value:
                result.extend(part.strip() for part in value.split(",") if part.strip())
            else:
                result.append(value)
        return result

    def list(self, conditions=None, limit=None, sort=None, targets=None, **kwargs):
        conditions = conditions or []

        level = _get_condition_value(conditions, "level") or "campaign"
        if level not in self.VALID_LEVELS:
            raise ValueError(f"Invalid level '{level}'. Valid values are: {sorted(self.VALID_LEVELS)}")

        requested_fields = self._requested_fields or list(self.DEFAULT_FIELDS)
        self._requested_fields = None  # consumed
        resolved_fields = self._resolve_graph_fields(requested_fields)

        params: dict[str, Any] = {"level": level}

        start_date = _get_condition_value(conditions, "start_date")
        end_date = _get_condition_value(conditions, "end_date")
        date_preset = _get_condition_value(conditions, "date_preset")

        if (start_date or end_date) and date_preset:
            logger.warning(
                "meta_ads.insights: both an explicit date range and date_preset were given; the explicit range wins"
            )
            date_preset = None

        today = datetime.now(timezone.utc).date()
        if start_date or end_date:
            since = start_date or (today - timedelta(days=30)).isoformat()
            until = end_date or today.isoformat()
            params["time_range"] = {"since": since, "until": until}
        elif date_preset:
            params["date_preset"] = date_preset
        else:
            since = today - timedelta(days=30)
            params["time_range"] = {"since": since.isoformat(), "until": today.isoformat()}

        time_increment = _get_condition_value(conditions, "time_increment")
        if time_increment is None:
            if "date_start" in resolved_fields or "date_stop" in resolved_fields:
                time_increment = 1
            else:
                time_increment = "all_days"
        params["time_increment"] = time_increment

        breakdowns = _get_condition_values(conditions, "breakdowns") or []
        breakdowns = self._normalize_list_values(breakdowns)

        # Auto-add any selected breakdown column (e.g. `SELECT age, impressions ...`)
        # that wasn't already requested via WHERE breakdowns=... . Without this, Graph
        # simply never returns that key and the column comes back all-None -- exactly
        # the silent-empty-column failure mode CLAUDE.md's handler checklist warns
        # about. Explicitly-requested breakdowns are preserved and kept first.
        for field in requested_fields:
            if field in self.BREAKDOWN_COLUMNS and field not in breakdowns:
                breakdowns.append(field)

        if breakdowns:
            for breakdown in breakdowns:
                if breakdown not in self.BREAKDOWN_COLUMNS:
                    raise ValueError(
                        f"Invalid breakdown '{breakdown}'. Valid values are: {self.BREAKDOWN_COLUMNS}"
                    )

            # Per-value validation above is a hard gate (the value list is
            # doc-enumerated). The *combination* is a soft, warn-only check: Meta
            # separately publishes an allow-list of valid permutations, but that list
            # is demonstrably out of sync with the value list (action_converted_
            # product_id appears in one and not the other), and the `filtering` param
            # precedent proves these docs under-document real API behaviour. So an
            # undocumented combination is logged, not rejected -- the API gets to
            # decide, and list() below re-raises with this context if it says no.
            if frozenset(breakdowns) not in self.DOCUMENTED_BREAKDOWN_PERMUTATIONS:
                logger.warning(
                    "meta_ads.insights: breakdown combination %s is not among the "
                    "documented valid permutations; sending it anyway since the "
                    "permutation table is known to be incomplete",
                    sorted(breakdowns),
                )

            # hourly_stats_aggregated_by_* silently zeroes reach/frequency/unique_*
            # instead of erroring -- a wrong-data risk worth surfacing even though it
            # isn't a hard failure.
            requested_hourly = self.HOURLY_BREAKDOWNS.intersection(breakdowns)
            if requested_hourly:
                zeroed_fields = {
                    field
                    for field in resolved_fields
                    if field in ("reach", "frequency") or field.startswith("unique_")
                }
                if zeroed_fields:
                    logger.warning(
                        "meta_ads.insights: hourly breakdown(s) %s combined with %s -- "
                        "Meta returns 0 for these fields rather than erroring when an "
                        "hourly breakdown is present",
                        sorted(requested_hourly),
                        sorted(zeroed_fields),
                    )

            params["breakdowns"] = breakdowns

        action_breakdowns = _get_condition_values(conditions, "action_breakdowns")
        if action_breakdowns:
            params["action_breakdowns"] = self._normalize_list_values(action_breakdowns)

        filtering = []
        for column, graph_field in (("campaign_id", "campaign.id"), ("adset_id", "adset.id"), ("ad_id", "ad.id")):
            value = _get_condition_value(conditions, column, ops=(FilterOperator.EQUAL,))
            if value is not None:
                filtering.append({"field": graph_field, "operator": "EQUAL", "value": value})
                continue
            values = _get_condition_values(conditions, column, ops=(FilterOperator.IN,))
            if values is not None:
                filtering.append({"field": graph_field, "operator": "IN", "value": values})
        if filtering:
            params["filtering"] = filtering

        params["fields"] = ",".join(resolved_fields)

        # See select(): for aggregate/complex-target queries the limit must not be
        # pushed down to Graph, since it would truncate the raw rows DuckDB aggregates.
        push_limit = self._push_limit
        self._push_limit = True  # consumed
        fetch_limit = limit if push_limit else None

        try:
            rows = self.handler.graph_get_all(f"{self.handler.account_path}/insights", params, limit=fetch_limit)
        except MetaAdsAPIError as exc:
            error_info = exc.error_info or {}
            # Meta signals a bad breakdown combination two different ways, so match both.
            # The documented pair is (2, 1504041) "Invalid Breakdowns", but live testing
            # shows an invalid combination actually comes back as a generic code 100
            # OAuthException with no subcode -- e.g. "(#100) Current combination of data
            # breakdown columns (action_type, age, country) is invalid". Keying only on
            # the documented pair would leave the enrichment as dead code for the case it
            # exists to explain, so fall back to matching the message.
            is_breakdown_error = (
                to_int(error_info.get("code")) == 2 and to_int(error_info.get("error_subcode")) == 1504041
            ) or "breakdown" in str(error_info.get("message") or "").lower()
            if breakdowns and is_breakdown_error:
                # Surface the requested set alongside the API's own message, since we
                # deliberately don't hard-block on the permutation table.
                detail = f"{exc} -- requested breakdowns {sorted(breakdowns)} were rejected by the API."
                # Only suggest alternatives for a genuine multi-breakdown combination
                # problem. A single-breakdown rejection (e.g. `dma` being retired in
                # favour of `comscore_market`) already carries an actionable message from
                # Meta naming the fix -- appending a permutation list there would bury it.
                if len(breakdowns) > 1:
                    # Restrict to permutations overlapping what was asked for, and cap
                    # the list: the full expansion is ~160 entries and dumping all of
                    # them turns a readable error into a wall of text.
                    requested = set(breakdowns)
                    related = sorted(
                        (sorted(perm) for perm in self.DOCUMENTED_BREAKDOWN_PERMUTATIONS if perm & requested),
                        key=lambda perm: (-len(set(perm) & requested), len(perm), perm),
                    )
                    if related:
                        shown = related[:10]
                        suffix = "" if len(related) <= 10 else f" (+{len(related) - 10} more)"
                        detail += f" Documented combinations involving these breakdowns: {shown}{suffix}"
                raise MetaAdsAPIError(detail, error_info) from exc
            if not exc.is_large_request_error():
                raise
            logger.info("meta_ads.insights: falling back to async report flow due to large request")
            rows = self._fetch_via_async_report(params, fetch_limit)

        return self._shape_rows(rows)

    def _fetch_via_async_report(self, params: dict[str, Any], limit: int | None) -> list[dict]:
        response = self.handler.graph_post(f"{self.handler.account_path}/insights", dict(params))
        report_run_id = response.get("report_run_id")
        if not report_run_id:
            raise RuntimeError("Meta Ads async insights report did not return a report_run_id")

        deadline = time.monotonic() + self.ASYNC_REPORT_TIMEOUT_SECONDS
        while True:
            status_payload = self.handler.graph_get(str(report_run_id), {})
            status = status_payload.get("async_status")
            if status == "Job Completed":
                break
            if status in {"Job Failed", "Job Skipped"}:
                raise RuntimeError(f"Meta Ads async insights report {status}: {status_payload}")
            if time.monotonic() >= deadline:
                raise TimeoutError("Meta Ads async insights report timed out waiting for completion")
            time.sleep(self.ASYNC_REPORT_POLL_SECONDS)

        return self.handler.graph_get_all(f"{report_run_id}/insights", {}, limit=limit)

    @staticmethod
    def _extract_action_value(actions: Any, action_types: list[str]) -> float:
        if not isinstance(actions, list):
            return 0
        for action_type in action_types:
            for entry in actions:
                if isinstance(entry, dict) and entry.get("action_type") == action_type:
                    try:
                        return float(entry.get("value", 0) or 0)
                    except (TypeError, ValueError):
                        return 0
        return 0

    def _compute_derived_columns(self, row: dict) -> dict:
        actions = row.get("actions")
        action_values = row.get("action_values")

        row["link_clicks"] = self._extract_action_value(actions, ["link_click"])
        row["landing_page_views"] = self._extract_action_value(actions, ["landing_page_view"])
        row["leads"] = self._extract_action_value(actions, ["lead"])
        row["post_engagements"] = self._extract_action_value(actions, ["post_engagement"])
        row["video_views"] = self._extract_action_value(actions, ["video_view"])
        row["purchases"] = self._extract_action_value(actions, ["omni_purchase", "purchase"])
        row["purchase_value"] = self._extract_action_value(action_values, ["omni_purchase", "purchase"])

        try:
            spend = float(row.get("spend") or 0)
        except (TypeError, ValueError):
            spend = 0

        row["roas"] = (row["purchase_value"] / spend) if spend > 0 else 0
        return row

    def _shape_rows(self, rows: list[dict]) -> pd.DataFrame:
        columns = self.get_columns()
        processed = [self._compute_derived_columns(dict(row)) for row in rows]

        df = pd.DataFrame(processed)
        if df.empty:
            return pd.DataFrame(columns=columns)

        for column in columns:
            if column not in df.columns:
                df[column] = None

        for column in self.RAW_NESTED_COLUMNS:
            df[column] = df[column].apply(lambda v: json.dumps(v) if isinstance(v, (dict, list)) else v)

        df = _to_numeric(df, self.METRIC_COLUMNS + self.DERIVED_COLUMNS)

        return df[columns]


# Expand each starred row of _BASE_BREAKDOWN_PERMUTATIONS into the powerset of
# _ACTION_JOINABLE layered on top; non-starred rows pass through unchanged (see the
# comment above _BASE_BREAKDOWN_PERMUTATIONS for why). This has to live at module
# level, not inside the class body: a comprehension in a class body only resolves its
# outermost iterable in the class's namespace -- nested `for`/`if` clauses run in the
# comprehension's own scope and can't see sibling class attributes like
# _ACTION_JOINABLE, so building this as a class-body one-liner raises NameError.
InsightsTable.DOCUMENTED_BREAKDOWN_PERMUTATIONS = frozenset(
    base | set(extra)
    for base, starred in InsightsTable._BASE_BREAKDOWN_PERMUTATIONS
    if starred
    for size in range(len(InsightsTable._ACTION_JOINABLE) + 1)
    for extra in itertools.combinations(InsightsTable._ACTION_JOINABLE, size)
) | frozenset(
    base for base, starred in InsightsTable._BASE_BREAKDOWN_PERMUTATIONS if not starred
)
