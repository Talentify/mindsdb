from __future__ import annotations

import json

import pandas as pd

from mindsdb.integrations.libs.api_handler import APIResource

from .utils import _get_condition_value, _get_condition_values, _join_list, _to_numeric


class AdSetsTable(APIResource):
    """Meta Ads ad sets. GET /{account_path}/adsets, or GET /{campaign_id}/adsets when
    campaign_id is given in WHERE (Pattern A: always fetch the full field list).

    Budget fields (daily_budget, lifetime_budget, budget_remaining, bid_amount,
    daily_min_spend_target, daily_spend_cap, lifetime_min_spend_target,
    lifetime_spend_cap) are returned by Graph in the account's currency minor units.
    Meta defines a per-currency offset controlling this: offset 100 (the common case,
    e.g. USD) means the value is expressed in 1/100ths of the base unit (divide by
    100 to get base units, e.g. cents to dollars); offset 1 (CLP, COP, CRC, HUF, ISK,
    IDR, JPY, KRW, PYG, TWD, VND) means the value already is the base unit, no
    division needed. Use the account's `currency` field (see account.py) to know
    which applies. We do not scale these values today.
    """

    COLUMNS = [
        "id",
        "name",
        "campaign_id",
        "status",
        "effective_status",
        "optimization_goal",
        "billing_event",
        "bid_strategy",
        "bid_amount",
        "daily_budget",
        "lifetime_budget",
        "budget_remaining",
        "destination_type",
        "start_time",
        "end_time",
        "created_time",
        "updated_time",
        "targeting",
        "promoted_object",
        "configured_status",
        "attribution_spec",
        "learning_stage_info",
        "issues_info",
        "daily_min_spend_target",
        "daily_spend_cap",
        "lifetime_min_spend_target",
        "lifetime_spend_cap",
        "frequency_control_specs",
        "source_adset_id",
        "dsa_payor",
        "dsa_beneficiary",
        # Flattened from `targeting` -- see _flatten_targeting(). geo_locations,
        # flexible_spec and exclusions stay JSON-only inside the `targeting` blob
        # (flexible_spec is a boolean expression tree; flattening it would lose the
        # AND/OR logic).
        "age_min",
        "age_max",
        "genders",
        "publisher_platforms",
        "device_platforms",
        "facebook_positions",
        "custom_audiences",
        "excluded_custom_audiences",
    ]

    NUMERIC_COLUMNS = [
        "bid_amount",
        "daily_budget",
        "lifetime_budget",
        "budget_remaining",
        "daily_min_spend_target",
        "daily_spend_cap",
        "lifetime_min_spend_target",
        "lifetime_spend_cap",
        "age_min",
        "age_max",
    ]
    JSON_COLUMNS = [
        "targeting",
        "promoted_object",
        "attribution_spec",
        "learning_stage_info",
        "issues_info",
        "frequency_control_specs",
        # custom_audiences/excluded_custom_audiences are lists of {id, name} objects,
        # not scalars -- they stay JSON. genders/publisher_platforms/device_platforms/
        # facebook_positions are lists of scalar strings and are comma-joined instead
        # (see LIST_COLUMNS below / _join_list in utils.py) to match how
        # campaigns.special_ad_categories already encodes the same kind of field.
        "custom_audiences",
        "excluded_custom_audiences",
    ]
    LIST_COLUMNS = ["genders", "publisher_platforms", "device_platforms", "facebook_positions"]

    # Columns derived from `targeting` client-side (see _flatten_targeting) rather
    # than requested directly from Graph -- they are not real top-level field names
    # and would 400 the request if sent as-is.
    #
    # VERIFICATION STATUS (live scan of 10 real ad sets, tasks/meta-ads-phase0-measurements.md,
    # Task 6f): only `age_min` and `age_max` were ever observed populated. The other six
    # were absent from every ad set scanned, so their flatten logic has never run against
    # a populated example -- treat them as unverified, NOT as verified-working.
    #
    # The key names are probably right rather than wrong: Meta omits unset targeting keys
    # instead of returning defaults, and the same scan did surface two keys we had not
    # accounted for at all (`locales`, `targeting_relaxation_types`), so it was capable of
    # seeing keys that exist. The likely explanation is that this account simply does not
    # set those targeting dimensions.
    #
    # To confirm: re-run the Task 6f scan against a more targeting-heavy account. Until
    # then, an always-empty `publisher_platforms` column here is unverified-not-broken.
    # The raw `targeting` JSON column remains the source of truth either way.
    _FLATTENED_TARGETING_COLUMNS = [
        "age_min",
        "age_max",
        "genders",
        "publisher_platforms",
        "device_platforms",
        "facebook_positions",
        "custom_audiences",
        "excluded_custom_audiences",
    ]
    _REQUEST_FIELDS = []
    for _col in COLUMNS:
        if _col not in _FLATTENED_TARGETING_COLUMNS:
            _REQUEST_FIELDS.append(_col)
    del _col

    def get_columns(self) -> list[str]:
        return self.COLUMNS

    @staticmethod
    def _flatten_targeting(row: dict) -> dict:
        """Flatten the fixed, commonly-filtered-on targeting sub-keys into their own
        columns. Every level can be absent; missing values become None, never a
        KeyError or an AttributeError. A `targeting` value that isn't a dict (e.g. a
        stray string) is treated as empty rather than raising. The raw `targeting`
        column (JSON-encoded) remains the source of truth for everything else,
        including geo_locations/flexible_spec/exclusions.
        """
        result = dict(row)
        targeting = row.get("targeting")
        if not isinstance(targeting, dict):
            targeting = {}

        result["age_min"] = targeting.get("age_min")
        result["age_max"] = targeting.get("age_max")
        result["genders"] = targeting.get("genders")
        result["publisher_platforms"] = targeting.get("publisher_platforms")
        result["device_platforms"] = targeting.get("device_platforms")
        result["facebook_positions"] = targeting.get("facebook_positions")
        result["custom_audiences"] = targeting.get("custom_audiences")
        result["excluded_custom_audiences"] = targeting.get("excluded_custom_audiences")

        return result

    def list(self, conditions=None, limit=None, sort=None, targets=None, **kwargs):
        conditions = conditions or []
        fields = ",".join(self._REQUEST_FIELDS)

        # Only consume campaign_id/effective_status on the path that honours them --
        # see the matching comment in campaigns.py (CLAUDE.md planner bug #3).
        ad_set_ids = _get_condition_values(conditions, "id")

        if ad_set_ids:
            if limit is not None:
                ad_set_ids = ad_set_ids[:limit]
            rows = [self.handler.graph_get(str(ad_set_id), {"fields": fields}) for ad_set_id in ad_set_ids]
        else:
            campaign_id = _get_condition_value(conditions, "campaign_id")
            effective_status = _get_condition_values(conditions, "effective_status")

            params = {"fields": fields}
            if effective_status:
                params["effective_status"] = [str(v) for v in effective_status]

            # adaptive_page_size=True: ad_sets now unconditionally requests several
            # large/nested fields (targeting, attribution_spec, learning_stage_info,
            # issues_info, frequency_control_specs, promoted_object). On a large
            # account this can trip Meta's oversized-request error at the default
            # page size; graph_get_all shrinks the page and retries rather than
            # hard-failing a previously-working table.
            if campaign_id is not None:
                rows = self.handler.graph_get_all(
                    f"{campaign_id}/adsets", params, limit=limit, adaptive_page_size=True
                )
            else:
                rows = self.handler.graph_get_all(
                    f"{self.handler.account_path}/adsets", params, limit=limit, adaptive_page_size=True
                )

        rows = [self._flatten_targeting(row) for row in rows]

        df = pd.DataFrame(rows)
        if df.empty:
            return pd.DataFrame(columns=self.COLUMNS)

        for column in self.COLUMNS:
            if column not in df.columns:
                df[column] = None

        for column in self.LIST_COLUMNS:
            df[column] = df[column].apply(_join_list)
        for column in self.JSON_COLUMNS:
            df[column] = df[column].apply(lambda v: json.dumps(v) if isinstance(v, (dict, list)) else v)
        df = _to_numeric(df, self.NUMERIC_COLUMNS)

        return df[self.COLUMNS]
