from __future__ import annotations

import json

import pandas as pd

from mindsdb.integrations.libs.api_handler import APIResource

from .utils import _get_condition_value, _get_condition_values, _join_list, _to_numeric


class CampaignsTable(APIResource):
    """Meta Ads campaigns. GET /{account_path}/campaigns (Pattern A: always fetch the
    full field list, let APIResource.select()/DuckDB narrow the projection).

    Budget fields (daily_budget, lifetime_budget, budget_remaining, spend_cap) are
    returned by Graph in the account's currency minor units. Meta defines a
    per-currency offset controlling this: offset 100 (the common case, e.g. USD)
    means the value is expressed in 1/100ths of the base unit (divide by 100 to get
    base units, e.g. cents to dollars); offset 1 (CLP, COP, CRC, HUF, ISK, IDR, JPY,
    KRW, PYG, TWD, VND) means the value already is the base unit, no division needed.
    Use the account's `currency` field (see account.py) to know which applies. We do
    not scale these values today.
    """

    COLUMNS = [
        "id",
        "name",
        "objective",
        "status",
        "effective_status",
        "buying_type",
        "bid_strategy",
        "daily_budget",
        "lifetime_budget",
        "budget_remaining",
        "spend_cap",
        "special_ad_categories",
        "start_time",
        "stop_time",
        "created_time",
        "updated_time",
        "configured_status",
        "account_id",
        "promoted_object",
        "issues_info",
        "special_ad_category_country",
        "source_campaign_id",
        "pacing_type",
        "topline_id",
        "adlabels",
        "primary_attribution",
    ]

    NUMERIC_COLUMNS = ["daily_budget", "lifetime_budget", "budget_remaining", "spend_cap"]

    # Object/list-valued fields, JSON-encoded into the DataFrame column like
    # object_story_spec is in ad_creatives.py.
    JSON_COLUMNS = [
        "promoted_object",
        "issues_info",
        "adlabels",
    ]

    # List-of-scalar-enum fields, comma-joined instead of JSON-encoded (see
    # _join_list in utils.py for why). special_ad_categories already used this
    # encoding pre-Phase-1; special_ad_category_country is the same family
    # (list<enum> of country codes) and follows the same rule.
    LIST_COLUMNS = ["special_ad_categories", "special_ad_category_country", "pacing_type"]

    def get_columns(self) -> list[str]:
        return self.COLUMNS

    def list(self, conditions=None, limit=None, sort=None, targets=None, **kwargs):
        conditions = conditions or []
        fields = ",".join(self.COLUMNS)

        # Only consume effective_status/status when the path we take can actually
        # honour them. The id lookup below fetches specific campaigns by id and has
        # no way to apply an effective_status/status filter server-side; reading
        # (and thus marking .applied = True on) those conditions here would mark them
        # handled without ever filtering by them, and SubSelectStepCall would then
        # skip re-filtering too -- silently dropping the filter (CLAUDE.md bug #3).
        campaign_ids = _get_condition_values(conditions, "id")

        if campaign_ids:
            if limit is not None:
                campaign_ids = campaign_ids[:limit]
            rows = [self.handler.graph_get(str(campaign_id), {"fields": fields}) for campaign_id in campaign_ids]
        else:
            effective_status = _get_condition_values(conditions, "effective_status")
            status = _get_condition_value(conditions, "status")

            params = {"fields": fields}
            if effective_status:
                params["effective_status"] = [str(v) for v in effective_status]
            if status is not None:
                params["filtering"] = [{"field": "campaign.status", "operator": "EQUAL", "value": status}]
            rows = self.handler.graph_get_all(f"{self.handler.account_path}/campaigns", params, limit=limit)

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
