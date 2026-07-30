from __future__ import annotations

import json

import pandas as pd

from mindsdb.integrations.libs.api_handler import APIResource

from .utils import _get_condition_value, _get_condition_values, _to_numeric


class AdSetsTable(APIResource):
    """Meta Ads ad sets. GET /{account_path}/adsets, or GET /{campaign_id}/adsets when
    campaign_id is given in WHERE (Pattern A: always fetch the full field list).

    Budget fields (daily_budget, lifetime_budget, budget_remaining, bid_amount) are in
    the account's currency minor units (cents), not major units.
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
    ]

    NUMERIC_COLUMNS = ["bid_amount", "daily_budget", "lifetime_budget", "budget_remaining"]
    JSON_COLUMNS = ["targeting", "promoted_object"]

    def get_columns(self) -> list[str]:
        return self.COLUMNS

    def list(self, conditions=None, limit=None, sort=None, targets=None, **kwargs):
        conditions = conditions or []
        fields = ",".join(self.COLUMNS)

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

            if campaign_id is not None:
                rows = self.handler.graph_get_all(f"{campaign_id}/adsets", params, limit=limit)
            else:
                rows = self.handler.graph_get_all(f"{self.handler.account_path}/adsets", params, limit=limit)

        df = pd.DataFrame(rows)
        if df.empty:
            return pd.DataFrame(columns=self.COLUMNS)

        for column in self.COLUMNS:
            if column not in df.columns:
                df[column] = None

        for column in self.JSON_COLUMNS:
            df[column] = df[column].apply(lambda v: json.dumps(v) if isinstance(v, (dict, list)) else v)
        df = _to_numeric(df, self.NUMERIC_COLUMNS)

        return df[self.COLUMNS]
