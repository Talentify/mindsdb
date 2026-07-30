from __future__ import annotations

import pandas as pd

from mindsdb.integrations.libs.api_handler import APIResource

from .utils import _get_condition_value, _get_condition_values, _to_numeric


class AdsTable(APIResource):
    """Meta Ads ads. GET /{account_path}/ads, or GET /{adset_id}/ads / GET /{campaign_id}/ads
    when adset_id/campaign_id are given in WHERE (Pattern A: always fetch the full field list).

    creative_id is flattened from the nested creative{id} field requested from Graph.
    bid_amount is in the account's currency minor units (cents), not major units.
    """

    COLUMNS = [
        "id",
        "name",
        "adset_id",
        "campaign_id",
        "status",
        "effective_status",
        "bid_amount",
        "creative_id",
        "preview_shareable_link",
        "created_time",
        "updated_time",
    ]

    NUMERIC_COLUMNS = ["bid_amount"]

    # Graph field names actually requested; creative_id is derived from the nested
    # creative{id} field rather than requested directly.
    _REQUEST_FIELDS = [col if col != "creative_id" else "creative{id}" for col in COLUMNS]

    def get_columns(self) -> list[str]:
        return self.COLUMNS

    def list(self, conditions=None, limit=None, sort=None, targets=None, **kwargs):
        conditions = conditions or []
        fields = ",".join(self._REQUEST_FIELDS)

        # Only consume adset_id/campaign_id/effective_status on the path that honours
        # them -- see the matching comment in campaigns.py (CLAUDE.md planner bug #3).
        # In particular, campaign_id must NOT be marked applied when adset_id also
        # picked the fetch path, since it would then never actually be filtered on.
        ad_ids = _get_condition_values(conditions, "id")

        if ad_ids:
            if limit is not None:
                ad_ids = ad_ids[:limit]
            rows = [self.handler.graph_get(str(ad_id), {"fields": fields}) for ad_id in ad_ids]
        else:
            adset_id = _get_condition_value(conditions, "adset_id")
            effective_status = _get_condition_values(conditions, "effective_status")

            params = {"fields": fields}
            if effective_status:
                params["effective_status"] = [str(v) for v in effective_status]

            if adset_id is not None:
                rows = self.handler.graph_get_all(f"{adset_id}/ads", params, limit=limit)
            else:
                campaign_id = _get_condition_value(conditions, "campaign_id")
                if campaign_id is not None:
                    rows = self.handler.graph_get_all(f"{campaign_id}/ads", params, limit=limit)
                else:
                    rows = self.handler.graph_get_all(f"{self.handler.account_path}/ads", params, limit=limit)

        for row in rows:
            creative = row.pop("creative", None) if isinstance(row, dict) else None
            row["creative_id"] = creative.get("id") if isinstance(creative, dict) else None

        df = pd.DataFrame(rows)
        if df.empty:
            return pd.DataFrame(columns=self.COLUMNS)

        for column in self.COLUMNS:
            if column not in df.columns:
                df[column] = None

        df = _to_numeric(df, self.NUMERIC_COLUMNS)

        return df[self.COLUMNS]
