from __future__ import annotations

import json

import pandas as pd

from mindsdb.integrations.libs.api_handler import APIResource

from .utils import _get_condition_value, _get_condition_values, _to_numeric


class AdsTable(APIResource):
    """Meta Ads ads. GET /{account_path}/ads, or GET /{adset_id}/ads / GET /{campaign_id}/ads
    when adset_id/campaign_id are given in WHERE (Pattern A: always fetch the full field list).

    creative_id is flattened from the nested creative{id} field requested from Graph.
    bid_amount is returned by Graph in the account's currency minor units. Meta
    defines a per-currency offset controlling this: offset 100 (the common case, e.g.
    USD) means the value is expressed in 1/100ths of the base unit (divide by 100 to
    get base units, e.g. cents to dollars); offset 1 (CLP, COP, CRC, HUF, ISK, IDR,
    JPY, KRW, PYG, TWD, VND) means the value already is the base unit, no division
    needed. Use the account's `currency` field (see account.py) to know which
    applies. We do not scale this value today.
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
        "configured_status",
        "targeting",
        "issues_info",
        "ad_review_feedback",
        "recommendations",
        "conversion_specs",
        "tracking_specs",
        "source_ad_id",
        "adlabels",
        # special_ad_categories intentionally NOT added here. It's documented and
        # valid on the ad node, but live validation against the real account
        # returned "(#3) App must be on whitelist" -- Meta gates Special Ad Category
        # (housing/employment/credit compliance) fields behind app review, and this
        # app isn't whitelisted. ads.py is Pattern A (one combined `fields=` string),
        # so keeping it would 400 every `SELECT ... FROM ads`, not just queries that
        # reference the column. Re-add only once the app passes that review.
        # campaigns.special_ad_categories is unaffected -- it shipped before Phase 1
        # and is verified separately.
    ]

    NUMERIC_COLUMNS = ["bid_amount"]
    JSON_COLUMNS = [
        "targeting",
        "issues_info",
        "ad_review_feedback",
        "recommendations",
        "conversion_specs",
        "tracking_specs",
        "adlabels",
    ]

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

            # adaptive_page_size=True: ads now unconditionally requests several
            # large/nested fields (targeting, recommendations, issues_info,
            # ad_review_feedback, conversion_specs, tracking_specs). On a large
            # account this can trip Meta's oversized-request error at the default
            # page size; graph_get_all shrinks the page and retries rather than
            # hard-failing a previously-working table.
            if adset_id is not None:
                rows = self.handler.graph_get_all(f"{adset_id}/ads", params, limit=limit, adaptive_page_size=True)
            else:
                campaign_id = _get_condition_value(conditions, "campaign_id")
                if campaign_id is not None:
                    rows = self.handler.graph_get_all(
                        f"{campaign_id}/ads", params, limit=limit, adaptive_page_size=True
                    )
                else:
                    rows = self.handler.graph_get_all(
                        f"{self.handler.account_path}/ads", params, limit=limit, adaptive_page_size=True
                    )

        for row in rows:
            creative = row.pop("creative", None) if isinstance(row, dict) else None
            row["creative_id"] = creative.get("id") if isinstance(creative, dict) else None

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
