from __future__ import annotations

import json

import pandas as pd

from mindsdb.integrations.libs.api_handler import APIResource

from .utils import _get_condition_values


class AdCreativesTable(APIResource):
    """Meta Ads ad creatives. GET /{account_path}/adcreatives (Pattern A: always fetch
    the full field list).

    title/body/link_url/call_to_action_type may be absent at the top level and only
    present nested inside object_story_spec.link_data (or .video_data). _flatten_creative()
    prefers the top-level value and falls back to the nested one; missing values become
    None, never a KeyError.
    """

    COLUMNS = [
        "id",
        "name",
        "status",
        "title",
        "body",
        "link_url",
        "image_url",
        "thumbnail_url",
        "video_id",
        "call_to_action_type",
        "effective_object_story_id",
        "object_story_spec",
        "instagram_permalink_url",
    ]

    def get_columns(self) -> list[str]:
        return self.COLUMNS

    @staticmethod
    def _flatten_creative(row: dict) -> dict:
        result = dict(row)
        story_spec = row.get("object_story_spec") or {}
        link_data = story_spec.get("link_data") or {}
        video_data = story_spec.get("video_data") or {}

        def _cta_type(data: dict):
            cta = data.get("call_to_action") or {}
            return cta.get("type")

        def _cta_link(data: dict):
            cta = data.get("call_to_action") or {}
            value = cta.get("value") or {}
            return value.get("link")

        result["title"] = row.get("title") or link_data.get("name") or video_data.get("title")
        result["body"] = row.get("body") or link_data.get("message") or video_data.get("message")
        result["link_url"] = (
            row.get("link_url") or link_data.get("link") or _cta_link(link_data) or _cta_link(video_data)
        )
        result["call_to_action_type"] = (
            row.get("call_to_action_type") or _cta_type(link_data) or _cta_type(video_data)
        )
        return result

    def list(self, conditions=None, limit=None, sort=None, targets=None, **kwargs):
        conditions = conditions or []
        fields = ",".join(self.COLUMNS)

        creative_ids = _get_condition_values(conditions, "id")

        if creative_ids:
            if limit is not None:
                creative_ids = creative_ids[:limit]
            rows = [self.handler.graph_get(str(creative_id), {"fields": fields}) for creative_id in creative_ids]
        else:
            rows = self.handler.graph_get_all(
                f"{self.handler.account_path}/adcreatives", {"fields": fields}, limit=limit
            )

        rows = [self._flatten_creative(row) for row in rows]

        df = pd.DataFrame(rows)
        if df.empty:
            return pd.DataFrame(columns=self.COLUMNS)

        for column in self.COLUMNS:
            if column not in df.columns:
                df[column] = None

        df["object_story_spec"] = df["object_story_spec"].apply(
            lambda v: json.dumps(v) if isinstance(v, (dict, list)) else v
        )

        return df[self.COLUMNS]
