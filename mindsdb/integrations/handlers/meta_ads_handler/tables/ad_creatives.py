from __future__ import annotations

import json

import pandas as pd

from mindsdb.integrations.libs.api_handler import APIResource

from .utils import _get_condition_values


class AdCreativesTable(APIResource):
    """Meta Ads ad creatives. GET /{account_path}/adcreatives (Pattern A: always fetch
    the full field list).

    title/body/link_url/call_to_action_type/description may be absent at the top level
    and only present nested inside object_story_spec — link_data, video_data, photo_data
    or template_data (template_data is the same shape as link_data, used by Dynamic
    Product Ads). photo_data has no title or link field at all; it only ever
    contributes to body/description via caption. video_data has no standalone link —
    its destination lives only at call_to_action.value.link.

    Dynamic Creative ads put their content in asset_feed_spec instead of
    object_story_spec. When a column is still empty after the object_story_spec chain,
    _flatten_creative() falls back to the first entry of the matching asset_feed_spec
    list (titles/bodies/descriptions/link_urls/call_to_action_types). The element shape
    inside those lists is not confirmed against a live creative, so parsing is
    defensive: a plain string is used as-is, a dict is read via
    text -> website_url -> display_url -> url -> value (first present wins), and any
    other shape yields None instead of raising. The raw asset_feed_spec column (JSON
    encoded, like object_story_spec) is kept as the source of truth regardless.

    Every level of these specs can be absent, or present but not a dict (e.g. a stray
    string); missing/wrong-shaped values become None, never a KeyError or an
    AttributeError -- see _as_dict().
    """

    COLUMNS = [
        "id",
        "name",
        "status",
        "title",
        "body",
        "description",
        "link_url",
        "image_url",
        "thumbnail_url",
        "video_id",
        "call_to_action_type",
        "object_type",
        "effective_object_story_id",
        "object_story_spec",
        "asset_feed_spec",
        "instagram_permalink_url",
        "product_set_id",
        "template_url_spec",
        "platform_customizations",
        "image_crops",
        "degrees_of_freedom_spec",
        "authorization_category",
        "effective_authorization_category",
    ]

    # Object/list-valued fields, JSON-encoded the same way object_story_spec and
    # asset_feed_spec already are below.
    JSON_COLUMNS = ["template_url_spec", "platform_customizations", "image_crops", "degrees_of_freedom_spec"]

    def get_columns(self) -> list[str]:
        return self.COLUMNS

    @staticmethod
    def _parse_asset_feed_element(element):
        """Best-effort read of one asset_feed_spec list element. Shape is undocumented
        for this account, so accept a plain string or a dict tried in priority order;
        anything else (or a missing key) yields None rather than raising.
        """
        if isinstance(element, str):
            return element
        if isinstance(element, dict):
            return (
                element.get("text")
                or element.get("website_url")
                or element.get("display_url")
                or element.get("url")
                or element.get("value")
            )
        return None

    @classmethod
    def _asset_feed_value(cls, asset_feed_spec: dict, key: str):
        items = asset_feed_spec.get(key)
        # Must be an actual sequence: a dict would raise on items[0], and a bare string
        # would silently yield its first character. Both are cheaper to reject than to
        # guess at, since the raw asset_feed_spec column preserves the real value.
        if not items or not isinstance(items, (list, tuple)):
            return None
        return cls._parse_asset_feed_element(items[0])

    @staticmethod
    def _as_dict(value) -> dict:
        """Treat anything that isn't a dict (missing, None, or a stray string/list)
        as empty rather than raising -- defence-in-depth against a shape we haven't
        seen live, matching the "never a KeyError" guarantee for AttributeError too.
        """
        return value if isinstance(value, dict) else {}

    @classmethod
    def _flatten_creative(cls, row: dict) -> dict:
        result = dict(row)
        story_spec = cls._as_dict(row.get("object_story_spec"))
        link_data = cls._as_dict(story_spec.get("link_data"))
        video_data = cls._as_dict(story_spec.get("video_data"))
        photo_data = cls._as_dict(story_spec.get("photo_data"))
        template_data = cls._as_dict(story_spec.get("template_data"))

        def _cta_type(data: dict):
            cta = data.get("call_to_action") or {}
            return cta.get("type")

        def _cta_link(data: dict):
            cta = data.get("call_to_action") or {}
            value = cta.get("value") or {}
            return value.get("link")

        result["title"] = (
            row.get("title") or link_data.get("name") or video_data.get("title") or template_data.get("name")
        )
        result["body"] = (
            row.get("body")
            or link_data.get("message")
            or video_data.get("message")
            or template_data.get("message")
            or photo_data.get("caption")
        )
        result["link_url"] = (
            row.get("link_url")
            or link_data.get("link")
            or _cta_link(link_data)
            or _cta_link(video_data)
            or template_data.get("link")
            or _cta_link(template_data)
        )
        result["call_to_action_type"] = (
            row.get("call_to_action_type") or _cta_type(link_data) or _cta_type(video_data) or _cta_type(template_data)
        )
        result["description"] = (
            # Top-level first: `description` is in COLUMNS, so it is requested from Graph
            # and may come back populated. Without this the nested chain below would
            # overwrite the API's own value with None.
            row.get("description")
            or link_data.get("description")
            or video_data.get("link_description")
            or template_data.get("description")
            or photo_data.get("caption")
        )

        asset_feed_spec = cls._as_dict(row.get("asset_feed_spec"))
        if asset_feed_spec:
            result["title"] = result["title"] or cls._asset_feed_value(asset_feed_spec, "titles")
            result["body"] = result["body"] or cls._asset_feed_value(asset_feed_spec, "bodies")
            result["description"] = result["description"] or cls._asset_feed_value(asset_feed_spec, "descriptions")
            result["link_url"] = result["link_url"] or cls._asset_feed_value(asset_feed_spec, "link_urls")
            result["call_to_action_type"] = result["call_to_action_type"] or cls._asset_feed_value(
                asset_feed_spec, "call_to_action_types"
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
        df["asset_feed_spec"] = df["asset_feed_spec"].apply(
            lambda v: json.dumps(v) if isinstance(v, (dict, list)) else v
        )
        for column in self.JSON_COLUMNS:
            df[column] = df[column].apply(lambda v: json.dumps(v) if isinstance(v, (dict, list)) else v)

        return df[self.COLUMNS]
