from __future__ import annotations

import hashlib
import hmac
import json
import time
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

import requests
from mindsdb_sql_parser import parse_sql

from mindsdb.integrations.handlers.meta_ads_handler.errors import (
    INSIGHTS_NON_RETRYABLE,
    INSIGHTS_RETRYABLE,
    MetaAdsAPIError,
    error_code_pair,
    is_large_request_error,
    to_int,
)
from mindsdb.integrations.handlers.meta_ads_handler.tables import (
    AccountTable,
    AdCreativesTable,
    AdSetsTable,
    AdsTable,
    CampaignsTable,
    InsightsTable,
)
from mindsdb.integrations.libs.api_handler import APIHandler
from mindsdb.integrations.libs.response import HandlerResponse as Response
from mindsdb.integrations.libs.response import HandlerStatusResponse as StatusResponse
from mindsdb.utilities import log

logger = log.getLogger(__name__)


# error.code values that indicate a transient / rate-limit condition worth retrying.
TRANSIENT_ERROR_CODES = {1, 2, 4, 17, 32, 613}


class MetaAdsHandler(APIHandler):
    """Read-only handler for the Meta Marketing API (Meta Ads / Graph API)."""

    name = "meta_ads"

    DEFAULT_API_VERSION = "v25.0"
    DEFAULT_PAGE_SIZE = 500
    MAX_PAGES = 200
    RETRY_BASE_SECONDS = 2
    MAX_RETRIES = 3
    MAX_BACKOFF_SECONDS = 30
    # Floor for graph_get_all's opt-in adaptive_page_size backoff (see graph_get_all).
    MIN_ADAPTIVE_PAGE_SIZE = 25

    def __init__(self, name: str, **kwargs):
        super().__init__(name)
        self.connection_data = kwargs.get("connection_data", {})
        self.session: requests.Session | None = None

        ad_account_id = str(self.connection_data.get("ad_account_id") or "").strip()
        if ad_account_id.startswith("act_"):
            ad_account_id = ad_account_id[len("act_"):]
        self.ad_account_id = ad_account_id

        self.access_token = self.connection_data.get("access_token")

        api_version = str(self.connection_data.get("api_version") or self.DEFAULT_API_VERSION).strip()
        if api_version and not api_version.startswith("v"):
            api_version = f"v{api_version}"
        self.api_version = api_version or self.DEFAULT_API_VERSION

        self.client_id = self.connection_data.get("client_id")
        self.client_secret = self.connection_data.get("client_secret")

        self.base_url = f"https://graph.facebook.com/{self.api_version}"
        self.account_path = f"act_{self.ad_account_id}"

        # Computed once (not per request) so we never hash the token/secret on every call.
        self._appsecret_proof = None
        if self.client_secret and self.access_token:
            self._appsecret_proof = hmac.new(
                self.client_secret.encode("utf-8"),
                self.access_token.encode("utf-8"),
                hashlib.sha256,
            ).hexdigest()

        self._register_table("campaigns", CampaignsTable(self))
        self._register_table("ad_sets", AdSetsTable(self))
        self._register_table("ads", AdsTable(self))
        self._register_table("ad_creatives", AdCreativesTable(self))
        self._register_table("insights", InsightsTable(self))
        self._register_table("account", AccountTable(self))

    def connect(self) -> None:
        if not self.ad_account_id:
            raise ValueError("ad_account_id is required")
        if not self.access_token:
            raise ValueError("access_token is required")
        if self.session is None:
            self.session = requests.Session()
        self.is_connected = True

    def disconnect(self) -> None:
        if self.session is not None:
            self.session.close()
            self.session = None
        super().disconnect()

    def check_connection(self) -> StatusResponse:
        response = StatusResponse(success=False)
        try:
            self.connect()
            self.graph_get(self.account_path, {"fields": "id,name,account_status"})
            response.success = True
        except Exception as exc:  # noqa: BLE001
            logger.error("Error connecting to Meta Ads: %s", exc)
            response.error_message = str(exc)
            self.disconnect()
        return response

    def native_query(self, query: str = None) -> Response:
        ast = parse_sql(query)
        return self.query(ast)

    # ------------------------------------------------------------------
    # HTTP layer
    # ------------------------------------------------------------------

    def _auth_headers(self) -> dict[str, str]:
        return {"Authorization": f"Bearer {self.access_token}"}

    def _merge_auth_params(self, params: dict[str, Any] | None) -> dict[str, Any]:
        merged = dict(params or {})
        if self._appsecret_proof:
            merged["appsecret_proof"] = self._appsecret_proof
        return merged

    @staticmethod
    def _encode_params(params: dict[str, Any]) -> dict[str, Any]:
        # Graph expects nested params (time_range, filtering, breakdowns lists) as
        # JSON-encoded strings, not repeated query params.
        encoded = {}
        for key, value in params.items():
            if isinstance(value, (dict, list)):
                encoded[key] = json.dumps(value)
            else:
                encoded[key] = value
        return encoded

    def graph_get(self, path: str, params: dict[str, Any] | None = None) -> dict:
        """Single GET request against the Graph API. Not paginated. Retries per
        _is_retryable()."""
        self.connect()
        url = f"{self.base_url}/{path}"
        request_params = self._encode_params(self._merge_auth_params(params))
        return self._get_with_retry(url, request_params)

    def graph_post(self, path: str, params: dict[str, Any] | None = None) -> dict:
        """Single POST request against the Graph API (used to kick off the async
        insights report flow). Deliberately NOT retried: retrying a POST that created
        a report_run_id server-side but failed to return it to us would spawn a
        duplicate async report job.
        """
        self.connect()
        url = f"{self.base_url}/{path}"
        request_params = self._encode_params(self._merge_auth_params(params))
        try:
            response = self.session.post(url, data=request_params, headers=self._auth_headers(), timeout=60)
        except requests.RequestException as exc:
            raise RuntimeError(f"Meta Ads request failed: {exc}") from exc

        if response.ok:
            return response.json()
        self._raise_for_error(response)

    @staticmethod
    def _strip_query_param(url: str, key: str) -> str:
        """Remove a single query param from a URL, preserving the rest. Used to drop
        a stale `limit` baked into Graph's own paging.next before re-adding our own
        (possibly shrunk) value -- otherwise both would be sent and Graph would see
        a duplicate `limit` key.
        """
        parts = urlsplit(url)
        query = [(k, v) for k, v in parse_qsl(parts.query, keep_blank_values=True) if k != key]
        return urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(query), parts.fragment))

    def graph_get_all(
        self,
        path: str,
        params: dict[str, Any] | None = None,
        limit: int | None = None,
        adaptive_page_size: bool = False,
    ) -> list[dict]:
        """Cursor-paginate over a Graph edge, following paging.next until limit rows
        are collected or paging.next is absent. Hard-caps total pages at MAX_PAGES.

        adaptive_page_size: opt-in only, default False. Every existing caller
        (InsightsTable included) keeps today's exact behaviour with zero test churn
        unless it explicitly passes True.

        Insights and the entity tables (campaigns/ad_sets/ads/...) have genuinely
        different correct responses to a large-request error: Insights escalates to
        its own async-report flow (see InsightsTable.list()) because its payloads are
        unbounded by nature (breakdown cardinality x date range); the entity tables
        just need a smaller page of an otherwise-bounded row set. A single global
        policy here would be wrong for at least one of them, so the strategy is
        opt-in per call site rather than a blanket change. ads.py/ad_sets.py (which
        carry the new large/nested Phase 1 fields) pass True; everything else,
        including InsightsTable, does not -- its `except MetaAdsAPIError` /
        async-report fallback is completely untouched by this parameter.

        When adaptive_page_size is True and a request fails with
        is_large_request_error(), the page size is halved and the SAME page is
        retried, down to a floor of MIN_ADAPTIVE_PAGE_SIZE. Once shrunk, the smaller
        size is kept for subsequent pages rather than reset -- if page 1 was too big,
        page 2 almost certainly is too, and resetting would re-pay the failure on
        every page. At the floor, the original MetaAdsAPIError is re-raised
        unchanged -- never wrapped -- so any caller's `except` sees exactly what it
        sees today.
        """
        self.connect()
        params = dict(params or {})

        page_size = self.DEFAULT_PAGE_SIZE
        if limit is not None:
            # Clamp to at least 1: a limit of 0 (or negative) must still page normally
            # instead of sending a nonsensical limit=0/-N to Graph.
            page_size = min(page_size, max(limit, 1))
        params.setdefault("limit", page_size)

        rows: list[dict] = []
        url = f"{self.base_url}/{path}"
        request_params = self._encode_params(self._merge_auth_params(params))
        pages = 0
        on_first_page = True

        while True:
            try:
                payload = self._get_with_retry(url, request_params)
            except MetaAdsAPIError as exc:
                can_shrink = adaptive_page_size and exc.is_large_request_error() and page_size > self.MIN_ADAPTIVE_PAGE_SIZE
                if not can_shrink:
                    raise
                new_page_size = max(page_size // 2, self.MIN_ADAPTIVE_PAGE_SIZE)
                logger.warning(
                    "meta_ads.graph_get_all: large-request error for path=%s; shrinking page size %s -> %s "
                    "and retrying the same page",
                    path,
                    page_size,
                    new_page_size,
                )
                page_size = new_page_size
                if on_first_page:
                    params["limit"] = page_size
                    request_params = self._encode_params(self._merge_auth_params(params))
                else:
                    retry_params = self._merge_auth_params(None)
                    retry_params["limit"] = page_size
                    url = self._strip_query_param(url, "limit")
                    request_params = self._encode_params(retry_params)
                continue

            on_first_page = False
            pages += 1
            rows.extend(payload.get("data", []))

            if limit is not None and len(rows) >= limit:
                return rows[:limit]

            next_url = (payload.get("paging") or {}).get("next")
            if not next_url:
                return rows
            if pages >= self.MAX_PAGES:
                logger.warning("meta_ads.graph_get_all: hit MAX_PAGES=%s cap for path=%s", self.MAX_PAGES, path)
                return rows

            # paging.next already carries the cursor and fields; just re-apply auth.
            # (It also carries whatever `limit` was in effect for the page that
            # generated it, so a shrunk page_size persists into later pages for free.)
            url = next_url
            request_params = self._encode_params(self._merge_auth_params(None))

    def _get_with_retry(self, url: str, params: dict[str, Any]) -> dict:
        attempt = 0
        while True:
            try:
                response = self.session.get(url, params=params, headers=self._auth_headers(), timeout=60)
            except requests.RequestException as exc:
                if attempt >= self.MAX_RETRIES:
                    raise RuntimeError(f"Meta Ads request failed: {exc}") from exc
                self._sleep_backoff(attempt)
                attempt += 1
                continue

            if response.ok:
                return response.json()

            error_info = self._parse_error(response)
            if self._is_retryable(response.status_code, error_info) and attempt < self.MAX_RETRIES:
                self._sleep_backoff(attempt)
                attempt += 1
                continue

            self._raise_for_error(response, error_info)

    @staticmethod
    def _raise_for_error(response: requests.Response, error_info: dict | None = None) -> None:
        if error_info is None:
            error_info = MetaAdsHandler._parse_error(response)
        # Never swallow the API's message -- our app surfaces it to users.
        raise MetaAdsAPIError(MetaAdsHandler._format_error(response, error_info), error_info=error_info)

    def _sleep_backoff(self, attempt: int) -> None:
        delay = min(self.RETRY_BASE_SECONDS * (2**attempt), self.MAX_BACKOFF_SECONDS)
        time.sleep(delay)

    @staticmethod
    def _parse_error(response: requests.Response) -> dict | None:
        try:
            payload = response.json()
        except ValueError:
            return None
        return payload.get("error") if isinstance(payload, dict) else None

    @classmethod
    def _is_retryable(cls, status_code: int, error_info: dict | None) -> bool:
        # Precedence matters here, and this order is the actual fix for the
        # wasted-backoff bug: the documented Insights (code, error_subcode) taxonomy
        # (tasks/meta-ads-api-research/error-codes.md) must be checked BEFORE the
        # legacy generic `code in TRANSIENT_ERROR_CODES` set below. Otherwise, e.g.,
        # code 2 / subcode 1504041 ("Invalid Breakdowns") matches the legacy bare
        # `code == 2` retryable set and we burn the whole backoff budget re-sending a
        # request that can never succeed.
        pair = error_code_pair(error_info)

        # 1. Documented non-retryable Insights pairs always win first.
        if pair is not None and pair in INSIGHTS_NON_RETRYABLE:
            return False

        # 2. Large-request signals are also not retryable -- already the case, kept
        #    here so InsightsTable's async-report fallback is reached quickly instead
        #    of after a full retry cycle.
        if error_info and is_large_request_error(error_info):
            return False

        # 3. Documented retryable Insights pairs.
        if pair is not None and pair in INSIGHTS_RETRYABLE:
            return True

        # 4. Transport-level signals: rate limiting / server errors.
        if status_code == 429 or status_code >= 500:
            return True

        # 5. Legacy generic Graph API codes -- kept as a fallback for errors outside
        #    the documented Insights taxonomy above (this set predates it and isn't
        #    confirmed wrong for the non-Insights cases it may still cover).
        if error_info and to_int(error_info.get("code")) in TRANSIENT_ERROR_CODES:
            return True

        # 6. Everything else is treated as non-retryable.
        return False

    @staticmethod
    def _format_error(response: requests.Response, error_info: dict | None) -> str:
        # Never swallow the API's message -- our app surfaces it to users.
        if error_info:
            parts = [f"Meta Ads API error ({response.status_code}): {error_info.get('message')}"]
            if error_info.get("type"):
                parts.append(f"type={error_info['type']}")
            if error_info.get("code") is not None:
                parts.append(f"code={error_info['code']}")
            if error_info.get("error_subcode") is not None:
                parts.append(f"error_subcode={error_info['error_subcode']}")
            if error_info.get("error_user_msg"):
                parts.append(f"error_user_msg={error_info['error_user_msg']}")
            return " ".join(parts)
        return f"Meta Ads API error ({response.status_code}): {response.text[:500]}"
