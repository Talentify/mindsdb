# Meta Marketing API — Insights Error Codes Reference

Sources:
- https://developers.facebook.com/documentation/ads-commerce/marketing-api/insights/error-codes.md
- https://developers.facebook.com/documentation/ads-commerce/marketing-api/insights/best-practices.md
(fetched as raw markdown; Marketing API v25.0+ docs)

Our handler: `mindsdb/integrations/handlers/meta_ads_handler/tables/insights.py` +
`mindsdb/integrations/handlers/meta_ads_handler/errors.py`. Current retry set: HTTP 429,
HTTP 5xx, and `error.code` in `{1, 2, 4, 17, 32, 613}` (exponential backoff, 2s base, 3
retries, 30s cap). Current large-request detection: `error.code == 1` with a message
containing "reduce the amount of data", or `error_subcode == 99`.

**Important scope note:** the error-codes page carries this warning verbatim:

> "Error code information for async sources will be available with Marketing API
> v25.0+."

This page documents an **Insights-specific error taxonomy** (keyed by `code` +
`error_subcode` pairs like `100`/`1504018`, `4`/`1504022`, `2`/`1504041`, `-3`/`1504045`).
This is **distinct** from the generic Graph API error codes our handler currently retries
on (`1`, `2`, `4`, `17`, `32`, `613` — the classic "ExceptionCode"/"OAuthException"
numbering used across the whole Graph API, not specific to Insights). The two numbering
schemes overlap only on the bare `code` value `2` and `4`; the `error_subcode` is what
actually disambiguates within this Insights-specific table. Nothing on this page mentions
`error_subcode == 99`, code `17`, code `32`, or code `613` at all — see the summary
delivered separately for what this implies.

## Error codes

Table transcribed verbatim from the source page (column order: Error Code | Error Subcode
| Source | Summary | Description):

| Code | Subcode | Source | Summary | Description | Retryable? | Recommended handling |
| --- | --- | --- | --- | --- | --- | --- |
| `-2` | `2490547` | Async | Report Failed | Generating the report failed. Try again later. | Not specified as retryable in this table; description says "Try again later" — treat as retryable with backoff. | Retry the async report submission after a delay. |
| `100` | `1504018` | Sync | Request Timed Out | Your request timed out. Try a smaller date range, fetch less data, or use async jobs. | Not a rate-limit code; not documented as blindly retryable — the fix is to shrink the request or go async, not just retry as-is. | Reduce date range / fields / breakdown cardinality, or fall back to the async report flow. |
| `4` | `1504022` | Async and Sync | Too Many API Requests | Your app has exceeded the allowed number of API requests. Wait before retrying. | Retryable after a wait. | Back off and retry; see "API Rate Limits" (best-practices doc, `#insightscallload`). |
| `2` | `1504038` | Sync | Request Timed Out | Your request timed out. Try a smaller date range, fetch less data, or use async jobs. | Same as `100`/`1504018` — not a blind-retry case. | Reduce date range / fields / breakdown cardinality, or fall back to the async report flow. |
| `4` | `1504039` | Async and Sync | Too Many API Requests | Your app has exceeded the allowed number of API requests. Wait before retrying. | Retryable after a wait. | Back off and retry; see "API Rate Limits". |
| `2` | `1504041` | Async and Sync | Invalid Breakdowns | No data is available for the requested metrics and breakdowns. Try different metrics or breakdowns. | Not retryable as-is — retrying the identical request will fail identically. | Fix the metrics/breakdowns combination (see breakdowns.md); do not retry unchanged. |
| `2` | `1504042` | Async and Sync | Invalid Custom Metrics | You are querying invalid custom metrics. Try selecting different ones. | Not retryable as-is. | Fix the custom metrics selection; do not retry unchanged. |
| `2` | `1504043` | Async and Sync | Intermittent Error | Your request encountered an intermittent error. Retry at a later time. | Retryable. | Retry later with backoff. |
| `2` | `1504044` | Sync | Unknown Error Occurred | An unexpected error occurred. Please refresh the page or try again. If the issue persists, contact Meta Support. | Retryable (limited attempts), then escalate. | Retry a small number of times; if it persists, surface to the user/support rather than looping. |
| `-3` | `1504045` | Async | Report Too Large | Your report was too large. Check the documentation for guidance and try again. | Not retryable unchanged — the request itself must shrink. | This is the documented **large-request** signal for the async path; reduce scope or paginate rather than blind-retry. See "Data Per Call Limits". |
| `100` | `3191001` | Async and Sync | Permission Error | The Ads Insights API denied your request due to insufficient permissions. | Not retryable. | Fix credentials/permissions; do not retry. |

The doc does not give an explicit "retryable: yes/no" column — the retryability judgments
above are derived from each row's Summary/Description text (e.g., "wait before retrying",
"try again later" vs. "try different metrics/breakdowns", "check the documentation").

Codes NOT present anywhere in this table: `1`, `17`, `32`, `613`, and `error_subcode 99`.
Not documented on this page.

## Rate limiting

From the best-practices page:

**`X-FB-Ads-Insights-Throttle` header** — a JSON header reporting utilization:
- `app_id_util_pct` — percentage of allocated capacity the associated `app_id` has
  consumed.
- `acc_id_util_pct` — percentage of allocated capacity the associated ad `account_id` has
  consumed.
- `ads_api_access_tier` — access tier designation affecting rate limits.

**`X-Ad-Account-Usage` header** — Insights calls are "also subject to the default ad
account limits shown in the `x-ad-account-usage` HTTP header."

(The doc did not surface an `X-Business-Use-Case-Usage` or `X-App-Usage` header
specifically on this page — only the two above. Not documented here.)

**Global rate limit handling**: during high system load, requests receive
`error_code = 4, CodeException (error subcode: 1504022), error_title: "Too many API
requests"`. Guidance: "During these periods, it is advised to reduce calls, wait a short
period, and query again."

**Backoff strategy** (quoted): "Add a back-off mechanism to slow down or pause your
`/insights` queries when you come close to hitting 100% utility for your application, or
for your ad account."

No specific numeric backoff schedule (base delay, multiplier, cap, retry count) is given
in the docs — our handler's own constants (2s base, 3 retries, 30s cap) are not
independently confirmed or contradicted by this page. Not documented.

## Large-result / async guidance

**Data Per Call Limits** (best-practices page): two constraint types apply to both sync
and async calls:
1. Limits by number of rows in the response.
2. Limits by number of data points needed to compute totals.

Exceeding either returns: **`error_code = 100`, `CodeException`, `error subcode: 1487534`**.

This is a **third** documented "too much data" signal, distinct from both:
- `-3` / `1504045` ("Report Too Large", async path), and
- `100` / `1504018` and `2` / `1504038` ("Request Timed Out... try a smaller date range,
  fetch less data, or use async jobs").

Recommendations from the doc for avoiding this class of error:
- Limit the query by date range or number of ad IDs.
- Avoid account-level queries that include high-cardinality breakdowns.
- Use the `/insights` edge directly against lower-level ad objects (ad/adset/campaign
  rather than account) to retrieve granular data.
- Apply `filtering` only to retrieve insights for ad objects that actually have data.
- Prefer `date_preset` over custom date ranges where possible.
- Batch multiple sync calls together.

**Asynchronous jobs for large requests** — documented flow:
1. `POST` to `<AD_OBJECT>/insights`, receiving an `id` for an Ad Report Run.
2. Poll the report's `async_status` field until it reaches `"Job Completed"` with
   `"async_percent_completion": 100`.
3. Query the `<AD_REPORT_RUN_ID>/insights` edge for results.

Note: `report_run_id` values "expire after 30 days."

### Codes/subcodes that specifically mean "too much data, go async"

Per this doc, the documented large-result signals are:
- `code 100` / `subcode 1487534` — Data Per Call Limits exceeded (row-count or
  data-point-count limit).
- `code -3` / `subcode 1504045` — "Report Too Large" (async path).
- `code 100` / `subcode 1504018` and `code 2` / `subcode 1504038` — "Request Timed Out...
  try a smaller date range, fetch less data, or use async jobs" (sync path timeouts,
  functionally a large-request signal even though framed as a timeout).

**Not documented anywhere on these two pages**: `code == 1` combined with a
"reduce the amount of data" message, or `error_subcode == 99`. Our handler's current
large-request detector (`errors.py`'s `is_large_request_error`) matches neither of these
documented signals.
