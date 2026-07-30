# Ad (adgroup) — Meta Marketing API v25.0 Reference Extract

Source: https://developers.facebook.com/documentation/ads-commerce/marketing-api/reference/adgroup.md
Also consulted (edges directly linked from the Ad node, ≤2 levels deep):
- https://developers.facebook.com/documentation/ads-commerce/marketing-api/reference/ad-account/ads.md (act_<id>/ads read params)
- https://developers.facebook.com/documentation/ads-commerce/marketing-api/reference/ad-campaign-group/ads.md (campaign/ads read params)
- https://developers.facebook.com/documentation/ads-commerce/marketing-api/reference/ad-campaign/ads.md (adset/ads read params)
- https://developers.facebook.com/documentation/ads-commerce/graph-api/reference/adgroup/insights.md (only place `filtering`/`sort` are documented for anything hanging off Ad)

> Note on descriptions: Meta's own reference page gives most Ad fields a one-word "description"
> that is just a restatement of the field name (e.g. `id` → "id", `name` → "name"). That is not
> a scraping artifact — it's genuinely what the doc says. Where the doc gives no real prose,
> this file says so rather than inventing one.

## Reading fields

| field | type | one-line meaning |
|---|---|---|
| id ✅ | numeric string | Ad ID. (doc gives no fuller description) |
| account_id | numeric string | Ad account ID that owns the ad. (no fuller description in doc) |
| ad_active_time | numeric string | Not documented beyond the field name. |
| ad_review_feedback | AdgroupReviewFeedback | Review/rejection feedback object for the ad (see linked reference `adgroup-review-feedback`). |
| ad_schedule_end_time | datetime | End time for an individual ad's own schedule (sales/app-promotion campaigns only, per the Creating-params doc). |
| ad_schedule_start_time | datetime | Start time for an individual ad's own schedule (sales/app-promotion campaigns only). |
| adlabels | list<AdLabel> | Ad labels attached to this ad. |
| adset | AdSet | The parent ad set object (expandable). |
| adset_id ✅ | numeric string | Parent ad set ID. |
| bid_amount ✅ | int32 | Bid amount for the ad (write path is deprecated — see Gotchas). |
| bid_info | map<string, unsigned int32> | Bid info map (not documented beyond type). |
| bid_type | enum {CPC, CPM, MULTI_PREMIUM, ABSOLUTE_OCPM, CPA} | Bidding type for the ad. |
| campaign | Campaign | Parent campaign object (expandable). |
| campaign_id ✅ | numeric string | Parent campaign ID. |
| configured_status | enum {ACTIVE, PAUSED, DELETED, ARCHIVED} | The status you set (as opposed to `effective_status`, which reflects computed delivery state). |
| conversion_domain | string | Domain where conversions happen (first+second level only, e.g. `facebook.com`); auto-populated from destination URL if unset. |
| conversion_specs | list<ConversionActionQuery> | Conversion action specs used for reporting/attribution. |
| created_time ✅ | datetime | Ad creation time. |
| creative | AdCreative | Ad creative object; our handler flattens `creative{id}` into `creative_id`. |
| creative_asset_groups_spec | AdCreativeAssetGroupsSpec | Structured spec for asset-group based creatives (Advantage+ style). |
| demolink_hash | string | Hash for a demo link (not documented further). |
| display_sequence | int32 | Ordering sequence of the ad within its campaign/ad set. |
| effective_status ✅ | enum {ACTIVE, PAUSED, DELETED, PENDING_REVIEW, DISAPPROVED, PREAPPROVED, PENDING_BILLING_INFO, CAMPAIGN_PAUSED, ARCHIVED, ADSET_PAUSED, IN_PROCESS, WITH_ISSUES} | Computed delivery status, folding in ad/adset/campaign pause+review state. |
| engagement_audience | bool | Whether a custom audience of users who engaged with this ad should be created. |
| failed_delivery_checks | list<DeliveryCheck> | Delivery checks the ad is currently failing. |
| is_autobid | bool | Whether the ad/ad set is using automatic bidding. |
| issues_info | list<AdgroupIssuesInfo> | Structured list of issues affecting the ad (disapproval reasons, policy issues, etc.). |
| last_updated_by_app_id | id | App ID that last modified the ad. |
| name ✅ | string | Ad name. |
| preview_shareable_link ✅ | string | Shareable ad-preview URL. |
| priority | unsigned int32 | Priority value for the ad. |
| recommendations | list<AdRecommendation> | Meta's configuration recommendations for this ad (also returnable via `execution_options=[include_recommendations]` on write calls). |
| source_ad | Ad | The ad this one was copied/duplicated from (object). |
| source_ad_id | numeric string | ID of the source ad, if this ad is a copy. |
| special_ad_categories | list<enum> | Special ad category classification (e.g. housing/employment/credit — enum values not enumerated on this page). |
| status ✅ | enum {ACTIVE, PAUSED, DELETED, ARCHIVED} | The status you set on the ad (creation/update value). |
| targeting | Targeting | Targeting spec — normally inherited from the ad set; present here for ads that override it. |
| tracking_and_conversion_with_defaults | TrackingAndConversionWithDefaults | Effective tracking/conversion config after applying account-level defaults. |
| tracking_specs | list<ConversionActionQuery> | Tracking specs used to log user actions taken on the ad. |
| updated_time ✅ | datetime | Last update time. |

**Total documented readable fields: 39.** Our handler exposes 10 of them directly (`creative_id` is a derived/flattened column, not a raw field) — marked ✅ above.

## Edges

| edge | what it returns | notable read params |
|---|---|---|
| `adcreatives` | Edge<AdCreative> — creatives associated with the ad | (not detailed on the Ad page itself) |
| `adrules_governed` | Edge<AdRule> — automated rules governing this ad | (not detailed) |
| `copies` | Edge<Adgroup> — ad copies created from this ad (also a POST target for duplication) | `adset_id`, `creative_parameters`, `rename_options`, `status_option` (write params, documented under Creating) |
| `insights` | Edge<AdsInsights> — performance/reporting data for the ad | `fields`, `filtering`, `level`, `limit`, `sort`, `time_range`, `time_ranges`, `time_increment`, `date_preset` (implied — see below), `summary`, `summary_action_breakdowns`, `use_account_attribution_setting`, `use_unified_attribution_setting`, `export_name`, `product_id_limit` — full list pulled from the insights reference page |
| `leads` | Edge<UserLeadGenInfo> — lead-gen form submissions attributed to this ad | (not detailed on this page) |
| `previews` | Edge<AdPreview> — rendered ad preview HTML/iframes | (not detailed) |
| `targetingsentencelines` | Edge<TargetingSentenceLine> — human-readable targeting summary lines | (not detailed) |

## Read params (GET)

### On the Ad node itself (`GET /<AD_ID>`)

| param | type | allowed values / format | what it does |
|---|---|---|---|
| `date_preset` | enum | `today, yesterday, this_month, last_month, this_quarter, maximum, data_maximum, last_3d, last_7d, last_14d, last_28d, last_30d, last_90d, last_week_mon_sun, last_week_sun_sat, last_quarter, last_year, this_week_mon_today, this_week_sun_today, this_year` | Predefined date range for aggregating insights-style metrics requested alongside the node. |
| `review_feedback_breakdown` | boolean (default `false`) | true/false | Controls whether `ad_review_feedback` is broken down (doc gives no further detail than the field name). |
| `time_range` | object `{'since':YYYY-MM-DD,'until':YYYY-MM-DD}` | ISO dates | Explicit date range (alternative to `date_preset`); invalid ranges are silently ignored. |

### On the `/ads` listing edges — identical param set on all three paths: `GET /act_<AD_ACCOUNT_ID>/ads`, `GET /<AD_CAMPAIGN_ID>/ads`, `GET /<AD_SET_ID>/ads`

| param | type | allowed values / format | what it does |
|---|---|---|---|
| `date_preset` | enum | same list as above | Predefined date range used to aggregate insights metrics returned alongside the listed ads. |
| `effective_status` | list<string> | values from the `effective_status` enum (`ACTIVE, PAUSED, DELETED, PENDING_REVIEW, DISAPPROVED, PREAPPROVED, PENDING_BILLING_INFO, CAMPAIGN_PAUSED, ARCHIVED, ADSET_PAUSED, IN_PROCESS, WITH_ISSUES`) | Server-side filter on ad effective status. On the adset/ads edge specifically, the doc notes: "When unset, defaults to not return deleted or archived ads." |
| `time_range` | object `{'since':YYYY-MM-DD,'until':YYYY-MM-DD}` | ISO dates | Date range used to aggregate insights metrics for the listed ads. |
| `updated_since` | integer | Unix timestamp | Only return ads updated at/after this time. |

**Not documented on any of the three `/ads` listing edges:** `filtering`, `sort`, `limit` do not appear in their Parameters tables. `limit` is still usable as a generic Graph API pagination parameter (it's part of the platform-wide paging contract, not object-specific), but it is not listed as an object-specific read param here. `filtering` and `sort` are documented on the **`insights`** edge only (see below) — the doc does not show them as valid params for the `/ads` listing edges themselves. Treat that as the authoritative answer, not an omission on my part: I did not find a `filtering`/`sort` param on `account/ads`, `campaign/ads`, or `adset/ads`.

## Filtering

The `adgroup` node's own reference page and its three `/ads` listing edges (account, campaign, adset) **do not document a `filtering` param**. The only place `filtering` (and `sort`) is documented among pages reachable from the Ad node is the **`insights`** edge (`/<AD_ID>/insights`), one hop away:

- `filtering` — type `list<Filter Object>`, default `Vec` (empty). Quoting the doc: *"Filters on the report data. This parameter is an array of filter objects."* Each filter object has:
  - `field` *string* — **required**
  - `operator` *enum* — **required**. Full operator list as documented: `EQUAL, NOT_EQUAL, GREATER_THAN, GREATER_THAN_OR_EQUAL, LESS_THAN, LESS_THAN_OR_EQUAL, IN_RANGE, NOT_IN_RANGE, CONTAIN, NOT_CONTAIN, CONTAINS_ANY, CONTAINS_ALL, NOT_CONTAINS_ANY, STEM_MATCH, IN, NOT_IN, STARTS_WITH, ENDS_WITH, ANY, ALL, AFTER, BEFORE, ON_OR_AFTER, ON_OR_BEFORE, NONE, TOP`
  - `value` *string* — **required**
- `sort` — type `list<string>`, default `Vec`. Quoting the doc: *"Field to sort the result, and direction of sorting. You can specify sorting direction by appending '_ascending' or '_descending' to the sort field. For example, 'reach_descending'. For actions, you can sort by action type in form of 'actions:<action_type>'. For example, ['actions:link_click_ascending']. This array supports no more than one element. By default, the sorting direction is ascending."`

Since this is an **insights-edge** feature, not a listing-edge feature, it filters/sorts *report rows* (impressions, spend, breakdowns, etc.), not raw Ad object rows. It is not documented as a mechanism for filtering the `/ads` edges by, e.g., `name` or `status`. If the handler wants server-side filtering of ad object listings, the only documented lever is `effective_status` (and `updated_since`/`time_range`/`date_preset` for time-based filtering) — not a generic `filtering` param.

## Nested / structured fields

Fields that return objects or lists and would need JSON-encoding or flattening in a SQL handler:

- `ad_review_feedback` → object (`AdgroupReviewFeedback`)
- `adlabels` → list of objects (`AdLabel`)
- `adset` → object (`AdSet`) — full sub-object if expanded
- `bid_info` → map<string, unsigned int32>
- `campaign` → object (`Campaign`)
- `conversion_specs` → list of objects (`ConversionActionQuery`)
- `creative` → object (`AdCreative`) — handler currently flattens only `creative{id}` → `creative_id`
- `creative_asset_groups_spec` → object (`AdCreativeAssetGroupsSpec`)
- `failed_delivery_checks` → list of objects (`DeliveryCheck`)
- `issues_info` → list of objects (`AdgroupIssuesInfo`)
- `recommendations` → list of objects (`AdRecommendation`)
- `source_ad` → object (`Ad`, self-referential)
- `special_ad_categories` → list<enum>
- `targeting` → object (`Targeting`) — typically large/nested (geo, demographics, placements, etc.)
- `tracking_and_conversion_with_defaults` → object (`TrackingAndConversionWithDefaults`)
- `tracking_specs` → list of objects (`ConversionActionQuery`)

## Gotchas

- **`bid_amount` write path is deprecated.** The Creating-params table says: *"Deprecated. We no longer allow setting the `bid_amount` value on an ad. Please set `bid_amount` for the ad set."* The field is still readable on the Ad node, but our handler should not assume it's settable there — this only affects writes, which the handler doesn't do, but worth flagging since the field name is identical at both ad and ad-set level and could be conflated.
- **Political ads require extra authorization.** `authorization_category=POLITICAL` plus Page-level "Issue, Electoral or Political Ads" authorization and advertiser identity verification are required before political-content ads can be created; not directly a read-path concern but affects what `ad_review_feedback`/`issues_info` may report for such ads.
- **Page Mentions are silently dropped.** The API accepts ad creation with a Page Mention but "will deliver the ad without the mention" — a doc-stated silent behavior gap, not an error.
- **DSA (EU) regulated locations need payor/beneficiary info** (`default_dsa_payor`, `default_dsa_beneficiary` at the ad-account level) before creating/copying ads targeting those locations. Doesn't block reads.
- **Youth targeting in EU/EEA/Switzerland** has been restricted since the week of Nov 6, 2023 — existing ad sets targeting youth there paused delivery; this affects what `effective_status`/`issues_info` may show for older ads, not the read mechanics themselves.
- **`status` vs `effective_status`.** `status` only reflects what you set (`ACTIVE`/`PAUSED`/`DELETED`/`ARCHIVED`); `effective_status` is the computed value and has many more states (`PENDING_REVIEW`, `DISAPPROVED`, `CAMPAIGN_PAUSED`, `ADSET_PAUSED`, `WITH_ISSUES`, etc.). Our handler already uses `effective_status` for filtering, which is the correct one for delivery-state filtering.
- **Update restrictions** (not a read concern, but relevant context): `adset_id` and `social_prefs` cannot be updated after creation; archived ads (`status=ARCHIVED`) only allow `name` and a status change to `DELETED`; ads in an ad set with `creative_sequence` set cannot be paused/archived/deleted.
- **No currency-minor-units note appears on this page** — `bid_amount` is documented simply as `int32`/`integer` with no explicit statement of minor-currency-unit semantics on the Ad node's own field or creation-param tables. (Flagging as "not documented here" rather than asserting a unit — this is commonly documented on the AdAccount/currency pages instead, which weren't in scope for this 2-level-deep pass.)
- **Most field "descriptions" on this reference page are literally just the field name repeated** (see the note at the top of this file). Anyone relying on this file for descriptions of `ad_active_time`, `bid_info`, `demolink_hash`, `priority`, etc. should know the source doc itself provides nothing beyond the name/type — this is not an extraction gap.
- **`filtering`/`sort` are not documented on `/ads` listing edges** — only on `/insights` (see Filtering section). Do not assume they work for filtering raw Ad rows.
- **Rate-limit and permission error codes repeat across every endpoint** (100 Invalid parameter, 190 Invalid OAuth token, 200 Permissions error, 613 rate limit exceeded, 80004 too many ad-account calls, 2635 deprecated API version, 3018 start date >37 months back for time_range). Worth handling generically rather than per-endpoint.
