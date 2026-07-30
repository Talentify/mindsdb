# Meta Marketing API — Campaign (`ad-campaign-group`) Reference Extract

Sources fetched:
- https://developers.facebook.com/documentation/ads-commerce/marketing-api/reference/ad-campaign-group.md
- https://developers.facebook.com/documentation/ads-commerce/marketing-api/reference/ad-account/campaigns.md (followed one level deep — the `campaigns` edge lives on the ad-account reference, not the campaign-group reference)

Our handler: `mindsdb/integrations/handlers/meta_ads_handler/tables/campaigns.py`

## Reading fields

| field | type | one-line meaning |
|---|---|---|
| `id` ✅ | numeric string | Campaign identifier (default field) |
| `account_id` | numeric string | Owning ad account ID |
| `adlabels` | list<AdLabel> | Ad labels attached to this campaign |
| `bid_strategy` ✅ | enum | Bidding strategy — see Enums |
| `boosted_object_id` | numeric string | Associated boosted object |
| `brand_lift_studies` | list<AdStudy> | Automated Brand Lift V2 studies |
| `budget_rebalance_flag` | bool | Auto daily rebalancing across ad sets (deprecated v7.0) |
| `budget_remaining` ✅ | numeric string | Unspent budget amount (minor currency units) |
| `buying_type` ✅ | string | `AUCTION` (default) or `RESERVED` |
| `campaign_group_active_time` | numeric string | Active running duration |
| `can_create_brand_lift_study` | bool | Brand lift study eligibility |
| `can_use_spend_cap` | bool | Spend cap capability |
| `configured_status` | enum | User-set status (as opposed to system-derived `effective_status`) — see Enums |
| `created_time` ✅ | datetime | Creation timestamp |
| `daily_budget` ✅ | numeric string | Daily budget allocation (minor currency units) |
| `effective_status` ✅ | enum | System-computed status — see Enums |
| `has_secondary_skadnetwork_reporting` | bool | Secondary SKAdNetwork flag |
| `is_adset_budget_sharing_enabled` | bool | Ad set budget sharing status |
| `is_budget_schedule_enabled` | bool | Budget scheduling enablement |
| `is_reels_trending_ads_enabled` | bool | Reels trending ads indicator |
| `is_skadnetwork_attribution` | bool | SKAdNetwork iOS 14+ targeting |
| `issues_info` | list<AdCampaignIssuesInfo> | Delivery-preventing issues (v3.2+) |
| `last_budget_toggling_time` | datetime | Last time budget-optimization toggled |
| `lifetime_budget` ✅ | numeric string | Total campaign budget (minor currency units) |
| `name` ✅ | string | Campaign display name |
| `objective` ✅ | string | Campaign marketing objective — see Enums |
| `pacing_type` | list<string> | Pacing configuration (typically `"standard"`) |
| `primary_attribution` | enum | Primary attribution model |
| `promoted_object` | AdPromotedObject | Cross-ad promotion target object |
| `smart_promotion_type` | enum | `GUIDED_CREATION`, `SMART_APP_PROMOTION` |
| `source_campaign` | Campaign | Source campaign for duplicates |
| `source_campaign_id` | numeric string | Source campaign ID reference |
| `special_ad_categories` ✅ | list<enum> | Special category declarations (v7.0+) — see Enums |
| `special_ad_category` | enum | Legacy single-value category (superseded by `special_ad_categories` in v7.0) |
| `special_ad_category_country` | list<enum> | Country-level category application (v7.0+) |
| `spend_cap` ✅ | numeric string | Maximum campaign spend limit (minor currency units) |
| `start_time` ✅ | datetime | Campaign commencement (read-only at campaign level; actually set at ad set level) |
| `status` ✅ | enum | Same value space as `configured_status`/`effective_status` (legacy field) — see Enums |
| `stop_time` ✅ | datetime | Campaign conclusion (read-only at campaign level; actually set at ad set level) |
| `topline_id` | numeric string | Topline identifier |
| `updated_time` ✅ | datetime | Last modification timestamp |

**Count: 39 documented readable fields vs. 16 exposed by our handler.**

## Enums

**`objective`**
Legacy objectives: `APP_INSTALLS`, `BRAND_AWARENESS`, `CONVERSIONS`, `EVENT_RESPONSES`, `LEAD_GENERATION`, `LINK_CLICKS`, `LOCAL_AWARENESS`, `MESSAGES`, `OFFER_CLAIMS`, `PAGE_LIKES`, `POST_ENGAGEMENT`, `PRODUCT_CATALOG_SALES`, `REACH`, `STORE_VISITS`, `VIDEO_VIEWS`.
ODAX (Outcome-Driven Ads Experience) objectives, replacing legacy objectives since Marketing API v17.0: `OUTCOME_APP_PROMOTION`, `OUTCOME_AWARENESS`, `OUTCOME_ENGAGEMENT`, `OUTCOME_LEADS`, `OUTCOME_SALES`, `OUTCOME_TRAFFIC`.
Example mapping given in the doc: legacy `BRAND_AWARENESS` → `OUTCOME_AWARENESS` (with `AD_RECALL_LIFT` optimization goal at ad-set level).

**`status` / `configured_status`** (same value space per the doc)
`ACTIVE`, `PAUSED`, `DELETED`, `ARCHIVED`.

**`effective_status`**
`ACTIVE`, `PAUSED`, `DELETED`, `ARCHIVED`, `IN_PROCESS`, `WITH_ISSUES`.
The `campaigns` edge on ad-account (a separate reference page) documents a **larger** set of `effective_status` values accepted as a *query filter*: `ACTIVE`, `PAUSED`, `DELETED`, `PENDING_REVIEW`, `DISAPPROVED`, `PREAPPROVED`, `PENDING_BILLING_INFO`, `CAMPAIGN_PAUSED`, `ARCHIVED`, `ADSET_PAUSED`, `IN_PROCESS`, `WITH_ISSUES`. (The field-level reference and the edge-filter reference disagree on the full list — quoting both as documented; the edge-filter list is a superset and is what to use when building the `effective_status` request param.)

**`buying_type`**
`AUCTION` (default, standard competitive bidding), `RESERVED` (reach and frequency; disabled for housing/employment/credit special ad categories).

**`bid_strategy`**
`LOWEST_COST_WITHOUT_CAP`, `LOWEST_COST_WITH_BID_CAP`, `COST_CAP`, `LOWEST_COST_WITH_MIN_ROAS`.
Deprecation: `TARGET_COST` was deprecated in Marketing API v9.0 (no longer a valid value).

**`special_ad_categories`**
`NONE`, `EMPLOYMENT`, `HOUSING`, `CREDIT`, `ISSUES_ELECTIONS_POLITICS`, `ONLINE_GAMBLING_AND_GAMING`, `FINANCIAL_PRODUCTS_SERVICES`.
All campaigns must declare this (array; use `NONE` or empty array if not applicable). `HOUSING`, `EMPLOYMENT`, `CREDIT` enforce targeting/audience restrictions; `ISSUES_ELECTIONS_POLITICS` does not restrict targeting.

## Edges

| edge | what it returns | notable read params |
|---|---|---|
| `ads` (Edge<Adgroup>) | Ads belonging to the campaign | not documented on this page |
| `adsets` (Edge<AdCampaign>) | Ad sets belonging to the campaign | not documented on this page |
| `ad_studies` (Edge<AdStudy>) | Ad studies attached to the campaign | not documented on this page |
| `adrules_governed` (Edge<AdRule>) | Ad rules governing this campaign | not documented on this page |
| `copies` (Edge<AdCampaignGroup>) | Duplicated copies of this campaign (also a write endpoint: `POST /{campaign_id}/copies`) | not documented on this page |
| `adlabels` | (field, not a paginated edge) list of `AdLabel` attached to the campaign | — |

Our handler currently only reads the parent `campaigns` edge (`GET /act_<id>/campaigns`) and single-campaign lookup (`GET /<campaign_id>`); none of the above sub-edges are implemented.

## Read params (GET)

From the `ad-account` → `campaigns` edge reference (one level deep):

| param | type | allowed values / format | what it does |
|---|---|---|---|
| `fields` | string | comma-separated field names | Standard Graph API field selection |
| `effective_status` | list of enums | `ACTIVE`, `PAUSED`, `DELETED`, `PENDING_REVIEW`, `DISAPPROVED`, `PREAPPROVED`, `PENDING_BILLING_INFO`, `CAMPAIGN_PAUSED`, `ARCHIVED`, `ADSET_PAUSED`, `IN_PROCESS`, `WITH_ISSUES` | Filters returned campaigns by effective status. Default (when omitted) excludes archived/deleted campaigns. |
| `is_completed` | boolean | `true`/`false` | Returns completed campaigns when `true` |
| `date_preset` | enum | `today`, `yesterday`, `this_month`, `last_month`, `this_quarter`, `maximum`, `data_maximum`, `last_3d`, `last_7d`, `last_14d`, `last_28d`, `last_30d`, `last_90d`, `last_week_mon_sun`, `last_week_sun_sat`, `last_quarter`, `last_year`, `this_week_mon_today`, `this_week_sun_today`, `this_year` | Predefined date range. **Applies only to attached `insights` summary data returned in the response `summary` block, not to which Campaign objects are returned** — the campaign nodes themselves have no "date" to filter on other than `created_time`/`updated_time`/`start_time`/`stop_time`, which are plain fields, not filterable via `date_preset`. |
| `time_range` | object | `{'since':'YYYY-MM-DD','until':'YYYY-MM-DD'}` | Custom date range, same scope as `date_preset` — feeds the `summary.insights` block, not campaign selection itself. Doc note: "the start date of the time range cannot be beyond 37 months from the current date." |
| `filtering` | array of `{field, operator, value}` | see Filtering section | Field-level filter on returned campaigns (e.g. `campaign.status`) |
| `updated_since` | — | not documented on either page fetched | not documented |
| `sort` | — | not documented on either page fetched | not documented |
| `limit` | — | not documented in field detail (standard Graph API paging param, behavior not spelled out on these two pages) | Page size |

**`summary`** (response-side, not a request param but worth flagging): the response can include a `summary` object with `total_count` (default) and `insights` (an aggregate analytics summary for all objects matched by the query) — this is where `date_preset`/`time_range` actually take effect, per the doc's structure.

## Filtering

The `ad-account`/`campaigns` edge page documents `filtering` as an available query parameter but does not spell out the operator list or give a JSON example on that page. The **operator list and filter-object shape** are documented (verbatim) on the `ad-label` reference page (for the `*bylabels` endpoints), which describes the same generic filter-object shape used across the Marketing API:

> "Field filtering uses filter objects with three properties: `field`, `operator`, and `value`. Valid operators include: `EQUAL`, `NOT_EQUAL`, `GREATER_THAN`, `GREATER_THAN_OR_EQUAL`, `LESS_THAN`, `LESS_THAN_OR_EQUAL`, `IN_RANGE`, `NOT_IN_RANGE`, `CONTAIN`, `NOT_CONTAIN`, `IN`, `NOT_IN`, `ANY`, `ALL`, `NONE`."

No JSON example for a campaign-specific `filtering` value was found on either fetched page. Our handler already uses the shape `{"field": "campaign.status", "operator": "EQUAL", "value": status}` (confirmed as a valid shape by the operator list above, but the specific `campaign.status` field name and this exact example are not directly quoted in the fetched docs — treat as inherited convention, not doc-confirmed).

**Not documented**: full list of which Campaign fields are filterable via `filtering` (only `campaign.status`, used by our handler, and the generic operator vocabulary above were found).

## Gotchas

- **Deprecations**:
  - `special_ad_category` (string) → replaced by `special_ad_categories` (array) in Marketing API v7.0.
  - `budget_rebalance_flag` deprecated in v7.0.
  - `TARGET_COST` bid strategy deprecated in v9.0.
  - `date_preset = lifetime` disabled in v10.0, replaced by `date_preset = maximum` (37-month lookback cap).
  - Legacy objectives being phased out in favor of ODAX (`OUTCOME_*`) objectives since v17.0.
  - Impressions optimization for legacy Post Engagement + `ON_POST` deprecated in v20.0.
- **Currency minor units**: `daily_budget`, `lifetime_budget`, `budget_remaining`, `spend_cap` are all returned in the ad account's currency **minor units** (e.g. cents), not major units — already noted correctly in our handler's docstring.
- **`start_time`/`stop_time` are read-only at the campaign level** — the doc states they are actually set at the ad set level, campaign-level values just reflect them.
- **`status` vs `effective_status` vs `configured_status`**: `status` is the legacy field; `configured_status` is the user-set value; `effective_status` is system-computed and has more possible values (especially as a query filter — see Enums note above on the two different documented `effective_status` lists).
- **Special ad categories require compliance**: `HOUSING`/`EMPLOYMENT`/`CREDIT`/political categories carry targeting restrictions and may require special ad-account access; not detailed further on the fetched pages ("not documented" beyond the targeting-restriction note).
- **37-month lookback window**: applies to `time_range`/`date_preset` insight data (error code 3018 if start date exceeds 37 months).
- **Rate limiting**: error code 80004 signals ad-account rate limiting; retry after a delay.
