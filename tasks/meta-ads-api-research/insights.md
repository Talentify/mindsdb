# Meta Marketing API — Ads Insights (`/{ad-object}/insights`) Reference Extract

Source: Graph API v25.0, docs at `developers.facebook.com/documentation/ads-commerce/marketing-api/...`
Fetched 2026-07-30. Every fact below traces to a page that returned content (listed at the
bottom). Where a documented answer could not be confirmed, it is marked **not documented**
rather than guessed.

Sources that worked:
- `.../insights.md` (overview)
- `.../insights/best-practices.md` (limits, async flow, rate limits)
- `.../reference/ad-account/insights.md` (full Fields + Parameters tables — this is the
  primary source for the tables below)
- `.../insights/breakdowns.md` (breakdown list + combination rules)
- `.../insights/parameters.md` (attribution windows, action_report_time, time_increment/date_preset details)
- Legacy `docs/marketing-api/reference/ads-action-stats` (AdsActionStats shape + full `action_type` enum)

Sources that 404'd (tried `.md` then non-`.md`, per stop condition — did not guess content):
- `.../reference/ads-insights.md`
- `.../reference/ads-action-stats.md`
- `docs/marketing-api/insights/filtering`
- `docs/marketing-api/insights/parameters/v25.0`
- `.../insights/parameters.md` filtering section specifically (page exists but the fetch
  tool truncated it before the filter-operator list; operator names below come from the
  worked examples embedded in `best-practices.md` plus the `filtering` parameter row in
  `reference/ad-account/insights.md`, not from an exhaustive operator table)

## Read params

| param | type | allowed values / format | what it does |
|---|---|---|---|
| `fields` | list\<string\> | comma-separated field names | Which metrics/dimensions to return. Default: a small set of "most used" fields (impressions, spend, etc. — exact default set not enumerated by the docs beyond "default" flags in the Fields table: `account_id`, `ad_id`, `adset_id`, `campaign_id`, `date_start`, `date_stop`, `impressions`, `spend` are marked `[default]`). |
| `level` | enum | `ad`, `adset`, `campaign`, `account` | Aggregation level of the result rows. |
| `date_preset` | enum | `today`, `yesterday`, `this_month`, `last_month`, `this_quarter`, `maximum`, `data_maximum`, `last_3d`, `last_7d`, `last_14d`, `last_28d`, `last_30d`, `last_90d`, `last_week_mon_sun`, `last_week_sun_sat`, `last_quarter`, `last_year`, `this_week_mon_today`, `this_week_sun_today`, `this_year` | Relative time range. Default `last_30d`. Ignored if `time_range` or `time_ranges` is also given. Note: `lifetime` was disabled in v10.0 and replaced by `maximum`, which returns a max of 37 months of data. |
| `time_range` | object | `{"since":"YYYY-MM-DD","until":"YYYY-MM-DD"}` | Single explicit date range. Ignored if `time_ranges` is given; takes precedence over `date_preset`. |
| `time_ranges` | list\<object\> | array of `{since,until}` objects, overlapping ranges allowed | Multiple ranges in one call. Ignored if combined with `date_preset`/`time_range`/`time_increment` (mutually exclusive with those). |
| `time_increment` | enum or integer | `monthly`, `all_days`, or an integer `1`–`90` (days per row) | Time-slice granularity of returned rows. Default `all_days` (one row for the whole range). Ignored if `time_ranges` is given. |
| `breakdowns` | list\<enum\> | see Breakdowns doc — `age`, `gender`, `country`, `region`, `dma`, `publisher_platform`, `platform_position`, `device_platform`, `impression_device`, `product_id`, `hourly_stats_aggregated_by_advertiser_time_zone`, `hourly_stats_aggregated_by_audience_time_zone`, and many more (full list in the Fields/Breakdowns section below) | Slices results into extra dimension rows. Only specific combinations are allowed — see Breakdowns section. `impression_device` cannot be used alone. |
| `action_breakdowns` | list\<enum\> | e.g. `action_type`, `action_device`, `action_destination`, `conversion_destination`, `action_target_id`, `action_reaction`, `action_video_type`, `action_video_sound`, `action_carousel_card_id`/`action_carousel_card_name`, `action_canvas_component_name` | Breaks down `actions`/`action_values` line items. Must be used together with `actions`/`action_values` (or another `AdsActionStats`-typed field) in `fields`. If omitted, `action_type` is implicitly applied. |
| `action_attribution_windows` | list\<enum\> | `1d_view`, `7d_view`, `28d_view`, `1d_click`, `7d_click`, `28d_click`, `dda`, `skan_view`, `skan_click`, plus postback variants (`*_all_conversions`, `*_first_conversion` per the AdsActionStats field list) | Attribution window(s) applied to action/conversion metrics. Default `default`, which means `["7d_click","1d_view"]`. |
| `action_report_time` | enum | `impression`, `conversion`, `mixed`, `lifetime` | Which point in time an action's stats are attributed to for reporting. **Note:** as of June 10, 2025, Meta disregards this param and `use_unified_attribution_setting`, mimicking Ads Manager (`mixed` behavior: on-Meta actions use impression time, off-Meta actions use conversion time) — see Discrepancy-with-Ads-Manager warning below. |
| `use_account_attribution_setting` | boolean | `true`/`false` | Use the ad account's attribution setting instead of the request-level params. Default `false`. |
| `use_unified_attribution_setting` | boolean | `true`/`false` | Use the ad set's unified attribution setting; supersedes the account setting. As of June 10 2025 this is effectively the only supported way to match Ads Manager numbers (see warning below) — Meta recommends setting it `true`. |
| `filtering` | list\<Filter Object\> | `[{"field": "<obj>.<field>", "operator": "<OP>", "value": <val>}, ...]` | Server-side filter on the underlying ad objects/metrics before aggregation. See Filtering section. |
| `sort` | list\<string\> | one field name with `_ascending` or `_descending` suffix, e.g. `impressions_descending` | Sort order of rows. Only one sort field supported. Default ascending. |
| `limit` | integer | row count | Max rows per page. |
| `after` / `before` | string (cursor) | opaque cursor from `paging.cursors` | Standard Graph API pagination cursors (documented generically for all edges, not insights-specific; **not documented in insights-specific pages fetched** — inferred only from the example response's `paging.cursors` object). |
| `default_summary` | boolean | `true`/`false` | Whether to include a `summary` block automatically. Default `false`. |
| `summary` | list\<string\> | field names | If set, requests a `summary` aggregate section over the listed fields. |
| `summary_action_breakdowns` | list\<enum\> | same enum space as `action_breakdowns` | `action_breakdowns` equivalent applied to the `summary` section only. |
| `product_id_limit` | integer | count | Caps how many product IDs appear per ad when breaking down by `product_id`. |
| `graph_cache` | boolean | `true`/`false` | Internal-only cache control. Default `true`. |
| `export_format` | string | `xls`, `csv` | Requests an exportable async report format (used with POST / async flow). |
| `export_name` | string | filename | Name for the exported report file. |
| `export_columns` | list\<string\> | field names | Restricts which fields appear in the exported report. |

**Async report flow params:** the same GET-style params (`fields`, `level`, `time_range`,
`filtering`, etc.) are sent on the `POST /{ad-object}/insights` call; it returns
`{"report_run_id": "<id>"}` instead of data — see "Async report flow" section.

## Filtering

Documented shape (from `reference/ad-account/insights.md` + worked examples in
`best-practices.md`):

```
filtering=[{"field": "<object>.<field>", "operator": "<OPERATOR>", "value": <value>}]
```

- `field` uses dot notation on the underlying ad object, e.g. `ad.impressions`,
  `campaign.id`, `adset.id`, `ad.id`, `ad.effective_status`.
- `operator` and `value` — **only the operators actually shown in worked examples were
  confirmed**: `EQUAL`, `GREATER_THAN`, `IN`. `STARTS_WITH` and `CONTAIN` are named
  explicitly in `best-practices.md` with a caveat (see below) but no JSON example was
  captured. **The full canonical operator enum (`NOT_EQUAL`, `LESS_THAN`, `IN_RANGE`,
  `NOT_IN`, `ANY`, `ALL`, `NONE`, `AFTER`, `BEFORE`, etc.) could not be confirmed from the
  fetched pages — the dedicated filtering doc 404'd in both `.md` and non-`.md` form, and
  a further attempt at a differently-named filtering page returned content the fetch tool
  truncated before reaching the operator table. Do not treat any operator not listed here
  as confirmed.**

Confirmed examples (verbatim from `best-practices.md`):

```
-d 'filtering=[{field:"ad.impressions",operator:"GREATER_THAN",value:0}]'
```

```
-d 'filtering=[{field:"ad.impressions",operator:"GREATER_THAN",value:0},]'
```

Documented caveat (verbatim): *"filtering with `STARTS_WITH` and `CONTAIN` does not
change the summary data. In this case, use the `IN` operator."* — i.e. those two
operators only filter the returned rows, not any `summary`/aggregate section.

Our handler already pushes down `EQUAL` and `IN` on `campaign.id` / `adset.id` / `ad.id`
(`tables/insights.py:304-314`) — that matches the confirmed operator set. Nothing here
contradicts current behavior; it's just incomplete relative to the full operator list.

## Fields — dimensions

| field | one-line meaning |
|---|---|
| ✅ `account_id` | Ad account ID [default] |
| `account_currency` | Currency used by ad account |
| ✅ `account_name` | Ad account name |
| ✅ `campaign_id` | Campaign ID [default] |
| ✅ `campaign_name` | Campaign name |
| `campaign_start` / `campaign_end` | Campaign start/end date |
| ✅ `adset_id` | Ad set ID [default] |
| ✅ `adset_name` | Ad set name |
| `adset_start` / `adset_end` | Ad set start/end date |
| ✅ `ad_id` | Ad ID [default] |
| ✅ `ad_name` | Ad name |
| ✅ `date_start` | Start date of data (reporting range) [default] |
| ✅ `date_stop` | End date of data (reporting range) [default] |
| ✅ `objective` | Campaign objective |
| ✅ `buying_type` | Method by which you pay for and target ads |
| `optimization_goal` | Optimization goal for the ad/ad set |
| `attribution_setting` | Default attribution window used when attribution result is calculated |
| `created_time` / `updated_time` | Object creation / last-update time |
| `country` / `region` / `dma` | Geo dimensions (also usable as `breakdowns`) |
| `device_platform` / `impression_device` / `publisher_platform` / `platform_position` | Placement/device dimensions (also usable as `breakdowns`) |
| `product_id` / `product_name` / `product_brand` / `product_category` / `product_content_id` / `product_retailer_id` / `product_group_content_id` / `product_group_retailer_id` / `product_vendor_id` / `product_custom_label_0..4` / `product_custom_number_0..4` | Catalog/product-level dimensions returned with `product_id` breakdown |
| `hourly_stats_aggregated_by_advertiser_time_zone` / `hourly_stats_aggregated_by_audience_time_zone` | Hourly bucket dimensions |
| `comscore_market` / `zip` | Third-party market / postal-code dimensions |

Note: many of the "generic breakdowns" (`age`, `gender`, `country`, `region`, `dma`,
`publisher_platform`, `platform_position`, `impression_device`, `device_platform`,
`product_id`, hourly variants, `action_*`, asset-id breakdowns like `image_asset`,
`video_asset`, `title_asset`, `body_asset`, `call_to_action_asset`, `link_url_asset`,
`description_asset`, `ad_format_asset`) are requested via the `breakdowns` /
`action_breakdowns` params rather than `fields`, but the docs also list several of them
as top-level Fields-table rows (they overlap). Full breakdown list and combination rules
are in the "Breakdowns" doc summary captured above (types 1/2 restrictions,
permutation table, hourly limitations).

## Fields — metrics

Grouped; ✅ = already exposed by our handler. This is the full Fields table from
`reference/ad-account/insights.md`, filtered to numeric/metric-shaped rows (types
`numeric string`, `list<AdsActionStats>`, `list<AdsInsightsResult>`, etc.). Dimension-only
rows are omitted here (see previous section).

### Delivery
| field | type | meaning |
|---|---|---|
| ✅ `impressions` | numeric string | Times ads were on screen [default] |
| ✅ `reach` | numeric string | Estimated number who saw ads at least once |
| `unique_impressions` | numeric string | People who saw ads at least once |
| ✅ `frequency` | numeric string | Estimated average times each person saw the ad |
| `full_view_impressions` / `full_view_reach` | numeric string | Full-view impression/reach counts |
| `social_spend` | numeric string | Total spend for ads shown with social information |
| `impressions_gross` / `impressions_auto_refresh` | string | Gross / auto-refresh impression variants |

### Cost / spend
| field | type | meaning |
|---|---|---|
| ✅ `spend` | numeric string | Total estimated spend [default] |
| ✅ `cpc` | numeric string | Average cost per click |
| ✅ `cpm` | numeric string | Average cost per 1,000 impressions |
| ✅ `cpp` | numeric string | Estimated average cost per 1,000 Accounts Center accounts reached |
| `today_spend` | numeric string | Estimated spend since 12 AM today |
| `cost_per_inline_link_click` ✅ | numeric string | Average inline link click cost |
| `cost_per_inline_post_engagement` | numeric string | Average inline post engagement cost |
| `cost_per_unique_click` | numeric string | Estimated average unique click cost |
| `cost_per_unique_inline_link_click` | numeric string | Estimated average unique inline link click cost |
| `cost_per_action_type` (raw, ✅ passthrough) | list\<AdsActionStats\> | Average cost per action type |
| `cost_per_conversion` / `cost_per_unique_conversion` | list\<AdsActionStats\> | Cost per (unique) conversion |
| `cost_per_outbound_click` / `cost_per_unique_outbound_click` | list\<AdsActionStats\> | Cost per (unique) outbound click |
| `cost_per_thruplay` | list\<AdsActionStats\> | Avg cost per ThruPlay (in development) |
| `cost_per_result` / `cost_per_objective_result` / `cost_per_action_result` | list\<AdsInsightsResult\> / AdsActionStats | Cost per result, keyed to the campaign objective |
| `cost_per_15_sec_video_view` / `cost_per_2_sec_continuous_video_view` | list\<AdsActionStats\> | Cost per video-view milestone |
| `cost_per_completed_video_view` | list\<AdsActionStats\> | Cost per completed video view |
| `cost_per_contact` / `cost_per_customize_product` / `cost_per_donate` / `cost_per_find_location` / `cost_per_schedule` / `cost_per_start_trial` / `cost_per_submit_application` / `cost_per_subscribe` | list\<AdsActionStats\> | Cost per goal-specific action (lead-gen/local/subscription objectives) |
| `cost_per_dda_countby_convs` | numeric string | Cost per DDA-attributed conversion |
| `cost_per_dwell` / `cost_per_dwell_3_sec` / `cost_per_dwell_5_sec` / `cost_per_dwell_7_sec` | numeric string | Cost per dwell / dwell-threshold |
| `cost_per_one_thousand_ad_impression` | list\<AdsActionStats\> | Cost per thousand impressions (action-typed variant) |
| `cost_per_total_action` | numeric string | Average cost per relevant action |
| `cost_per_ad_click` | list\<AdsActionStats\> | Cost per ad click |
| `cost_per_unique_action_type` | list\<AdsActionStats\> | Estimated average unique action cost |

### Clicks / engagement
| field | type | meaning |
|---|---|---|
| ✅ `clicks` | numeric string | Total ad clicks |
| ✅ `unique_clicks` | numeric string | Total unique ad clicks |
| ✅ `ctr` | numeric string | Click-through rate |
| ✅ `unique_ctr` | numeric string | Unique click-through rate (not in current Fields table verbatim — kept as-is; still valid) |
| ✅ `inline_link_clicks` | numeric string | Clicks to destinations within a 1-day-click window |
| ✅ `inline_link_click_ctr` | numeric string | Inline link CTR |
| `inline_post_engagement` | numeric string | Total inline post engagement actions (1-day-click) |
| `outbound_clicks` / `outbound_clicks_ctr` | list\<AdsActionStats\> | Clicks/CTR to off-platform destinations |
| `website_clicks` / `website_ctr` | numeric string / list\<AdsActionStats\> | Website link clicks / CTR |
| `app_store_clicks` | numeric string | Clicks to app store (unavailable with `breakdowns`) |
| `call_to_action_clicks` | numeric string | CTA button clicks |
| `newsfeed_clicks` / `newsfeed_impressions` / `newsfeed_avg_position` | numeric string | News-feed-specific delivery (unavailable with `breakdowns`) |
| `ad_click_actions` / `ad_impression_actions` | list\<AdsActionStats\> | Ad-level click/impression action breakdown |
| `actions_per_impression` | numeric string | Actions ÷ impressions |
| `deeplink_clicks` | numeric string | App deeplink clicks |
| `instant_experience_clicks_to_open` / `instant_experience_clicks_to_start` / `instant_experience_outbound_clicks` | numeric string / list\<AdsActionStats\> | Instant Experience (Canvas) engagement |
| `canvas_avg_view_percent` / `canvas_avg_view_time` | numeric string | Instant Experience view depth/time |
| `interactive_component_tap` | list\<AdsActionStats\> | Interactive component taps |
| `thumb_stops` | numeric string | Display-ad "dwell" count |
| `dwell_3_sec` / `dwell_5_sec` / `dwell_7_sec` / `dwell_rate` | numeric string | Dwell counts / dwell rate |
| `landing_page_view_per_link_click` | numeric string | Landing page views per link click (ratio version of our derived `landing_page_views`) |

### Video
| field | type | meaning |
|---|---|---|
| `video_play_actions` | list\<AdsActionStats\> | Video play starts (in development) |
| `video_p25/p50/p75/p95/p100_watched_actions` | list\<AdsActionStats\> | Video plays reaching each % threshold |
| `video_30_sec_watched_actions` | list\<AdsActionStats\> | Video plays of 30+ seconds |
| `video_6_sec_watched_actions` | list\<AdsActionStats\> | 6-second video watches |
| `video_avg_time_watched_actions` | list\<AdsActionStats\> | Average watch time |
| `video_complete_watched_actions` | list\<AdsActionStats\> | Completed (30+s) views |
| `video_completed_view_or_15s_passed_actions` | list\<AdsActionStats\> | Completed view or 15s-passed |
| `video_continuous_2_sec_watched_actions` | list\<AdsActionStats\> | 2-second continuous watches |
| `video_time_watched_actions` | list\<AdsActionStats\> | Total video time watched |
| `video_play_curve_actions` / `video_play_retention_0_to_15s_actions` / `video_play_retention_20_to_60s_actions` / `video_play_retention_graph_actions` | list\<AdsHistogramStats\> | Retention-curve histograms (video fields incompatible with hourly breakdowns; retention graph incompatible with `region` breakdown) |
| `conditional_time_spent_ms_over_2s/3s/6s/10s/15s_actions` | list\<AdsActionStats\> | Time-spent-over-threshold actions |

### Conversions / actions (generic action-stat containers)
| field | type | meaning |
|---|---|---|
| ✅ `actions` (raw passthrough) | list\<AdsActionStats\> | Total actions attributed to ads, keyed by `action_type` |
| ✅ `action_values` (raw passthrough) | list\<AdsActionStats\> | Total value of conversions attributed to ads |
| `conversions` / `conversion_values` | list\<AdsActionStats\> | Conversions / conversion values (separate from `actions`/`action_values`) |
| `total_actions` / `total_unique_actions` / `total_action_value` | numeric string | Aggregate action counters |
| `unique_actions` | **not documented** in the fetched Fields table (referenced in the audit brief but not found as a top-level field name here — likely superseded by `total_unique_actions` / per-action `unique` sub-keys inside `AdsActionStats`) |
| `results` / `result_rate` / `objective_results` / `objective_result_rate` / `actions_results` | list / AdsActionStats | Results measured against the ad's objective |
| `mobile_app_purchase_roas` / `website_purchase_roas` / `catalog_segment_value_*_roas` | list\<AdsActionStats\> | ROAS variants by purchase source |
| ✅ `purchase_roas` (raw passthrough) | list\<AdsActionStats\> | Purchase ROAS from connected business tools |
| `contact_actions` / `contact_value` | list\<AdsActionStats\> | Contact actions/value (lead-gen-adjacent objective) |
| `customize_product_actions` / `customize_product_value` | list\<AdsActionStats\> | Product customization actions/value |
| `donate_actions` / `donate_value` | list\<AdsActionStats\> | Donation actions/value |
| `find_location_actions` / `find_location_value` | list\<AdsActionStats\> | Location-finding actions/value |
| `schedule_actions` / `schedule_value` | list\<AdsActionStats\> | Appointment-scheduling actions/value |
| `start_trial_actions` / `start_trial_value` | list\<AdsActionStats\> | Trial-start actions/value |
| `subscribe_actions` / `subscribe_value` | list\<AdsActionStats\> | Subscription actions/value |
| `recurring_subscription_payment_actions` | list\<AdsActionStats\> | Recurring subscription payments |
| `cancel_subscription_actions` | list\<AdsActionStats\> | Cancelled subscriptions |
| `submit_application_actions` / `submit_application_value` | list\<AdsActionStats\> | Application-submission actions/value |
| `converted_product_*` (quantity, value, offline_purchase, omni_purchase, website_pixel_purchase, app_custom_event_fb_mobile_purchase, ...) | list\<AdsActionStats\> | Product-level (catalog) purchase attribution, keyed per product |
| `catalog_segment_actions` / `catalog_segment_value` / `catalog_segment_value_in_catalog_currency` | list\<AdsActionStats\> | Catalog-segment-promotion actions/value |
| `dda_countby_convs` / `cost_per_dda_countby_convs` / `dda_results` | numeric string / list | Data-driven-attribution counters |
| `private_attribution_conversions` | unsigned integer | Private (aggregated/on-device) attribution conversions |
| `adjusted_offline_purchase` | numeric string | Offline purchases attributed to ads |
| `card_views` | numeric string | Product-card views (in development) |
| `shops_assisted_purchases` / `total_card_view` / `product_views` | string | Shops/catalog engagement counters |

### Quality rankings
**Not documented** in the fetched Fields table: the standard Ads Manager "quality ranking",
"engagement rate ranking", and "conversion rate ranking" fields did not appear verbatim in
this page's Fields table. `relevance_score` appears only indirectly, in the Breakdowns doc's
"unavailable with breakdowns" list, meaning it exists as a field but its type/description
were not captured here — treat as **not documented** rather than guessing its shape.

### Attribution-dependent (values vary materially by `action_attribution_windows` / `use_unified_attribution_setting`)
All `AdsActionStats`-typed fields above are attribution-dependent by construction (each
entry can carry per-window sub-values: `1d_click`, `7d_click`, `28d_click`, `1d_view`,
`7d_view`, `28d_view`, plus `*_all_conversions`/`*_first_conversion` variants, `dda`,
`inline`, `incrementality`, `1d_ev`). Additionally:
- `attribution_setting` (dimension) reports which window was actually used.
- `multi_event_conversion_attribution_setting` / `anchor_event_attribution_setting` /
  `anchor_events_performance_indicator` — attribution-setting metadata fields, exact
  semantics **not documented** beyond the field name + one-line description already
  listed in the Fields table.

## Action-type fields

`actions`, `action_values`, `conversions`, `conversion_values`, `cost_per_action_type`,
`ad_click_actions`, `ad_impression_actions`, `outbound_clicks`, and all the goal-specific
`*_actions`/`*_value` fields above are all typed `list<AdsActionStats>`. Each list entry
(`AdsActionStats`) has this shape (confirmed from legacy `docs/marketing-api/reference/ads-action-stats`):

| sub-field | meaning |
|---|---|
| `action_type` | which action this entry counts (see enum below) |
| `value` | metric value under the **default** attribution window |
| `1d_click` / `7d_click` / `28d_click` | value under that click-attribution window |
| `1d_click_all_conversions` / `1d_click_first_conversion` (and `7d_`/`28d_` equivalents) | all-conversions vs. first-conversion variants per click window |
| `1d_view` / `7d_view` / `28d_view` | value under that view-attribution window |
| `1d_view_all_conversions` / `1d_view_first_conversion` (and `7d_`/`28d_` equivalents) | all-conversions vs. first-conversion variants per view window |
| `1d_ev` | engaged-view (1-day) attribution value |
| `incrementality` | value under the incremental-attribution model |
| `dda` | value under the data-driven-attribution model |
| `inline` | value attributable to interaction on the ad unit itself |
| `action_device` | device the action occurred on (`Desktop`, `iPhone`, `iPad`, `Android Smartphone`, `Android Tablet`, `Offline`, `N/A`) — populated when `action_device` is in `action_breakdowns` |
| `action_destination` | destination the user went to after the action |
| `action_video_type` / `action_video_sound` | video-specific breakdown context |
| `action_carousel_card_name` | which carousel card the action happened on |
| `action_reaction` | reaction-type breakdown (Like, Love, Haha, Wow, Sad, Angry) |

### `action_type` enum values that matter most for an ads-analytics product

- **Purchases:** `offsite_conversion.fb_pixel_purchase` (pixel/off-site purchase),
  `onsite_conversion.purchase` (on-Facebook purchase), `omni_purchase` (omni-channel
  aggregate — **this is what our handler currently reads for `purchases`/`purchase_value`
  via `["omni_purchase", "purchase"]`; note plain `"purchase"` is not in this enum list —
  worth double-checking against a live response**), `mobile_app_purchase` variants under
  `app_custom_event.fb_mobile_purchase`, `converted_product_*` for catalog/product-level.
- **Leads:** `offsite_conversion.fb_pixel_lead`, `lead` (grouped: offsite + on-Facebook),
  `onsite_conversion.lead_grouped`, `leadgen_grouped`.
- **Link clicks:** `link_click`.
- **Landing page views:** `landing_page_view`.
- **Video views:** `video_view` (3-second video views) — note there is no separate
  `action_type` for longer thresholds; those are separate top-level fields
  (`video_p25_watched_actions`, etc.), not `action_type` values.
- **Post engagement:** `post_engagement` (grouped), plus its components `like`,
  `comment`, `post` (shares), `post_reaction`.
- **Page engagement:** `page_engagement` (grouped).
- **Registrations:** `offsite_conversion.fb_pixel_complete_registration`,
  `app_custom_event.fb_mobile_complete_registration`,
  `omni_complete_registration`.
- **Add to cart:** `offsite_conversion.fb_pixel_add_to_cart`,
  `app_custom_event.fb_mobile_add_to_cart`, `omni_add_to_cart`.
- **Checkout initiated:** `offsite_conversion.fb_pixel_initiate_checkout`,
  `app_custom_event.fb_mobile_initiated_checkout`, `omni_initiated_checkout`.
- **App installs:** `app_install`, `mobile_app_install`, `omni_app_install`.
- **Custom conversions:** `offsite_conversion.custom.<custom_conv_id>`,
  `offsite_conversion.fb_pixel_custom`.
- **Messaging:** `onsite_conversion.messaging_conversation_started_7d`,
  `onsite_conversion.messaging_first_reply`, `onsite_conversion.messaging_user_subscribed`,
  `onsite_conversion.messaging_block`.
- Full enum captured is long (mobile-app events, contact/donate/schedule/subscribe/trial
  families, click-to-call variants, omni-channel variants for install/add-to-cart/
  registration/view-content/search/checkout/achievement/activation/level/rate/spend-
  credits/tutorial/custom) — see the complete list already written above in "Fields —
  metrics → Conversions / actions" plus the dedicated enum dump captured during research
  (available on request; trimmed here to keep this file focused, but nothing was omitted
  from what the source page returned — ask if you want the raw full dump re-appended).

**Important — the doc explicitly separates `purchase` semantics:** the plain string
`purchase` (used today in our `_extract_action_value(actions, ["omni_purchase",
"purchase"])`) was **not** found in the documented `action_type` enum from this source;
only `offsite_conversion.fb_pixel_purchase`, `onsite_conversion.purchase`, and
`omni_purchase` were. This doesn't mean `"purchase"` never appears (older API versions /
some ad accounts may still emit it), but it's not a documented-current value — flagged
for follow-up rather than silently trusted.

## Async report flow

1. `POST /{ad-object}/insights` with the same params as GET → returns
   `{"report_run_id": "<id>"}`. Do not persist `report_run_id` long-term: it expires
   after 30 days.
2. Poll `GET /{report_run_id}` until `async_status` is `Job Completed` and
   `async_percent_completion` is `100`.
   - Status values: `Job Not Started`, `Job Started`, `Job Running`, `Job Completed`,
     `Job Failed` (review query and retry), `Job Skipped` (expired — resubmit).
   - As of v25.0, a failed report returns `error_code`, `error_message`, `error_subcode`,
     `error_user_title`, `error_user_msg` by default.
3. `GET /{report_run_id}/insights` to page through the final result (same `data`/`paging`
   shape as the sync endpoint).

Meta's guidance on **when you must use async**: no hard row/time threshold is documented;
the docs say sync calls can return out-of-memory/timeout errors and there's "no explicit
limit for when a query will fail" — recommended pattern is "try sync first, fall back to
async on timeout," which is exactly what our handler already does (`_fetch_via_async_report`
triggered on `error.code == 1` + "reduce the amount of data" / subcode 99).

There's also a separate, non-versioned **export** convenience endpoint
(`facebook.com/ads/ads_insights/export_report/`) that takes a `report_run_id` and returns
a human-readable `xls`/`csv` file — explicitly not covered by Graph API versioning/breaking-change policy, so treat as unstable if ever used.

## Limits and best practices

- **Data-per-call limits:** two kinds — max rows in response, and max data points needed
  to compute a summary row. Both apply to sync and async calls. Exceeding either returns
  `error_code=100` (`CodeException`, subcode `1487534`).
- **Mitigations recommended by Meta:** narrow the date range or ad-id set; request only
  needed metrics; split into multiple queries; avoid account-level queries with
  high-cardinality breakdowns (`action_target_id`, `product_id`) combined with wide date
  ranges (e.g. `lifetime`/`maximum`); fetch IDs at a higher level first (with `level` +
  `filtering`), then batch-request `/insights` per lower-level object; prefer
  `date_preset` over custom `time_range` (custom ranges are "less efficient to run").
  Insights refresh every 15 minutes and stop changing 28 days after being reported (but
  may keep updating for a few days after an ad completes).
- **Reach/breakdowns restriction (since June 10, 2025):** `reach` (and dependents
  `frequency`, `cpp`) is omitted from standard sync queries that combine `breakdowns`
  with a `start_date` more than 13 months old. To still get old `reach` with breakdowns,
  use async jobs (max 10 requests/account/day) and watch the
  `x-Fb-Ads-Insights-Reach-Throttle` header; past the limit you get: *"Reach-related
  metric breakdowns are unavailable due to rate limit threshold."*
- **Rate limiting:** every response carries `x-fb-ads-insights-throttle`
  (`{app_id_util_pct, acc_id_util_pct, ads_api_access_tier}`) and the account is also
  subject to the generic `x-ad-account-usage` limits. Over-limit → `error_code=4`
  (`CodedException`). Global overload can additionally return `error_code=4`, subcode
  `1504022`, `"Too many API requests"`. Best practice: back off before hitting 100%
  utilization on either dimension, pace calls using the account's own timezone, and use
  `date_preset` over custom ranges.
- **Access tiers:** "Standard Access" → **Limited Access**; "Advanced Access" →
  **Full Access** (renamed, same permission id). Full-Access qualification threshold
  lowered from 1,500 to **500** Marketing API calls in the trailing 15 days.
- **Attribution discrepancy warning (June 10, 2025):** `use_unified_attribution_setting`
  and `action_report_time` are now disregarded server-side; the API mimics Ads Manager —
  attributed values follow the ad set's attribution settings, inline/on-ad actions are
  folded into `1d_click`/`1d_view` windows (standalone `inline` window data no longer
  returned), and action reporting time becomes `mixed` (on-Meta actions use impression
  time, off-Meta actions use conversion time). Meta explicitly recommends setting
  `use_unified_attribution_setting=true` to match Ads Manager numbers going forward —
  **our handler sends neither `use_unified_attribution_setting` nor
  `action_report_time`/`action_attribution_windows` today, so it is silently on the
  (now largely moot, per this warning) legacy default rather than an explicit, documented
  choice.**
