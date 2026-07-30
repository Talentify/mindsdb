# Meta Marketing API — Insights Breakdowns Reference

Source: https://developers.facebook.com/documentation/ads-commerce/marketing-api/insights/breakdowns.md
(fetched as raw markdown; Marketing API v25.0+ docs)

Our handler: `mindsdb/integrations/handlers/meta_ads_handler/tables/insights.py`
Currently allowed `breakdowns` values (`BREAKDOWN_COLUMNS`): age, gender, country, region,
publisher_platform, platform_position, impression_device, device_platform — marked ✅ below.
`action_breakdowns` is passed through unvalidated by the handler.

## Important note on the doc's own structure

The source page presents **one single flat table titled "Generic breakdowns"**, covering
values used with the `breakdowns` request parameter. It does **not** provide a second,
separately-enumerated table for the `action_breakdowns` request parameter. The "Action
breakdown" section only says:

> "Group results in the `actions` field using `action_breakdowns` parameter. If not
> specified, `action_type` is implicitly added."

— without listing a standalone `action_breakdowns` enum. The `action_`-prefixed values
below (action_device, action_canvas_component_name, action_carousel_card_id,
action_carousel_card_name, action_destination, action_reaction, action_target_id,
action_type, action_video_sound, action_video_type) appear in the "Generic breakdowns"
table and again in the "Combining Breakdowns" permutations table, and are the ones the
"Action breakdown" section context describes as used for grouping the `actions` array —
this is the closest the docs come to defining the `action_breakdowns` value set. Treat the
grouping below as best-effort inference from the single table, **not a confirmed separate
enum** — the doc does not explicitly certify these are *exhaustively* the only legal
`action_breakdowns` values, and `action_converted_product_id` (which appears only in the
Combining Breakdowns table, not the Generic breakdowns table) reinforces that the two
tables aren't perfectly in sync.

## `breakdowns` — complete value list (from the "Generic breakdowns" table)

| Breakdown | What it splits by | Notes |
| --- | --- | --- |
| `age` ✅ | Age range of reached people | Combinable with gender; see permutations |
| `gender` ✅ | Gender of reached people ("not specified" for unlisted) | Combinable with age |
| `country` ✅ | Country location of reached people | |
| `region` ✅ | Regions where reached people located | Video action metrics (`video_p25/50/75/95/100_watched_actions`, `video_avg_time_watched_actions`) don't support this breakdown |
| `publisher_platform` ✅ | Platform showing ad (Facebook, Instagram, Audience Network) | Combinable with platform_position, impression_device |
| `platform_position` ✅ | Ad placement within platform (e.g., Facebook desktop Feed) | Combinable with publisher_platform |
| `impression_device` ✅ | Device where last ad served (e.g., "iPhone") | May be temporarily unavailable per an availability notice below; combinable with publisher_platform |
| `device_platform` ✅ | Device type (mobile/desktop) when viewing/clicking ad | Not listed as a standalone row in the "Combining Breakdowns" permutations table (see caveats) |
| `dma` | Designated Market Area — 210 US geographic regions measured by Nielsen | Unavailable for `estimated_ad_recall_rate` or `video_thruplay_watched_actions`; uses sampling, low-volume regions may not appear; Type 1 breakdown (unavailable for off-Meta action metrics) |
| `hourly_stats_aggregated_by_advertiser_time_zone` | Hourly breakdown by advertiser's timezone | Does not support unique fields (`unique_*`), `reach`, or `frequency` (these return 0 when used); Type 1 breakdown (off-Meta action metrics); may be temporarily unavailable, see notice below |
| `hourly_stats_aggregated_by_audience_time_zone` | Hourly breakdown by audience's timezone | Same hourly restrictions as above; Type 1 breakdown; may be temporarily unavailable, see notice below |
| `frequency_value` | Times ad in a Reservation campaign served to each account | Used with `reach` only (frequency per unique user); may be temporarily unavailable, see notice below |
| `product_id` | ID of product in impression, click, or action | Type 2 breakdown (unavailable for off-Meta action metrics) |
| `app_id` | ID of application associated with ad account or campaign | Filtering `app_id` via the `filtering` field is not supported |
| `skan_campaign_id` | Raw campaign ID from SKAN postback (iOS 15+) | |
| `skan_conversion_id` | Assigned Conversion ID (Priority ID) for SKAdNetwork event | Filtering via `filtering` field is not supported |
| `is_conversion_id_modeled` | Boolean: 0 = not modeled, 1 = modeled conversion_bits | |
| `user_segment_key` | User segment (new/existing) for Advantage+ Shopping Campaigns | |
| `place_page_id` | ID of place page in impression or click | |
| `ad_format_asset` | ID of ad format asset in impression, click, or action | Dynamic Creative breakdown — see restricted metric set below |
| `body_asset` | ID of body asset involved in impression, click, or action | Dynamic Creative breakdown — see restricted metric set below |
| `call_to_action_asset` | ID of call-to-action asset | Dynamic Creative breakdown — see restricted metric set below |
| `description_asset` | ID of description asset | Dynamic Creative breakdown — see restricted metric set below |
| `image_asset` | ID of image asset involved in impression, click, or action | Unavailable at ad account level for Dynamic Creative assets; Dynamic Creative breakdown |
| `link_url_asset` | ID of URL asset | Dynamic Creative breakdown — see restricted metric set below |
| `title_asset` | ID of title asset | Dynamic Creative breakdown — see restricted metric set below |
| `video_asset` | ID of video asset | Unavailable at ad account level for Dynamic Creative assets; Dynamic Creative breakdown |
| `action_device` | The device where conversion occurred (e.g., "Desktop") | Type 2 breakdown (off-Meta action metrics restriction); see "action_breakdowns" section note above |
| `action_destination` | Destination after clicking ad (Facebook Page, external URL, app) | Type 2 breakdown; see note above |
| `action_target_id` | ID of destination after clicking ad | Type 2 breakdown; see note above |
| `action_type` | Kind of actions taken on ad, Page, app, or event | Implicitly added when `action_breakdowns` is not specified; see note above |
| `action_reaction` | Number of reactions on ads or boosted posts | See note above |
| `action_carousel_card_id` | ID of carousel card engaged with in ad | Documented jointly as `action_carousel_card_id / action_carousel_card_name`; see note above |
| `action_carousel_card_name` | Carousel card identified by headline | Documented jointly as `action_carousel_card_id / action_carousel_card_name`; see note above |
| `action_canvas_component_name` | Name of component within Canvas ad | Type 2 breakdown; see note above |
| `action_video_sound` | Sound status (on/off) when video plays | See note above |
| `action_video_type` | Video metrics breakdown | See note above |

Total documented values in this table: **37**. Our handler allows **8**; **29 are missing**.

## `action_breakdowns` — complete value list

**Not documented as a standalone enumerated list.** The source page does not provide a
second table for this parameter (see "Important note on the doc's own structure" above).
The values that the doc's text and the "Combining Breakdowns" permutations table associate
with grouping the `actions`/`action_values` arrays are the `action_`-prefixed rows already
listed in the table above, plus one value that appears **only** in the permutations table
and not in the Generic breakdowns table:

| Breakdown | What it splits by | Notes |
| --- | --- | --- |
| `action_converted_product_id` | Not documented (no description given) | "Limited availability for Collaborative Ads"; appears only in the Combining Breakdowns permutations table, not in the Generic breakdowns table |

Our handler passes `action_breakdowns` through unvalidated, so this gap does not cause
false rejections — but it also means no client-side guard exists against typos or
unsupported values reaching the API.

## Fields that must never be requested together with ANY breakdown

Per the doc's "Limitations" section, do not request these fields when any `breakdowns`
value is present:
- `app_store_clicks`
- `newsfeed_avg_position`
- `newsfeed_clicks`
- `relevance_score`
- `newsfeed_impressions`

None of these are in our handler's `METRIC_COLUMNS`/`DIMENSION_COLUMNS`/`RAW_NESTED_COLUMNS`
today, so this is not currently exploitable, but it should be guarded if any of these
fields are ever added.

## Off-Meta action metrics restrictions

**Type 1 breakdowns** (unavailable for off-Meta action metrics): `region`, `dma`,
`hourly_stats_aggregated_by_audience_time_zone`, `hourly_stats_aggregated_by_advertiser_time_zone`.
- API will not return unsupported off-Meta metrics when combined with these.

**Type 2 breakdowns** (unavailable for off-Meta action metrics): `action_device`,
`action_destination`, `action_target_id`, `product_id`,
`action_carousel_card_id`/`action_carousel_card_name`, `action_canvas_component_name`.
- Off-Meta web metrics are returned without a breakdown value; off-Meta mobile metrics are
  not returned at all.
- Breakdowns remain supported for on-Meta metrics (impressions, link clicks).
- Historical data prior to April 27, 2021 is unaffected.

Action metrics are also unavailable when aggregating across multiple attribution settings,
or when requested with the impacted breakdowns above (off-Meta and action types only) —
**except** when `action_attribution_windows=1d_click,7d_click,1d_view,incrementality` is
set.

## Valid combinations

> "Due to storage constraints, only specific permutations are available." Asterisks (\*)
> in the source table mark permutations additionally joinable with `action_type`,
> `action_target_id`, and `action_destination` (name for `action_target_id`).

Documented valid permutations (quoted verbatim from the source table):

| Permutation |
| --- |
| `action_converted_product_id` — Limited availability for Collaborative Ads |
| `action_type` \* |
| `action_type, action_converted_product_id` — Limited availability for Collaborative Ads |
| `action_target_id` \* |
| `action_device` \* |
| `action_device, impression_device` \* |
| `action_device, publisher_platform` \* |
| `action_device, publisher_platform, impression_device` \* |
| `action_device, publisher_platform, platform_position` \* |
| `action_device, publisher_platform, platform_position, impression_device` \* |
| `action_reaction` |
| `action_type, action_reaction` |
| `age` \* |
| `gender` \* |
| `age, gender` \* |
| `app_id, skan_conversion_id` |
| `country` \* |
| `region` \* |
| `publisher_platform` \* |
| `publisher_platform, impression_device` \* |
| `publisher_platform, platform_position` \* |
| `publisher_platform, platform_position, impression_device` \* |
| `product_id` \* |
| `hourly_stats_aggregated_by_advertiser_time_zone` \* |
| `hourly_stats_aggregated_by_audience_time_zone` \* |
| `action_carousel_card_id / action_carousel_card_name` |
| `action_carousel_card_id / action_carousel_card_name, impression_device` |
| `action_carousel_card_id / action_carousel_card_name, country` |
| `action_carousel_card_id / action_carousel_card_name, age` |
| `action_carousel_card_id / action_carousel_card_name, gender` |
| `action_carousel_card_id / action_carousel_card_name, age, gender` |

**This is an allow-list, not a "combine anything not forbidden" rule.** Notably absent
from this list, of the 8 breakdowns our handler allows:
- `device_platform` does not appear as a standalone permutation row anywhere in this table.
- `impression_device` never appears alone — only combined with `publisher_platform`
  (optionally with `platform_position`).
- No row combines `age`/`gender` with `country` or `region`, or `country` with `region`,
  or any of {age, gender, country, region} with {publisher_platform, platform_position,
  impression_device, device_platform}.

Additional combining limitations (quoted/paraphrased from the doc):
- `video_*` fields cannot be requested with hourly stats breakdowns.
- `video_avg_time_watched_actions` cannot be requested with the `region` breakdown.
- `action_type` is implicitly added as `action_breakdowns` when the parameter is not
  specified.

## Interactions and caveats

- **Hourly breakdowns** (`hourly_stats_aggregated_by_advertiser_time_zone`,
  `hourly_stats_aggregated_by_audience_time_zone`): do not support unique fields
  (`unique_*`), `reach`, or `frequency` — when used, `reach` and `frequency` return `0`
  rather than erroring. This is a silent-wrong-data risk, not a hard rejection.
- **`dma`**: unavailable for `estimated_ad_recall_rate` or `video_thruplay_watched_actions`;
  uses sampling, so low-volume regions may not appear or may be scaled to a power of 2.
  The doc recommends querying corresponding impressions for enhanced accuracy.
- **`frequency_value`**: intended for use with `reach` only (frequency per unique user).
- **Video action metrics** (`video_p25_watched_actions`, `video_p50_watched_actions`,
  `video_p75_watched_actions`, `video_p95_watched_actions`, `video_p100_watched_actions`):
  don't support the `region` breakdown.
- **Dynamic Creative asset breakdowns** (`ad_format_asset`, `body_asset`,
  `call_to_action_asset`, `description_asset`, `image_asset`, `link_url_asset`,
  `title_asset`, `video_asset`) only support this restricted metric set: `impressions`,
  `clicks`, `spend`, `reach`, `actions`, `action_values`. `image_asset` and `video_asset`
  are additionally unavailable at the ad-account level for Dynamic Creative assets.
- **Temporary availability notices (effective August 6, 2026)**: `frequency_value`,
  `hourly_stats_aggregated_by_audience_time_zone`, and `impression_device` (standalone and
  combined) "may be unavailable for some accounts" — the doc's workaround is to enable via
  Ads Manager or use asynchronous report jobs. This directly affects one of our 8 currently
  allowed breakdowns (`impression_device`).
- No API-version gating for any of the above was stated in this page beyond the general
  "Marketing API v25.0+" framing of the whole doc and the async-source error-code caveat
  noted in error-codes.md.

## Related documentation referenced by this page
- https://developers.facebook.com/docs/graph-api/reference/adgroup/insights
- https://developers.facebook.com/docs/marketing-api/asset-feed
- https://developers.facebook.com/docs/marketing-api/reference/ads-action-stats
- https://developers.facebook.com/documentation/ads-commerce/marketing-api/out-of-cycle-changes/occ-2026#may-8--2026
