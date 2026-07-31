# Meta Ads Handler

Read-only MindsDB handler for the Meta Marketing API (Graph API `v25.0` by default). Registers
six tables under one connection. This document is written for an LLM agent generating SQL
against this connector. Read it in full before writing a query.

## 1. What this connector is

- Read-only. No `INSERT`/`UPDATE`/`DELETE`/`CREATE` support.
- Talks to `https://graph.facebook.com/{api_version}`.
- Six tables: `campaigns`, `ad_sets`, `ads`, `ad_creatives`, `insights`, `account`.
- Class type `api` — see section 3 for what this means for query execution.

### Connection args (`connection_args.py`)

| Arg | Required | Notes |
|---|---|---|
| `ad_account_id` | yes | Numeric Meta ad account id **without** the `act_` prefix (e.g. `1234567890`). The handler prefixes `act_` itself internally. If you pass `act_1234567890` the handler strips the prefix before using it, but do not rely on that — always configure it without the prefix. |
| `access_token` | yes | Long-lived Meta Marketing API user or system-user access token. Secret. |
| `api_version` | no | Defaults to `v25.0`. A bare version number (e.g. `25.0`) is auto-prefixed with `v`. |
| `client_id` | no | Meta app id. Informational only — never sent on requests. |
| `client_secret` | no | Meta app secret. If provided, used to compute `appsecret_proof`, which is attached to every request automatically. Secret. |

## 2. Tables and columns

Every column below is copied verbatim from the corresponding `tables/*.py` `COLUMNS` list.
"JSON string" means the column is a JSON-encoded object/array — use DuckDB JSON functions to
read inside it. "Comma-joined string" means a list of scalar values joined with `,` — use
`LIKE '%value%'` to match one element.

### `campaigns` (`tables/campaigns.py`)

| Column | Encoding |
|---|---|
| `id` | plain |
| `name` | plain |
| `objective` | plain |
| `status` | plain |
| `effective_status` | plain |
| `buying_type` | plain |
| `bid_strategy` | plain |
| `daily_budget` | numeric (minor units — see section 6) |
| `lifetime_budget` | numeric (minor units) |
| `budget_remaining` | numeric (minor units) |
| `spend_cap` | numeric (minor units) |
| `special_ad_categories` | comma-joined string |
| `start_time` | plain |
| `stop_time` | plain |
| `created_time` | plain |
| `updated_time` | plain |
| `configured_status` | plain |
| `account_id` | plain |
| `promoted_object` | JSON string |
| `issues_info` | JSON string |
| `special_ad_category_country` | comma-joined string |
| `source_campaign_id` | plain (string; `"0"` sentinel — see section 6) |
| `pacing_type` | comma-joined string |
| `topline_id` | plain (string; may be `"0"`) |
| `adlabels` | JSON string |
| `primary_attribution` | plain |

### `ad_sets` (`tables/ad_sets.py`)

| Column | Encoding |
|---|---|
| `id` | plain |
| `name` | plain |
| `campaign_id` | plain |
| `status` | plain |
| `effective_status` | plain |
| `optimization_goal` | plain |
| `billing_event` | plain |
| `bid_strategy` | plain |
| `bid_amount` | numeric (minor units) |
| `daily_budget` | numeric (minor units) |
| `lifetime_budget` | numeric (minor units) |
| `budget_remaining` | numeric (minor units) |
| `destination_type` | plain |
| `start_time` | plain |
| `end_time` | plain |
| `created_time` | plain |
| `updated_time` | plain |
| `targeting` | JSON string (full targeting spec — source of truth) |
| `promoted_object` | JSON string |
| `configured_status` | plain |
| `attribution_spec` | JSON string |
| `learning_stage_info` | JSON string |
| `issues_info` | JSON string |
| `daily_min_spend_target` | numeric (minor units) |
| `daily_spend_cap` | numeric (minor units) |
| `lifetime_min_spend_target` | numeric (minor units) |
| `lifetime_spend_cap` | numeric (minor units) |
| `frequency_control_specs` | JSON string |
| `source_adset_id` | plain (string; `"0"` sentinel — see section 6) |
| `dsa_payor` | plain |
| `dsa_beneficiary` | plain |
| `age_min` | numeric — flattened from `targeting.age_min`. **Confirmed working in live testing.** |
| `age_max` | numeric — flattened from `targeting.age_max`. **Confirmed working in live testing.** |
| `genders` | comma-joined string — flattened from `targeting.genders`. **Never observed populated live; may always be empty. Use `targeting` JSON instead if this is empty.** |
| `publisher_platforms` | comma-joined string — flattened from `targeting.publisher_platforms`. Same caveat as `genders`. |
| `device_platforms` | comma-joined string — flattened from `targeting.device_platforms`. Same caveat as `genders`. |
| `facebook_positions` | comma-joined string — flattened from `targeting.facebook_positions`. Same caveat as `genders`. |
| `custom_audiences` | JSON string — flattened from `targeting.custom_audiences` (list of `{id, name}` objects). Same caveat as `genders`. |
| `excluded_custom_audiences` | JSON string — flattened from `targeting.excluded_custom_audiences`. Same caveat as `genders`. |

`geo_locations`, `flexible_spec`, and `exclusions` are **not** flattened into their own columns
(the raw `targeting` JSON is the only place to read them — `flexible_spec` is a boolean
expression tree that cannot be safely flattened).

### `ads` (`tables/ads.py`)

| Column | Encoding |
|---|---|
| `id` | plain |
| `name` | plain |
| `adset_id` | plain |
| `campaign_id` | plain |
| `status` | plain |
| `effective_status` | plain |
| `bid_amount` | numeric (minor units) |
| `creative_id` | plain — flattened from the nested `creative{id}` field |
| `preview_shareable_link` | plain |
| `created_time` | plain |
| `updated_time` | plain |
| `configured_status` | plain |
| `targeting` | JSON string |
| `issues_info` | JSON string |
| `ad_review_feedback` | JSON string |
| `recommendations` | JSON string |
| `conversion_specs` | JSON string |
| `tracking_specs` | JSON string |
| `source_ad_id` | plain (string; `"0"` sentinel — see section 6) |
| `adlabels` | JSON string |

**`ads` has no `special_ad_categories` column.** Meta gates that field behind an app-whitelist
review (`(#3) App must be on whitelist`); requesting it 400s the entire `ads` table because
`ads` fetches one combined `fields=` string. Do not add it to a query against `ads`.
`campaigns.special_ad_categories` is unaffected and works.

### `ad_creatives` (`tables/ad_creatives.py`)

| Column | Encoding |
|---|---|
| `id` | plain |
| `name` | plain |
| `status` | plain |
| `title` | plain — derived, see below |
| `body` | plain — derived |
| `description` | plain — derived |
| `link_url` | plain — derived |
| `image_url` | plain |
| `thumbnail_url` | plain |
| `video_id` | plain |
| `call_to_action_type` | plain — derived |
| `object_type` | plain |
| `effective_object_story_id` | plain |
| `object_story_spec` | JSON string |
| `asset_feed_spec` | JSON string |
| `instagram_permalink_url` | plain |
| `product_set_id` | plain |
| `template_url_spec` | JSON string |
| `platform_customizations` | JSON string |
| `image_crops` | JSON string |
| `degrees_of_freedom_spec` | JSON string |
| `authorization_category` | plain |
| `effective_authorization_category` | plain |

`title`/`body`/`description`/`link_url`/`call_to_action_type` are populated with a fallback
chain: the top-level Graph field first, then `object_story_spec.link_data` /
`.video_data` / `.photo_data` / `.template_data`, then (if still empty) the first entry of the
matching `asset_feed_spec` list. `photo_data` never contributes a title or link, only
`body`/`description` via `caption`. The raw `object_story_spec`/`asset_feed_spec` JSON is the
source of truth if you need anything not in this fallback chain.

### `insights` (`tables/insights.py`)

Dimensions:

| Column |
|---|
| `date_start` |
| `date_stop` |
| `account_id` |
| `account_name` |
| `campaign_id` |
| `campaign_name` |
| `adset_id` |
| `adset_name` |
| `ad_id` |
| `ad_name` |
| `objective` |
| `buying_type` |

Metrics (all numeric):

| Column |
|---|
| `impressions` |
| `reach` |
| `frequency` |
| `clicks` |
| `unique_clicks` |
| `spend` |
| `cpc` |
| `cpm` |
| `cpp` |
| `ctr` |
| `unique_ctr` |
| `inline_link_clicks` |
| `inline_link_click_ctr` |
| `cost_per_inline_link_click` |

Raw nested columns (JSON strings — Meta's `actions`/`action_values`/etc. arrays):

| Column |
|---|
| `actions` |
| `action_values` |
| `cost_per_action_type` |
| `purchase_roas` |

Derived columns (computed client-side from `actions`/`action_values`, all numeric):

| Column | Computed from |
|---|---|
| `link_clicks` | `actions` where `action_type = 'link_click'` |
| `landing_page_views` | `actions` where `action_type = 'landing_page_view'` |
| `leads` | `actions` where `action_type = 'lead'` |
| `post_engagements` | `actions` where `action_type = 'post_engagement'` |
| `video_views` | `actions` where `action_type = 'video_view'` |
| `purchases` | `actions` where `action_type` is `omni_purchase` or `purchase` (first match wins) |
| `purchase_value` | `action_values`, same `omni_purchase`/`purchase` matching |
| `roas` | `purchase_value / spend` (0 if `spend` is 0 or missing) |

Breakdown columns (only populated when requested — see section 4; full 38-value list from
`BREAKDOWN_COLUMNS`):

```
age, gender, country, region, publisher_platform, platform_position, impression_device,
device_platform, dma, comscore_market, hourly_stats_aggregated_by_advertiser_time_zone,
hourly_stats_aggregated_by_audience_time_zone, frequency_value, product_id, app_id,
skan_campaign_id, skan_conversion_id, is_conversion_id_modeled, user_segment_key,
place_page_id, ad_format_asset, body_asset, call_to_action_asset, description_asset,
image_asset, link_url_asset, title_asset, video_asset, action_device, action_destination,
action_target_id, action_type, action_reaction, action_converted_product_id,
action_carousel_card_id,
action_carousel_card_name, action_canvas_component_name, action_video_sound,
action_video_type
```

`dma` is in this list but the live API now rejects it (`"(#100) dma breakdown is no longer
supported ... use comscore_market breakdown"`). `comscore_market` is the replacement and is
confirmed working live.

### `account` (`tables/account.py`)

Single-row table — always returns exactly one row. A fetch failure raises rather than
returning an empty result.

| Column | Encoding |
|---|---|
| `id` | plain |
| `account_id` | plain |
| `name` | plain |
| `account_status` | numeric (raw code — see `account_status_label`) |
| `currency` | plain — **check this before interpreting any money column anywhere in this connector** |
| `timezone_name` | plain |
| `timezone_offset_hours_utc` | numeric |
| `business_name` | plain |
| `amount_spent` | numeric (minor units) |
| `spend_cap` | numeric (minor units) |
| `funding_source` | plain |
| `created_time` | plain |
| `balance` | numeric (arrives as a numeric string over the wire, e.g. `"23799"`; already coerced to numeric here) |
| `disable_reason` | numeric (raw code — see `disable_reason_label`) |
| `min_daily_budget` | numeric |
| `opportunity_score` | numeric |
| `capabilities` | JSON string |
| `end_advertiser_name` | plain |
| `timezone_id` | numeric |
| `age` | numeric (arrives as a float, e.g. `1981.2596643519`) |
| `is_prepay_account` | plain |
| `tax_id_status` | numeric (raw code — see `tax_id_status_label`) |
| `account_status_label` | plain — derived, human-readable label for `account_status` (e.g. `ACTIVE`, `DISABLED`, `PENDING_RISK_REVIEW`; `None` if the code is unmapped) |
| `tax_id_status_label` | plain — derived label for `tax_id_status` (e.g. `REQUIRED`, `SUBMITTED`) |
| `disable_reason_label` | plain — derived label for `disable_reason`. Only `0` (`NONE`) and `15` (`COMPROMISED_AD_ACCOUNT`) are mapped; every other code resolves to `None` (undocumented gap, not a bug) |

## 3. How querying actually works

`meta_ads` is registered as `class_type = "api"`. MindsDB's planner splits every `SELECT`
against it into two steps:

1. **Handler fetch** — the handler's `list()` method (via `APIResource.select()`) calls the
   Graph API and returns a raw `DataFrame` with the columns above.
2. **DuckDB pass** — DuckDB runs your *entire original SQL* on top of that DataFrame.

**Consequence**: `CASE WHEN`, `SUM`/`AVG`/other aggregates, `GROUP BY`, `ORDER BY`, arithmetic,
`JOIN`, and window functions are **not implemented by this handler** — DuckDB does all of it
after the fetch. Write ordinary SQL; do not avoid these constructs out of a belief the handler
must understand them.

One planner-specific caveat for `insights`: when your query has `GROUP BY` or any non-plain
target (an aggregate, `CASE`, arithmetic, etc.), the SQL `LIMIT` is **not** enforced at the API
level — the handler fetches the full unfiltered row set so the aggregation is correct, and
the effective row limit on the aggregated result is whatever DuckDB does with `LIMIT`
downstream. This is a planner-level characteristic, not something a query can opt out of.

## 4. The `insights` table — WHERE pseudo-columns

`insights` accepts several `WHERE` conditions that are **not real columns**. The handler reads
them off the parsed condition list, converts them into Graph API parameters, and removes them
from the DataFrame filtering step. They must be plain **`=` equality** (or `IN (...)` where
noted) — the handler looks them up by exact column name and operator; `>`, `<`, `LIKE`,
`!=`, or wrapping them in a function will NOT be recognized as a parameter and will instead be
evaluated (probably uselessly, since these aren't real columns) by DuckDB after the fetch.

| Pseudo-column | Operator | Effect |
|---|---|---|
| `level` | `=` | One of `'account'`, `'campaign'`, `'adset'`, `'ad'`. Default: `'campaign'`. Anything else raises `ValueError`. |
| `start_date` | `=` | `'YYYY-MM-DD'`. Combined with `end_date` (or defaulted) into Graph `time_range = {since, until}`. |
| `end_date` | `=` | `'YYYY-MM-DD'`. Same as above. |
| `date_preset` | `=` | Mutually exclusive with `start_date`/`end_date`. **If both are given, the explicit `start_date`/`end_date` range wins** and a warning is logged server-side — the query still runs, just with the range you gave, silently discarding `date_preset`. |
| `time_increment` | `=` | Int (as a string or number), or `'all_days'` / `'monthly'`. |
| `breakdowns` | `=` or `IN (...)` | One or more values from the 37-value list in section 2. Comma-joined single string values are also split (e.g. `breakdowns = 'age,gender'`). |
| `action_breakdowns` | `=` or `IN (...)` | Passed through unvalidated — Meta's docs never exhaustively enumerate legal values here. |
| `campaign_id` | `=` or `IN (...)` | Pushed to Graph as a `filtering` entry on `campaign.id`. |
| `adset_id` | `=` or `IN (...)` | Pushed to Graph as a `filtering` entry on `adset.id`. |
| `ad_id` | `=` or `IN (...)` | Pushed to Graph as a `filtering` entry on `ad.id`. |

**Date defaulting**: if neither `start_date`/`end_date` nor `date_preset` is given, the query
defaults to the last 30 days ending today (UTC).

**`time_increment` defaulting**: if not given explicitly, it defaults to `1` (daily rows) when
`date_start` or `date_stop` is among the fields being fetched, otherwise `'all_days'` (a single
aggregated row for the whole range). If you want a daily time series, either select `date_start`
or set `time_increment = 1` explicitly.

**Breakdown auto-inclusion**: if you `SELECT` a column that is itself a breakdown name (e.g.
`SELECT age, impressions FROM insights`) without also putting it in `WHERE breakdowns = ...`,
the handler adds it to the breakdowns request automatically. You do not need to set both.

**Invalid breakdown values** (typo, not in the 37-value list) raise a client-side `ValueError`
immediately, before any API call.

**Invalid breakdown *combinations*** (a value combination outside Meta's documented allow-list)
are **not** blocked client-side — a warning is logged and the combination is sent to the API
anyway, because the documented permutation table is known to be incomplete. If Meta rejects the
combination, the error message includes the specific breakdowns you asked for and, when
possible, a list of documented combinations that do include at least one of them.

**Hourly breakdowns** (`hourly_stats_aggregated_by_advertiser_time_zone`,
`hourly_stats_aggregated_by_audience_time_zone`) silently return `0` for `reach`, `frequency`,
and any `unique_*` metric instead of erroring, when combined with those breakdowns. This is
Meta's documented behavior, not a connector bug — a warning is logged but the query still
returns the (zeroed) data.

## 5. Filtering: what is pushed to the API vs applied locally

Be precise here: only what is listed below is pushed to the Graph API as a real API-level
filter/parameter. Everything else in your `WHERE` clause is applied by DuckDB **after** the
full fetch — the results are still correct, but the cost is data volume (and rate-limit
exposure on large accounts).

| Table | Pushed to API | Not pushed (fetched then filtered locally) |
|---|---|---|
| `campaigns` | `id` (`=`/`IN` — fetches those campaigns individually by id, page-limited), `effective_status` (`=`/`IN`), `status` (`=`, via a Graph `filtering` entry) | everything else |
| `ad_sets` | `id` (`=`/`IN`, per-id fetch), `campaign_id` (`=`, scopes the listing edge to `{campaign_id}/adsets`), `effective_status` (`=`/`IN`) | everything else |
| `ads` | `id` (`=`/`IN`, per-id fetch), `adset_id` (`=`, scopes the listing edge to `{adset_id}/ads`), `campaign_id` (`=`, scopes to `{campaign_id}/ads`, only when `adset_id` is not also given), `effective_status` (`=`/`IN`) | everything else |
| `ad_creatives` | `id` (`=`/`IN`, per-id fetch) | everything else |
| `insights` | see section 4 pseudo-columns | anything not in that table |
| `account` | n/a (single-row fetch, no filtering) | n/a |

**General filter pushdown is not implemented yet** (e.g. `WHERE name LIKE '%Q4%'` on
`campaigns` will always be fetched-then-filtered, never sent to Graph as a native filter).
This is a known, deliberate Phase 2 gap, not an oversight to work around client-side.

On `ads`/`ad_sets`, note that when `id` is given as `=`/`IN`, that overrides the
`campaign_id`/`adset_id` scoping path entirely — the handler fetches each id individually and
`campaign_id`/`adset_id` are then treated as ordinary post-fetch filters instead.

## 6. Gotchas that produce wrong answers

- **`"0"` sentinel, not `NULL`.** `campaigns.source_campaign_id`, `ad_sets.source_adset_id`,
  and `ads.source_ad_id` return the literal string `"0"` when the field does not apply — never
  `NULL`. `WHERE source_ad_id IS NOT NULL` matches every single row. Use
  `WHERE source_ad_id != '0'` instead. (Measured live across all three tables —
  `tasks/meta-ads-phase0-measurements.md`.)

- **Money columns are in minor units, and the connector does no scaling.** All budget/spend
  columns (`daily_budget`, `lifetime_budget`, `budget_remaining`, `spend_cap`, `bid_amount`,
  `daily_min_spend_target`, `daily_spend_cap`, `lifetime_min_spend_target`,
  `lifetime_spend_cap`, `amount_spent`, `spend` in `insights`) come back from Graph in minor
  units. For most currencies the offset is 100 (divide by 100 to get base units — e.g. cents
  to dollars). For **CLP, COP, CRC, HUF, ISK, IDR, JPY, KRW, PYG, TWD, VND** the offset is 1
  (the value already is the base unit — do not divide). Query `account.currency` first and
  apply the correct divisor yourself in SQL; the handler never scales these values.

- **There is no `date_range` parameter, and top-level `date_preset`/`time_range` do not
  filter rows on `campaigns`/`ad_sets`/`ads`.** Date-scoping only works through the
  `insights` table's `start_date`/`end_date`/`date_preset` pseudo-columns (section 4). Do not
  try to date-filter `campaigns`/`ad_sets`/`ads` directly — use a `JOIN` against `insights`
  instead (see worked example 3).

  Measured live: sending `date_range=this_month` to the `/ads` edge returned all 25 rows
  spanning 2023-2026, i.e. it filtered nothing. Sending `date_range=TOTAL_GARBAGE_XYZ`
  returned HTTP 200 and byte-identical rows — the Graph API **silently ignores query
  parameters it does not recognise**, so an invented parameter name fails completely
  silently rather than erroring. Never infer from a 200 response that a parameter was
  honoured. A parameter the API genuinely understands rejects bad input: compare
  `insights.date_preset(GARBAGE)`, which returns `(#100) date_preset must be one of the
  following values: today, yesterday, this_month, ...`.

  To scope the nested insights edge in a raw Graph call (not needed via SQL, but useful when
  debugging), the date parameter attaches to the *field*, not the query string:
  `fields=id,insights.date_preset(this_month){spend,date_start,date_stop}`. Measured: this
  changes both the returned spend and the `date_start`/`date_stop` window; a top-level
  `date_preset` does not.

- **`sort` does not work** on any listing edge — measured live: even an invalid sort value
  returns `200 OK` with unchanged row order, so there is no observable effect and no rejection
  to learn a valid enum from either. Do not attempt to push ordering into the API. Use SQL
  `ORDER BY`; DuckDB applies it correctly after the fetch.

- **JSON columns are strings, not structs.** `targeting`, `object_story_spec`,
  `asset_feed_spec`, `promoted_object`, `issues_info`, `adlabels`, `actions`, `action_values`,
  `cost_per_action_type`, `purchase_roas`, `capabilities`, and the other `JSON string` columns
  listed in section 2 are JSON-encoded text, not DuckDB structs. `WHERE targeting = '...'`
  compares the raw serialized string. Use DuckDB JSON functions (`json_extract`, `->`, `->>`)
  to read inside them — see worked example 6.

- **Comma-joined columns need `LIKE`, not `=`, for multi-value matching.**
  `special_ad_categories`, `special_ad_category_country`, `pacing_type`, `genders`,
  `publisher_platforms`, `device_platforms`, `facebook_positions` are comma-joined strings.
  `WHERE special_ad_categories = 'HOUSING'` only matches a row whose *entire* value is exactly
  `HOUSING`. Use `WHERE special_ad_categories LIKE '%HOUSING%'` to match one value among several.

- **`ad_sets`' flattened targeting columns beyond `age_min`/`age_max` may always be empty.**
  `genders`, `publisher_platforms`, `device_platforms`, `facebook_positions`,
  `custom_audiences`, `excluded_custom_audiences` were never observed populated in live
  testing (10 real ad sets scanned) — this could mean the account simply doesn't set those
  targeting dimensions, or that the flatten logic has a gap; it has not been confirmed either
  way. `age_min`/`age_max` **are** confirmed working. If a query needs one of the unconfirmed
  columns and gets nothing back, fall back to parsing the raw `targeting` JSON column, which is
  always the source of truth.

- **`ads` has no `special_ad_categories` column — do not add it to a query against `ads`.**
  It is whitelist-gated by Meta (`(#3) App must be on whitelist`) and requesting it would 400
  the entire `ads` table, since `ads` requests one combined `fields=` string per query.
  `campaigns.special_ad_categories` exists and works — use `JOIN` against `campaigns` if you
  need both.

## 7. Worked examples

Replace `<your_meta_ads_db>` with your configured connection name.

**1. Total spend by campaign for the current month**

```sql
SELECT campaign_name, SUM(spend) AS total_spend
FROM <your_meta_ads_db>.insights
WHERE start_date = '2026-07-01' AND end_date = '2026-07-30'
GROUP BY campaign_name;
```

**2. Daily time series (one row per day) for a single campaign**

```sql
SELECT date_start, impressions, clicks, spend
FROM <your_meta_ads_db>.insights
WHERE campaign_id = '<real_campaign_id>'
  AND start_date = '2026-07-01'
  AND end_date = '2026-07-30'
ORDER BY date_start;
```

**3. Joining insights to campaign metadata (objective isn't in `insights`' dimension list at the
adset/ad level, so pull it from `campaigns` instead)**

```sql
SELECT i.campaign_name, c.objective, i.spend
FROM <your_meta_ads_db>.insights AS i
JOIN <your_meta_ads_db>.campaigns AS c
  ON i.campaign_id = c.id
WHERE i.start_date = '2026-07-01' AND i.end_date = '2026-07-30';
```

**4. Filtering ads by status, pushed to the API**

```sql
SELECT id, name, effective_status
FROM <your_meta_ads_db>.ads
WHERE effective_status IN ('ACTIVE', 'PAUSED');
```

**5. Reading inside a JSON column**

```sql
SELECT id, name,
       json_extract_string(targeting, '$.geo_locations.countries[0]') AS first_country
FROM <your_meta_ads_db>.ad_sets
WHERE campaign_id = '<real_campaign_id>';
```

> `<real_campaign_id>` must be an id that actually exists. When you filter `ad_sets` by
> `campaign_id`, the handler fetches `GET <campaign_id>/adsets` rather than the account-wide
> edge, so a non-existent id fails with `(#100) Tried accessing nonexisting field (adsets)`
> instead of returning an empty result. The same filter on `insights` returns 0 rows silently.
> Verified live.

**6. Aggregation with `CASE WHEN` — DuckDB does the computation, not the handler**

```sql
SELECT
  campaign_name,
  SUM(CASE WHEN spend > 0 THEN spend ELSE 0 END) AS spend_total,
  SUM(purchases) AS purchases_total,
  SUM(purchase_value) / NULLIF(SUM(spend), 0) AS blended_roas
FROM <your_meta_ads_db>.insights
WHERE start_date = '2026-07-01' AND end_date = '2026-07-30'
GROUP BY campaign_name
ORDER BY spend_total DESC;
```

**7. Breakdown by age and gender**

```sql
SELECT age, gender, impressions, spend
FROM <your_meta_ads_db>.insights
WHERE start_date = '2026-07-01'
  AND end_date = '2026-07-30'
  AND breakdowns IN ('age', 'gender');
```

**8. Converting minor-unit spend to base currency units (check `account.currency` first)**

```sql
SELECT campaign_name, spend / 100.0 AS spend_in_currency_base_units
FROM <your_meta_ads_db>.insights
WHERE start_date = '2026-07-01' AND end_date = '2026-07-30';
-- Divide by 1 instead of 100 if account.currency is one of:
-- CLP, COP, CRC, HUF, ISK, IDR, JPY, KRW, PYG, TWD, VND
```

## 8. Errors you may hit

| Situation | What happens | What to change in your SQL |
|---|---|---|
| Rate limiting (`error.code` `80004`, "too many calls to this ad-account") | Returned as an HTTP **400**, not a 429 (measured live: `Meta Ads API error (400): There have been too many calls to this ad-account ... code=80004 error_subcode=2446079`). Because it arrives as a 400, generic "retry on 429" logic will never fire for it. **Not retried** by this connector by design — it decays over roughly an hour, so retrying immediately just burns another call against the same limit. | Wait, then re-run the query. Reduce query frequency/concurrency against the same ad account. Don't loop retries in application code. |
| Large/oversized request (`insights` only) | On `insights`, a synchronous fetch that fails with Meta's "reduce the amount of data" signal (or the matching documented `(code, error_subcode)` pairs) automatically falls back to Meta's async report flow (`POST .../insights` → poll `report_run_id` → paginate the report). This can take noticeably longer than a normal query — up to 5 minutes before it times out. | Narrow the date range, drop a breakdown, or reduce `time_increment` granularity if you don't need the fallback's extra latency. |
| Large/oversized request (`ads`/`ad_sets` only) | These tables always request several large/nested fields (`targeting`, `recommendations`, `issues_info`, etc.). If Graph rejects the default page size, the connector automatically halves the page size and retries the same page (down to a floor), rather than failing the query. | No SQL change needed — this is transparent. If you see it happen repeatedly, a narrower `WHERE campaign_id = ...`/`adset_id = ...` scope will reduce how much data any one page has to carry. |
| Invalid `breakdowns` value (typo) | `ValueError` raised client-side before any API call, listing the valid 37 values. | Fix the value against the list in section 2. |
| Invalid `breakdowns` *combination* | The API rejects it (commonly a generic `(#100)` `OAuthException`, sometimes the documented `(2, 1504041)` "Invalid Breakdowns" pair). The connector's error message repeats your requested breakdowns and, when possible, lists documented combinations that include at least one of them. | Pick one of the suggested combinations, or drop to a single breakdown. |
| Invalid `level` value on `insights` | `ValueError` raised client-side immediately. | Use one of `'account'`, `'campaign'`, `'adset'`, `'ad'`. |
| `dma` breakdown | The API rejects it live: `"(#100) dma breakdown is no longer supported ... use comscore_market breakdown"`. | Use `comscore_market` instead. |
| Requesting `special_ad_categories` on `ads` | `(#3) App must be on whitelist` — 400s the whole `ads` table, since `ads` fetches all its columns in one request regardless of what you `SELECT`. | Don't reference `special_ad_categories` in any query against `ads`. Use `campaigns.special_ad_categories` if you need it, joined by `campaign_id`. |
| Generic transient errors (HTTP 429, HTTP 5xx, or documented retryable Insights `(code, error_subcode)` pairs) | Retried automatically with exponential backoff (up to 3 attempts, capped at 30s between attempts). | No SQL change needed. If it still fails after retries, the underlying Graph API error message is surfaced as-is — read it, it names the real cause. |
