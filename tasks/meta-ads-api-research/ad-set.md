# Meta Marketing API — Ad Set (`ad-campaign` node) reference extract

Source: https://developers.facebook.com/documentation/ads-commerce/marketing-api/reference/ad-campaign.md
(confirmed: this page documents the **Ad Set**; Meta's internal node name for it is `ad-campaign`.
The **Campaign** node is a separate object, `ad-campaign-group`, not covered here.)

Supplementary sources fetched:
- `.../reference/ad-account/adsets.md` (adsets edge read parameters)
- `https://developers.facebook.com/docs/marketing-api/targeting-specs` (targeting spec fields — `.md` variant of this URL 404'd, fell back to non-`.md`)
- `https://developers.facebook.com/docs/graph-api/using-graph-api` (attempted for `filtering` operator docs — see Gotchas)

---

## Reading fields

Every readable field documented on the ad-campaign.md page. ✅ = already exposed by our handler.

| Field | Type | Meaning |
|---|---|---|
| ✅ `id` | numeric string | ID for the Ad Set |
| `account_id` | numeric string | ID for the Ad Account associated with this Ad Set |
| `adlabels` | list<AdLabel> | Ad Labels associated with this ad set |
| `adset_schedule` | list<DayPart> | Delivery schedule for a single day |
| `asset_feed_id` | numeric string | ID of the asset feed that contains content to create ads |
| `attribution_spec` | list<AttributionSpec> | Conversion attribution spec used for attributing conversions for optimization |
| `bid_adjustments` | AdBidAdjustments | Map of bid adjustment types to values |
| `bid_amount` | unsigned int32 | Bid cap or target cost for this ad set based on `optimization_goal` |
| ✅ `bid_amount`* | | *(listed once; see above — handler already exposes this)* |
| `bid_constraints` | AdCampaignBidConstraint | Bid constraints for the ad set |
| `bid_info` | map<string, unsigned int32> | Map of bid objective to bid value |
| ✅ `bid_strategy` | enum | Bid strategy for this ad set when using AUCTION as buying type |
| ✅ `billing_event` | enum | The billing event for this ad set |
| `brand_safety_config` | BrandSafetyCampaignConfig | Brand safety configuration |
| ✅ `budget_remaining` | numeric string | Remaining budget of this Ad Set |
| `campaign` | Campaign | The campaign that contains this ad set (object, vs. `campaign_id`) |
| `campaign_active_time` | numeric string | Campaign running length |
| `campaign_attribution` | enum | App-ads attribution type indicator |
| ✅ `campaign_id` | numeric string | The ID of the campaign that contains this ad set |
| `configured_status` | enum | The status set at the ad set level (can differ from `effective_status` due to parent campaign) |
| `contextual_bundling_spec` | ContextualBundlingSpec | Specs of contextual bundling ad set setup |
| ✅ `created_time` | datetime | Time when this Ad Set was created |
| `creative_sequence` | list<numeric string> | Order of the adgroup sequence shown to users |
| ✅ `daily_budget` | numeric string | Daily budget of the set, in account currency |
| `daily_min_spend_target` | numeric string | Daily minimum spend target, in account currency |
| `daily_spend_cap` | numeric string | Daily spend cap, in account currency |
| ✅ `destination_type` | string | Destination of ads in this Ad Set |
| `dsa_beneficiary` | string | Beneficiary of all ads in this ad set (EU DSA) |
| `dsa_payor` | string | Payor of all ads in this ad set (EU DSA) |
| ✅ `effective_status` | enum | The effective status of the adset |
| ✅ `end_time` | datetime | End time, in UTC UNIX timestamp |
| `frequency_control_specs` | list<AdCampaignFrequencyControlSpecs> | Frequency control specs for this ad set (REACH/THRUPLAY objectives) |
| `instagram_user_id` | numeric string | Instagram account ID used for ads |
| `is_dynamic_creative` | bool | Whether this ad set is a dynamic creative ad set |
| `is_incremental_attribution_enabled` | bool | Whether the campaign should use incremental attribution optimization |
| `issues_info` | list<AdCampaignIssuesInfo> | Issues preventing delivery (populates with flagged custom-audience/conversion info per Sept 2025 restriction) |
| `learning_stage_info` | AdCampaignLearningStageInfo | Whether the ranking/delivery system is still learning |
| ✅ `lifetime_budget` | numeric string | Lifetime budget of the set, in account currency |
| `lifetime_imps` | int32 | Lifetime impressions for campaigns with `buying_type=FIXED_CPM` |
| `lifetime_min_spend_target` | numeric string | Lifetime minimum spend target, in account currency |
| `lifetime_spend_cap` | numeric string | Lifetime spend cap, in account currency |
| `min_budget_spend_percentage` | numeric string | Min budget spend percentage |
| `multi_optimization_goal_weight` | string | Multi optimization goal weight |
| ✅ `name` | string | Name of the ad set |
| ✅ `optimization_goal` | enum | The optimization goal this ad set is using |
| `optimization_sub_event` | string | Optimization sub event for a specific optimization goal |
| `pacing_type` | list<string> | Pacing type — standard or using ad scheduling |
| ✅ `promoted_object` | AdPromotedObject | The object this ad set is promoting across all its ads |
| `recommendations` | list<AdRecommendation> | Recommendations for this ad set, if any |
| `recurring_budget_semantics` | bool | If true, daily spend may exceed daily budget but weekly won't exceed 7x |
| `regional_regulated_categories` | list<enum> | Regional regulated categories (e.g. TAIWAN_FINSERV, INDIA_FINSERV, BRAZIL_REGULATION) |
| `regional_regulation_identities` | RegionalRegulationIdentities | Regional regulation identity declarations |
| `review_feedback` | string | Reviews for dynamic creative ad |
| `rf_prediction_id` | id | Reach and frequency prediction ID |
| `source_adset` | AdSet | The source ad set that this ad set was copied from (object) |
| `source_adset_id` | numeric string | The source ad set id that this ad set was copied from |
| ✅ `start_time` | datetime | Start time, in UTC UNIX timestamp |
| ✅ `status` | enum | The status set at the ad set level |
| ✅ `targeting` | Targeting | Targeting spec |
| `targeting_optimization_types` | list<KeyValue:string,int32> | Targeting options relaxed and used as an optimization signal |
| `time_based_ad_rotation_id_blocks` | list<list<integer>> | Ad creative displayed at custom date ranges in a campaign |
| `time_based_ad_rotation_intervals` | list<unsigned int32> | Date range when specific ad creative displays |
| ✅ `updated_time` | datetime | Time when the Ad Set was updated |
| `use_new_app_click` | bool | If set, allows Mobile App Engagement ads to optimize for LINK_CLICKS |
| `value_rule_set_id` | numeric string | Value rule set ID |

**Total documented readable fields: 61** (deduped) vs. **19** currently exposed by our handler.
Two entries above (`campaign` and `source_adset`) are object-typed "expandable" fields distinct from
the scalar `campaign_id` / `source_adset_id` we already expose — counted once each in the total.

---

## Enums

### `optimization_goal`
`NONE`, `APP_INSTALLS`, `AD_RECALL_LIFT`, `ENGAGED_USERS`, `EVENT_RESPONSES`, `IMPRESSIONS`,
`LEAD_GENERATION`, `QUALITY_LEAD`, `LINK_CLICKS`, `OFFSITE_CONVERSIONS`, `PAGE_LIKES`,
`POST_ENGAGEMENT`, `QUALITY_CALL`, `REACH`, `LANDING_PAGE_VIEWS`, `VISIT_INSTAGRAM_PROFILE`,
`ENGAGED_PAGE_VIEWS`, `VALUE`, `THRUPLAY`, `DERIVED_EVENTS`, `APP_INSTALLS_AND_OFFSITE_CONVERSIONS`,
`CONVERSATIONS`, `IN_APP_VALUE`, `MESSAGING_PURCHASE_CONVERSION`,
`MESSAGING_DEEP_CONVERSATION_AND_FOLLOW`, `SUBSCRIBERS`, `REMINDERS_SET`,
`MEANINGFUL_CALL_ATTEMPT`, `PROFILE_VISIT`, `PROFILE_AND_PAGE_ENGAGEMENT`,
`ADVERTISER_SILOED_VALUE`, `AUTOMATIC_OBJECTIVE`, `MESSAGING_APPOINTMENT_CONVERSION`

### `billing_event`
`APP_INSTALLS`, `CLICKS`, `IMPRESSIONS`, `LINK_CLICKS`, `NONE`, `OFFER_CLAIMS`, `PAGE_LIKES`,
`POST_ENGAGEMENT`, `THRUPLAY`, `PURCHASE`, `LISTING_INTERACTION`

### `bid_strategy`
`LOWEST_COST_WITHOUT_CAP`, `LOWEST_COST_WITH_BID_CAP`, `COST_CAP`, `LOWEST_COST_WITH_MIN_ROAS`

### `destination_type`
Documented as an enum (our handler's `string` typing undersells it):
`WEBSITE`, `APP`, `MESSENGER`, `APPLINKS_AUTOMATIC`, `WHATSAPP`, `INSTAGRAM_DIRECT`, `FACEBOOK`,
`MESSAGING_MESSENGER_WHATSAPP`, `MESSAGING_INSTAGRAM_DIRECT_MESSENGER`,
`MESSAGING_INSTAGRAM_DIRECT_MESSENGER_WHATSAPP`, `MESSAGING_INSTAGRAM_DIRECT_WHATSAPP`,
`SHOP_AUTOMATIC`, `ON_AD`, `ON_POST`, `ON_EVENT`, `ON_VIDEO`, `ON_PAGE`, `INSTAGRAM_PROFILE`,
`FACEBOOK_PAGE`, `INSTAGRAM_PROFILE_AND_FACEBOOK_PAGE`, `INSTAGRAM_LIVE`, `FACEBOOK_LIVE`, `IMAGINE`

### `status`
`ACTIVE`, `PAUSED`, `DELETED`, `ARCHIVED`

### `effective_status`
`ACTIVE`, `PAUSED`, `DELETED`, `CAMPAIGN_PAUSED`, `ARCHIVED`, `IN_PROCESS`, `WITH_ISSUES`
(the `adsets` edge read-parameter version of `effective_status` accepts a wider filter list —
see Read params below — which also includes `PENDING_REVIEW`, `DISAPPROVED`, `PREAPPROVED`,
`PENDING_BILLING_INFO`, `ADSET_PAUSED`)

### `configured_status`
`ACTIVE`, `PAUSED`, `DELETED`, `ARCHIVED`

---

## `targeting` shape

Fetched from the Facebook targeting-specs reference (the `.md` variant of this URL 404'd; the
non-`.md` HTML version resolved). Key fields worth flattening into their own SQL columns:

| Key | Type | Meaning |
|---|---|---|
| `geo_locations` | object | Geographic targeting by country, region, city, or zip code |
| `age_min` | integer | Minimum age for targeting (used with `age_max`) |
| `age_max` | integer | Maximum age for targeting (used with `age_min`) |
| `age_range` | array | Used with the audience-suggestions feature |
| `genders` | array<int> | 1 = male, etc. |
| `custom_audiences` | array | Audience IDs or audience objects for inclusion |
| `excluded_custom_audiences` | array | Audience IDs or audience objects for exclusion |
| `locales` | array | Locale codes; limit 50 |
| `device_platforms` | array | Target device platforms (e.g. mobile, desktop) |
| `publisher_platforms` | array | Target publisher platforms (e.g. facebook, audience_network, instagram, messenger) |
| `facebook_positions` | array | Placement positions on Facebook (e.g. feed) |
| `interests` | array | Array of objects with `id` and optional `name` |
| `behaviors` | array | Array of targeting behavior categories |
| `life_events` | array | Array of objects with `id` and optional `name` |
| `relationship_statuses` | array<int> | 1–4, 6 |
| `industries` | array | Array of objects with `id`/`name` |
| `income` | array | Array of objects with `id`/`name` |
| `family_statuses` | array | Array of objects with `id`/`name` |
| `education_schools` | array | Limit 200 |
| `education_statuses` | array<int> | Codes 1–13 |
| `college_years` | array<int> | Earliest allowed year is 1980 |
| `education_majors` | array | Limit 200 |
| `work_employers` | array | Limit 200 |
| `work_positions` | array | Limit 200 |
| `user_os` | array | OS targeting values (see Targeting Search API) |
| `user_device` | array | Device values matching `user_os` |
| `excluded_user_device` | array | Devices to exclude |
| `wireless_carrier` | array | Only documented value: `Wifi` |
| `user_adclusters` | array | ID/name pairs for BCT clusters; limit 50 |
| `targeting_automation` | object | Automated audience expansion config (`individual_setting` params) |

**Not found on the pages fetched** (present in our handler's targeting JSON blob per real-world
usage, but no field-level doc page resolved for these in this pass — do not treat as confirmed
absent from the API, only "not documented in sources checked"): `flexible_spec`, `exclusions`,
`instagram_positions`, `targeting_relaxation_types`. A dedicated `ad-set/targeting-specs` sub-page
would need to be located to close this gap.

**Recommendation for flattening**: `geo_locations` (JSON, too nested to flatten meaningfully),
`age_min`, `age_max`, `genders`, `publisher_platforms`, `device_platforms`, `facebook_positions`,
`custom_audiences`, `excluded_custom_audiences` are the highest-value flat columns — they're the
ones most commonly used in WHERE clauses for audience/placement analysis.

---

## `promoted_object` shape

| Key | Type | When it appears |
|---|---|---|
| `application_id` | int | Mobile/canvas app promotion |
| `pixel_id` | numeric string/int | Website conversion tracking |
| `page_id` | numeric string | Page-like/page-engagement objectives |
| `object_store_url` | URL | Mobile/digital store URI (single) |
| `object_store_urls` | list<URL> | Multiple store URIs |
| `offer_id` | numeric string/int | Offer from a Facebook Page |
| `custom_event_type` | enum | Standard vs. custom app events |
| `custom_event_str` | string | Custom app event name |
| `product_set_id` | numeric string/int | Product Set within a catalog |
| `product_catalog_id` | numeric string/int | Product Catalog |
| `product_item_id` | numeric string/int | Individual product item |
| `event_id` | numeric string/int | Facebook Event promotion |
| `offline_conversion_data_set_id` | numeric string/int | Offline conversions dataset |
| `job_listing_id` | numeric string/int | Marketplace job listing |
| `instagram_profile_id` | numeric string/int | Instagram profile promotion |
| `fundraiser_campaign_id` | numeric string/int | Fundraiser campaign |
| `mcme_conversion_id` | numeric string/int | MCME conversion |
| `conversion_goal_id` | numeric string/int | Conversion Goal objective |
| `offsite_conversion_event_id` | numeric string/int | Offsite conversion event |
| `boosted_product_set_id` | numeric string/int | Boosted Product Set |
| `lead_ads_form_event_source_type` | enum | `inferred`, `meta_source`, `offsite_crm`, `offsite_web`, `onsite_crm`, `onsite_crm_single_event`, `onsite_clo_dep_aet`, `onsite_web`, `onsite_p2b_call`, `onsite_messaging`, `qualified_lead_file` |
| `lead_ads_custom_event_type` | enum | Standard/custom app event, lead ads variant |
| `lead_ads_custom_event_str` | string | Custom event name, lead ads variant |
| `lead_ads_offsite_conversion_type` | enum | `default`, `clo` |
| `lead_ads_selected_pixel_id` | numeric string/int | Lead ads pixel selection |
| `lead_ads_follow_up_event` | enum | `whatsapp_conversations` |
| `value_semantic_type` | enum | `VALUE`, `MARGIN`, `LIFETIME_VALUE` |
| `variation` | enum | `OMNI_CHANNEL_SHOP_AUTOMATIC_DATA_COLLECTION`, `PRODUCT_SET_AND_APP`, `PRODUCT_SET_AND_IN_STORE`, `PRODUCT_SET_AND_OMNICHANNEL`, `PRODUCT_SET_AND_PHONE_CALL`, `PRODUCT_SET_AND_WEBSITE`, `PRODUCT_SET_AND_WEBSITE_AND_PHONE_CALL`, `PRODUCT_SET_WEBSITE_APP_AND_INSTORE` |
| `passback_pixel_id` | numeric string/int | Tracking pixel passback |
| `passback_application_id` | numeric string/int | Tracking application passback |
| `product_set_optimization` | enum | `enabled`, `disabled` |
| `full_funnel_objective` | enum | `OFFER_CLAIMS`, `PAGE_LIKES`, `EVENT_RESPONSES`, `POST_ENGAGEMENT`, `WEBSITE_CONVERSIONS`, `LINK_CLICKS`, `VIDEO_VIEWS`, `LOCAL_AWARENESS`, `PRODUCT_CATALOG_SALES`, `LEAD_GENERATION`, `BRAND_AWARENESS`, `STORE_VISITS`, `REACH`, `APP_INSTALLS`, `MESSAGES`, `OUTCOME_AWARENESS`, `OUTCOME_ENGAGEMENT`, `OUTCOME_LEADS`, `OUTCOME_SALES`, `OUTCOME_TRAFFIC`, `OUTCOME_APP_PROMOTION` |
| `dataset_split_id` | numeric string/int | Dataset split |
| `dataset_split_ids` | array<numeric string> | Multiple dataset splits |
| `custom_attribution_source_ids` | array<numeric string> | Custom attribution sources |
| `multi_event_product` | int64 | Multi-event optimization |
| `product_sales_channel` | enum | `ONLINE`, `IN_STORE`, `OMNI` |
| `anchor_event_config` | JSON object | Multi-event optimization config |
| `multi_event_conversion_info` | JSON object | CLO (conversion-lift-optimized) campaigns config |
| `live_video_destination` | string | Live video promotion |
| `smart_pse_enabled` | bool | Smart product set enablement |
| `smart_pse_setting` | enum | `ENABLED`, `DISABLED` |
| `omnichannel_object` | object | Contains `app`, `pixel`, `onsite` arrays |
| `whats_app_business_phone_number_id` | numeric string/int | WhatsApp business phone number |
| `whatsapp_phone_number` | string | WhatsApp phone number |

---

## Edges

| Edge | What it returns | Notable read params |
|---|---|---|
| `activities` | The activities of this ad set | not documented |
| `ad_studies` | The ad studies containing this ad set | not documented |
| `adcreatives` | The creatives of this ad set | not documented |
| `adrules_governed` | Ad rules that govern this ad set (by default, only rules that mention the ad set directly by id, or indirectly via `entity_type`) | not documented |
| `ads` | The ads under this ad set | not documented (see ad node reference) |
| `asyncadrequests` | Async ad requests for this ad set | not documented |
| `copies` | The copies of this ad set | not documented |
| `delivery_estimate` | The delivery estimate for this ad set | not documented |
| `message_delivery_estimate` | Delivery estimation of the marketing message campaign | not documented |
| `targetingsentencelines` | The human-readable targeting description sentence for this ad set | not documented |

---

## Read params (GET)

From the `ad-account/adsets` edge (i.e. `GET /act_<AD_ACCOUNT_ID>/adsets`):

| Param | Type | Allowed values / format | What it does |
|---|---|---|---|
| `date_preset` | enum | `TODAY`, `YESTERDAY`, `THIS_MONTH`, `LAST_MONTH`, `THIS_QUARTER`, `MAXIMUM`, `DATA_MAXIMUM`, `LAST_3D`, `LAST_7D`, `LAST_14D`, `LAST_28D`, `LAST_30D`, `LAST_90D`, `LAST_WEEK_MON_SUN`, `LAST_WEEK_SUN_SAT`, `LAST_QUARTER`, `LAST_YEAR`, `THIS_WEEK_MON_TODAY`, `THIS_WEEK_SUN_TODAY`, `THIS_YEAR` | Predefined date range used to aggregate **insights metrics** returned alongside the ad sets — not a filter on which ad sets are returned. `lifetime` was disabled in v10.0 and replaced by `maximum` (max 37 months of data). |
| `time_range` | object | `{'since':'YYYY-MM-DD','until':'YYYY-MM-DD'}` | Same insights-aggregation purpose as `date_preset`, at custom granularity. Invalid ranges are silently ignored. |
| `effective_status` | list<enum> | `ACTIVE`, `PAUSED`, `DELETED`, `PENDING_REVIEW`, `DISAPPROVED`, `PREAPPROVED`, `PENDING_BILLING_INFO`, `CAMPAIGN_PAUSED`, `ARCHIVED`, `ADSET_PAUSED`, `IN_PROCESS`, `WITH_ISSUES` | **Actually filters which ad sets are returned** by effective status — this is a real server-side filter, unlike `date_preset`/`time_range` above. |
| `is_completed` | boolean | true/false | Filters ad sets by completed status |
| `updated_since` | integer | Unix timestamp | Returns only ad sets updated at/after this time — server-side incremental-sync filter |

**Important distinction confirmed from the docs**: on the `adsets` edge, `date_preset` and
`time_range` govern the *insights metrics window* attached to the response, not which ad sets are
returned. They are not a substitute for filtering ad sets by `start_time`/`end_time`/`created_time`.
No separate `sort` param was found documented on this edge.

---

## Filtering

**Not documented** in the sources checked. Attempts:
- `.../reference/ad-campaign.md` — no dedicated "Filtering" section beyond the `effective_status`
  and `updated_since` params captured above.
- `https://developers.facebook.com/docs/graph-api/using-graph-api` (retried once without `.md`) —
  confirmed no filtering/operator section present on that page; it covers nodes, edges, fields,
  access tokens, HTTP basics, versioning, and CRUD only.
- A `.../guides/filtering.md` guess 404'd and was not retried further per the stop condition.

We could not confirm the general Graph API `filtering` param (with `EQUAL`/`IN`/`CONTAIN`/etc.
operators, commonly used on other edges like Insights) is supported on the `adsets` edge, nor its
exact JSON shape. This needs a follow-up fetch against the actual `filtering` reference page
(likely under the Insights or async-report docs) before we build it into the handler.

---

## Gotchas

- **Deprecation**: `date_preset=lifetime` disabled since Graph API v10.0; replaced by
  `date_preset=maximum` (returns max 37 months of data).
- **Attribution**: `attribution_spec` (list<AttributionSpec>) controls the conversion attribution
  window used for optimization — not currently exposed by our handler.
- **Currency minor units**: budgets/spend fields (`daily_budget`, `lifetime_budget`,
  `budget_remaining`, `daily_spend_cap`, `lifetime_spend_cap`, `bid_amount`, etc.) are documented
  as `numeric string` in the account's currency; the page does not spell out minor-unit scaling
  (e.g. cents vs. whole units) — treat as **not documented** here, verify against account currency
  behavior before assuming a scale factor.
- **EU DSA fields required for EU targeting**: `dsa_payor` and `dsa_beneficiary` (both string, max
  512 chars) are mandatory when targeting includes EU territories; they auto-populate from account
  defaults if configured, but are still present as read fields worth exposing.
- **Regional regulation categories**: `special_ad_category` (mentioned in the page's prose, not
  in the reading-fields table — likely a write-only/creation field) plus read fields
  `regional_regulated_categories` (enum list: `TAIWAN_FINSERV`, `TAIWAN_UNIVERSAL`,
  `AUSTRALIA_FINSERV`, `INDIA_FINSERV`, `SINGAPORE_UNIVERSAL`, `THAILAND_UNIVERSAL`,
  `BRAZIL_REGULATION`) and `regional_regulation_identities` require corresponding beneficiary/payor
  declarations.
- **Custom-audience/custom-conversion restriction (effective Sept 2, 2025)**: any custom audience
  or custom conversion suggesting specific health conditions or financial status is flagged and
  blocked from being used to run campaigns; flagged items populate `issues_info` and active
  campaigns using them face delivery/performance issues.
- **Error codes surfaced when reading**: `2635` (deprecated API version), `100` (invalid
  parameter), `80004` (rate limited — "too many calls to this ad-account"), `190` (invalid OAuth
  token), `200` (permissions error), `2500` (error parsing graph query).
- **Special-access fields**: no field on this page was explicitly marked as requiring special
  Meta App Review access in the extracted text; several `promoted_object` sub-keys
  (`offline_conversion_data_set_id`, `mcme_conversion_id`, `lead_ads_*`) strongly imply
  Advanced Access / feature-gated permissions in practice, but this was **not documented** on the
  page itself — don't hardcode an access-level assumption without checking the permissions
  reference separately.
- **Targeting sub-doc gap**: `flexible_spec`, `exclusions`, `instagram_positions`, and
  `targeting_relaxation_types` are known real fields on the targeting spec from general API
  familiarity, but no fetched page in this pass documented them — flagged as "not documented in
  sources checked," not confirmed absent.

---

## Follow-up: currency units and targeting sub-keys

Note on tooling: the `.md` suffix only resolves on top-level `documentation/ads-commerce/...`
pages; every second-level `developers.facebook.com/docs/marketing-api/reference/...` page 404s
with `.md` appended and must be fetched as a plain URL. Used plain URLs throughout this pass.

### Q1 — currency minor units: RESOLVED

Found the authoritative mechanism at `https://developers.facebook.com/docs/marketing-api/currencies`
(plain URL — this one page confirms the general rule for the whole API, independent of any single
field's wording):

> "Each currency has an offset which specifies how the platform handles its subdivisions. The
> offset ensures the minimum bid, such as '1', is a usable value for the currency."
>
> "If a currency has an offset of 100 then the minimum bid equals 1/100 of the base currency unit."
> (example: USD, API value `1` = $0.01)
>
> "If a currency has an offset of 1 then the minimum bid equals one base currency unit." (example:
> JPY, API value `1` = ¥1)

**Currencies with offset = 1 (whole-unit, no minor-unit scaling)**: CLP, COP, CRC, HUF, ISK, IDR,
JPY, KRW, PYG, TWD, VND. **All other currencies use offset = 100** (standard cents-style scaling).

Field-level corroboration, quoted verbatim from the reference pages:

- `spend_cap` (Ad Account, `docs/marketing-api/reference/ad-account`): *"Value specified in basic
  unit of the currency, for example 'cents' for USD."* — Note: the same page's **Updating** section
  for `spend_cap` says *"Value specified in standard denomination of the currency, e.g. 23.50 for
  USD $23.50"* — i.e. **read and write use different units for this one field**; read returns
  minor-unit integers, write accepts standard-denomination decimals. Worth flagging in our
  docstrings if we ever add write support.
- `spend_cap` (Campaign, `docs/marketing-api/reference/ad-campaign-group`): *"Defined as integer
  value of subunit in your currency with a minimum value of $100 USD (or approximate local
  equivalent)."* — confirms subunit (minor-unit) representation for campaign-level `spend_cap` too.
- `bid_amount` (Ad Set, `docs/marketing-api/reference/ad-campaign`): *"The bid amount's unit is
  cents for currencies like USD, EUR, and the basic unit for currencies like JPY, KRW."* — this is
  the clearest per-field statement and it matches the offset table exactly (JPY/KRW are both
  offset-1 currencies above).
- `daily_budget` / `lifetime_budget` (both Ad Set and Campaign): descriptions only say *"defined in
  your account currency"* / *"Daily budget of this campaign"* — **no explicit minor-vs-standard
  unit statement on either reference page**.
- `budget_remaining` (both Ad Set and Campaign): *"Remaining budget"* — no unit statement at all.
- `amount_spent` (Ad Account): *"Current amount spent by the account with respect to `spend_cap`.
  Or total amount in the absence of `spend_cap`."* — no unit statement.

**Verdict**: The offset mechanism is Meta's one documented, authoritative rule, and it is
consistent across every field that does state its unit explicitly (`spend_cap` on both nodes,
`bid_amount`) — all read as minor-unit/subunit integers, scaled by the currency's offset (÷100 for
the vast majority of currencies, ÷1 i.e. no scaling for the eleven currencies listed above). No
reference page states `daily_budget`, `lifetime_budget`, `budget_remaining`, or `amount_spent` use
a *different* convention from `spend_cap`/`bid_amount` — but none of the four explicitly says they
follow it either. I could not find a single page that names all of our four handler fields per
node and states "minor units" in one sentence, so treat the daily/lifetime/remaining/amount_spent
fields as **inferred-by-consistency, not directly confirmed** — everything Meta does document
points the same direction, but this stops short of your bar for shipping an unverified factor of
100. Recommend verifying `daily_budget`/`amount_spent` against one live account with known spend
before relying on it in production math, per your stated plan.

### Q2 — targeting sub-keys: 3 of 4 resolved

- **`targeting_relaxation_types`** — CONFIRMED, found on `docs/marketing-api/targeting-specs`.
  Type: object/dict. Shape: nested key-value pairs, keys `lookalike` and `custom_audience`, values
  `0` or `1`. Purpose per the page: *"When using age and gender suggestions, Meta shows ads beyond
  these settings when doing so is likely to improve ad performance"* — it's a toggle for whether
  Meta is allowed to relax those two targeting dimensions during delivery. **Verdict: stays JSON**
  — only two fixed sub-keys, not worth two extra flat columns for a delivery-relaxation toggle
  that's rarely filtered on.
- **`flexible_spec`** — CONFIRMED, found on `docs/marketing-api/audiences/reference/flexible-targeting`.
  Type/shape verbatim: *"Array of arrays. Each contains a targeting segment in appropriate format,
  such as interests, behaviors, and demographics."* Constraints: *"The top-level array has a limit
  of 25, and the secondary-level array has a limit of 1,000."* Logic: *"Facebook evaluates each
  top-level array element in `flexible_spec` with AND"* / *"second-level array elements with OR"*.
  **Verdict: stays JSON** — it's a variable-depth AND-of-ORs boolean expression tree over arbitrary
  targeting segments; there's no fixed key set to flatten into columns without losing the logic
  structure itself.
- **`exclusions`** — NOT DOCUMENTED. Checked `targeting-specs`, `flexible-targeting`, and a guessed
  `reference/ad-campaign/targeting` page (404). The `flexible-targeting` page's own text says it
  does not reference an `exclusions` key at all in its 14 listed targeting fields. Given
  `excluded_custom_audiences` and `excluded_user_device` already exist as separate documented
  fields (see main targeting table above), `exclusions` as a distinct named key may not exist as
  such — or may be an undocumented/deprecated alias. **Not documented in sources checked** — don't
  add a column or code path for a bare `exclusions` key without further confirmation.
- **`instagram_positions`** — NOT DOCUMENTED. Checked the same three pages plus
  `reference/targeting-search` (404). `facebook_positions` is documented; no parallel
  `instagram_positions` field was found on any page reached in this pass, despite it being a
  real-world placements concept. **Not documented in sources checked** — flag as a genuine
  research gap rather than ship it as a guessed column name.
