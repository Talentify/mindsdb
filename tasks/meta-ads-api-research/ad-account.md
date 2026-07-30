# Meta Marketing API — Ad Account reference extract

Source: https://developers.facebook.com/documentation/ads-commerce/marketing-api/reference/ad-account.md
(fetched as raw markdown; no sub-page was followed because none of the linked enum/object
pages were needed to answer the checklist below — object sub-fields for nested types like
`funding_source_details`, `rf_spec`, etc. are NOT expanded here; see Gotchas)

Handler file for cross-reference: `mindsdb/integrations/handlers/meta_ads_handler/tables/account.py`

## Reading fields

| field | type | one-line meaning |
|---|---|---|
| `id` ✅ | string | `act_{ad_account_id}` — default field |
| `account_id` ✅ | numeric string | Ad account ID without `act_` prefix — default field |
| `account_status` ✅ | unsigned int32 | Status code: 1=ACTIVE, 2=DISABLED, 3=UNSETTLED, 7=PENDING_RISK_REVIEW, 8=PENDING_SETTLEMENT, 9=IN_GRACE_PERIOD, 100=PENDING_CLOSURE, 101=CLOSED, 201=ANY_ACTIVE, 202=ANY_CLOSED |
| `age` | float | Days since the account was opened |
| `agency_client_declaration` | AgencyClientDeclaration (object) | Agency details; requires Admin privileges to read |
| `amount_spent` ✅ | numeric string | Spend so far relative to `spend_cap`, in account currency |
| `attribution_spec` | list\<AttributionSpec\> | **Deprecated** (iOS 14 impact) |
| `balance` | numeric string | Outstanding bill amount |
| `brand_safety_content_filter_levels` | list\<string\> | Content filter levels for in-content/feed ads |
| `business` | Business (object ref) | Associated Business Manager |
| `business_city` | string | Business address — city |
| `business_country_code` | string | Business address — country code |
| `business_name` ✅ | string | Business name on the account |
| `business_state` | string | Business address — state abbreviation |
| `business_street` | string | Business address — street line 1 |
| `business_street2` | string | Business address — street line 2 |
| `business_zip` | string | Business address — zip/postal code |
| `can_create_brand_lift_study` | bool | Whether the account can create a brand lift study |
| `capabilities` | list\<string\> | Account capability flags |
| `created_time` ✅ | datetime (ISO 8601) | Account creation timestamp |
| `currency` ✅ | string | ISO 4217 currency code |
| `default_dsa_beneficiary` | string | Default DSA (Digital Services Act) beneficiary |
| `default_dsa_payor` | string | Default DSA payor |
| `direct_deals_tos_accepted` | bool | Whether Direct Deals ToS was accepted |
| `disable_reason` | unsigned int32 | Why the account was disabled (0=NONE … 15=COMPROMISED_AD_ACCOUNT) |
| `end_advertiser` | numeric string | Entity the ads target (Page/App ID) |
| `end_advertiser_name` | string | Name of the target entity |
| `existing_customers` | list\<string\> | Custom audience IDs used for Automated Shopping Ads |
| `expired_funding_source_details` | FundingSourceDetails (object) | Historical payment method info |
| `extended_credit_invoice_group` | ExtendedCreditInvoiceGroup (object) | Associated credit invoice group |
| `failed_delivery_checks` | list\<DeliveryCheck\> | Delivery checks that failed |
| `fb_entity` | unsigned int32 | Facebook entity type code |
| `funding_source` ✅ | numeric string | Payment method ID |
| `funding_source_details` | FundingSourceDetails (object) | Payment method details (id, coupon info, amounts, type code 0–20) |
| `has_migrated_permissions` | bool | Whether permissions have been migrated |
| `has_page_authorized_adaccount` | bool | Political-content authorization status |
| `io_number` | numeric string | Insertion order number |
| `is_attribution_spec_system_default` | bool | Whether `attribution_spec` is the system default |
| `is_direct_deals_enabled` | bool | Direct Deals eligibility |
| `is_in_3ds_authorization_enabled_market` | bool | Whether the account is in a 3DS-required payment market |
| `is_notifications_enabled` | bool | Notification preference |
| `is_personal` | unsigned int32 | Whether the account is for private/non-business use |
| `is_prepay_account` | bool | Prepay vs. postpay billing |
| `is_tax_id_required` | bool | Whether a tax ID is required |
| `line_numbers` | list\<integer\> | Associated line numbers |
| `media_agency` | numeric string | Agency ID (Page/App or NONE/UNFOUND) |
| `min_campaign_group_spend_cap` | numeric string | Minimum allowed campaign-group spend cap |
| `min_daily_budget` | unsigned int32 | Minimum allowed daily budget |
| `name` ✅ | string | Account name (defaults to first admin's name if unset) |
| `offsite_clo_signal_status` | int32 | Conversion-lift-optimization signal status |
| `offsite_pixels_tos_accepted` | bool | Offsite pixel ToS acceptance |
| `opportunity_score` | float | 0–100 optimization score |
| `opportunity_score_weight` | integer | Remaining budget (in cents) used for weighted scoring |
| `owner` | numeric string | Account owner ID |
| `partner` | numeric string | Partner ID (Page/App or NONE/UNFOUND) |
| `po_number` | string | Purchase order number |
| `rf_spec` | ReachFrequencySpec (object) | Reach & Frequency configuration |
| `show_checkout_experience` | bool | Whether the pre-paid checkout experience is shown |
| `spend_cap` ✅ | numeric string | Maximum spend limit (0 = no cap) |
| `tax_id` | string | Tax identifier |
| `tax_id_status` | unsigned int32 | VAT status (0=Unknown, 1=not required US/CA, 2=required, 3=submitted, 4=offline validation failed, 5=personal account) |
| `tax_id_type` | string | Tax ID classification |
| `timezone_id` | unsigned int32 | Timezone identifier |
| `timezone_name` ✅ | string | Timezone name |
| `timezone_offset_hours_utc` ✅ | float | UTC offset in hours |
| `tos_accepted` | map\<string, int32\> | ToS acceptance per type (1 = accepted) |
| `user_tasks` | list\<string\> | Task assignments for the requesting user |
| `user_tos_accepted` | map\<string, int32\> | User-level ToS acceptance (requires user access token) |
| `ad_account_promotable_objects` | AdAccountPromotableObjects (object) | Promoted-object-related fields |

Total: **62 documented readable fields** found in the fetched extract vs. **12** currently exposed by our handler.

Note: the source page is a third-party markdown mirror rather than Meta's own developer
site tree structure, so field coverage is best-effort — treat this as a strong working
list, not a guaranteed 1:1 match with Meta's canonical reference. No field name or enum
value below was invented; anything not stated in the fetched page is marked "not documented".

## Edges

| edge | what it returns | notable read params |
|---|---|---|
| `activities` | AdAccount activity log (AdActivity) | not documented in this extract |
| `adcreatives` | Ad creatives belonging to the account | not documented in this extract |
| `advideos` | Videos associated with the account | not documented in this extract |
| `applications` | Connected applications | not documented in this extract |
| `advertisable_applications` | Apps promotable from this account (v2.4+) | not documented in this extract |
| `asyncadcreatives` | Async ad-creative creation requests | not documented in this extract |
| `account_controls` | AdAccountBusinessConstraints — Advantage+ shopping audience controls (age/geo) | not documented in this extract |
| `ads_reporting_mmm_reports` | Marketing-mix-modeling reports | not documented in this extract |
| `ads_reporting_mmm_schedulers` | MMM report scheduling | not documented in this extract |
| `broadtargetingcategories` | Broad targeting categories for audience selection | not documented in this extract |
| `connected_instagram_accounts` | Connected Instagram accounts (ShadowIGUser) | not documented in this extract |
| `instagram_accounts` | Connected Instagram accounts (alternative form) | not documented in this extract |
| `customaudiences` | Custom audiences owned/shared with the account | not documented in this extract |
| `customaudiencestos` | Custom audience terms-of-service records | not documented in this extract |
| `customconversions` | Custom conversion definitions | not documented in this extract |
| `delivery_estimate` | Estimated delivery for an ad configuration | requires a targeting/optimization spec — exact param names not documented in this extract |
| `deprecatedtargetingadsets` | Ad sets using deprecated targeting | not documented in this extract |
| `dsa_recommendations` | DSA-related recommendations | not documented in this extract |
| `generatepreviews` | Generates ad creative previews | not documented in this extract |
| `impacting_ad_studies` | Ad studies (research) affecting this account | not documented in this extract |
| `mcmeconversions` | Multi-channel marketing event conversions | not documented in this extract |
| `minimum_budgets` | Minimum daily budgets by currency | not documented in this extract |
| `promote_pages` | Facebook Pages promotable from this account | not documented in this extract |
| `reachestimate` | Reach estimate for a targeting spec | requires a targeting spec — exact param names not documented in this extract |
| `saved_audiences` | Saved audience configurations | not documented in this extract |
| `targetingbrowse` / `targetingsearch` / `targetingsuggestions` / `targetingvalidation` | AdAccountTargetingUnified — browse/search/suggest/validate targeting options | not documented in this extract |

Not present in the fetched extract but known from our own handler already: `campaigns`,
`adsets`, `ads`, `insights` (we already expose these as their own tables, so they're
omitted from the ranking below).

## Read params (GET)

| param | type | allowed values / format | what it does |
|---|---|---|---|
| `fields` | comma-separated string | any of the field names above | Selects which fields to return. Our handler already uses this — it always requests all 12 handler columns |
| `access_token` | string | valid user/system-user token | Auth — not a filter, but required on every request |

The fetched extract does not document any account-level filtering/paging params for the
node GET itself (the account is a single resource, not a list) — `limit`, `after`/`before`,
and `filtering` apply to the **edges** (e.g. `adcreatives`, `customaudiences`), not to the
account node. Exact edge-level param names were not documented in this extract; would
require following each edge's own reference page (out of the 2-level budget for this pass).

## Nested / structured fields

Fields that return an object or list — a SQL handler must JSON-encode or flatten these:

- `agency_client_declaration` — object (AgencyClientDeclaration)
- `attribution_spec` — list of objects (deprecated)
- `business` — object reference (Business)
- `capabilities` — list\<string\>
- `brand_safety_content_filter_levels` — list\<string\>
- `existing_customers` — list\<string\>
- `expired_funding_source_details` — object (FundingSourceDetails)
- `extended_credit_invoice_group` — object (ExtendedCreditInvoiceGroup)
- `failed_delivery_checks` — list of objects (DeliveryCheck)
- `funding_source_details` — object (FundingSourceDetails)
- `line_numbers` — list\<integer\>
- `rf_spec` — object (ReachFrequencySpec)
- `tos_accepted` — map\<string, int32\>
- `user_tos_accepted` — map\<string, int32\>
- `user_tasks` — list\<string\>
- `ad_account_promotable_objects` — object (AdAccountPromotableObjects)

Sub-fields of these nested object types (e.g. what's inside `FundingSourceDetails` or
`ReachFrequencySpec`) were not fetched — that would require following those object
reference pages individually, which is beyond the 2-level follow budget for this pass.
Flag as "not documented" until a follow-up pass expands them.

## Gotchas

- **`account_status` is an enum code, not a label.** Our handler returns the raw integer
  (1, 2, 3, 7, 8, 9, 100, 101, 201, 202) with no label mapping. If any downstream query or
  dashboard expects a string like `"ACTIVE"`, it will get `1` instead. Worth adding a
  code→label mapping if human-readable status is wanted.
- **`amount_spent` / `spend_cap` currency minor-units claim is not confirmed in this
  fetched extract.** Our handler's docstring (`tables/account.py`) asserts they are in
  minor units (cents), but the fetched reference page did not explicitly state this for
  the Ad Account node — it only says "numeric string." This is a widely-known Marketing
  API convention elsewhere (e.g. Insights `spend`), but should be verified against Meta's
  canonical page before relying on it, rather than treating our docstring as confirmed by
  this research pass.
- **`attribution_spec` is deprecated** (iOS 14 impact) — do not add to the handler as a
  forward-looking column.
- **Several fields require elevated privileges**: `agency_client_declaration` requires
  Admin privileges; `user_tos_accepted` requires a user (not system-user) access token.
  A handler using a system-user token may get `null`/omitted values or a permission error
  for these.
- **`disable_reason` and `tax_id_status` are also enum codes**, not labels — same caveat
  as `account_status`.
- **DSA fields (`default_dsa_payor`, `default_dsa_beneficiary`) are paired**: per the
  fetched extract, the API does not allow setting only one — this is a write-side
  constraint, but relevant context if these fields ever show up unset/paired oddly on read.
- **Account-level limits exist** (from the "Account Limits" section of the doc): e.g. max
  25 ad accounts per person, 25 users per account, 6,000 non-archived ads for a regular
  account (50,000 for bulk accounts) — not fields, but useful context if building
  usage/quota-monitoring queries against this handler.
- The source page also documents create/update/delete operations (POST to
  `/act_{id}`, `/{business_id}/adaccount`, etc.) and an `/ads_volume` endpoint for
  running/in-review ad counts — irrelevant to a **read-only** handler, so omitted from
  the tables above, but `/ads_volume` could be a candidate for a small dedicated
  "account health" table if ever needed.
