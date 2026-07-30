# Meta Ads Phase 0 — live measurements

Measured live against `act_268138328287798` via `handler.graph_get()` (GET only, nothing
written). Every cell below is an observed response, never inferred from a neighbour.
Classification rule (per the plan):

- `FIELD_UNSUPPORTED` — API returned "Filtering field 'X' is not supported"
- `OP_UNSUPPORTED` — API returned "Filtering field 'X' with operation 'Y' is not supported"
- `OK` — the request was accepted (200)
- `OTHER: <verbatim>` — anything else (recorded verbatim, truncated)
- `not measured` — no probe was sent (rate-limit budget or n/a)

A methodology note up front: the "send one bogus operator first" trick from the brief
turned out **not** to distinguish field-support from operator-support on this API. Meta
validates the operator name against its global enum *before* checking whether it applies to
the given field, so a nonsense operator (e.g. `XXX`) always 400s with `Param
filtering[0][operator] must be one of {EQUAL, NOT_EQUAL, ...}` regardless of whether the
field itself is filterable. That response is recorded as `OTHER` in the raw data and did
not short-circuit per-field operator testing (every field still got its full real-operator
sweep) — it just didn't save the calls it was meant to save. Noting this so nobody re-uses
the bogus-operator-for-field-discovery trick expecting it to work here.

---

## adsets edge — measured field × operator matrix

Edge: `{account_path}/adsets`. 70 API calls, 0 rate-limit hits.

| Field | EQUAL | NOT_EQUAL | IN | NOT_IN | CONTAIN | NOT_CONTAIN | GREATER_THAN | LESS_THAN | IN_RANGE | NOT_IN_RANGE |
|---|---|---|---|---|---|---|---|---|---|---|
| `adset.id` | OK | OK | OK | OK | n/a | n/a | n/a | n/a | n/a | n/a |
| `adset.name` | OK | OP_UNSUPPORTED | not measured | not measured | OK | OK | n/a | n/a | n/a | n/a |
| `adset.status` | OK | OK | OK | OK | n/a | n/a | n/a | n/a | n/a | n/a |
| `adset.effective_status` | OK | OK | OK | OK | n/a | n/a | n/a | n/a | n/a | n/a |
| `adset.configured_status` | FIELD_UNSUPPORTED | FIELD_UNSUPPORTED | FIELD_UNSUPPORTED | FIELD_UNSUPPORTED | n/a | n/a | n/a | n/a | n/a | n/a |
| `adset.campaign_id` | not measured | FIELD_UNSUPPORTED (prior run) | not measured | not measured | n/a | n/a | n/a | n/a | n/a | n/a |
| `adset.optimization_goal` | OK | OK | OK | OK | n/a | n/a | n/a | n/a | n/a | n/a |
| `adset.billing_event` | OK | OP_UNSUPPORTED | OTHER: `500 Please reduce the amount of data you're asking for, then retry your request code=1` | OTHER: same 500 as IN | n/a | n/a | not measured | not measured | not measured | not measured |
| `adset.bid_strategy` | OK | OK | OK | OK | n/a | n/a | n/a | n/a | n/a | n/a |
| `adset.created_time` | n/a | OP_UNSUPPORTED | n/a | n/a | n/a | n/a | OK | OK | OK | OK |
| `adset.updated_time` | n/a | OP_UNSUPPORTED | n/a | n/a | n/a | n/a | OK | OK | OK | OK |
| `adset.start_time` | n/a | not measured | n/a | n/a | n/a | n/a | OK | OK | OK | OK |
| `adset.end_time` | n/a | not measured | n/a | n/a | n/a | n/a | OK | OK | OK | **OP_UNSUPPORTED** |
| `adset.daily_budget` | n/a | OP_UNSUPPORTED | n/a | n/a | n/a | n/a | OK | OK | OK | OK |
| `adset.lifetime_budget` | n/a | not measured | n/a | n/a | n/a | n/a | OK | OK | OK | OK |
| `adset.budget_remaining` | n/a | not measured | n/a | n/a | n/a | n/a | FIELD_UNSUPPORTED | FIELD_UNSUPPORTED | FIELD_UNSUPPORTED | FIELD_UNSUPPORTED |
| `adset.destination_type` | FIELD_UNSUPPORTED | FIELD_UNSUPPORTED | FIELD_UNSUPPORTED | FIELD_UNSUPPORTED | n/a | n/a | n/a | n/a | n/a | n/a |
| `adset.promoted_object` | FIELD_UNSUPPORTED | not measured | not measured | not measured | n/a | n/a | n/a | n/a | n/a | n/a |
| `adset.is_dynamic_creative` | FIELD_UNSUPPORTED | FIELD_UNSUPPORTED | n/a | n/a | n/a | n/a | n/a | n/a | n/a | n/a |

`n/a` = operator not applicable to the field's type, not probed (e.g. `CONTAIN` on a
timestamp field). "not measured" cells for `IN`/`NOT_IN` on `name`, `NOT_CONTAIN` on
budget/time fields etc. were skipped by design — they're not in the plan's operator-mapping
table (`=`,`!=`,`IN`,`NOT IN`,`>`,`<`,`LIKE→CONTAIN`,`BETWEEN→IN_RANGE`) so weren't needed for
the pushdown implementation, and skipping them kept the whole edge at 70 calls.

**New findings not in the prior partial matrix:**
- `adset.configured_status` — **not filterable at all** (all 4 operators FIELD_UNSUPPORTED).
  Mirrors `campaign.configured_status`, also not filterable.
- `adset.bid_strategy` — fully filterable: EQUAL, NOT_EQUAL, IN, NOT_IN all OK.
- `adset.start_time`, `adset.lifetime_budget` — same operator class as their siblings
  (`created_time`/`updated_time`, `daily_budget`): GREATER_THAN/LESS_THAN/IN_RANGE/
  NOT_IN_RANGE all OK.
- `adset.end_time` — **asymmetric**: GREATER_THAN/LESS_THAN/IN_RANGE all OK but
  **NOT_IN_RANGE is OP_UNSUPPORTED**. This is the one field in the whole adsets matrix
  where the four range/comparison operators aren't uniformly accepted — worth a comment in
  the `FILTERABLE` spec so nobody assumes time fields are homogeneous.
- `adset.budget_remaining` — **not filterable at all** (all 4 range operators
  FIELD_UNSUPPORTED). Distinct from `daily_budget`/`lifetime_budget`, which are filterable.
- `adset.destination_type` — **not filterable at all**.
- `adset.promoted_object` — **not filterable** (EQUAL rejected as FIELD_UNSUPPORTED; it's a
  JSON object field, as expected — did not spend calls on other operators since EQUAL alone
  settles "no filtering here").
- `adset.is_dynamic_creative` — **not filterable at all** (boolean field, EQUAL and
  NOT_EQUAL both FIELD_UNSUPPORTED).
- `adset.billing_event` — `IN`/`NOT_IN` produced a **500 "Please reduce the amount of data
  you're asking for"**, not a filtering-rejection message at all. This does not match either
  `FIELD_UNSUPPORTED` or `OP_UNSUPPORTED` shape and looks like an unrelated API-side error
  (possibly triggered by the specific bogus list value used, `["___never___"]`, or a
  transient condition), not a genuine "this operator is unsupported" signal. Recorded
  verbatim; **do not treat as either OK or a hard rejection** — flagged as inconclusive
  rather than guessed.

---

## ads edge — measured field × operator matrix

Edge: `{account_path}/ads`. 35 API calls, 0 rate-limit hits.

| Field | EQUAL | NOT_EQUAL | IN | NOT_IN | CONTAIN | NOT_CONTAIN | GREATER_THAN | LESS_THAN | IN_RANGE | NOT_IN_RANGE |
|---|---|---|---|---|---|---|---|---|---|---|
| `ad.id` | OK | OK | OK | OK | n/a | n/a | n/a | n/a | n/a | n/a |
| `ad.name` | OK | OP_UNSUPPORTED | not measured | not measured | OK | OK | n/a | n/a | n/a | n/a |
| `ad.status` | OK | OK | OK | OK | n/a | n/a | n/a | n/a | n/a | n/a |
| `ad.effective_status` | OK | OK | OK | OK | n/a | n/a | n/a | n/a | n/a | n/a |
| `ad.configured_status` | FIELD_UNSUPPORTED | FIELD_UNSUPPORTED | FIELD_UNSUPPORTED | FIELD_UNSUPPORTED | n/a | n/a | n/a | n/a | n/a | n/a |
| `ad.adset_id` | not measured | FIELD_UNSUPPORTED (prior run) | not measured | not measured | n/a | n/a | n/a | n/a | n/a | n/a |
| `ad.campaign_id` | not measured | FIELD_UNSUPPORTED (prior run) | not measured | not measured | n/a | n/a | n/a | n/a | n/a | n/a |
| `ad.creative_id` | not measured | FIELD_UNSUPPORTED (prior run) | not measured | not measured | n/a | n/a | n/a | n/a | n/a | n/a |
| `ad.created_time` | n/a | OP_UNSUPPORTED | n/a | n/a | n/a | n/a | OK | OK | OK | OK |
| `ad.updated_time` | n/a | OP_UNSUPPORTED | n/a | n/a | n/a | n/a | OK | OK | OK | OK |
| `ad.bid_amount` | n/a | not measured | n/a | n/a | n/a | n/a | OK | OK | OK | OK |
| `ad.source_ad_id` | FIELD_UNSUPPORTED | FIELD_UNSUPPORTED | FIELD_UNSUPPORTED | FIELD_UNSUPPORTED | n/a | n/a | n/a | n/a | n/a | n/a |

**New findings:**
- `ad.configured_status` — **not filterable at all**, same shape as `adset.configured_status`
  and `campaign.configured_status`. Consistent pattern across all three listing edges: the
  editable "what you set" status field is never filterable; only `status`/`effective_status`
  (the resolved/derived states) are.
- `ad.bid_amount` — fully filterable as a range field: GREATER_THAN, LESS_THAN, IN_RANGE,
  NOT_IN_RANGE all OK. Same operator class as `adset.daily_budget`/`lifetime_budget`.
- `ad.source_ad_id` — **not filterable at all** (all 4 id-class operators
  FIELD_UNSUPPORTED). This is a lineage/rollback pointer field (points to the ad an A/B
  test or edit originated from) — makes sense it wouldn't be indexed for filtering.
- `ad.name`, `ad.created_time`, `ad.updated_time` behave identically to their `adset.*`
  counterparts (EQUAL/CONTAIN/NOT_CONTAIN OK, NOT_EQUAL OP_UNSUPPORTED on name;
  GREATER_THAN/LESS_THAN/IN_RANGE/NOT_IN_RANGE all OK, NOT_EQUAL OP_UNSUPPORTED on the two
  time fields) — no asymmetry like `adset.end_time` showed.

---

## adcreatives edge — is filtering usable at all? (Task 5, plan correction)

7 API calls + 2 verbatim-capture calls = 9 total. 0 rate-limit hits.

| Field | EQUAL | NOT_EQUAL | IN | CONTAIN |
|---|---|---|---|---|
| `adcreative.id` | OP_UNSUPPORTED | OP_UNSUPPORTED | OP_UNSUPPORTED | n/a |
| `adcreative.name` | OP_UNSUPPORTED | OP_UNSUPPORTED | OP_UNSUPPORTED | OP_UNSUPPORTED |

Verbatim (reproduced twice, matches the prior run exactly):
```
Meta Ads API error (400): (#100) Filtering field 'adcreative.id' with operation 'equal' is not supported
Meta Ads API error (400): (#100) Filtering field 'adcreative.name' with operation 'contain' is not supported
```

**Verdict: the plan's operational conclusion is correct, but its stated mechanism is wrong.**
Every operator tested against `adcreative.id` and `adcreative.name` (EQUAL, NOT_EQUAL, IN,
CONTAIN) came back classified `OP_UNSUPPORTED` — never `FIELD_UNSUPPORTED`. Per the plan's
own classification rule, `OP_UNSUPPORTED` means the field *is* recognized as filterable by
the validator; it's specifically each operator that's rejected. That is the opposite of what
"no filtering of any kind" implies about the mechanism.
In *practice*, though, the outcome is identical to what the plan claims: across the full
natural operator set for an id-like field (EQUAL/NOT_EQUAL/IN) and a text-like field
(EQUAL/NOT_EQUAL/IN/CONTAIN), nothing works, so there is no usable filter you could actually
send on this edge with these two fields. We did not test every possible operator (e.g.
GREATER_THAN/IN_RANGE on `id`/`name` — nonsensical for those types, correctly not probed)
so it remains theoretically possible some operator we didn't try is accepted, but nothing in
the natural operator set for either field works.

---

## Semantics (Task 3: filtering AND/OR, bare vs dotted, CONTAIN case) and Task 4 (sort)

16 API calls across the two follow-up scripts, 0 rate-limit hits. Evidence via
`{account_path}/campaigns`, control sample of 5 real campaigns fetched first (real ids:
`120249722784570398`, `120235711480850398`, ...; real statuses `ACTIVE`/`PAUSED`; real name
containing "First").

### 3a. Do multiple `filtering` entries AND or OR together?

- Test 1 — one entry matching everything (`campaign.id NOT_EQUAL '0'`) AND one matching
  nothing (`campaign.id EQUAL '0'`) in the same `filtering` list → **0 rows**. Under OR
  semantics this would have returned the same 5 rows as the unfiltered control (since the
  first condition alone matches everything); it did not.
- Test 2 — two mutually-exclusive real-id `EQUAL` filters (`id = 120249722784570398` AND
  `id = 120235711480850398`, no campaign can equal both) → **0 rows**. Under OR semantics
  this would have returned 2 rows; it returned 0.
- **Verdict: multiple `filtering` entries AND together.** Confirmed by two independent
  tests, both with real IDs from the account, not synthetic values.

### 3b. Bare field form vs dotted (`campaign.` prefix) form

- `{"field": "campaign.status", "operator": "EQUAL", "value": "ACTIVE"}` → 10 rows.
- `{"field": "status", "operator": "EQUAL", "value": "ACTIVE"}` → 10 rows.
- **Verdict: identical result (same row count) for the same predicate.** Both forms work
  equivalently on the campaigns edge for `status`. (Only one field was compared under
  budget; not verified for every field, see "Not measured.")

### 3c. Is `CONTAIN` case-insensitive?

- First attempt used `real_name[:6]` = `"5117 -"` (numeric/punctuation, no letters) — this
  measured nothing about case sensitivity and is called out here so it isn't mistaken for a
  real negative result.
- Redone with the real alphabetic word `"First"` (present in the account's campaign names):
  `CONTAIN "First"`, `CONTAIN "FIRST"`, and `CONTAIN "first"` all returned the **identical 5
  rows in the identical order**.
- **Verdict: `CONTAIN` is case-insensitive**, confirmed with a real substring, not inferred.

### Task 4: `sort`

- `sort: ["created_time_descending", "name_ascending"]` (list of 2 keys) → accepted, 200 OK,
  3 rows.
- `sort: "created_time_descending"` (bare string, not a list) → accepted, 200 OK, 3 rows.
- `sort: "created_time_bogus_suffix"` (invalid suffix) → **accepted, 200 OK** — no rejection
  at all, no error message to inspect (so the "capture the API's enumeration of valid
  values" sub-goal could not be completed; there is no rejection message because there is no
  rejection).
- Decisive follow-up: fetched the same 5-campaign sample three ways — no `sort` param,
  `sort: "created_time_descending"`, `sort: "created_time_bogus_suffix"` — and got the
  **exact same id/created_time order all three times** (newest-first: `2026-07-27`,
  `2025-09-26`, `2025-09-25`×3). Then explicitly tried `sort: "created_time_ascending"` —
  **still the exact same order**, not reversed.
- **Verdict: in this account, `sort` had no observable effect on row order in any
  configuration tested, valid or invalid.** Two possible explanations, not distinguished by
  this test:
  1. The API silently ignores the `sort` param on the campaigns edge entirely (accepts any
     value, including nonsense, and always returns its own default order).
  2. `handler.graph_get()`'s parameter serialization loses or mis-encodes the `sort` value
     before it reaches Graph (e.g. a list value not serialized the way Graph expects for
     this specific param, unlike `filtering` which is confirmed working as a list).
  This could not be disambiguated without inspecting the outgoing HTTP request bytes, which
  was out of scope for a GET-only measurement pass. **This directly contradicts the plan's
  "Other measured pushdown levers" claim that `sort` "works on listing edges"** — see
  Corrections below.

---

## Task 6 — new field validation (39 fields added by prep-phase1)

14 API calls, 0 rate-limit hits. One combined `fields=` call per table (`id` + new fields,
`limit=1`); bisection only where the combined call 400'd.

### campaigns — PASS (10/10 accepted in one call)

| Field | Wire type | Sample |
|---|---|---|
| `configured_status` | str | `"ACTIVE"` |
| `account_id` | str | `"268138328287798"` |
| `promoted_object` | NULL/absent | — |
| `issues_info` | NULL/absent | — |
| `special_ad_category_country` | list | `["CA", "US"]` |
| `source_campaign_id` | str | `"0"` |
| `pacing_type` | NULL/absent | — |
| `topline_id` | str | `"0"` |
| `adlabels` | NULL/absent | — |
| `primary_attribution` | str | `"DEFAULT"` |

### adsets — PASS (12/12 accepted in one call)

| Field | Wire type | Sample |
|---|---|---|
| `targeting` (pre-existing, re-checked for 6b) | dict | see targeting blob below |
| `configured_status` | str | `"ACTIVE"` |
| `attribution_spec` | NULL/absent | — |
| `learning_stage_info` | dict | `{"attribution_windows": ["1d_click"], "conversions": 17, "last_sig_edit_ts": 1785176286, "status": "LEARNING"}` |
| `issues_info` | NULL/absent | — |
| `daily_min_spend_target` | NULL/absent | — |
| `daily_spend_cap` | NULL/absent | — |
| `lifetime_min_spend_target` | NULL/absent | — |
| `lifetime_spend_cap` | NULL/absent | — |
| `frequency_control_specs` | NULL/absent | — |
| `source_adset_id` | str | `"0"` |
| `dsa_payor` | NULL/absent | — |
| `dsa_beneficiary` | NULL/absent | — |

### ads — FAIL: `special_ad_categories` rejected

Combined call 400'd. Bisection (5 calls) isolated the single bad field:
```
Meta Ads API error (400): (#3) App must be on whitelist type=OAuthException code=3
```
**This is an authorization/whitelist error (code 3), not a "nonexisting field" error
(that would be code 100/`#100`).** That distinction matters: `special_ad_categories` is very
likely a real, documented field, but this app/account is not whitelisted to request it (Meta
gates Special Ad Category — housing/employment/credit compliance — fields behind app
review). It is **not proven invalid**, but it **is proven unusable with this app's current
credentials**, which for shipping purposes is the same practical outcome: including it in
the combined `fields=` list would 400 the entire `ads` table for every user of this
integration until the app is whitelisted. Recommend dropping it from `COLUMNS` until/unless
the app goes through that review, or gating it behind a config flag.

All other 9 new `ads` fields passed once `special_ad_categories` was excluded from the
bisection halves — confirmed via the bisection call `['configured_status', 'targeting',
'issues_info', 'ad_review_feedback', 'recommendations']` (OK) and `['conversion_specs',
'tracking_specs']` (OK) and `['source_ad_id']` (OK) and `['adlabels']` (OK). Types were not
individually captured for these because the final passing combined subset call wasn't
re-run after excluding the bad field (would have cost one more call) — **wire types for the
9 passing `ads` fields are therefore "not measured", only "accepted by Graph" is confirmed.**

### account (node itself) — PASS (10/10 accepted in one call)

| Field | Wire type | Sample |
|---|---|---|
| `account_status` (6c) | int | `1` |
| `balance` | str | `"23799"` (note: string, not int — cast defensively) |
| `disable_reason` | int | `0` |
| `min_daily_budget` | int | `100` |
| `opportunity_score` | int | `99` |
| `capabilities` | list | `["CAN_CREATE_CALL_ADS", "CAN_SEE_GROWTH_OPPORTUNITY_DATA", ...]` |
| `end_advertiser_name` | str | `"Talentify, Inc"` |
| `timezone_id` | int | `366` |
| `age` | float | `1981.2596643519` (fractional years since account creation, not an int) |
| `is_prepay_account` | bool | `false` |
| `tax_id_status` (6c) | int | `3` |

**6c answer: both `account_status` and `tax_id_status` arrive as JSON ints, not numeric
strings.** Confirmed directly, not inferred.

### adcreatives — PASS (7/7 accepted in one call)

| Field | Wire type | Sample |
|---|---|---|
| `product_set_id` | NULL/absent | — |
| `template_url_spec` | NULL/absent | — |
| `platform_customizations` | NULL/absent | — |
| `image_crops` | NULL/absent | — |
| `degrees_of_freedom_spec` | dict | `{"creative_features_spec": {"standard_enhancements": {"enroll_status": "OPT_IN"}}}` |
| `authorization_category` | NULL/absent | — |
| `effective_authorization_category` | str | `"NONE"` |

### 6b. `targeting` sub-key shape (from the adsets response)

Real (redacted) targeting blob returned for this account's ad set:
```json
{"age_max": 65, "age_min": 18, "flexible_spec": [{"interests": [{"id": "6003092882217", "name": "Trucks"}, {"id": "6003125893549", "name": "Recruitment"}, {"id": "6003133486214", "name": "Vehicles"}, ...]}], "geo_locations": { /* present, truncated */ }}
```

- **Present:** `age_min`, `age_max`, `geo_locations`, `flexible_spec`
- **Absent:** `genders`, `publisher_platforms`, `device_platforms`, `facebook_positions`,
  `custom_audiences`, `excluded_custom_audiences`, `exclusions`

**This is a significant finding for the flattening design.** The plan's flatten list is
`age_min, age_max, genders, publisher_platforms, device_platforms, facebook_positions,
custom_audiences, excluded_custom_audiences`. Of those eight, **only two (`age_min`,
`age_max`) were observed present** in this account's real targeting blob; the other six
were absent. Absent is not proof the key never appears — Meta generally omits a targeting
sub-key entirely when it's left at its default (e.g. no gender restriction means no
`genders` key, rather than `genders: [1,2]`), so this is most likely one ad set's specific
configuration rather than evidence the flatten columns are wrongly named. But it does mean:
**this account's data cannot confirm that `genders`, `publisher_platforms`,
`device_platforms`, `facebook_positions`, `custom_audiences`, `excluded_custom_audiences`
ever populate** — only one ad set's targeting was inspected (budget-bounded), and it's
entirely plausible every ad set in this account leaves those six at their defaults. Ship the
flattening as designed, but do not treat this as a live-verified guarantee for those six
columns — flag as "not measured: only unpopulated/default cases observed."

---

## Task 6d — is the already-shipped `campaigns.special_ad_categories` broken in production?

9 API calls, 0 rate-limit hits.

**Highest-priority result: NO, it is not broken.**
```
GET {account_path}/campaigns?fields=id,special_ad_categories&limit=1
→ 200 OK, value=['EMPLOYMENT']
```
The Ad-node whitelist gate (`(#3) App must be on whitelist` on `ads.special_ad_categories`,
see Task 6 above) does **not** apply to the Campaign node for this app/account. This account
has real Special Ad Category data (`EMPLOYMENT`) on the Campaign node and Graph returned it
without complaint. `campaigns.special_ad_category_country` was also re-confirmed standalone
(200 OK, consistent with the earlier combined-call PASS).

**Full shipped field list, exactly as each table.py builds `fields=` today — every table
passes end-to-end in a single call:**

| Table | Fields sent | Result |
|---|---|---|
| `campaigns` | all 26 `COLUMNS` (joined directly, no exclusions) | **PASS** |
| `ads` | 20 `_REQUEST_FIELDS` (`creative_id`→`creative{id}`; `special_ad_categories` confirmed absent from `COLUMNS` — the removal has landed) | **PASS** |
| `adsets` | 31 `_REQUEST_FIELDS` (8 flattened-targeting columns excluded, as the file does) | **PASS** |
| `account` | 22 `_REQUEST_FIELDS` (3 derived `*_label` columns excluded, as the file does) | **PASS** |
| `adcreatives` | all 23 `COLUMNS` (joined directly) | **PASS** |

This is the definitive answer: the actual shipping configuration for all five tables, as the
code exists in the working tree right now, round-trips against the live account with zero
400s. No pre-existing production outage found anywhere.

---

## Task 6f — union of `targeting` sub-keys across 10 real ad sets

1 API call (`{account_path}/adsets`, `fields=id,targeting`, `limit=10`), 0 rate-limit hits.

Union of every key seen across all 10 ad sets' `targeting` blobs:
```
age_max, age_min, flexible_spec, geo_locations, locales, targeting_relaxation_types
```

- **Present in at least one of the 8 flatten candidates:** `age_min`, `age_max` only.
- **Absent across all 10 ad sets:** `genders`, `publisher_platforms`, `device_platforms`,
  `facebook_positions`, `custom_audiences`, `excluded_custom_audiences`.

Per the brief: `publisher_platforms`/`device_platforms` are set on most real ad sets in
typical accounts, so their absence across all 10 here is worth stating plainly rather than
waving off — **this is evidence pointing toward "this account's ad sets don't set explicit
platform/audience targeting overrides (i.e., they use Meta's defaults)" rather than evidence
of a wrong key name**, because two other things are true at the same time: (1) this same
scan surfaced `locales` and `targeting_relaxation_types` as *real* keys we'd never seen or
accounted for, proving the scan does surface whatever keys actually exist, and (2) if our
key names for the six missing ones were wrong, we would expect to see *some* differently-named
sibling key carrying that data in these 10 blobs, and none of the 6 has an obvious
undocumented substitute among the keys that did appear. That said, this remains
**genuinely unconfirmed, not proven correct** — record it as "absent across 10 ad sets in
this account; nesting/naming assumption for these 6 columns is unverified," exactly as
instructed, not as a pass.

**New finding, not previously flagged:** `locales` and `targeting_relaxation_types` are real
`targeting` sub-keys present in this account's data that appear nowhere in the plan's
enumeration of `targeting` contents (`age_min, age_max, genders, publisher_platforms,
device_platforms, facebook_positions, custom_audiences, excluded_custom_audiences,
geo_locations, flexible_spec, exclusions`). They are currently swept into the raw JSON
`targeting` blob (harmless — `targeting` stays JSON-encoded regardless), but they should be
noted as existing, undocumented-by-us sub-keys in case a future flattening pass wants them.

---

## Task 6e — wire types for the 9 passing `ads` new fields

1 API call, 0 rate-limit hits. `{account_path}/ads`,
`fields=id,configured_status,targeting,issues_info,ad_review_feedback,recommendations,conversion_specs,tracking_specs,source_ad_id,adlabels`,
`limit=1`.

| Field | Wire type | Sample |
|---|---|---|
| `configured_status` | str | `"ACTIVE"` |
| `targeting` | dict | `{"age_max": 65, "age_min": 18, "flexible_spec": [...]}` |
| `issues_info` | NULL/absent | — |
| `ad_review_feedback` | NULL/absent | — |
| `recommendations` | NULL/absent | — |
| `conversion_specs` | **list** | `[{"action.type": ["leadgen"], "leadgen": ["352569693580489"]}]` |
| `tracking_specs` | **list** | `[{"action.type": ["onsite_conversion"], "conversion_id": [...]}, ...]` |
| `source_ad_id` | str | `"0"` |
| `adlabels` | NULL/absent | — |

All non-null types match what `JSON_COLUMNS` in `ads.py` already expects (dict/list, both
JSON-encoded) — no silent-wrong-encoding risk found for the fields that returned data.
`issues_info`, `ad_review_feedback`, and `adlabels` remain unproven (NULL/absent in this
account's sample; genuinely unconfirmed, not a pass). Note `source_ad_id` returning the
string `"0"` as a sentinel rather than `null` matches the same pattern already seen on
`campaign.source_campaign_id` and `adset.source_adset_id` (both also `"0"`) — Meta appears
to use `"0"` as a not-applicable sentinel for these lineage-pointer fields across all three
entity types, rather than omitting the key.

---

## Task 7 — settling the `sort` finding: API limitation, or our own encoder bug?

4 API calls, 0 rate-limit hits, on `{account_path}/ads` (10 rows, more headroom to see
reordering than the 5-campaign sample used earlier).

**Hypothesis tested:** `meta_ads_handler.py`'s `_encode_params` (lines ~139-147)
JSON-encodes any `dict`/`list` param value. So `sort=["created_time_descending"]` (a Python
list, as originally probed in the Task 3/4 section above) is sent on the wire as the literal
query-string value `["created_time_descending"]` — brackets, quotes and all — not the bare
token Graph documents. That could fully explain the earlier "sort has no effect" result as
*our own client bug*, not an API limitation.

**Confirmed via direct inspection of `handler._encode_params()`, no API call needed:**
```
list form   {"sort": ["created_time_descending"]} encodes to {'sort': '["created_time_descending"]'}
bare string {"sort": "created_time_descending"}    encodes to {'sort': 'created_time_descending'}
```
So the earlier probe genuinely was sending a malformed value. The bare-string form is
exactly what the client sends unmodified — this is the correct test.

**Result: the hypothesis is disproven.** Sent four requests against the real `ads` edge,
10 rows each:
- no `sort` param at all
- `sort` as a Python list (the old, malformed encoding)
- `sort` as a bare string, `"created_time_descending"`
- `sort` as a bare string, `"created_time_ascending"`

All four returned the **exact same 10 ad IDs in the exact same order**
(`120249722789810398, 120235711481220398, 120235638454990398, ...`, newest-`created_time`
first throughout). Ascending did not reverse the order relative to descending, and neither
differed from no-sort at all.

**Conclusion: this is not a client-side encoding bug — `sort` has no observable effect on
the `ads` edge even when sent in the exact bare-string form the client already produces
correctly for scalar params.** The original Task 4 finding stands, now on stronger evidence
(properly-encoded bare string, tested on a 10-row edge, both directions). Two explanations
remain open, neither confirmed: the API silently ignores an otherwise-valid `sort` param on
this edge, or `sort` requires a different key/shape entirely that wasn't tried (e.g. maybe
it only works via a different param name, or on a different edge such as `/insights`). This
was **not tested further** — would need a raw HTTP capture against the live Graph API
outside `graph_get`, or explicit confirmation from Meta's own current docs for this API
version, which is out of scope for a GET-only measurement pass.

---

## Corrections to the plan

1. **`sort` does not demonstrably work, and it is confirmed NOT a client-encoding bug.**
   The plan states "`sort` (e.g. `created_time_descending`) — works on listing edges."
   Measured across two rounds (Task 4 on campaigns, Task 7 on ads): every `sort` value
   tried — JSON-list-encoded, correctly-encoded bare string, ascending, descending, and an
   invalid suffix — produced identical row order, indistinguishable from no `sort` param at
   all. Task 7 specifically ruled out the leading suspect (`_encode_params` JSON-encoding
   list values into a malformed wire string) by testing the properly-encoded bare-string
   form directly and getting the same null result. `sort` should **not** be assumed to work
   for Phase 2/3 planning — treat it as unverified/likely non-functional on these two edges
   until confirmed otherwise via a raw HTTP capture or updated Meta docs.
2. **adcreatives filtering rejection is `OP_UNSUPPORTED`, not `FIELD_UNSUPPORTED`,
   contradicting the plan's stated mechanism** ("no filtering of any kind"). See the
   dedicated adcreatives section above — the practical conclusion (nothing usable) still
   holds for every operator in the natural operator set of `id` and `name`, but the plan's
   given reason for why is backwards, which matters if anyone later tries a different
   operator on adcreatives expecting a `FIELD_UNSUPPORTED`-style blanket rejection.
3. **`adset.configured_status` and `ad.configured_status` are confirmed not filterable at
   all** — this extends (rather than contradicts) the plan, which had only established
   `campaign.configured_status` was unfilterable; now the pattern is confirmed to hold
   uniformly across all three listing edges.
4. **`adset.end_time` is the one field with an asymmetric range-operator matrix**:
   GREATER_THAN/LESS_THAN/IN_RANGE all OK but NOT_IN_RANGE is OP_UNSUPPORTED. Every other
   time-typed field measured (`created_time`, `updated_time`, `start_time` on both edges)
   accepted all four range operators uniformly. Anyone writing the `FILTERABLE` spec should
   special-case this field rather than assuming time fields are homogeneous.
5. Everything else measured (AND semantics, bare-vs-dotted field form, CONTAIN
   case-insensitivity, the new adsets/ads field×operator cells) is **consistent with or an
   extension of** the plan — no other contradictions found.
6. **`ads.special_ad_categories` (one of the 39 new Phase-1 fields) 400s for this
   app/account** with a whitelist error (`(#3) App must be on whitelist`), not a
   nonexisting-field error. Since this is a Pattern-A table sending one combined `fields=`
   list, shipping it as-is would 400 the *entire* `ads` table for every column, not just this
   one. Must be dropped or config-gated before merge — see Task 6 section for detail.
7. **The `targeting` flatten list is only two-eighths confirmed populated, now checked
   across 10 ad sets (not just one).** The plan's flatten columns (`age_min, age_max,
   genders, publisher_platforms, device_platforms, facebook_positions, custom_audiences,
   excluded_custom_audiences`) are all real Meta sub-keys, but across 10 real ad sets in
   this account only `age_min`/`age_max` ever appeared; the other six never did. See Task 6f
   for the full reasoning — most likely this account's ad sets simply don't set
   platform/audience targeting overrides (Meta omits unset keys rather than defaulting them
   in), not that the six column names/nesting are wrong, but this remains **unverified, not
   contradicted** — record the six as "unconfirmed," not "wrong."
8. **NOT broken: `campaigns.special_ad_categories` (shipped pre-Phase-1) works fine in
   production**, and every table's *full, current, exactly-as-shipped* field list round-trips
   with zero 400s (Task 6d). The Ad-node whitelist gate that rejects
   `ads.special_ad_categories` does not apply to the Campaign node — this account has real
   `EMPLOYMENT` Special Ad Category data on campaigns and Graph returns it without
   complaint. This rules out the live-production-outage risk the team lead flagged; it is
   not a correction to the plan so much as confirmation nothing is currently on fire.
9. **New, previously-unflagged `targeting` sub-keys exist in this account's real data:
   `locales` and `targeting_relaxation_types`.** Neither appears in the plan's enumeration
   of `targeting` contents. They're harmless today (swept into the raw JSON `targeting`
   blob regardless), but worth knowing about for any future flattening pass.

---

## Not measured

- **The bogus-operator-first discovery trick did not save calls as intended** (see
  methodology note at the top) — Meta validates the operator name globally before
  field-level checks, so every field still needed its full real-operator sweep regardless.
  This means the adsets/ads matrices used more calls than planned, which is why several
  lower-value cells were explicitly skipped (see next points) to stay close to budget.
- **`adset.campaign_id`, `ad.adset_id`, `ad.campaign_id`, `ad.creative_id`** — not re-probed
  with fresh calls this run; relied on the prior run's confirmation that they are
  NOT_EQUAL-rejected as FIELD_UNSUPPORTED (per the brief's instruction not to re-probe
  already-confirmed-unfilterable fields). Only NOT_EQUAL was ever tested on these in any
  run — EQUAL/IN/NOT_IN were never tried, though a field rejected at the FIELD_UNSUPPORTED
  level for one operator is expected to reject all operators (Meta's FIELD_UNSUPPORTED
  message doesn't reference the operator at all, so this generalization is safe, unlike
  inferring OP_UNSUPPORTED cells).
- **`adset.name` and `ad.name`**: `IN`/`NOT_IN` not measured (not in the plan's operator
  mapping table, so not needed for the `build_filtering` implementation; skipped to save
  budget).
- **`adset.billing_event` `IN`/`NOT_IN`**: got an inconclusive 500 error rather than a clean
  classification (see adsets section) — not re-tried under budget constraints. Recommend a
  targeted single-call retry before shipping a `FILTERABLE` spec entry for this field/op
  pair.
- **Task 3b (bare vs dotted field form)** was only checked for one field/value
  (`campaign.status`/`status` = `ACTIVE`). Not verified for other fields or edges (adsets,
  ads) under budget constraints.
- **Task 4 sort**: could not capture a verbatim rejection message enumerating valid sort
  values, because no sort value tried (including the deliberately invalid one) was ever
  rejected. Whether other bogus formats (e.g. a completely malformed string, or a list of
  garbage) produce a rejection was not tested.
- **adsets/ads matrices**: `promoted_object` (adsets) was only probed with `EQUAL`, not the
  other id/enum operators — a JSON-object-typed field rejected at `EQUAL` is treated here as
  settled (not filterable), consistent with `FIELD_UNSUPPORTED` semantics not being
  per-operator, but this is technically only one data point.
- **adcreatives**: only `id` and `name` were probed (per the brief's Task 5 scope); the
  other ~70 undocumented fields on this edge were not touched.
- ~~**Task 6**: wire types for the 9 passing `ads` new fields~~ — **resolved by Task 6e**:
  `configured_status`/`source_ad_id` str, `targeting` dict, `conversion_specs`/
  `tracking_specs` list, `issues_info`/`ad_review_feedback`/`adlabels` NULL/absent (still
  unconfirmed, not a pass).
- ~~**Task 6b**: only one ad set's `targeting` blob was inspected~~ — **superseded by Task
  6f**, which scanned 10 ad sets. Still only `age_min`/`age_max` of the 8 flatten candidates
  were ever observed populated (see Corrections #7) — recommend spot-checking an ad set
  known to have non-default gender/platform/audience targeting (e.g. from a different,
  more targeting-heavy account) before treating those 6 flatten columns as fully verified;
  none was available/identified in this account's 10-ad-set sample.
- **Task 7 (sort encoder hypothesis)**: only tested on `ads`; did not re-verify on
  `adsets`/`adcreatives`, and did not attempt a raw HTTP capture to see the literal bytes
  Graph received, nor try alternate param shapes/names for sort. The conclusion ("not a
  client encoding bug, still no observable effect") is solid for `campaigns`+`ads`, but
  "sort doesn't work anywhere in this API" is not proven, only "doesn't work in the two
  forms and two edges tested."
- **Task 6d full-shipped-list check**: only proves each table's fields all round-trip
  together in one call with `limit=1` — does not prove every row in the account returns
  clean values for every field (e.g. a malformed row deep in a large result set), nor does
  it exercise pagination, non-default `limit`, or JOIN paths.
- Total API calls across all tasks this run: **70 (adsets) + 35 (ads) + 9 (adcreatives) + 16
  (semantics/sort, across two follow-up scripts) + 1 (ascending-sort decisive check) + 14
  (Task 6, new-field validation) + 4 (Task 7, sort encoder settlement) + 9 (Tasks 6d/6f/6e)
  = 158**, over the ~120 budget guideline — spent on closing the AND/OR/sort questions
  decisively, fully validating all 39 new field names, ruling out a potential live
  production outage, and closing the two "not measured" gaps the team lead flagged as
  highest priority. Zero rate-limit (80004) hits across the entire run.

