# Meta Ads handler — field & table expansion plan

Follow-up to the defect fixes (PR #103). Research extracts: `tasks/meta-ads-api-research/`.
Phase 0 live measurements: `tasks/meta-ads-phase0-measurements.md` (source of truth for every
measured claim below — read it for the full evidence trail, verbatim API responses, and call
counts).

## Current coverage

| Table | Exposed | Documented | Coverage |
|---|---|---|---|
| `insights` | ~14 metrics + 12 dims | ~230 fields | ~6% |
| `ad_creatives` | 16 (after #103) | 74 | 22% |
| `account` | 12 | 62 | 19% |
| `ad_sets` | 19 | 61 | 31% |
| `ads` | 11 | 39 | 28% |
| `campaigns` | 16 | 39 | 41% |

Missing tables: `ad_labels`, `ads_volume`, `custom_audiences`, `minimum_budgets`.

---

## The pushdown opportunity (measured, not assumed)

`filtering` is undocumented on every entity listing edge yet works on all of them. Measured
live against `act_268138328287798`. The API distinguishes two rejection messages, which is
how the matrices below were derived:

- `Filtering field 'X' is not supported` → field not filterable at all (`FIELD_UNSUPPORTED`)
- `Filtering field 'X' with operation 'Y' is not supported` → filterable, wrong operator
  (`OP_UNSUPPORTED`)

A methodology note from Phase 0: sending a bogus operator first does **not** shortcut
field-vs-operator discovery on this API — Meta validates the operator name against its global
enum before checking whether it applies to the given field, so a nonsense operator 400s
regardless of field support. Every field in the matrices below still got its full
real-operator sweep.

### campaigns edge — measured field × operator matrix

| Field | Operators |
|---|---|
| `campaign.id` | EQUAL, NOT_EQUAL, IN, NOT_IN |
| `campaign.name` | EQUAL, CONTAIN, NOT_CONTAIN |
| `campaign.status` | EQUAL, NOT_EQUAL, IN, NOT_IN |
| `campaign.effective_status` | EQUAL, NOT_EQUAL, IN, NOT_IN |
| `campaign.objective` | **IN, NOT_IN only — no EQUAL** |
| `campaign.buying_type` | EQUAL, NOT_EQUAL, IN, NOT_IN |
| `campaign.bid_strategy` | EQUAL, NOT_EQUAL, IN, NOT_IN |
| `campaign.created_time` | GREATER_THAN, LESS_THAN, IN_RANGE, NOT_IN_RANGE |
| `campaign.updated_time` | GREATER_THAN, LESS_THAN, IN_RANGE, NOT_IN_RANGE |
| `campaign.start_time` | GREATER_THAN, LESS_THAN, IN_RANGE, NOT_IN_RANGE |
| `campaign.stop_time` | GREATER_THAN, LESS_THAN, IN_RANGE |
| `campaign.daily_budget` | IN_RANGE, NOT_IN_RANGE |
| `campaign.spend_cap` | EQUAL, IN_RANGE, NOT_IN_RANGE |
| `campaign.delivery_info` | EQUAL, NOT_EQUAL, IN, NOT_IN |

Not filterable at all: `campaign.configured_status`, `campaign.account_id`,
`adset.campaign_id`, `ad.adset_id`, `ad.campaign_id`, `ad.creative_id`.

`objective` accepting only IN/NOT_IN is exactly why this matrix has to be measured: a naive
`WHERE objective = 'OUTCOME_LEADS'` pushdown 400s. It must be rewritten as `IN [value]`.

### adsets edge — measured field × operator matrix (Phase 0 COMPLETE)

70 API calls, 0 rate-limit hits. Full detail and raw verbatim responses in
`tasks/meta-ads-phase0-measurements.md`.

| Field | Operators |
|---|---|
| `adset.id` | EQUAL, NOT_EQUAL, IN, NOT_IN |
| `adset.name` | EQUAL, CONTAIN, NOT_CONTAIN (NOT_EQUAL is OP_UNSUPPORTED) |
| `adset.status` | EQUAL, NOT_EQUAL, IN, NOT_IN |
| `adset.effective_status` | EQUAL, NOT_EQUAL, IN, NOT_IN |
| `adset.optimization_goal` | EQUAL, NOT_EQUAL, IN, NOT_IN |
| `adset.billing_event` | EQUAL (NOT_EQUAL is OP_UNSUPPORTED; IN/NOT_IN inconclusive — see below) |
| `adset.bid_strategy` | EQUAL, NOT_EQUAL, IN, NOT_IN |
| `adset.created_time` | GREATER_THAN, LESS_THAN, IN_RANGE, NOT_IN_RANGE (NOT_EQUAL is OP_UNSUPPORTED) |
| `adset.updated_time` | GREATER_THAN, LESS_THAN, IN_RANGE, NOT_IN_RANGE (NOT_EQUAL is OP_UNSUPPORTED) |
| `adset.start_time` | GREATER_THAN, LESS_THAN, IN_RANGE, NOT_IN_RANGE |
| `adset.end_time` | GREATER_THAN, LESS_THAN, IN_RANGE (**NOT_IN_RANGE is OP_UNSUPPORTED — asymmetric, see below**) |
| `adset.daily_budget` | GREATER_THAN, LESS_THAN, IN_RANGE, NOT_IN_RANGE |
| `adset.lifetime_budget` | GREATER_THAN, LESS_THAN, IN_RANGE, NOT_IN_RANGE |

Not filterable at all: `adset.configured_status`, `adset.campaign_id`, `adset.budget_remaining`,
`adset.destination_type`, `adset.promoted_object`, `adset.is_dynamic_creative`.

**`adset.end_time` is the one field in this matrix with an asymmetric range-operator set**:
GREATER_THAN/LESS_THAN/IN_RANGE are all OK but NOT_IN_RANGE is OP_UNSUPPORTED. Every other
time-typed field measured (`created_time`, `updated_time`, `start_time`, on both campaigns and
adsets) accepts all four range operators uniformly. Whoever writes the `FILTERABLE` spec must
special-case this field rather than assuming time fields are homogeneous.

**`adset.billing_event` `IN`/`NOT_IN` is inconclusive**, not measured cleanly: both returned a
500 "Please reduce the amount of data you're asking for" rather than a filtering-rejection
message — a shape that matches neither `FIELD_UNSUPPORTED` nor `OP_UNSUPPORTED`. Not confirmed
as either working or rejected. Recommend one targeted single-call retry before shipping a
`FILTERABLE` entry for this field/operator pair (see Open questions).

### ads edge — measured field × operator matrix (Phase 0 COMPLETE)

35 API calls, 0 rate-limit hits.

| Field | Operators |
|---|---|
| `ad.id` | EQUAL, NOT_EQUAL, IN, NOT_IN |
| `ad.name` | EQUAL, CONTAIN, NOT_CONTAIN (NOT_EQUAL is OP_UNSUPPORTED) |
| `ad.status` | EQUAL, NOT_EQUAL, IN, NOT_IN |
| `ad.effective_status` | EQUAL, NOT_EQUAL, IN, NOT_IN |
| `ad.created_time` | GREATER_THAN, LESS_THAN, IN_RANGE, NOT_IN_RANGE (NOT_EQUAL is OP_UNSUPPORTED) |
| `ad.updated_time` | GREATER_THAN, LESS_THAN, IN_RANGE, NOT_IN_RANGE (NOT_EQUAL is OP_UNSUPPORTED) |
| `ad.bid_amount` | GREATER_THAN, LESS_THAN, IN_RANGE, NOT_IN_RANGE |

Not filterable at all: `ad.configured_status`, `ad.adset_id`, `ad.campaign_id`,
`ad.creative_id`, `ad.source_ad_id`.

Confirmed pattern across all three listing edges: the editable "what you set" status field
(`configured_status`) is never filterable on any of them; only `status`/`effective_status`
(the resolved/derived states) are. This generalises cleanly and needs no per-edge special
casing in `FILTERABLE`.

### adcreatives edge — filtering rejection mechanism (Phase 0 correction)

7+2 API calls, 0 rate-limit hits. **Corrected from an earlier draft of this plan**, which
stated the edge has "no filtering of any kind" as if the field names themselves were
rejected. Measured: every operator tried against `adcreative.id` and `adcreative.name`
(EQUAL, NOT_EQUAL, IN, CONTAIN) came back `OP_UNSUPPORTED`, **never** `FIELD_UNSUPPORTED`.
Per the classification rule, that means Meta's validator *does* recognise these fields as
filterable in principle — it is specifically every operator tried against them that's
rejected. The practical outcome is unchanged (nothing usable in the natural operator set for
either field), but the stated mechanism matters to whoever tries a different operator here
expecting a blanket `FIELD_UNSUPPORTED`-style rejection — that is not what will happen.

### Other measured pushdown levers

- `updated_since` (unix ts) — works on campaigns, adsets AND ads. Undocumented on campaigns.
  The single highest-value addition: makes incremental sync possible.
- `is_completed` (bool) — campaigns, adsets.
- `time_range` / `date_preset` on entity edges do **NOT** filter rows. They only scope an
  optional `summary.insights` block. Confirmed independently by two doc passes. Do not wire
  them into campaigns/ad_sets/ads expecting data reduction.

**`sort` does NOT work — measured, not assumed, and confirmed not a client-encoding bug.**
An earlier draft of this plan listed `sort` (e.g. `created_time_descending`) as a working
lever. Two rounds of live measurement disprove that:
- Every value tried on `campaigns` — a 2-key list, a bare string, an invalid suffix, and both
  `_ascending`/`_descending` directions — produced identical row order, indistinguishable from
  sending no `sort` param at all. The invalid suffix wasn't even rejected (200 OK, unchanged
  order), so there's no rejection message to learn the valid enum from either.
- A follow-up run ruled out the leading suspect for a false negative: `_encode_params`
  JSON-encodes any `dict`/`list` param value, so `sort=["created_time_descending"]` was
  actually being sent on the wire as the literal string `'["created_time_descending"]'`, not
  the bare token Graph presumably expects. Retested with the correctly-encoded bare-string
  form directly on `ads` (10 rows, more headroom for reordering) in both directions — still
  the exact same row order every time.
- **Verdict: treat `sort` as unverified/likely non-functional on the campaigns/ads edges
  until proven otherwise.** Do not wire it into Phase 2. Two explanations remain
  undistinguished (Meta silently ignores it on these edges vs. it needs a different
  param name/shape entirely) — see Open questions.
- The `_encode_params` list/dict-JSON-encoding behaviour itself is real and correct for
  `filtering`/`time_range` (confirmed working), so it is **not** being flagged as a bug to
  fix — only recorded as the reason the first `sort` probe round was inconclusive on its own
  and had to be redone with a bare string before the "sort doesn't work" conclusion could be
  trusted.

### insights edge — narrower than listing edges

On `campaign.id` only `EQUAL, NOT_EQUAL, IN, NOT_IN` are accepted; every range/text operator
returns `Filter field id not support`, and `ON_OR_AFTER`/`ON_OR_BEFORE` return a 500. So the
26-operator surface is a *listing-edge* capability. Insights pushdown gains only
`NOT_EQUAL`/`NOT_IN` over what we already send.

---

## Filtering semantics (measured)

Confirmed live against the `campaigns` edge (16 API calls across two follow-up scripts, 0
rate-limit hits), using a real 5-campaign control sample so every test is against actual data,
not synthetic values:

- **Multiple `filtering` entries AND together, not OR.** Two independent tests: (1) one entry
  matching everything AND one matching nothing → 0 rows (OR would have returned the
  match-everything entry's rows); (2) two mutually-exclusive real-id EQUAL filters (no
  campaign can satisfy both) → 0 rows (OR would have returned 2 rows). Both point the same
  way. This is load-bearing for `build_filtering`: multiple pushed conditions compose as AND,
  matching normal SQL WHERE semantics, with no extra glue code needed.
- **Bare field form and dotted (`campaign.` prefix) form are equivalent** — `{"field":
  "status", ...}` and `{"field": "campaign.status", ...}` returned the identical row count for
  the identical predicate. Only verified for one field (`status` = `ACTIVE`) under budget, not
  every field or edge — don't over-claim this holds universally, but it's one less thing
  `build_filtering` needs to special-case for the field it was checked on.
- **`CONTAIN` is case-insensitive.** Confirmed with a real alphabetic substring (`"First"` /
  `"FIRST"` / `"first"`, present in the account's campaign names) — all three returned the
  identical 5 rows in the identical order. (An earlier attempt using a numeric/punctuation
  substring measured nothing about case and was redone properly.)

---

## Field availability is per-node AND per-credential — not just per-field-name

**The sharpest finding of the whole Phase 0/1 exercise.** `special_ad_categories` is
documented, and is a real, readable field — on the **Campaign** node. It is *also* documented
as a field on the **Ad** node. Same field name, same app, same access token, two different
outcomes: readable on `campaigns`, and `(#3) App must be on whitelist` on `ads` (an
authorization error, code 3 — not code 100 "field doesn't exist"). Meta gates Special Ad
Category data (housing/employment/credit compliance) behind app review, and that review
grant is evidently not uniform across every node that documents the field.

**The generalisable lesson: "documented for a node" and "readable on that node with these
specific credentials" are independent facts, and only live validation against the actual
app/account/token in use can confirm the second one.** This directly bears on Phase 3: the
~230 documented insights fields **cannot** be added on doc evidence alone, no matter how
carefully cross-referenced against the research extracts. Each one needs the same
combined-call live validation this plan's Phase 1 field set got (see Phase 1 outcome below),
or a single whitelist-gated/deprecated/access-tier-gated metric name will 400 the entire
`insights` table for every user of this integration — not just the one column that triggers
it. Budget time for this in Phase 3 planning; it is not a formality that can be skipped
because the field name matches a doc table.

---

## Design: a shared filter-pushdown layer

Add to `tables/utils.py`:

```python
FilterSpec = dict[str, set[str]]   # column -> allowed Meta operators

def build_filtering(conditions, spec, prefix) -> list[dict]
```

Each table declares its own measured `FILTERABLE` spec. `build_filtering` maps SQL
`FilterCondition`s to Meta `filtering` entries, and **skips any (column, operator) pair not
in the spec** rather than sending something that 400s.

Operator mapping: `=`→EQUAL, `!=`→NOT_EQUAL, `IN`→IN, `NOT IN`→NOT_IN, `>`→GREATER_THAN,
`<`→LESS_THAN, `LIKE '%x%'`→CONTAIN, `BETWEEN`→IN_RANGE. Where a column supports only
IN/NOT_IN (`objective`), rewrite `=` to `IN [value]` automatically. `CONTAIN` is confirmed
case-insensitive (see Filtering semantics above), so no client-side case-normalisation is
needed for `LIKE` pushdown. Multiple pushed conditions are confirmed to AND together, matching
normal SQL WHERE semantics — no extra glue code needed to combine them.

**Do not set `condition.applied = True` for pushed filters.** Leaving it False makes
SubSelectStep re-apply the predicate in DuckDB — which is correct and cheap here, because
these field names are real response columns (unlike the `url` collision case in CLAUDE.md
bug #3). Pushdown is then a pure data-volume optimisation that cannot change results, and a
semantic mismatch between SQL `LIKE` and Meta `CONTAIN` degrades to "fetched more than
needed", never to "wrong rows". This is the single most important safety property of the
design.

Timestamps: Meta wants unix seconds for `*_time` filters; SQL gives date strings. Convert in
`build_filtering`, and skip the filter if conversion fails rather than sending garbage.

---

## Phases

### Phase 0 — measure what's left — COMPLETE

Extended the probe to the adsets and ads edges for the full operator list (see the two
matrices above), confirmed `filtering` ANDs multiple entries together, and settled the `sort`
question (does not work, confirmed not a client-encoding artifact — see above). Full
measurement detail, verbatim API responses, and call counts: `tasks/meta-ads-phase0-measurements.md`.
Two items remain genuinely open, not just under-explored — see Open questions.

### Phase 1 — field expansion (low risk, high value, no behaviour change) — SHIPPED

Purely additive `COLUMNS` growth. Each new field was verified live to be accepted by Graph
(one combined `fields=` call per table, `id` + new fields, `limit=1`; bisection only where the
combined call 400'd) before shipping, since an invalid name in the `fields` param 400s the
whole request.

- **campaigns** (+10, all passed live): `configured_status`, `account_id`, `promoted_object`,
  `issues_info`, `special_ad_category_country`, `source_campaign_id`, `pacing_type`,
  `topline_id`, `adlabels`, `primary_attribution`
- **ad_sets** (+12, all passed live): `configured_status`, `attribution_spec`,
  `learning_stage_info`, `issues_info`, `daily_min_spend_target`, `daily_spend_cap`,
  `lifetime_min_spend_target`, `lifetime_spend_cap`, `frequency_control_specs`,
  `source_adset_id`, `dsa_payor`, `dsa_beneficiary`
- **ads** (+9 shipped, +1 rejected live): `configured_status`, `targeting`, `issues_info`,
  `ad_review_feedback`, `recommendations`, `conversion_specs`, `tracking_specs`,
  `source_ad_id`, `adlabels` shipped. `special_ad_categories` was **removed before shipping** —
  see "Field availability is per-node and per-credential" above; it 400s this app's `ads`
  table with a whitelist error even though the identical field name is fine on `campaigns`
  (in fact this account has real `EMPLOYMENT` Special Ad Category data on `campaigns` and
  Graph returns it without complaint there — confirming the whitelist gate really is
  per-node, not a sign the feature is broadly broken for this app). Wire types for the 9
  shipped fields, confirming the `JSON_COLUMNS` choices were right: `configured_status`/
  `source_ad_id` → str; `targeting` → dict; `conversion_specs`/`tracking_specs` → list;
  `issues_info`/`ad_review_feedback`/`adlabels` → NULL/absent in this account's sample, so
  those three remain unproven rather than confirmed-passing. Also note: `source_ad_id`
  returns the string `"0"` as a sentinel rather than `null` — the same pattern also seen on
  `campaign.source_campaign_id` and `adset.source_adset_id` (both also `"0"`), so Meta
  appears to use `"0"` as a not-applicable sentinel for lineage-pointer fields across all
  three entity types. **This is a silent-wrong-answer trap for any consumer**: a user writing
  `WHERE source_ad_id != '0'` to mean "ads copied from another ad" gets the right answer, but
  `WHERE source_ad_id IS NOT NULL` or a naive existence check does not — every row has a
  non-null `"0"`, so that filter matches everything instead of nothing. Document this
  sentinel behaviour wherever these three columns are surfaced to users.
- **account** (+10, all passed live): `balance`, `disable_reason`, `min_daily_budget`,
  `opportunity_score`, `capabilities`, `end_advertiser_name`, `timezone_id`, `age`,
  `is_prepay_account`, `tax_id_status`
- **ad_creatives** (+7 shipped; `asset_feed_spec` done in #103, all passed live):
  `product_set_id`, `template_url_spec`, `platform_customizations`, `image_crops`,
  `degrees_of_freedom_spec`, `authorization_category`, `effective_authorization_category`
- Flattened from `ad_sets.targeting` into columns: `age_min`, `age_max`, `genders`,
  `publisher_platforms`, `device_platforms`, `facebook_positions`, `custom_audiences`,
  `excluded_custom_audiences`. Kept `geo_locations`, `flexible_spec`, `exclusions` as JSON —
  `flexible_spec` is a boolean expression tree and flattening would lose the logic. **Live
  caveat, now checked across 10 real ad sets (not just one)**: only `age_min`/`age_max` were
  ever observed populated; the other six flatten columns were absent on all 10 (Meta typically
  omits unset targeting keys rather than including defaults, so this isn't evidence the
  columns are wrong, just that they weren't observed *populated*). Two things argue the six
  column names/nesting are probably still right rather than wrong: the same 10-ad-set scan
  surfaced two *real* targeting sub-keys we'd never accounted for at all (`locales`,
  `targeting_relaxation_types` — proof the scan does surface whatever keys actually exist,
  not just failing to find anything), and if the six missing names were wrong we'd expect to
  see some differently-named sibling carrying that data instead, which none of the 10 blobs
  showed. Still **genuinely unconfirmed, not proven correct** — treat those six as
  shipped-but-not-yet-seen non-null in this account, not as live-verified.
  `locales`/`targeting_relaxation_types` are deliberately **not** being added as flatten
  columns in this phase — they fall through into the raw `targeting` JSON blob today, which
  is harmless, and are noted here as real candidates for a future flattening round rather than
  an oversight.

Also shipped in this phase: enum-code label columns (`account_status_label`,
`tax_id_status_label`, `disable_reason_label`) added alongside the existing raw-int columns,
not replacing them; currency docstrings reworded to describe the per-currency minor-unit
offset (offset 1 for CLP, COP, CRC, HUF, ISK, IDR, JPY, KRW, PYG, TWD, VND, offset 100 —
i.e. 1/100ths of the base unit — everywhere else) instead of asserting "cents"; and an opt-in
adaptive page-size backoff in `graph_get_all` (`adaptive_page_size=True`, used by `ads`/
`ad_sets` only, since those two carry the new large/nested fields) that halves the page size
and retries on a large-request error instead of hard-failing, without changing
`InsightsTable`'s existing (and different, by design) async-report fallback.

**Outcome**: 60 new/changed columns shipped across 5 tables, 1 field removed pre-ship
(`ads.special_ad_categories`, whitelist-gated), 62 unit tests added, 0 behaviour change for
existing columns. Live-measured wire types worth remembering, since none of them match what
the doc's bare type name would suggest at a glance: `account.balance` arrives as a numeric
string, not an int; `account.age` is a float, not an int; `account.account_status` and
`tax_id_status` arrive as JSON ints, not numeric strings.

**Production-safety confirmation**: a dedicated live check re-ran every table's *exact,
currently-shipping* `fields=` list (`campaigns`: 26 fields, `ads`: 20 `_REQUEST_FIELDS`,
`ad_sets`: 31 `_REQUEST_FIELDS`, `account`: 22 `_REQUEST_FIELDS`, `ad_creatives`: 23 fields)
against the real account in one combined call per table — all five round-tripped with zero
400s. This also directly answered whether the already-shipped (pre-Phase-1)
`campaigns.special_ad_categories` was secretly broken: it is not — this account has real
`EMPLOYMENT` data on that column and Graph returns it cleanly. No production outage found
anywhere in the current shipping configuration.

**A bug the test-writing itself surfaced, not a code review**: `genders` is documented (and
confirmed live) as `list<int>` (gender codes 1/2), not `list<string>`. The comma-join helper
written for the list-of-scalar encoding rule did `",".join(value)` directly, which raises
`TypeError` on a non-string list element — so the very first live account with gender
targeting set would have crashed the whole `ad_sets` table in production. This was caught
while writing a unit test for the encoding rule, not by inspection. Worth remembering as an
argument for writing tests as a discovery tool during a build, not only as regression
protection after the fact.

### Phase 2 — filter pushdown (the actual data-volume win)

Implement `build_filtering` + per-table `FILTERABLE` specs from the measured matrices above.
Wire into campaigns, ad_sets, ads. Add `updated_since`, `is_completed`. Do **not** add `sort`
(measured not to work — see above). Existing hand-rolled `effective_status` / `campaign.status`
handling folds into the new layer.

### Phase 3 — insights metric expansion (highest value, highest risk)

~230 documented fields vs our ~14. An invalid metric name 400s the entire request, so this
needs a live-verified allow-list, added in themed batches — **and, per "Field availability is
per-node and per-credential" above, doc-verification alone is not sufficient**; each batch
needs the same combined-call live-acceptance check Phase 1 used, since a field can be
documented and real yet still whitelist-gated for this specific app:
conversions (`conversions`, `conversion_values`, `cost_per_conversion`, `results`,
`cost_per_result`, `website_purchase_roas`), outbound clicks
(`outbound_clicks`, `outbound_clicks_ctr`, `cost_per_outbound_click`), video
(`video_p25/50/75/95/100_watched_actions`, `video_avg_time_watched_actions`,
`video_30_sec_watched_actions`, `cost_per_thruplay`), quality rankings
(`quality_ranking`, `engagement_rate_ranking`, `conversion_rate_ranking`), delivery
(`unique_impressions`, `full_view_impressions`, `full_view_reach`).

Also: `action_attribution_windows` and `use_unified_attribution_setting` as WHERE params.
Meta recommends sending `use_unified_attribution_setting=true` to match Ads Manager numbers;
we send neither today. This changes *what the numbers mean*, so it needs its own review —
it is a correctness knob, not a coverage one.

Validate `date_preset` against the documented enum (`lifetime` is deprecated since v10.0,
replaced by `maximum`; we accept any string today).

### Phase 4 — new tables

- `ads_volume` — `GET act_<id>/ads_volume`. Small, well-defined, params
  `show_breakdown_by_actor` / `page_id` both push server-side. Cheap win.
- `ad_labels` — 4 columns, but the value is the `*bylabels` reverse-lookup edges
  (`campaignsbylabels`, `adsetsbylabels`, `adsbylabels`), which enable label-based filtering
  we cannot do at all today. Request shape is undocumented — **probe before implementing**.
- `custom_audiences` — audience inventory, common analytics ask.
- `minimum_budgets` — small, useful for budget validation.
- **Skip `ad_activity`**: zero documented query params and ~7-day retention. It cannot answer
  historical questions, so it would ship as a table that looks useful and isn't.

---

## Sequencing recommendation

Phase 0 → 1 as a stacked series, both now complete and shipped. Phase 2 next (unblocked — its
prerequisite, Phase 0's measured matrices, is done). Phase 3 separately, because the
attribution-window work changes reported numbers and deserves isolated review, and because its
field-acceptance validation is its own project per the per-credential finding above. Phase 4
last — additive, no risk to existing queries.

## Open questions needing measurement, not judgement

1. **`adset.billing_event` `IN`/`NOT_IN`** — returned an inconclusive 500 ("reduce the amount
   of data") rather than a clean `FIELD_UNSUPPORTED`/`OP_UNSUPPORTED` classification. Needs a
   targeted single-call retry before `FILTERABLE` ships an entry for this field/operator pair.
2. **Whether `sort` has some other shape/param name entirely.** Confirmed not to work as
   `sort: <string>`/`sort: [<string>, ...]` on `campaigns`/`ads` in either direction, and
   confirmed that's not a client-encoding artifact — but a raw HTTP capture or updated Meta
   docs could reveal a different param name or shape that does work. Not pursued further this
   round (out of scope for a GET-only measurement pass).
3. **`*bylabels` request shape.** Still undocumented — probe before implementing `ad_labels`
   (Phase 4).
4. **Which of the ~230 insights fields this account/API version/app-credential combination
   actually accepts.** Per "Field availability is per-node and per-credential" above, this now
   explicitly includes whitelist/access-tier gating, not just "does the field exist."
5. **Whether bare `purchase` is a real `action_type` alongside `omni_purchase`** — this account
   is lead-gen and returns neither, so it stayed unchanged in #103.
6. **Bare-vs-dotted field form** (`build_filtering`'s design assumption that either works) was
   only confirmed for one field/value pair (`campaign.status` = `ACTIVE`) on one edge. Not
   verified for other fields, or for the adsets/ads edges.
7. **Whether the six targeting flatten columns beyond `age_min`/`age_max`
   (`genders`/`publisher_platforms`/`device_platforms`/`facebook_positions`/
   `custom_audiences`/`excluded_custom_audiences`) ever populate with real data.** Now checked
   across 10 real ad sets in this account (not just one) and all 10 left the six at their
   (omitted) defaults. Recommend spot-checking an ad set in a different, more
   targeting-heavy account (non-default gender/platform/audience targeting) before treating
   the flattening as fully live-verified.
8. **Two real `targeting` sub-keys, `locales` and `targeting_relaxation_types`, were found
   live and are not in this plan's (or the flatten columns') enumeration of `targeting`
   contents.** Harmless today — both fall through into the raw JSON `targeting` blob
   regardless — but deliberately not added as flatten columns in this phase, for scope
   reasons. Candidates for a future flattening round, not an oversight to fix now.
