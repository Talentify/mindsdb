# Meta Ads handler — defect fixes (spec)

Three defects found by auditing the shipped handler against the Meta Marketing API
reference docs (research extracts in `tasks/meta-ads-api-research/`) plus live read-only
probes against the configured account.

Branch: `fix/meta-ads-defects`, stacked on `feat/meta-ads-handler` (PR #101).
Scope: correctness only. Field/table expansion is a separate follow-up.

**Guiding lesson from this audit:** Meta's reference pages are incomplete. `filtering` is
undocumented on every entity listing edge yet works on all of them, with a 26-operator
vocabulary. So: never hard-reject something merely because the docs omit it. Prefer
"send it and let the API decide, with a warning" over a client-side rejection, except
where we are validating against a list the docs *do* enumerate exhaustively.

---

## Defect 1 — `ad_creatives` returns empty text for whole creative classes

File: `mindsdb/integrations/handlers/meta_ads_handler/tables/ad_creatives.py`

`_flatten_creative()` checks top-level → `link_data` → `video_data` only. Confirmed gaps:

| Variant | Current result | Cause |
|---|---|---|
| Photo page-post ads | `body` is None | text lives in `photo_data.caption`, never read |
| Dynamic Product Ads | all 4 flattened cols None | `template_data` never read |
| Dynamic Creative | all 4 flattened cols None | content is in `asset_feed_spec`, outside `object_story_spec` |

### Documented shapes (from `tasks/meta-ads-api-research/ad-creative.md`)

`object_story_spec` children: `link_data`, `photo_data`, `video_data`, `text_data`,
`template_data`, `product_data`.

- `link_data` **and `template_data`** are the same type (`AdCreativeLinkData`):
  `name` = headline/title, `message` = body, `link` = destination,
  `description` = description, `call_to_action` = `{type, value:{link}}`
- `video_data`: `title` = title, `message` = body, `link_description` = description,
  **no standalone `link`** — destination is only `call_to_action.value.link`
- `photo_data`: `caption` = body/description. **No title field at all. No link field.**

### Required behaviour

Resolution order per column (first non-empty wins):

- `title`: top-level `title` → `link_data.name` → `video_data.title` → `template_data.name`
  (photo_data contributes nothing — it has no title)
- `body`: top-level `body` → `link_data.message` → `video_data.message` →
  `template_data.message` → `photo_data.caption`
- `link_url`: top-level `link_url` → `link_data.link` → `link_data.call_to_action.value.link`
  → `video_data.call_to_action.value.link` → `template_data.link` →
  `template_data.call_to_action.value.link`
- `call_to_action_type`: top-level → `link_data` → `video_data` → `template_data` CTA `type`

New columns to add:
- `description` — `link_data.description` → `video_data.link_description` →
  `template_data.description` → `photo_data.caption`
- `object_type` — request from Graph directly
- `asset_feed_spec` — request from Graph, JSON-encode like `object_story_spec`

### Dynamic Creative (`asset_feed_spec`) fallback

When a column is still None after the `object_story_spec` chain and `asset_feed_spec` is
present, fall back to its first entry:
`titles[0]` → title, `bodies[0]` → body, `descriptions[0]` → description,
`link_urls[0]` → link_url, `call_to_action_types[0]` → call_to_action_type.

**Parse defensively — do not hard-code one element shape.** The docs describe these as
lists but we could not confirm the element shape against a live dynamic creative (this
account has none). Accept, for each element: a plain string; or a dict, reading
`text` → `website_url` → `display_url` → `url` → `value`, first present wins. Anything
else yields None rather than raising. The raw `asset_feed_spec` column preserves the
truth regardless, so a wrong guess degrades to None instead of corrupting data.

`text_data` is referenced by `object_story_spec` but its shape is not documented — do NOT
invent key names for it.

---

## Defect 2 — breakdown validation is wrong in both directions

File: `mindsdb/integrations/handlers/meta_ads_handler/tables/insights.py`

1. `BREAKDOWN_COLUMNS` allows 8 of 37 documented values; the other 29 are rejected
   client-side with a `ValueError` before any request is made.
2. Validation is per-value, but Meta publishes an allow-list of *permutations*. Sets like
   `age,country` or `region,device_platform` pass our check and then 400 at the API.

### Required behaviour

- Expand `BREAKDOWN_COLUMNS` to all 37 values in `tasks/meta-ads-api-research/breakdowns.md`
  ("breakdowns — complete value list"). Copy them exactly; do not add any value not in
  that table. Keep per-value validation — that list is doc-enumerated, so a value outside
  it is a typo.
- Add `action_converted_product_id` to a separate `ACTION_BREAKDOWN_COLUMNS` constant used
  only for `action_breakdowns`, which stays pass-through/unvalidated (the docs never
  enumerate that param exhaustively — see the research file's structural note).
- `get_columns()` returns all 37 so `SELECT dma, impressions FROM insights` works and the
  auto-add-selected-breakdown logic in `list()` covers every value.
- Add `DOCUMENTED_BREAKDOWN_PERMUTATIONS`: the permutation rows from the research file, each
  as a `frozenset`. When the requested breakdown set is not among them, **log a warning
  naming the set and continue** — do not raise. Rationale: the permutation table is
  demonstrably out of sync with the value table (`action_converted_product_id` appears in
  one and not the other), and the `filtering` precedent proves these docs under-document.
  The API is the authority; our job is to warn early, not to block.
- Warn when `hourly_stats_aggregated_by_*` is combined with a selected `reach`,
  `frequency`, or any `unique_*` column: the API silently returns 0 rather than erroring.
  This is a silent-wrong-data risk and the user deserves to be told.
- When the API rejects with code 2 / subcode 1504041 ("Invalid Breakdowns"), re-raise with
  the requested breakdown set and the documented permutation list appended to the message.

---

## Defect 3 — retry and large-request detection don't match the documented taxonomy

Files: `mindsdb/integrations/handlers/meta_ads_handler/errors.py`,
`mindsdb/integrations/handlers/meta_ads_handler/meta_ads_handler.py`

Insights uses a `code` + `error_subcode` taxonomy. Our retry set `{1,2,4,17,32,613}` and
our large-request detector (`code==1` + `"reduce the amount of data"`, or subcode 99)
match **nothing** in it. Two consequences:

- The async-report fallback probably never fires for the errors it exists to handle.
- `code 2` is retried unconditionally, so `2/1504041` (invalid breakdowns) and `2/1504042`
  (invalid custom metrics) burn the full backoff budget re-sending a request that cannot
  ever succeed.

### Required behaviour

Add to `errors.py`, keyed on `(code, error_subcode)`:

```
INSIGHTS_RETRYABLE      = {(4, 1504022), (4, 1504039), (2, 1504043), (2, 1504044), (-2, 2490547)}
INSIGHTS_NON_RETRYABLE  = {(2, 1504041), (2, 1504042), (100, 3191001)}
INSIGHTS_LARGE_REQUEST  = {(100, 1487534), (-3, 1504045), (100, 1504018), (2, 1504038)}
```

- `is_large_request_error()`: return True for any `INSIGHTS_LARGE_REQUEST` pair. **Keep**
  the existing `code == 1` + message match and the subcode-99 check as an additional
  fallback — they are undocumented but were presumably added against observed behaviour,
  and the docs are proven incomplete. Removing them could regress a real case.
- `_is_retryable()` precedence, first match wins:
  1. `INSIGHTS_NON_RETRYABLE` pair → False
  2. large-request → False (already the case; keep, so the async fallback is reached fast)
  3. `INSIGHTS_RETRYABLE` pair → True
  4. HTTP 429 or >=500 → True
  5. legacy generic `code in {1,2,4,17,32,613}` → True
  6. otherwise False

Step 1 must precede step 5 — that is the actual bug fix for the wasted-backoff case.

---

## Non-defect, verified — do not "fix"

- **Currency units.** Docstrings say minor units ("cents"). Live probe: USD account,
  `min_daily_budget: 100` (= $1.00), `amount_spent: "233649"` (= $2,336.49) — confirms
  minor units. We never divide, only `pd.to_numeric`, so there is no money bug. But Meta's
  offset is **1**, not 100, for CLP, COP, CRC, HUF, ISK, IDR, JPY, KRW, PYG, TWD, VND.
  Fix the *docstrings* to describe the per-currency offset instead of asserting "cents";
  do not add any scaling arithmetic.
- **`ads.py` fetching via `/{adset_id}/ads` paths.** Live probe confirms `ad.adset_id`,
  `ad.campaign_id` and `ad.creative_id` are not filterable at all, so the path-based
  approach is correct, not a workaround.
- **Insights default 30-day range** matches Meta's own `last_30d` default.
- **Conditional `time_increment` default** is a deliberate enhancement over the documented
  `all_days` default. Leave it; just note it in the docstring as intentional.

---

## Verification

MindsDB is running at `http://localhost:47334` with a live connection named
`meta_ads_fa0d3484_10b3_4baa_bb35_b3bb35d5896c`. Verify through it with `mindsdb_sdk`
(or `POST /api/sql/query`), not only by reading code:

- `SELECT id, name, title, body, description, link_url, call_to_action_type, object_type
   FROM <conn>.ad_creatives LIMIT 20` — body/title populated where the creative has text.
- `SELECT dma, impressions FROM <conn>.insights WHERE date_preset = 'last_7d'` — a
  previously-rejected breakdown now reaches the API.
- `SELECT age, country, impressions FROM <conn>.insights` — logs the permutation warning
  and still issues the request.
- Existing shapes must keep working: `SELECT campaign_name, SUM(spend) FROM <conn>.insights
   WHERE start_date = '...' AND end_date = '...' GROUP BY campaign_name`.

Report the actual SQL run and the actual output. Do not report success from code reading.
