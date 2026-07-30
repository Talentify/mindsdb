# Meta Marketing API — Ad Label Reference Extract

Source fetched:
- https://developers.facebook.com/documentation/ads-commerce/marketing-api/reference/ad-label.md

No `ads-commerce/marketing-api/reference/ad-label/*` sub-page was needed — the fetched page
was self-contained for reading purposes. The API is not currently implemented in our
handler at all (no `ad_labels` table exists).

## Reading fields

| field | type | one-line meaning |
|---|---|---|
| `id` | numeric string | Ad Label identifier |
| `name` | string | Label name (default field) |
| `created_time` | datetime | Creation timestamp |
| `updated_time` | datetime | Last modification timestamp |

That is the complete documented field list for `AdLabel` — 4 fields total, no more were found on the fetched page.

## Enums

Not applicable — `AdLabel` has no enum-valued fields in the documented field list above.

## Edges

| edge | what it returns | notable read params |
|---|---|---|
| `adcreatives` | Ad creatives associated with this label | not documented on this page |
| `ads` | Ads associated with this label | not documented on this page |
| `adsets` | Ad sets associated with this label | not documented on this page |
| `campaigns` | Campaigns associated with this label | not documented on this page |

## Read params (GET)

Not documented in field-level detail on this page beyond the standard `fields` selector. The page does not spell out pagination (`limit`, `after`/`before`) or sort params for the `AdLabel` node itself or its `adcreatives`/`ads`/`adsets`/`campaigns` edges.

## Filtering

Labels attach to campaigns/ad sets/ads/creatives, and the **reverse lookup** (read the labelled objects back, filtered by label) is done through four dedicated query endpoints documented on this page, not through the `AdLabel` node's own edges:

- `/campaignsbylabels`
- `/adsetsbylabels`
- `/adsbylabels`
- `/adcreativesbylabels`

Doc quote on label matching: "Operators supported are `ALL` and `ANY` for exact matching; partial string matching is not supported." — i.e., you can ask for objects that have ALL of a given label set or ANY of a given label set, but not a substring/fuzzy match on label name.

The page also documents the generic Marketing API filter-object shape (this list is the one referenced from `campaign.md` as the general operator vocabulary, since the campaign/ad-account pages did not spell it out themselves):

> "Field filtering uses filter objects with three properties: `field`, `operator`, and `value`. Valid operators include: `EQUAL`, `NOT_EQUAL`, `GREATER_THAN`, `GREATER_THAN_OR_EQUAL`, `LESS_THAN`, `LESS_THAN_OR_EQUAL`, `IN_RANGE`, `NOT_IN_RANGE`, `CONTAIN`, `NOT_CONTAIN`, `IN`, `NOT_IN`, `ANY`, `ALL`, `NONE`."

No JSON example of a `*bylabels` request/filter payload was given verbatim on the fetched page — its exact request shape (e.g., whether label(s) are passed as `adlabels` param, and the parameter name for the ALL/ANY operator selection) is **not documented** on this page.

## How labels attach to campaigns/ad sets/ads, and how to read them back

- **Attaching**: `adlabels` is both a readable field (`list<AdLabel>`) on Campaign (and, per the doc, on ad set/ad/ad creative objects) and a writable param on create/update calls for those objects.
  - `POST /{campaign_id}/adlabels`, `POST /{ad_id}/adlabels`, `POST /{ad_creative_id}/adlabels` — required param `adlabels` (list of objects). Doc note: "This endpoint overrides all set of labels associated with this object, whereas /{OBJECT_ID}/adlabels modifies (adds new or reuses specified)." (Doc's own phrasing is slightly self-contradictory/unclear about which of the two endpoint forms overrides vs. modifies — quoted as written; not resolved further since it wasn't spelled out with a second example on the page.)
  - No separate `POST /{adset_id}/adlabels` endpoint was explicitly listed among the four given (only creative/ad/campaign) even though `adsets` appears as a readable edge on `AdLabel` — **not documented** whether ad sets support the same write endpoint; treat as likely-but-unconfirmed.
- **Reading the label's own field** (forward direction): request `adlabels` as a field when reading a Campaign/AdSet/Ad/AdCreative, returning the array of attached `AdLabel` objects (`id`, `name`, etc.).
- **Reading labelled objects back** (reverse direction): use the four `*bylabels` endpoints listed under Filtering above, passing the label(s) to match with `ALL`/`ANY` semantics.

## Gotchas

- **Limits**: maximum 100,000 non-deleted ad labels per ad account; maximum 50 labels per object specification (i.e., per campaign/ad set/ad/creative).
- **No partial matching**: label lookups via `*bylabels` are exact-match only (`ALL`/`ANY` over full label strings), not substring search.
- **Deprecated pattern**: the doc frames `AdLabel` as the intended replacement for encoding metadata into object names (e.g. `"[client]-[fmp]-[autogen]-[18-34-oregon]-[custaudience-141]"`) — not a deprecation of an API feature, but a documented migration recommendation worth flagging if our handler ever needs to parse legacy name-encoded metadata.
- **Currency/special access**: not applicable — no currency or special-access-gated fields found on this page.
- **Ambiguous override-vs-modify doc language**: see the attaching-labels bullet above; the doc's own wording around `/{OBJECT_ID}/adlabels` create vs. modify semantics is unclear and should be verified against live API behavior before relying on it, rather than assumed from this text alone.
