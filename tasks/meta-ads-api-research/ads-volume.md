## What it is
Returns how many ads on an ad account are currently running or in review, counted
against the per-Page ad limit Meta introduced in 2021. It answers "how close is this
account/page to hitting the running-ads cap right now" — it is a live gauge, not a
historical time series.

## Endpoint(s)
```
GET /v<API_VERSION>/act_<AD_ACCOUNT_ID>/ads_volume
GET /v<API_VERSION>/act_<AD_ACCOUNT_ID>/ads_volume?show_breakdown_by_actor=true
GET /v<API_VERSION>/act_<AD_ACCOUNT_ID>/ads_volume?page_id=<PAGE_ID>
```
Exposed only as an edge on the **ad account** node (`act_<AD_ACCOUNT_ID>`). Not
documented as an edge on campaign, ad set, or ad nodes.

## Reading fields

| field | type | one-line meaning |
|---|---|---|
| `ads_running_or_in_review_count` | integer | Count of ads running or in review for the relevant actor (Page) |
| `current_account_ads_running_or_in_review_count` | integer | Count of ads running/in review that belong to the current ad account, for that actor |
| `actor_id` | string | ID of the entity the limit is enforced against — currently always a Page ID |
| `recommendations` | array | Recommendations related to ad volume/limits (may be empty) |

## Read params (GET)

| param | type | allowed values / format | what it does |
|---|---|---|---|
| `access_token` | string | valid access token | Required authentication |
| `show_breakdown_by_actor` | boolean | `true` / `false` | Breaks the response down per `actor_id` (Page) instead of a single aggregate |
| `page_id` | string | a Facebook Page ID | Filters the result to ad volume for one specific Page |

No `since`, `until`, `time_range`, `date_preset`, `category`, or `business_id` params are
documented — this endpoint is a point-in-time snapshot, not a date-ranged report, so
there is nothing to filter by date server-side.

## Enums

None documented. `recommendations` is described as an array but its item schema is not
documented on this page.

Status logic (documented, not an enum field returned by this endpoint but relevant to
interpreting the counts): an ad counts as running/in review when
`effective_status == 1` (active), OR `configured_status == active` AND
`effective_status` is `9` (pending review) or `17` (pending processing); the ad
account itself must be `1` (active), `8` (pending settlement), or `9` (grace period).
Day-parted/scheduled ads count for the whole day they're scheduled on, not partial
periods; future-scheduled ads don't count yet.

## Gotchas

- **Retention/time window**: not applicable — this is a current-state snapshot, not a
  historical report. No lookback window is documented.
- **Rate limits**: not documented on this page.
- **Required permissions**: not documented on this page beyond a valid `access_token`.
- **Account-type availability**: not documented on this page — no statement restricting
  it to certain account types.
- The count is enforced **per Page** (`actor_id`), and the same Page's ad volume can be
  shared across multiple ad accounts — `current_account_ads_running_or_in_review_count`
  vs `ads_running_or_in_review_count` distinguishes "this account's contribution" from
  "the actor's total".
