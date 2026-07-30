# Ad Activity

## What it is
Returns a log of key updates to an ad account and the objects beneath it (campaigns,
ad sets, ads, audiences, billing) — who changed what and when. It answers "what changed
in this account recently and who did it", e.g. budget edits, status changes, ad review
outcomes, audience sharing.

## Endpoint(s)
```
GET /v<API_VERSION>/act_<AD_ACCOUNT_ID>/activities
```
Exposed only as an edge on the **ad account** node. The documentation (both the
ads-commerce reference page and the classic Marketing API reference page for
`ad-account/activities`) does not document this edge on campaign, ad set, or ad nodes
directly — only on the account root.

## Reading fields

| field | type | one-line meaning |
|---|---|---|
| `actor_id` | numeric string | ID of the user/app that performed the action |
| `actor_name` | string | Name of the actor |
| `application_id` | numeric string | ID of the application that made the change |
| `application_name` | string | Name of the application |
| `date_time_in_timezone` | string | Event date/time rendered in the account's timezone |
| `event_time` | datetime | Timestamp of the event (default field) |
| `event_type` | enum | Classification of what happened (see Enums) |
| `extra_data` | string | JSON-encoded payload with event-specific details |
| `object_id` | numeric string | ID of the object that was changed |
| `object_name` | string | Name of the object that was changed |
| `object_type` | string | Type of the object that was changed (e.g. campaign, adset, ad) |
| `translated_event_type` | string | Localized/human-readable version of `event_type` |

## Read params (GET)

**Not documented.** Both source pages fetched for this audit state verbatim: *"This
endpoint doesn't have any parameters."* No `since`, `until`, `time_range`, `date_preset`,
`category`, or `business_id` params are documented for this edge — despite those being
common Graph API conventions elsewhere. Do not assume they work; this was checked
against two doc pages (the ads-commerce `.md` reference and the classic
`marketing-api/reference/ad-account/activities` page) and neither lists any parameters.

## Enums

Complete `event_type` value list as documented:

**Account:** `ad_account_update_spend_limit`, `ad_account_reset_spend_limit`,
`ad_account_remove_spend_limit`, `ad_account_set_business_information`,
`ad_account_update_status`, `ad_account_add_user_to_role`,
`ad_account_remove_user_from_role`, `add_images`, `edit_images`, `delete_images`,
`ad_account_update_audience_type_url_parameter`, `adaccount_update_audience_segment`,
`create_adaccount_agency_fee`, `update_adaccount_agency_fee`,
`update_adaccount_agency_fee_status`

**Billing:** `ad_account_billing_charge`, `ad_account_billing_charge_failed`,
`ad_account_billing_chargeback`, `ad_account_billing_chargeback_reversal`,
`ad_account_billing_decline`, `ad_account_billing_refund`, `billing_event`,
`add_funding_source`, `remove_funding_source`

**Campaign:** `create_campaign_group`, `create_campaign_legacy`,
`update_campaign_name`, `update_campaign_run_status`,
`update_campaign_group_spend_cap`, `update_campaign_budget`, `campaign_ended`,
`update_campaign_group_ad_scheduling`, `update_campaign_group_delivery_type`,
`update_campaign_budget_optimization_toggling_status`,
`update_budget_flex_toggle_status`, `update_delivery_type_cross_level_shift`,
`update_campaign_group_high_demand_periods`,
`update_campaign_group_budget_scheduling_state`,
`create_campaign_group_agency_fee`, `update_campaign_group_agency_fee`,
`merge_campaigns`, `update_campaign_budget_split`, `update_campaign_ad_scheduling`,
`update_campaign_delivery_destination`, `update_campaign_delivery_type`,
`update_campaign_schedule`, `update_campaign_high_demand_periods`,
`update_campaign_budget_scheduling_state`, `update_campaign_conversion_goal`,
`update_campaign_value_adjustment_rule`

**Ad set:** `create_ad_set`, `update_ad_set_bidding`, `update_ad_set_bid_strategy`,
`update_ad_set_budget`, `update_ad_set_duration`, `update_ad_set_run_status`,
`update_ad_set_name`, `update_ad_set_optimization_goal`,
`update_ad_set_target_spec`, `update_ad_set_ad_keywords`,
`update_ad_set_bid_adjustments`, `update_ad_set_spend_cap`,
`update_ad_set_min_spend_target`, `update_ad_set_learning_stage_status`,
`update_ad_set_value_rules`, `update_ad_set_cost_bidding_mode`,
`di_ad_set_learning_stage_exit`

**Ad:** `create_ad`, `ad_review_approved`, `ad_review_declined`,
`update_ad_creative`, `edit_and_update_ad_creative`, `update_ad_bid_info`,
`update_ad_bid_type`, `update_ad_run_status`,
`update_ad_run_status_to_be_set_after_review`, `update_ad_friendly_name`,
`update_ad_targets_spec`, `first_delivery_event`, `update_ad_labels`

**Audience:** `create_audience`, `update_audience`, `delete_audience`,
`share_audience`, `receive_audience`, `unshare_audience`,
`remove_shared_audience`, `update_adgroup_stop_delivery`,
`create_custom_audience_appeal`, `reject_custom_audience_appeal`,
`accept_custom_audience_appeal`, `apply_restrictions_custom_audience`

**Other:** `unknown`, `account_spending_limit_reached`,
`campaign_spending_limit_reached`, `lifetime_budget_spent`,
`conversion_event_updated`, `funding_event_initiated`, `funding_event_successful`

No separate `category` enum is documented — `event_type` above is the only
classification field documented on this endpoint.

## Gotchas

- **Retention window**: documented verbatim as returning **"one week's data by
  default"**. No documented parameter to widen this window (see Read params above).
- **Rate limits**: not documented on either source page.
- **Required permissions**: not documented on either source page.
- **Account-type availability**: not documented on either source page — no statement
  that it is restricted to certain account types.
- Create/Update/Delete are explicitly **not supported** — read-only endpoint.
