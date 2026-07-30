# Meta Marketing API — Ad Creative reference extract

Source: https://developers.facebook.com/documentation/ads-commerce/marketing-api/reference/ad-creative.md
Sub-pages: ad-creative-object-story-spec, ad-asset-feed-spec, ad-creative-link-data,
ad-creative-video-data, ad-creative-photo-data, ad-creative-link-data-call-to-action
(all `docs/marketing-api/reference/...`, `.md` suffix 404s on these — see Gotchas).

## Reading fields

| field | type | one-line meaning |
|---|---|---|
| `id` | numeric string | Unique creative ID. ✅ |
| `account_id` | numeric string | Ad account this creative belongs to. |
| `actor_id` | numeric string | Page ID acting as the creative's actor. |
| `ad_disclaimer_spec` | AdCreativeAdDisclaimer | Disclaimer data on the creative. |
| `adlabels` | list<AdLabel> | Labels for grouping with related ad objects. |
| `applink_treatment` | enum | Fallback action for Dynamic Ads when app isn't installed. |
| `asset_feed_spec` | AdAssetFeedSpec | Dynamic Creative asset variations spec (see below). |
| `authorization_category` | enum | Political-ad labeling category (not usable for Dynamic Ads). |
| `body` | string | Ad body text; not supported for video post creatives. ✅ |
| `branded_content` | AdCreativeBrandedContentAds | Branded content data. |
| `branded_content_sponsor_page_id` | numeric string | Page ID of branded-content sponsor. |
| `bundle_folder_id` | numeric string | Dynamic Ad's bundle folder ID. |
| `call_to_action` | AdCreativeLinkDataCallToAction | CTA for a creative built from an existing Instagram post. |
| `call_to_action_type` | enum | CTA button/header text type. ✅ |
| `categorization_criteria` | enum | Dynamic Category Ad categorization field (e.g. brand). |
| `category_media_source` | enum | Dynamic Ad rendering mode for category ads. |
| `collaborative_ads_lsb_image_bank_id` | numeric string | CPAS local-delivery image bank ID. |
| `contextual_multi_ads` | AdCreativeContextualMultiAds | Contextual multi-ad spec. |
| `creative_sourcing_spec` | AdCreativeSourcingSpec | Creative sourcing spec. |
| `degrees_of_freedom_spec` | AdCreativeDegreesOfFreedomSpec | Enabled transformation types for the creative. |
| `destination_set_id` | numeric string | Product Set ID for a Destination (Travel) Catalog. |
| `dynamic_ad_voice` | string | Voice control for Dynamic Ads (Store Traffic objective). |
| `effective_authorization_category` | enum | Actual political-ad category (may differ from `authorization_category`). |
| `effective_instagram_media_id` | numeric string | Instagram post ID used in the ad. |
| `effective_object_story_id` | token (Post ID) | Page post ID used, regardless of published status. ✅ |
| `enable_direct_install` | bool | Enable Direct Install on supported devices. |
| `enable_launch_instant_app` | bool | Enable Instant App on supported devices. |
| `existing_post_title` | string | Title of an existing post used by the creative. |
| `facebook_branded_content` | AdCreativeFacebookBrandedContent | Facebook Branded Content fields. |
| `format_transformation_spec` | list<AdCreativeFormatTransformationSpec> | Format transformation spec. |
| `generative_asset_spec` | AdCreativeGenerativeAssetSpec | Generative asset spec. |
| `image_crops` | AdsImageCrops | Crop dimensions for the specified image. |
| `image_hash` | string | Image hash (mutually exclusive with `image_url`). |
| `image_url` | string | Creative image URL (mutually exclusive with `image_hash`). ✅ |
| `instagram_permalink_url` | string | URL of an Instagram post run as an ad. ✅ |
| `instagram_user_id` | numeric string | Instagram actor ID. |
| `interactive_components_spec` | AdCreativeInteractiveComponentsSpec | Interactive components on the ad. |
| `link_destination_display_url` | string | Overwrites display URL when `object_url` is a click tag. |
| `link_og_id` | numeric string | Open Graph ID for the link, if landing page has OG tags. |
| `link_url` | string | Facebook Page tab URL (landing tab identification). ✅ |
| `marketing_message_structured_spec` | AdCreativeMarketingMessageStructuredSpec | Structured marketing message spec. |
| `media_sourcing_spec` | AdCreativeMediaSourcingSpec | Media sourcing spec. |
| `messenger_sponsored_message` | string | JSON string of a Messenger sponsored message. |
| `name` | string | Creative name as seen in the ad account library (max 100 chars). ✅ |
| `object_id` | numeric string | ID of the Facebook object being promoted. |
| `object_store_url` | string | iTunes/Google Play destination URL for app ads. |
| `object_story_id` | token (Post ID) | Published Page post ID used in the ad; null if unpublished. |
| `object_story_spec` | AdCreativeObjectStorySpec | Spec for creating a new unpublished page post as an ad (see below). ✅ |
| `object_type` | enum | Type of Facebook object advertised (PAGE, DOMAIN, EVENT, etc.). |
| `object_url` | string | URL opened when a link ad is clicked. |
| `page_welcome_message` | string | Page welcome message for click-to-Messenger ads. |
| `photo_album_source_object_story_id` | string | Source object story ID for a photo album. |
| `place_page_set_id` | numeric string | Page set ID for Local Awareness creatives. |
| `platform_customizations` | AdCreativePlatformCustomization | Per-placement media overrides. |
| `playable_asset_id` | numeric string | ID of the playable asset in the creative. |
| `portrait_customizations` | AdCreativePortraitCustomizations | Portrait-mode rendering customizations (Stories, IGTV, etc). |
| `product_data` | list<AdCreativeProductData> | Product data for catalog-related experiences. |
| `product_set_id` | numeric string | Product set ID grouping related products (Dynamic Ads). |
| `product_suggestion_settings` | AdCreativeProductSuggestionSettings | Product suggestion settings. |
| `recommender_settings` | AdCreativeRecommenderSettings | Product-recommendation display settings (Dynamic Ads). |
| `referral_id` | numeric string | Referral Ad Configuration ID. |
| `source_facebook_post_id` | numeric string | Source Facebook post ID. |
| `source_instagram_media_id` | numeric string | Source Instagram post ID used to create the ad. |
| `status` | enum | Creative status: ACTIVE, IN_PROCESS, WITH_ISSUES, DELETED. ✅ |
| `template_url` | string | Third-party click-tracking template URL (Dynamic Ads). |
| `template_url_spec` | AdCreativeTemplateURLSpec | Structured third-party click-tracking spec (Dynamic Ads). |
| `threads_media_id` | numeric string | Threads media ID. |
| `threads_user_id` | numeric string | Threads user ID. |
| `thumbnail_id` | numeric string | Thumbnail asset ID. |
| `thumbnail_url` | string | Thumbnail image URL. ✅ |
| `title` | string | Title for a link ad not tied to a page. ✅ |
| `url_tags` | string | Query-string params appended/replacing URLs from page-post ads. |
| `use_page_actor_override` | bool | Show the Page associated with app ads. |
| `video_id` | numeric string | Facebook object ID of the video in the creative. ✅ |
| `wamo_whatsapp_identity_spec` | AdCreativeWAMOWhatsAppIdentitySpec | WhatsApp identity spec (WAMO). |

**Count: 74 documented readable fields; our handler exposes 13** (`id`, `name`, `status`, `title`,
`body`, `link_url`, `image_url`, `thumbnail_url`, `video_id`, `call_to_action_type`,
`effective_object_story_id`, `object_story_spec`, `instagram_permalink_url`).

## object_story_spec shape

Top level (`AdCreativeObjectStorySpec`):

| field | type | meaning |
|---|---|---|
| `page_id` | numeric string | Page the unpublished post is created on. |
| `instagram_user_id` | numeric string | Instagram account the ad posts to. |
| `link_data` | AdCreativeLinkData | Link page post / carousel ad spec. |
| `photo_data` | AdCreativePhotoData | Photo page post spec. |
| `video_data` | AdCreativeVideoData | Video page post spec. |
| `text_data` | AdCreativeTextData | Text-only page post spec (not resolved further — not documented in fetched content). |
| `template_data` | AdCreativeLinkData | Template link page post for Dynamic Product Ads — **reuses the `AdCreativeLinkData` shape**, same field names as `link_data`. |
| `product_data` | list<AdCreativeProductData> | Products for catalog experiences (not resolved further). |

### `link_data` (also `template_data`)

| field | type | carries |
|---|---|---|
| `name` | string | **headline/title**. Overwrites the link title in preview. Ignored for `LIKE_PAGE` CTA. |
| `message` | string | **body**. Required for carousel ads. |
| `link` | string | **destination link**. Required to match the CTA link URL for carousel ads. |
| `caption` | string | Link caption shown under the title (not the description). Must reflect the actual URL; unused on Instagram. |
| `description` | string | **description**. Overwrites the link's description text. Unused on Instagram. |
| `call_to_action` | AdCreativeLinkDataCallToAction | **call to action** — `{type, value}`; `value.link` can carry an alternate destination link (3rd-level nested object, not fully resolved — see Gotchas). Default on Instagram is `LEARN_MORE`; `LIKE_PAGE` unsupported there. |
| `image_hash` | string | **image** (library hash). Mutually exclusive with `picture`. |
| `picture` | string | **image** (URL). Mutually exclusive with `image_hash`. |

### `video_data`

| field | type | carries |
|---|---|---|
| `title` | string | **headline/title**. Cannot be combined with `LIKE_PAGE` CTA. |
| `message` | string | **body**. |
| `link_description` | string | **description**. Overwrites the video post's description. |
| `call_to_action` | AdCreativeLinkDataCallToAction | **call to action** (same `{type, value}` shape as link_data; `value.link` is the destination link for video ads — `video_data` has no standalone `link` field). |
| `image_hash` / `image_url` | string | **image** (thumbnail). Mutually exclusive pair, same pattern as link_data's image_hash/picture. |
| `video_id` | numeric string | The video asset. |
| `additional_image_index`, `caption_ids`, `collection_thumbnails`, `customization_rules_spec`, `offer_id`, `page_welcome_message`, `post_click_configuration`, `retailer_item_ids`, `targeting`, `branded_content_*` | various | Secondary/format-specific fields, not part of the flattening logic. |

### `photo_data`

| field | type | carries |
|---|---|---|
| `caption` | string | **body/description** of the image — photo_data has **no separate title/headline field at all**. |
| `image_hash` | string | **image** (library hash). Mutually exclusive with `url`. |
| `url` | string | **image** (direct URL); saved into the ad account's image library. Mutually exclusive with `image_hash`. |
| `page_welcome_message` | string | Messenger greeting text. |
| `branded_content_shared_to_sponsor_status`, `branded_content_sponsor_page_id`, `branded_content_sponsor_relationship` | various | Branded-content fields. |

Note: `photo_data` has **no `link`/destination-link field** — photo page-post ads have no click-through URL at the creative level.

### `template_data`

Same object type as `link_data` (`AdCreativeLinkData`) — identical field names/behavior, used for
Dynamic Product Ads templates (e.g. `{{product.name}}`-style macros are typically placed in these
same `name`/`message`/`description` fields, though macro syntax itself is not documented in the
fetched content).

## asset_feed_spec / dynamic creative

`AdAssetFeedSpec` (Dynamic Creative — automatically experiments across variations):

| field | type | purpose |
|---|---|---|
| `bodies` | list<AdAssetFeedSpecBody> | Body-text variants. |
| `titles` | list<AdAssetFeedSpecTitle> | Title/headline variants. |
| `descriptions` | list<AdAssetFeedSpecDescription> | Description variants. |
| `captions` | list<AdAssetFeedSpecCaption> | Caption variants. |
| `images` | list<AdAssetFeedSpecImage> | Image asset variants. |
| `videos` | list<AdAssetFeedSpecVideo> | Video asset variants. |
| `link_urls` | list<AdAssetFeedSpecLinkURL> | Destination-link variants. |
| `call_to_action_types` | list<enum> | Candidate CTA types (100+ enum values). |
| `audios` | list<AdAssetAudios> | Audio asset variants. |
| `ad_formats` | list<enum> | Candidate ad format(s). |
| `asset_customization_rules` | list<AdAssetFeedSpecAssetCustomizationRule> | Rules targeting specific variant combinations. |
| `groups` | list<AdAssetFeedSpecGroupRule> | Grouping rules for variants. |
| `optimization_type` | enum | `ASSET_CUSTOMIZATION`, `LANGUAGE`, `PLACEMENT`, `REGULAR`, `FORMAT_AUTOMATION`. |
| `app_product_page_id` | string | Custom store listing ID for app-install campaigns. |

**Flat title/body fields for dynamic creatives**: `titles`/`bodies`/`descriptions`/`captions` are
each a **list** of variant objects, not a single string — structurally different from the flat
`title`/`body`/`description` fields on `link_data`/`video_data`. The exact per-item field name
inside `AdAssetFeedSpecTitle`/`AdAssetFeedSpecBody` (e.g. whether it's `text`) is **not documented**
in the fetched content — not confirmed here. What is confirmed: when a creative uses
`asset_feed_spec`, our handler's flattening (which only reads `object_story_spec.link_data`/
`.video_data` and the top-level `title`/`body`) will find **none of these fields** there, because
Dynamic Creative content lives exclusively under `asset_feed_spec`, not `object_story_spec`.

## Read params (GET)

| param | type | allowed values / format | what it does |
|---|---|---|---|
| `fields` | string | comma-separated field names | Selects which fields to return (used by the handler already). |
| `thumbnail_width` | int | pixels, default 64 | Width used when rendering `thumbnail_url`. |
| `thumbnail_height` | int | pixels, default 64 | Height used when rendering `thumbnail_url`. |
| `limit` | int | Graph API standard paging param | Page size (used by the handler already; standard Graph API paging, not creative-specific). |

No creative-specific server-side **filtering** parameter (e.g. a `filtering` array scoped to
`adcreatives`) is documented in the fetched content for the `act_<id>/adcreatives` edge. The only
documented constraint is a hard cap: **"Only returns 50,000 ad creatives; pagination past this
point is unavailable."**

## Gotchas

- **`photo_data` has no title/headline and no destination-link field at all.** Our flattening only
  checks `link_data` and `video_data` — a photo-only creative's `title`/`body`/`link_url` will
  correctly fall through to `None`/`None`/`None` for title/link, but `body` should arguably read
  `photo_data.caption` too; currently it does not, so photo creatives silently get `body = None`
  even though `caption` holds descriptive text.
- **`text_data` (text-only page posts)** is referenced on `object_story_spec` but not resolved in
  the fetched content — not documented here. If used, our flattening (link_data/video_data only)
  would miss it entirely.
- **`template_data` reuses `link_data`'s shape** (`AdCreativeLinkData`) but our flattening never
  reads `object_story_spec.template_data` — Dynamic Product Ad template creatives fall through to
  `None` for title/body/link/CTA even though the data exists at
  `object_story_spec.template_data.{name,message,link,call_to_action}`.
- **Dynamic Creative (`asset_feed_spec`) content is invisible to our flattening.** Since it's list-
  based, not a single string, our current top-level → link_data → video_data fallback chain will
  yield `None` for `title`/`body`/`link_url`/`call_to_action_type` on any creative built via
  `asset_feed_spec`, even though rich content exists there.
- **`video_data` has no standalone `link` field** — the destination link for a video ad lives only
  at `video_data.call_to_action.value.link`. Our handler's `_cta_link()` already handles this
  correctly for video_data (falls back to CTA value link), but note it does **not** do the
  equivalent fallback for `photo_data` (which has no link at all, so this is moot) or for
  `template_data` (unreached, per above).
  `AdCreativeLinkDataCallToActionValue` (the nested object under `call_to_action.value`) was not
  resolved past `link` — its full field list (e.g. `link_caption`, `app_link_spec`) is **not
  documented** in the fetched content; only `value.link` is confirmed used by the handler's
  existing `_cta_link()` logic.
- **`effective_authorization_category` can differ from `authorization_category`** — the system may
  reclassify a creative as political even if not explicitly labeled; useful for compliance
  reporting, currently not exposed.
- **The `.md` doc-fetch suffix 404s on second-level reference pages** (`ad-creative-object-story-
  spec.md`, `ad-asset-feed-spec.md`, and all `ad-creative-*-data.md` variants) — only the top-level
  `ad-creative.md` page supports it. The non-`.md` URLs worked for all sub-pages fetched here.
- No field in the documented set is marked deprecated in the fetched content; the only
  deprecation-related text found was a generic API-version warning (error code 2635), unrelated to
  any specific field.
