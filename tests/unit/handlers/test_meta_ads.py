import hashlib
import hmac
import json
import os
import sys
import types
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy import types as sqlalchemy_types

os.environ.setdefault("MINDSDB_STORAGE_DIR", "/tmp/mindsdb_meta_ads_test")
os.makedirs(os.environ["MINDSDB_STORAGE_DIR"], exist_ok=True)

mind_castle_module = types.ModuleType("mind_castle")
sqlalchemy_type_module = types.ModuleType("mind_castle.sqlalchemy_type")


class SecretData(sqlalchemy_types.TypeDecorator):
    impl = sqlalchemy_types.String
    cache_ok = True

    def __init__(self, *args, **kwargs):
        super().__init__()


sqlalchemy_type_module.SecretData = SecretData
mind_castle_module.sqlalchemy_type = sqlalchemy_type_module
sys.modules.setdefault("mind_castle", mind_castle_module)
sys.modules.setdefault("mind_castle.sqlalchemy_type", sqlalchemy_type_module)

try:
    from mindsdb_sql_parser import parse_sql

    from mindsdb.integrations.handlers.meta_ads_handler.connection_args import connection_args
    from mindsdb.integrations.handlers.meta_ads_handler.meta_ads_handler import MetaAdsHandler
    from mindsdb.integrations.handlers.meta_ads_handler.tables.ad_creatives import AdCreativesTable
    from mindsdb.integrations.handlers.meta_ads_handler.tables.ad_sets import AdSetsTable
    from mindsdb.integrations.handlers.meta_ads_handler.tables.ads import AdsTable
    from mindsdb.integrations.handlers.meta_ads_handler.tables.campaigns import CampaignsTable
    from mindsdb.integrations.handlers.meta_ads_handler.tables.insights import InsightsTable
    from mindsdb.integrations.libs.response import RESPONSE_TYPE, TableResponse
except ImportError:
    pytestmark = pytest.mark.skip("Meta Ads handler not installed")


SESSION_PATCH_PATH = "mindsdb.integrations.handlers.meta_ads_handler.meta_ads_handler.requests.Session"


def _build_json_response(payload, status_code=200):
    response = MagicMock()
    response.status_code = status_code
    response.ok = status_code < 400
    response.text = str(payload)
    response.json.return_value = payload
    return response


@pytest.fixture
def handler():
    return MetaAdsHandler(
        "meta_ads",
        connection_data={
            "ad_account_id": "act_123456",
            "access_token": "test_token",
            "api_version": "25.0",
        },
    )


def _attach_session(handler_obj, session):
    """Connect the handler with a mocked requests.Session (no network calls)."""
    with patch(SESSION_PATCH_PATH, return_value=session):
        handler_obj.connect()
    return handler_obj


# ---------------------------------------------------------------------------
# 1. connection_args
# ---------------------------------------------------------------------------


def test_connection_args_match_spec():
    assert list(connection_args.keys()) == [
        "ad_account_id",
        "access_token",
        "api_version",
        "client_id",
        "client_secret",
    ]
    assert connection_args["ad_account_id"]["required"] is True
    assert connection_args["ad_account_id"].get("secret", False) is False
    assert connection_args["access_token"]["required"] is True
    assert connection_args["access_token"]["secret"] is True
    assert connection_args["api_version"].get("required", False) is False
    assert connection_args["api_version"].get("secret", False) is False
    assert connection_args["client_id"].get("required", False) is False
    assert connection_args["client_id"].get("secret", False) is False
    assert connection_args["client_secret"].get("required", False) is False
    assert connection_args["client_secret"]["secret"] is True


# ---------------------------------------------------------------------------
# 2. normalization
# ---------------------------------------------------------------------------


def test_ad_account_id_strips_act_prefix_and_api_version_gets_v_prefix(handler):
    assert handler.ad_account_id == "123456"
    assert handler.account_path == "act_123456"
    assert handler.api_version == "v25.0"
    assert handler.base_url == "https://graph.facebook.com/v25.0"


def test_api_version_already_prefixed_is_left_untouched():
    handler = MetaAdsHandler(
        "meta_ads",
        connection_data={
            "ad_account_id": "123456",
            "access_token": "test_token",
            "api_version": "v25.0",
        },
    )
    assert handler.api_version == "v25.0"


def test_api_version_defaults_when_missing():
    handler = MetaAdsHandler(
        "meta_ads",
        connection_data={"ad_account_id": "123456", "access_token": "test_token"},
    )
    assert handler.api_version == MetaAdsHandler.DEFAULT_API_VERSION


# ---------------------------------------------------------------------------
# 3. appsecret_proof
# ---------------------------------------------------------------------------


def test_appsecret_proof_is_computed_and_sent_when_client_secret_present():
    handler = MetaAdsHandler(
        "meta_ads",
        connection_data={
            "ad_account_id": "123456",
            "access_token": "test_token",
            "client_secret": "shh_secret",
        },
    )
    expected = hmac.new(b"shh_secret", b"test_token", hashlib.sha256).hexdigest()
    assert handler._appsecret_proof == expected

    session = MagicMock()
    session.get.return_value = _build_json_response({"data": []})
    _attach_session(handler, session)

    handler.graph_get("act_123456/campaigns", {"fields": "id"})

    sent_params = session.get.call_args.kwargs["params"]
    assert sent_params["appsecret_proof"] == expected


def test_appsecret_proof_absent_without_client_secret(handler):
    assert handler._appsecret_proof is None

    session = MagicMock()
    session.get.return_value = _build_json_response({"data": []})
    _attach_session(handler, session)

    handler.graph_get("act_123456/campaigns", {"fields": "id"})

    sent_params = session.get.call_args.kwargs["params"]
    assert "appsecret_proof" not in sent_params


# ---------------------------------------------------------------------------
# 4. connect() validation
# ---------------------------------------------------------------------------


def test_connect_requires_access_token():
    handler = MetaAdsHandler("meta_ads", connection_data={"ad_account_id": "123456"})
    with pytest.raises(ValueError, match="access_token is required"):
        handler.connect()


def test_connect_requires_ad_account_id():
    handler = MetaAdsHandler("meta_ads", connection_data={"access_token": "test_token"})
    with pytest.raises(ValueError, match="ad_account_id is required"):
        handler.connect()


# ---------------------------------------------------------------------------
# 5. check_connection()
# ---------------------------------------------------------------------------


def test_check_connection_success(handler):
    session = MagicMock()
    session.get.return_value = _build_json_response({"id": "act_123456", "name": "Acme", "account_status": 1})

    with patch(SESSION_PATCH_PATH, return_value=session):
        response = handler.check_connection()

    assert response.success is True
    assert response.error_message is None
    session.get.assert_called_once()


def test_check_connection_failure(handler):
    session = MagicMock()
    session.get.return_value = _build_json_response(
        {"error": {"message": "Invalid OAuth access token", "type": "OAuthException", "code": 190}},
        status_code=400,
    )

    with patch(SESSION_PATCH_PATH, return_value=session):
        response = handler.check_connection()

    assert response.success is False
    assert "Invalid OAuth access token" in response.error_message
    assert handler.session is None  # disconnected on failure


# ---------------------------------------------------------------------------
# 6. Graph error body -> RuntimeError
# ---------------------------------------------------------------------------


def test_graph_get_raises_runtime_error_with_api_message(handler):
    session = MagicMock()
    session.get.return_value = _build_json_response(
        {
            "error": {
                "message": "Invalid parameter",
                "type": "OAuthException",
                "code": 100,
                "error_subcode": 33,
                "error_user_msg": "The ad account id is invalid",
            }
        },
        status_code=400,
    )
    _attach_session(handler, session)

    with pytest.raises(RuntimeError) as exc_info:
        handler.graph_get("act_123456/campaigns", {"fields": "id"})

    message = str(exc_info.value)
    assert "Invalid parameter" in message
    assert "code=100" in message
    assert "error_subcode=33" in message
    assert "The ad account id is invalid" in message


# ---------------------------------------------------------------------------
# 7. retry behaviour
# ---------------------------------------------------------------------------


def test_retries_on_429_then_succeeds(handler):
    session = MagicMock()
    session.get.side_effect = [
        _build_json_response({"error": {"message": "Rate limited", "code": 4}}, status_code=429),
        _build_json_response({"data": [{"id": "1"}]}),
    ]
    _attach_session(handler, session)

    with patch("mindsdb.integrations.handlers.meta_ads_handler.meta_ads_handler.time.sleep") as mock_sleep:
        result = handler.graph_get("act_123456/campaigns", {"fields": "id"})

    assert result == {"data": [{"id": "1"}]}
    assert session.get.call_count == 2
    mock_sleep.assert_called_once()


def test_does_not_retry_on_auth_error_code_190(handler):
    session = MagicMock()
    session.get.return_value = _build_json_response(
        {"error": {"message": "Invalid OAuth access token", "type": "OAuthException", "code": 190}},
        status_code=400,
    )
    _attach_session(handler, session)

    with patch("mindsdb.integrations.handlers.meta_ads_handler.meta_ads_handler.time.sleep") as mock_sleep:
        with pytest.raises(RuntimeError, match="Invalid OAuth access token"):
            handler.graph_get("act_123456/campaigns", {"fields": "id"})

    assert session.get.call_count == 1
    mock_sleep.assert_not_called()


# ---------------------------------------------------------------------------
# 8. graph_get_all pagination
# ---------------------------------------------------------------------------


def test_graph_get_all_follows_paging_next_and_concatenates(handler):
    session = MagicMock()
    session.get.side_effect = [
        _build_json_response(
            {
                "data": [{"id": "1"}, {"id": "2"}],
                "paging": {"next": "https://graph.facebook.com/v25.0/act_123456/campaigns?after=cursor1"},
            }
        ),
        _build_json_response({"data": [{"id": "3"}]}),
    ]
    _attach_session(handler, session)

    rows = handler.graph_get_all("act_123456/campaigns", {"fields": "id"})

    assert [row["id"] for row in rows] == ["1", "2", "3"]
    assert session.get.call_count == 2
    # paging.next is followed directly, with auth re-applied.
    assert session.get.call_args_list[1].args[0] == (
        "https://graph.facebook.com/v25.0/act_123456/campaigns?after=cursor1"
    )
    assert "Bearer test_token" in session.get.call_args_list[1].kwargs["headers"]["Authorization"]


def test_graph_get_all_honours_limit(handler):
    session = MagicMock()
    session.get.side_effect = [
        _build_json_response(
            {
                "data": [{"id": "1"}, {"id": "2"}],
                "paging": {"next": "https://graph.facebook.com/v25.0/act_123456/campaigns?after=cursor1"},
            }
        ),
        _build_json_response({"data": [{"id": "3"}, {"id": "4"}]}),
    ]
    _attach_session(handler, session)

    rows = handler.graph_get_all("act_123456/campaigns", {"fields": "id"}, limit=3)

    assert [row["id"] for row in rows] == ["1", "2", "3"]
    # limit should never be exceeded, and only fetched as many pages as needed.
    assert session.get.call_count == 2


# ---------------------------------------------------------------------------
# 9. campaigns table
# ---------------------------------------------------------------------------


def test_campaigns_select_star_returns_all_columns(handler):
    session = MagicMock()
    session.get.return_value = _build_json_response(
        {
            "data": [
                {
                    "id": "1",
                    "name": "Campaign 1",
                    "objective": "OUTCOME_TRAFFIC",
                    "status": "ACTIVE",
                    "effective_status": "ACTIVE",
                    "special_ad_categories": ["HOUSING", "EMPLOYMENT"],
                    "daily_budget": "1000",
                }
            ]
        }
    )
    _attach_session(handler, session)

    table = CampaignsTable(handler)
    query = parse_sql("SELECT * FROM campaigns")

    df = table.select(query)

    assert list(df.columns) == table.get_columns()
    assert df.iloc[0]["special_ad_categories"] == "HOUSING,EMPLOYMENT"
    assert df.iloc[0]["daily_budget"] == 1000


# ---------------------------------------------------------------------------
# 10. ads nested creative flattening
# ---------------------------------------------------------------------------


def test_ads_flattens_nested_creative_id(handler):
    session = MagicMock()
    session.get.return_value = _build_json_response(
        {
            "data": [
                {
                    "id": "10",
                    "name": "Ad 1",
                    "adset_id": "20",
                    "campaign_id": "1",
                    "creative": {"id": "999"},
                }
            ]
        }
    )
    _attach_session(handler, session)

    table = AdsTable(handler)
    query = parse_sql("SELECT * FROM ads")

    df = table.select(query)

    assert df.iloc[0]["creative_id"] == "999"
    assert "creative" not in df.columns

    sent_fields = session.get.call_args.kwargs["params"]["fields"]
    assert "creative{id}" in sent_fields
    assert "creative_id" not in sent_fields.split(",")


# ---------------------------------------------------------------------------
# 11. ad_creatives nested object_story_spec flattening
# ---------------------------------------------------------------------------


def test_ad_creatives_falls_back_to_object_story_spec_link_data(handler):
    session = MagicMock()
    session.get.return_value = _build_json_response(
        {
            "data": [
                {
                    "id": "555",
                    "name": "Creative 1",
                    "object_story_spec": {
                        "link_data": {
                            "name": "Nested Title",
                            "message": "Nested Body",
                            "link": "https://example.com/landing",
                            "call_to_action": {"type": "SHOP_NOW"},
                        }
                    },
                }
            ]
        }
    )
    _attach_session(handler, session)

    table = AdCreativesTable(handler)
    query = parse_sql("SELECT * FROM ad_creatives")

    df = table.select(query)

    row = df.iloc[0]
    assert row["title"] == "Nested Title"
    assert row["body"] == "Nested Body"
    assert row["link_url"] == "https://example.com/landing"
    assert row["call_to_action_type"] == "SHOP_NOW"
    assert json.loads(row["object_story_spec"])["link_data"]["name"] == "Nested Title"


def test_ad_creatives_prefers_top_level_values_over_nested(handler):
    session = MagicMock()
    session.get.return_value = _build_json_response(
        {
            "data": [
                {
                    "id": "556",
                    "name": "Creative 2",
                    "title": "Top-level Title",
                    "object_story_spec": {"link_data": {"name": "Nested Title"}},
                }
            ]
        }
    )
    _attach_session(handler, session)

    table = AdCreativesTable(handler)
    query = parse_sql("SELECT * FROM ad_creatives")

    df = table.select(query)

    assert df.iloc[0]["title"] == "Top-level Title"


# ---------------------------------------------------------------------------
# 12. insights Pattern B regression guard
# ---------------------------------------------------------------------------


def test_insights_aggregation_query_still_requests_all_underlying_fields(handler):
    session = MagicMock()
    session.get.return_value = _build_json_response(
        {"data": [{"campaign_name": "Campaign 1", "spend": "12.34"}]}
    )
    _attach_session(handler, session)

    table = InsightsTable(handler)
    query = parse_sql(
        "SELECT campaign_name, SUM(spend) AS total_spend FROM insights GROUP BY campaign_name"
    )

    table.select(query)

    sent_fields = session.get.call_args.kwargs["params"]["fields"].split(",")
    assert "spend" in sent_fields
    assert "campaign_name" in sent_fields


# ---------------------------------------------------------------------------
# 13. insights derived-column dependency forcing
# ---------------------------------------------------------------------------


def test_insights_selecting_roas_forces_action_values_and_spend(handler):
    session = MagicMock()
    session.get.return_value = _build_json_response(
        {"data": [{"spend": "10", "action_values": [{"action_type": "purchase", "value": "50"}]}]}
    )
    _attach_session(handler, session)

    table = InsightsTable(handler)
    query = parse_sql("SELECT roas FROM insights")

    table.select(query)

    sent_fields = session.get.call_args.kwargs["params"]["fields"].split(",")
    assert "action_values" in sent_fields
    assert "spend" in sent_fields
    assert "roas" not in sent_fields


# ---------------------------------------------------------------------------
# 14. insights derived-column extraction
# ---------------------------------------------------------------------------


def test_insights_derived_columns_extracted_with_fallback_and_zero_default(handler):
    session = MagicMock()
    session.get.return_value = _build_json_response(
        {
            "data": [
                {
                    "spend": "100",
                    "actions": [
                        {"action_type": "link_click", "value": "5"},
                        {"action_type": "omni_purchase", "value": "3"},
                    ],
                    "action_values": [{"action_type": "omni_purchase", "value": "200"}],
                },
                {
                    # no actions/action_values at all -- everything derived should default to 0.
                    "spend": "0",
                },
            ]
        }
    )
    _attach_session(handler, session)

    table = InsightsTable(handler)
    query = parse_sql("SELECT * FROM insights")

    df = table.select(query)

    row0 = df.iloc[0]
    assert row0["link_clicks"] == 5
    assert row0["purchases"] == 3  # omni_purchase preferred
    assert row0["purchase_value"] == 200
    assert row0["roas"] == 2.0  # 200 / 100

    row1 = df.iloc[1]
    assert row1["link_clicks"] == 0
    assert row1["purchases"] == 0
    assert row1["purchase_value"] == 0
    assert row1["roas"] == 0


def test_insights_purchases_falls_back_to_plain_purchase_action_type(handler):
    session = MagicMock()
    session.get.return_value = _build_json_response(
        {
            "data": [
                {
                    "spend": "50",
                    "actions": [{"action_type": "purchase", "value": "7"}],
                    "action_values": [{"action_type": "purchase", "value": "70"}],
                }
            ]
        }
    )
    _attach_session(handler, session)

    table = InsightsTable(handler)
    query = parse_sql("SELECT * FROM insights")

    df = table.select(query)

    assert df.iloc[0]["purchases"] == 7
    assert df.iloc[0]["purchase_value"] == 70


# ---------------------------------------------------------------------------
# 15. insights date range resolution
# ---------------------------------------------------------------------------


def test_insights_defaults_to_last_30_days(handler):
    session = MagicMock()
    session.get.return_value = _build_json_response({"data": []})
    _attach_session(handler, session)

    table = InsightsTable(handler)
    query = parse_sql("SELECT * FROM insights")

    table.select(query)

    sent_time_range = json.loads(session.get.call_args.kwargs["params"]["time_range"])
    from datetime import datetime, timedelta, timezone

    today = datetime.now(timezone.utc).date()
    assert sent_time_range["until"] == today.isoformat()
    assert sent_time_range["since"] == (today - timedelta(days=30)).isoformat()


def test_insights_explicit_start_and_end_date_build_time_range(handler):
    session = MagicMock()
    session.get.return_value = _build_json_response({"data": []})
    _attach_session(handler, session)

    table = InsightsTable(handler)
    query = parse_sql("SELECT * FROM insights WHERE start_date = '2026-01-01' AND end_date = '2026-01-31'")

    table.select(query)

    sent_time_range = json.loads(session.get.call_args.kwargs["params"]["time_range"])
    assert sent_time_range == {"since": "2026-01-01", "until": "2026-01-31"}
    assert "date_preset" not in session.get.call_args.kwargs["params"]


def test_insights_date_preset_alone_is_passed_through(handler):
    session = MagicMock()
    session.get.return_value = _build_json_response({"data": []})
    _attach_session(handler, session)

    table = InsightsTable(handler)
    query = parse_sql("SELECT * FROM insights WHERE date_preset = 'last_30d'")

    table.select(query)

    sent_params = session.get.call_args.kwargs["params"]
    assert sent_params["date_preset"] == "last_30d"
    assert "time_range" not in sent_params


# ---------------------------------------------------------------------------
# 16. insights time_increment default rule
# ---------------------------------------------------------------------------


def test_insights_time_increment_defaults_to_daily_when_date_start_selected(handler):
    session = MagicMock()
    session.get.return_value = _build_json_response({"data": []})
    _attach_session(handler, session)

    table = InsightsTable(handler)
    query = parse_sql("SELECT date_start, spend FROM insights")

    table.select(query)

    assert session.get.call_args.kwargs["params"]["time_increment"] == 1


def test_insights_time_increment_defaults_to_all_days_when_date_start_not_selected(handler):
    session = MagicMock()
    session.get.return_value = _build_json_response({"data": []})
    _attach_session(handler, session)

    table = InsightsTable(handler)
    query = parse_sql("SELECT campaign_name, spend FROM insights")

    table.select(query)

    assert session.get.call_args.kwargs["params"]["time_increment"] == "all_days"


def test_insights_time_increment_explicit_value_overrides_default(handler):
    session = MagicMock()
    session.get.return_value = _build_json_response({"data": []})
    _attach_session(handler, session)

    table = InsightsTable(handler)
    query = parse_sql("SELECT campaign_name, spend FROM insights WHERE time_increment = 7")

    table.select(query)

    assert session.get.call_args.kwargs["params"]["time_increment"] == 7


# ---------------------------------------------------------------------------
# 17. insights WHERE campaign_id -> filtering + applied marking
# ---------------------------------------------------------------------------


def test_insights_campaign_id_filter_becomes_filtering_param_and_is_applied(handler):
    session = MagicMock()
    session.get.return_value = _build_json_response(
        {"data": [{"campaign_id": "42", "spend": "10"}]}
    )
    _attach_session(handler, session)

    table = InsightsTable(handler)
    query = parse_sql("SELECT campaign_id, spend FROM insights WHERE campaign_id = '42'")

    df = table.select(query)

    sent_filtering = json.loads(session.get.call_args.kwargs["params"]["filtering"])
    assert sent_filtering == [{"field": "campaign.id", "operator": "EQUAL", "value": "42"}]

    # This is the CLAUDE.md planner bug #3 regression guard: the handler-consumed WHERE
    # column must be published via df.attrs so SubSelectStepCall doesn't re-filter it.
    assert df.attrs["_applied_where_columns"] == {"campaign_id"}
    assert len(df) == 1


def test_insights_campaign_id_in_filter_becomes_in_operator(handler):
    session = MagicMock()
    session.get.return_value = _build_json_response(
        {"data": [{"campaign_id": "1", "spend": "1"}, {"campaign_id": "2", "spend": "2"}]}
    )
    _attach_session(handler, session)

    table = InsightsTable(handler)
    query = parse_sql("SELECT campaign_id, spend FROM insights WHERE campaign_id IN ('1', '2')")

    df = table.select(query)

    sent_filtering = json.loads(session.get.call_args.kwargs["params"]["filtering"])
    assert sent_filtering == [{"field": "campaign.id", "operator": "IN", "value": ["1", "2"]}]
    assert df.attrs["_applied_where_columns"] == {"campaign_id"}


# ---------------------------------------------------------------------------
# 18. insights validation
# ---------------------------------------------------------------------------


def test_insights_invalid_level_raises_value_error(handler):
    table = InsightsTable(handler)
    query = parse_sql("SELECT * FROM insights WHERE level = 'bogus'")

    with pytest.raises(ValueError, match="Invalid level"):
        table.select(query)


def test_insights_invalid_breakdown_raises_value_error(handler):
    session = MagicMock()
    session.get.return_value = _build_json_response({"data": []})
    _attach_session(handler, session)

    table = InsightsTable(handler)
    query = parse_sql("SELECT * FROM insights WHERE breakdowns = 'not_a_real_breakdown'")

    with pytest.raises(ValueError, match="Invalid breakdown"):
        table.select(query)


# ---------------------------------------------------------------------------
# 19. insights async fallback for large requests
# ---------------------------------------------------------------------------


def test_insights_falls_back_to_async_report_on_large_request_error(handler):
    # error.code == 1 / error_subcode 99 ("reduce the amount of data") is deliberately
    # excluded from _is_retryable(), so the synchronous GET fails on the very first
    # call and the async fallback kicks in immediately (no retry budget wasted).
    session = MagicMock()
    session.get.side_effect = [
        _build_json_response(
            {
                "error": {
                    "message": "Please reduce the amount of data you're asking for",
                    "code": 1,
                    "error_subcode": 99,
                }
            },
            status_code=400,
        ),
        # poll GET /{report_run_id} -- job completed.
        _build_json_response({"async_status": "Job Completed", "async_percent_completion": 100}),
        # GET /{report_run_id}/insights -- final rows.
        _build_json_response({"data": [{"campaign_id": "1", "spend": "999"}]}),
    ]
    session.post.return_value = _build_json_response({"report_run_id": "999888777"})
    _attach_session(handler, session)

    table = InsightsTable(handler)
    query = parse_sql("SELECT campaign_id, spend FROM insights")

    with patch(
        "mindsdb.integrations.handlers.meta_ads_handler.tables.insights.time.sleep"
    ) as mock_poll_sleep, patch(
        "mindsdb.integrations.handlers.meta_ads_handler.tables.insights.time.monotonic",
        side_effect=[0.0, 1.0, 2.0, 3.0],
    ):
        df = table.select(query)

    session.post.assert_called_once()
    assert df.iloc[0]["campaign_id"] == "1"
    assert df.iloc[0]["spend"] == 999
    mock_poll_sleep.assert_not_called()  # job completed on the first poll, no waiting needed


def test_insights_async_fallback_raises_on_job_failed(handler):
    session = MagicMock()
    session.get.side_effect = [
        _build_json_response(
            {"error": {"message": "reduce the amount of data", "code": 1, "error_subcode": 99}},
            status_code=400,
        ),
        _build_json_response({"async_status": "Job Failed"}),
    ]
    session.post.return_value = _build_json_response({"report_run_id": "999888777"})
    _attach_session(handler, session)

    table = InsightsTable(handler)
    query = parse_sql("SELECT campaign_id, spend FROM insights")

    with pytest.raises(RuntimeError, match="Job Failed"):
        table.select(query)


def test_insights_does_not_fall_back_on_unrelated_error(handler):
    session = MagicMock()
    session.get.return_value = _build_json_response(
        {"error": {"message": "Invalid OAuth access token", "code": 190}},
        status_code=400,
    )
    _attach_session(handler, session)

    table = InsightsTable(handler)
    query = parse_sql("SELECT campaign_id, spend FROM insights")

    with pytest.raises(RuntimeError, match="Invalid OAuth access token"):
        table.select(query)

    session.post.assert_not_called()


# ---------------------------------------------------------------------------
# 20. numeric coercion
# ---------------------------------------------------------------------------


def test_insights_string_metrics_are_coerced_to_numeric(handler):
    session = MagicMock()
    session.get.return_value = _build_json_response(
        {"data": [{"campaign_id": "1", "impressions": "1234", "cpc": "12.34"}]}
    )
    _attach_session(handler, session)

    table = InsightsTable(handler)
    query = parse_sql("SELECT campaign_id, impressions, cpc FROM insights")

    df = table.select(query)

    assert df.iloc[0]["impressions"] == 1234
    assert df.iloc[0]["cpc"] == pytest.approx(12.34)


def test_campaigns_budget_fields_are_coerced_to_numeric(handler):
    session = MagicMock()
    session.get.return_value = _build_json_response(
        {"data": [{"id": "1", "daily_budget": "5000", "lifetime_budget": "0"}]}
    )
    _attach_session(handler, session)

    table = CampaignsTable(handler)
    query = parse_sql("SELECT * FROM campaigns")

    df = table.select(query)

    assert df.iloc[0]["daily_budget"] == 5000
    assert df.iloc[0]["lifetime_budget"] == 0


# ---------------------------------------------------------------------------
# Extra: ad_sets / ads WHERE routing to nested edges (not in the numbered list,
# but directly exercised by the spec's WHERE tables so covered here too).
# ---------------------------------------------------------------------------


def test_ad_sets_campaign_id_filter_routes_to_nested_edge(handler):
    session = MagicMock()
    session.get.return_value = _build_json_response({"data": [{"id": "20", "campaign_id": "1"}]})
    _attach_session(handler, session)

    table = AdSetsTable(handler)
    query = parse_sql("SELECT * FROM ad_sets WHERE campaign_id = '1'")

    df = table.select(query)

    called_url = session.get.call_args.args[0]
    assert called_url == "https://graph.facebook.com/v25.0/1/adsets"
    assert df.iloc[0]["campaign_id"] == "1"


def test_native_query_returns_handler_response(handler):
    session = MagicMock()
    session.get.return_value = _build_json_response({"data": [{"id": "1", "name": "Campaign 1"}]})
    _attach_session(handler, session)

    response = handler.native_query("SELECT id, name FROM campaigns")

    # BaseHandler.__init_subclass__ auto-wraps native_query() to normalize the legacy
    # HandlerResponse returned by APIHandler.query() into a TableResponse.
    assert isinstance(response, TableResponse)
    assert response.type == RESPONSE_TYPE.TABLE
    assert response.data_frame.iloc[0]["name"] == "Campaign 1"
