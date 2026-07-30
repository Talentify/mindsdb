"""Regression tests for IntegrationController.modify().

These guard the `check_connection` parameter, which upstream commit 46bc61a2f
("Updated ALTER DATABASE Commands to Check Connection", #11861) added to both
DatabaseController.update()'s call site and modify() itself. A fork merge once kept
the caller and dropped the callee, so every `ALTER DATABASE <data integration>`
raised `TypeError: modify() got an unexpected keyword argument 'check_connection'`
before reaching any of the logic below.

modify() is exercised directly rather than through `ALTER DATABASE` SQL because the
executor test harness imports the agents stack, which needs optional ML dependencies
that are not installed in a plain unit-test environment.
"""

import os
from unittest.mock import MagicMock, patch

import pytest

os.environ.setdefault("MINDSDB_STORAGE_DIR", "/tmp/mindsdb_integration_controller_test")

from mindsdb.interfaces.database.integrations import IntegrationController  # noqa: E402


def _make_record(data, name="meta_ads_1", engine="meta_ads"):
    record = MagicMock()
    record.data = data
    record.name = name
    record.engine = engine
    return record


def _make_controller(record):
    """Build a controller without running __init__ (which scans every handler dir)."""
    controller = IntegrationController.__new__(IntegrationController)
    controller.handlers_cache = MagicMock()
    controller._get_integration_record = MagicMock(return_value=record)
    controller.create_tmp_handler = MagicMock()
    return controller


def _ok_handler():
    handler = MagicMock()
    handler.check_connection.return_value = MagicMock(success=True, error_message=None)
    return handler


def _failing_handler(error_message="Invalid OAuth access token"):
    handler = MagicMock()
    handler.check_connection.return_value = MagicMock(success=False, error_message=error_message)
    return handler


def test_modify_accepts_check_connection_kwarg():
    """The regression guard: DatabaseController.update() calls modify() with this kwarg."""
    record = _make_record({"access_token": "old"})
    controller = _make_controller(record)
    controller.create_tmp_handler.return_value = _ok_handler()

    with patch("mindsdb.interfaces.database.integrations.db") as mock_db:
        controller.modify("meta_ads_1", {"access_token": "new"}, check_connection=True)

    assert mock_db.session.commit.called


def test_modify_preserves_omitted_keys():
    """Pushing access_token alone must not drop the other connection args."""
    record = _make_record(
        {
            "access_token": "old",
            "ad_account_id": "1234567890",
            "api_version": "v25.0",
            "client_id": "app-id",
            "client_secret": "app-secret",
        }
    )
    controller = _make_controller(record)

    with patch("mindsdb.interfaces.database.integrations.db"):
        controller.modify("meta_ads_1", {"access_token": "new"})

    assert record.data == {
        "access_token": "new",
        "ad_account_id": "1234567890",
        "api_version": "v25.0",
        "client_id": "app-id",
        "client_secret": "app-secret",
    }


def test_modify_rejects_bad_connection_without_persisting():
    """A token that fails check_connection must not be written to the record."""
    original = {"access_token": "old", "ad_account_id": "1234567890"}
    record = _make_record(dict(original))
    controller = _make_controller(record)
    controller.create_tmp_handler.return_value = _failing_handler()

    with patch("mindsdb.interfaces.database.integrations.db") as mock_db:
        with pytest.raises(Exception) as exc_info:
            controller.modify("meta_ads_1", {"access_token": "bad"}, check_connection=True)

    assert "Connection test failed" in str(exc_info.value)
    assert "Invalid OAuth access token" in str(exc_info.value)
    assert record.data == original
    assert not mock_db.session.commit.called


def test_modify_skips_connection_test_when_not_requested():
    """check_connection defaults to False, so no temp handler is built."""
    record = _make_record({"access_token": "old"})
    controller = _make_controller(record)

    with patch("mindsdb.interfaces.database.integrations.db"):
        controller.modify("meta_ads_1", {"access_token": "new"})

    assert not controller.create_tmp_handler.called
    assert record.data["access_token"] == "new"


def test_modify_evicts_cached_handler():
    """The live handler must not keep serving the old token."""
    record = _make_record({"access_token": "old"})
    controller = _make_controller(record)

    with patch("mindsdb.interfaces.database.integrations.db"):
        controller.modify("meta_ads_1", {"access_token": "new"})

    controller.handlers_cache.delete.assert_called_once_with("meta_ads_1")


def test_modify_refuses_demo_object():
    record = _make_record({"is_demo": True, "access_token": "old"})
    controller = _make_controller(record)

    with patch("mindsdb.interfaces.database.integrations.db"):
        with pytest.raises(ValueError, match="demo object"):
            controller.modify("meta_ads_1", {"access_token": "new"})
