import json
import sys
from datetime import datetime, timezone
from types import ModuleType, SimpleNamespace

import pytest
from mindsdb_sql_parser import parse_sql

const_mod = ModuleType("mindsdb.integrations.libs.const")
setattr(const_mod, "HANDLER_TYPE", SimpleNamespace(DATA="data"))
setattr(const_mod, "HANDLER_SUPPORT_LEVEL", SimpleNamespace(COMMUNITY="community"))
setattr(
    const_mod,
    "HANDLER_CONNECTION_ARG_TYPE",
    SimpleNamespace(STR="str", INT="int", BOOL="bool", URL="url", PATH="path", DICT="dict", PWD="pwd"),
)
sys.modules.setdefault("mindsdb.integrations.libs.const", const_mod)

from mindsdb.integrations.handlers.langsmith_handler import Handler as LangSmithHandlerExport
from mindsdb.integrations.handlers.langsmith_handler.langsmith_handler import LangSmithHandler
from mindsdb.integrations.utilities.sql_utils import FilterCondition, FilterOperator


class _Run(SimpleNamespace):
    pass


class _Thread(SimpleNamespace):
    pass


class FakeClient:
    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.list_runs_calls = []
        self.list_threads_calls = []
        self.read_thread_calls = []

    def list_runs(self, **kwargs):
        self.list_runs_calls.append(kwargs)
        return iter([_Run(id="r1", name="run1", run_type="chain", extra={"metadata": {"k": 1}}, inputs={"a": 1}, outputs={"b": 2}, tags=["x"], start_time=datetime(2026, 6, 1, tzinfo=timezone.utc), end_time=None, latency=None)])

    def list_threads(self, **kwargs):
        self.list_threads_calls.append(kwargs)
        return [_Thread(thread_id="t1", run_count=2, min_start_time=datetime(2026, 6, 1, tzinfo=timezone.utc), max_start_time=datetime(2026, 6, 2, tzinfo=timezone.utc), runs=[{"id": "r1"}])]

    def read_thread(self, **kwargs):
        self.read_thread_calls.append(kwargs)
        return iter([_Run(id="r2", name="run2", run_type="llm", extra={}, inputs={}, outputs={}, tags=[], start_time=datetime(2026, 6, 1, tzinfo=timezone.utc), end_time=datetime(2026, 6, 1, 0, 0, 30, tzinfo=timezone.utc), latency=None)])


class ManyRunsClient(FakeClient):
    def list_runs(self, **kwargs):
        self.list_runs_calls.append(kwargs)
        return iter([
            _Run(id=f"r{i}", name=f"run{i}", run_type="chain", extra={}, inputs={}, outputs={}, tags=[])
            for i in range(30)
        ])


def _handler(fake_client=None, connection_data=None):
    if connection_data is None:
        connection_data = {"project_name": "demo"}
    h = LangSmithHandler("langsmith", connection_data=connection_data)
    h.connect = lambda: fake_client or FakeClient()  # type: ignore[method-assign]
    return h


def test_handler_metadata_import_without_sdk():
    assert LangSmithHandlerExport is not None


def test_runs_translate_params_and_mark_applied(monkeypatch):
    fake = FakeClient()
    h = _handler(fake)
    table = h._tables["runs"]
    conds = [
        FilterCondition("project_name", FilterOperator.EQUAL, "demo"),
        FilterCondition("start_time", FilterOperator.EQUAL, datetime(2026, 6, 1, tzinfo=timezone.utc)),
        FilterCondition("start_time", FilterOperator.GREATER_THAN_OR_EQUAL, datetime(2026, 6, 1, tzinfo=timezone.utc)),
        FilterCondition("filter", FilterOperator.EQUAL, "source='x'"),
    ]
    df = table.list(conditions=conds, limit=None)
    assert fake.list_runs_calls[0]["project_name"] == "demo"
    assert fake.list_runs_calls[0]["filter"] == "source='x'"
    assert fake.list_runs_calls[0]["limit"] == 1000
    assert conds[0].applied is True
    assert conds[2].applied is True
    assert conds[1].applied is False
    assert len(df) == 1 and df.iloc[0]["latency_seconds"] is None
    assert json.loads(df.iloc[0]["metadata"]) == {"k": 1}


def test_runs_do_not_consume_wrong_operator_for_native_param():
    fake = FakeClient()
    h = _handler(fake, connection_data={})
    cond = FilterCondition("project_name", FilterOperator.GREATER_THAN, "demo")

    with pytest.raises(ValueError, match="project_name"):
        h._tables["runs"].list(conditions=[cond], limit=20)


def test_runs_overfetch_when_unsupported_filter_remains():
    fake = FakeClient()
    h = _handler(fake)
    table = h._tables["runs"]
    table.list(conditions=[FilterCondition("name", FilterOperator.EQUAL, "run1")], limit=20)
    assert fake.list_runs_calls[0]["limit"] == 1000


def test_runs_keep_sql_limit_when_all_filters_are_native():
    fake = FakeClient()
    h = _handler(fake)
    table = h._tables["runs"]
    table.list(conditions=[FilterCondition("project_name", FilterOperator.EQUAL, "demo"), FilterCondition("filter", FilterOperator.EQUAL, "source='x'")], limit=20)
    assert fake.list_runs_calls[0]["limit"] == 20


def test_runs_support_complex_select_targets():
    h = _handler(FakeClient())
    df = h._tables["runs"].select(parse_sql("SELECT SUM(total_tokens) AS tokens FROM runs"))
    assert "total_tokens" in df.columns


def test_complex_where_uses_scan_limit_before_outer_filtering():
    fake = FakeClient()
    h = _handler(fake)

    h._tables["runs"].select(parse_sql("SELECT id FROM runs WHERE LOWER(name) = 'run1' LIMIT 20"))

    assert fake.list_runs_calls[0]["limit"] == 1000


def test_complex_where_does_not_apply_sql_limit_before_outer_filtering():
    h = _handler(ManyRunsClient())

    df = h._tables["runs"].select(parse_sql("SELECT id FROM runs WHERE LOWER(name) = 'run1' LIMIT 20"))

    assert len(df) == 30


def test_complex_where_referencing_pseudo_param_raises():
    h = _handler(FakeClient())

    with pytest.raises(ValueError, match="project_name"):
        h._tables["runs"].select(parse_sql("SELECT id FROM runs WHERE project_name = 'demo' OR name = 'run1'"))


def test_unsupported_pseudo_param_operator_raises_at_select_level():
    h = _handler(FakeClient())

    with pytest.raises(ValueError, match="filter"):
        h._tables["runs"].select(parse_sql("SELECT id FROM runs WHERE filter != 'eq(name, \\\"x\\\")'"))


def test_is_root_requires_boolean_value():
    h = _handler(FakeClient())

    with pytest.raises(ValueError, match="is_root"):
        h._tables["runs"].select(parse_sql("SELECT id FROM runs WHERE is_root = 'true'"))


def test_sql_order_by_not_sent_to_langsmith_sdk():
    fake = FakeClient()
    h = _handler(fake)

    h._tables["runs"].select(parse_sql("SELECT id FROM runs WHERE project_name = 'demo' ORDER BY name DESC"))

    assert "order" not in fake.list_runs_calls[0]
    assert "order_by" not in fake.list_runs_calls[0]


def test_consumed_handler_params_are_propagated_to_dataframe_attrs():
    h = _handler(FakeClient())
    df = h._tables["runs"].select(parse_sql("SELECT id FROM runs WHERE project_name = 'demo'"))

    assert df.attrs["_applied_where_columns"] == {"project_name"}


def test_threads_flattening_and_thread_runs_require_thread_id():
    fake = FakeClient()
    h = _handler(fake)
    threads = h._tables["threads"].list(conditions=[FilterCondition("project_name", FilterOperator.EQUAL, "demo")], limit=5)
    assert threads.iloc[0]["thread_id"] == "t1"
    assert json.loads(threads.iloc[0]["runs"]) == [{"id": "r1"}]
    with pytest.raises(ValueError):
        h._tables["thread_runs"].list(conditions=[], limit=5)


def test_thread_runs_order_and_default_limit():
    fake = FakeClient()
    h = _handler(fake)
    df = h._tables["thread_runs"].list(conditions=[FilterCondition("thread_id", FilterOperator.EQUAL, "t1"), FilterCondition("order", FilterOperator.EQUAL, "desc")], limit=None)
    assert fake.read_thread_calls[0]["thread_id"] == "t1"
    assert fake.read_thread_calls[0]["order"] == "desc"
    assert fake.read_thread_calls[0]["limit"] == 100
    assert df.iloc[0]["latency_seconds"] == 30.0
    assert df.iloc[0]["thread_id"] == "t1"


def test_thread_id_is_selectable_from_thread_runs():
    h = _handler(FakeClient())
    df = h._tables["thread_runs"].select(parse_sql("SELECT thread_id, name FROM thread_runs WHERE thread_id = 't1'"))

    assert "thread_id" in df.columns
    assert df.iloc[0]["thread_id"] == "t1"


def test_thread_runs_rejects_invalid_order():
    h = _handler(FakeClient())

    with pytest.raises(ValueError, match="order"):
        h._tables["thread_runs"].list(
            conditions=[
                FilterCondition("thread_id", FilterOperator.EQUAL, "t1"),
                FilterCondition("order", FilterOperator.EQUAL, "sideways"),
            ],
            limit=5,
        )
