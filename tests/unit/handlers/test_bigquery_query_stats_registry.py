import pytest

try:
    from mindsdb.integrations.handlers.bigquery_handler import query_stats_registry as reg
except ImportError:
    pytestmark = pytest.mark.skip("Bigquery handler not installed")


@pytest.fixture(autouse=True)
def _clear_registry():
    reg._registry.clear()
    reg._last_evict = 0.0
    yield
    reg._registry.clear()
    reg._last_evict = 0.0


def test_accumulate_then_pop_returns_stats_without_internal_ts():
    reg.accumulate("q1", bytes_billed=1024, cache_hit=False, project_id="p")

    stats = reg.pop("q1")

    assert stats == {"total_bytes_billed": 1024, "cache_hit": False, "project_id": "p"}
    # The id is consumed on pop.
    assert reg.pop("q1") == {}


def test_accumulate_sums_bytes_and_ands_cache_hit_across_calls():
    # Mimics a JOIN that hits the handler once per BigQuery table.
    reg.accumulate("q1", bytes_billed=1000, cache_hit=True, project_id="p")
    reg.accumulate("q1", bytes_billed=500, cache_hit=False, project_id="p")

    stats = reg.pop("q1")

    assert stats["total_bytes_billed"] == 1500
    # cache_hit only stays True when ALL sub-queries were cache hits.
    assert stats["cache_hit"] is False


def test_pop_unknown_id_returns_empty_dict():
    assert reg.pop("nope") == {}


def test_evict_removes_expired_entries(monkeypatch):
    clock = {"now": 1000.0}
    monkeypatch.setattr(reg.time, "monotonic", lambda: clock["now"])

    reg.accumulate("old", bytes_billed=1, cache_hit=False, project_id="p")

    # Advance past TTL + eviction interval, then trigger another accumulate.
    clock["now"] = 1000.0 + reg._TTL_SECONDS + reg._EVICT_INTERVAL_SECONDS + 1
    reg.accumulate("new", bytes_billed=2, cache_hit=False, project_id="p")

    assert reg.pop("old") == {}
    assert reg.pop("new")["total_bytes_billed"] == 2
