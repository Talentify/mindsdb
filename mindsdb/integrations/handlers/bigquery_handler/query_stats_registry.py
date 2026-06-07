"""Thread-safe in-process registry for BigQuery query execution stats.

Stats are stored keyed by a caller-supplied query_id, accumulated across
multiple handler invocations for the same logical query (e.g. JOINs that
hit the handler once per BQ table), then popped by the caller after the
query returns.
"""
import threading
import time

_lock = threading.Lock()
_registry: dict[str, dict] = {}
_MAX_ENTRIES = 10_000
_TTL_SECONDS = 300.0
_EVICT_INTERVAL_SECONDS = 60.0
_last_evict = 0.0


def accumulate(query_id: str, bytes_billed: int, cache_hit: bool, project_id: str) -> None:
    """Accumulate BigQuery stats for query_id.

    Sums bytes_billed across multiple invocations (JOIN across BQ tables).
    cache_hit stays True only when ALL sub-queries were cache hits.
    """
    now = time.monotonic()
    with _lock:
        _evict(now)
        if len(_registry) >= _MAX_ENTRIES:
            return
        if query_id in _registry:
            _registry[query_id]["total_bytes_billed"] += bytes_billed
            _registry[query_id]["cache_hit"] = _registry[query_id]["cache_hit"] and cache_hit
        else:
            _registry[query_id] = {
                "total_bytes_billed": bytes_billed,
                "cache_hit": cache_hit,
                "project_id": project_id,
                "_ts": now,
            }


def pop(query_id: str) -> dict:
    """Pop and return stats for query_id, or empty dict if not found."""
    with _lock:
        entry = _registry.pop(query_id, {})
    entry.pop("_ts", None)
    return entry


def _evict(now: float) -> None:
    """Remove TTL-expired entries. Must be called with _lock held.

    Throttled to scan at most once per _EVICT_INTERVAL_SECONDS so that the
    O(n) sweep does not run on every accumulate() call.
    """
    global _last_evict
    if now - _last_evict < _EVICT_INTERVAL_SECONDS:
        return
    _last_evict = now
    expired = [k for k, v in _registry.items() if now - v["_ts"] > _TTL_SECONDS]
    for k in expired:
        del _registry[k]
