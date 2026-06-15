import json
from dataclasses import asdict, is_dataclass
from datetime import datetime
from types import SimpleNamespace

import pandas as pd
from mindsdb_sql_parser.ast.select.identifier import Identifier

from mindsdb.integrations.libs.api_handler import APIResource
from mindsdb.integrations.utilities.sql_utils import (
    extract_comparison_conditions,
    filter_dataframe,
    sort_dataframe,
    SortColumn,
)

DEFAULT_SCAN_LIMIT = 1000


def _to_plain(value):
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, datetime):
        return value
    if isinstance(value, dict):
        return {k: _to_plain(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_to_plain(v) for v in value]
    if is_dataclass(value):
        return _to_plain(asdict(value))
    if isinstance(value, SimpleNamespace):
        return _to_plain(vars(value))
    if hasattr(value, "model_dump"):
        return _to_plain(value.model_dump())
    if hasattr(value, "dict"):
        try:
            return _to_plain(value.dict())
        except Exception:
            pass
    if hasattr(value, "__dict__"):
        return _to_plain({k: v for k, v in vars(value).items() if not k.startswith("_")})
    return value


def _json_dumps(value):
    if value is None:
        return None
    return json.dumps(_to_plain(value), default=str, sort_keys=True)


def _get(obj, *names, default=None):
    for name in names:
        if isinstance(obj, dict) and name in obj:
            return obj[name]
        if hasattr(obj, name):
            return getattr(obj, name)
    return default


def _run_row(run):
    extra = _get(run, "extra", default={}) or {}
    metadata = extra.get("metadata") if isinstance(extra, dict) else _get(extra, "metadata")
    start = _get(run, "start_time")
    end = _get(run, "end_time")
    latency = _get(run, "latency")
    if latency is None and start and end:
        latency = (end - start).total_seconds()
    elif hasattr(latency, "total_seconds"):
        latency = latency.total_seconds()
    return {
        "id": _get(run, "id"), "name": _get(run, "name"), "run_type": _get(run, "run_type"),
        "trace_id": _get(run, "trace_id"), "parent_run_id": _get(run, "parent_run_id"),
        "session_id": _get(run, "session_id"), "status": _get(run, "status"), "error": _get(run, "error"),
        "start_time": start, "end_time": end, "latency_seconds": latency,
        "total_tokens": _get(run, "total_tokens"), "prompt_tokens": _get(run, "prompt_tokens"), "completion_tokens": _get(run, "completion_tokens"),
        "total_cost": _get(run, "total_cost"), "prompt_cost": _get(run, "prompt_cost"), "completion_cost": _get(run, "completion_cost"),
        "reference_example_id": _get(run, "reference_example_id"), "manifest_id": _get(run, "manifest_id"), "dotted_order": _get(run, "dotted_order"),
        "in_dataset": _get(run, "in_dataset"), "app_path": _get(run, "app_path"),
        "inputs": _json_dumps(_get(run, "inputs")), "outputs": _json_dumps(_get(run, "outputs")), "metadata": _json_dumps(metadata),
        "extra": _json_dumps(extra), "tags": _json_dumps(_get(run, "tags")), "events": _json_dumps(_get(run, "events")),
        "serialized": _json_dumps(_get(run, "serialized")), "feedback_stats": _json_dumps(_get(run, "feedback_stats")),
        "parent_run_ids": _json_dumps(_get(run, "parent_run_ids")), "child_run_ids": _json_dumps(_get(run, "child_run_ids")),
        "attachments": _json_dumps(_get(run, "attachments")),
    }


def _thread_run_row(run, thread_id):
    row = _run_row(run)
    row["thread_id"] = thread_id
    return row


def _project_row(project):
    extra = _get(project, "extra", default={}) or {}
    metadata = extra.get("metadata") if isinstance(extra, dict) else _get(extra, "metadata")
    project_id = _get(project, "id")
    return {
        "id": project_id,
        "project_id": project_id,
        "name": _get(project, "name"),
        "description": _get(project, "description"),
        "start_time": _get(project, "start_time"),
        "end_time": _get(project, "end_time"),
        "tenant_id": _get(project, "tenant_id"),
        "reference_dataset_id": _get(project, "reference_dataset_id"),
        "extra": _json_dumps(extra),
        "metadata": _json_dumps(metadata),
        "run_count": _get(project, "run_count"),
        "latency_p50": _get(project, "latency_p50"),
        "latency_p99": _get(project, "latency_p99"),
        "total_tokens": _get(project, "total_tokens"),
        "prompt_tokens": _get(project, "prompt_tokens"),
        "completion_tokens": _get(project, "completion_tokens"),
        "last_run_start_time": _get(project, "last_run_start_time"),
        "feedback_stats": _json_dumps(_get(project, "feedback_stats")),
        "session_feedback_stats": _json_dumps(_get(project, "session_feedback_stats")),
        "run_facets": _json_dumps(_get(project, "run_facets")),
        "total_cost": _get(project, "total_cost"),
        "prompt_cost": _get(project, "prompt_cost"),
        "completion_cost": _get(project, "completion_cost"),
        "first_token_p50": _get(project, "first_token_p50"),
        "first_token_p99": _get(project, "first_token_p99"),
        "error_rate": _get(project, "error_rate"),
    }


def _collect_identifiers(node, seen=None):
    if node is None:
        return set()
    seen = seen or set()
    if id(node) in seen:
        return set()
    seen.add(id(node))

    if isinstance(node, Identifier):
        return {node.parts[-1]}
    if isinstance(node, (list, tuple, set)):
        names = set()
        for item in node:
            names.update(_collect_identifiers(item, seen))
        return names
    if isinstance(node, dict):
        names = set()
        for item in node.values():
            names.update(_collect_identifiers(item, seen))
        return names

    names = set()
    for attr in ("args", "arg", "rules", "default"):
        if hasattr(node, attr):
            names.update(_collect_identifiers(getattr(node, attr), seen))
    return names


def _has_unextracted_where(where, pseudo_columns=None):
    if where is None:
        return False
    pseudo_columns = pseudo_columns or set()
    extracted = extract_comparison_conditions(where, strict=False)
    if not extracted:
        pseudo_refs = _collect_identifiers(where).intersection(pseudo_columns)
        if pseudo_refs:
            refs = ", ".join(sorted(pseudo_refs))
            raise ValueError(f"Unsupported complex WHERE condition on LangSmith parameter(s): {refs}")
        return True
    has_unextracted = False
    for item in extracted:
        if isinstance(item, list):
            continue
        has_unextracted = True
        pseudo_refs = _collect_identifiers(item).intersection(pseudo_columns)
        if pseudo_refs:
            refs = ", ".join(sorted(pseudo_refs))
            raise ValueError(f"Unsupported complex WHERE condition on LangSmith parameter(s): {refs}")
    return has_unextracted


class LangSmithBaseTable(APIResource):
    def select(self, query):
        conditions = self._extract_conditions(query.where)

        limit = None
        if query.limit:
            limit = query.limit.value

        sort = None
        if query.order_by and len(query.order_by) > 0:
            sort = []
            for an_order in query.order_by:
                field_name = an_order.field.parts[-1] if hasattr(an_order.field, "parts") else str(an_order.field)
                sort.append(SortColumn(field_name, an_order.direction.upper() != "DESC"))

        targets = []
        for col in query.targets:
            if isinstance(col, Identifier):
                targets.append(col.parts[-1])

        result = self.list(
            conditions=conditions,
            limit=limit,
            sort=sort,
            targets=targets,
            force_scan=_has_unextracted_where(query.where, self.pseudo_columns),
        )

        filters = []
        for cond in conditions:
            if not cond.applied:
                filters.append([cond.op.value, cond.column, cond.value])

        result = filter_dataframe(result, filters)

        applied_where_cols = {cond.column.lower() for cond in conditions if cond.applied}
        if applied_where_cols:
            result.attrs["_applied_where_columns"] = applied_where_cols

        if sort:
            sort_columns = []
            for idx, a_sort in enumerate(sort):
                if not a_sort.applied:
                    sort_columns.append(query.order_by[idx])

            result = sort_dataframe(result, sort_columns)

        if limit is not None and not _has_unextracted_where(query.where, self.pseudo_columns) and len(result) > limit:
            result = result[: int(limit)]

        return result


class LangSmithProjectsTable(LangSmithBaseTable):
    pseudo_columns = {"name_contains", "reference_dataset_name", "reference_free", "include_stats", "dataset_version", "metadata"}

    def list(self, conditions=None, limit=None, sort=None, targets=None, force_scan=False):
        if force_scan and limit is None:
            limit = DEFAULT_SCAN_LIMIT
        elif force_scan:
            limit = max(int(limit), DEFAULT_SCAN_LIMIT)
        return self.handler._list_projects(conditions or [], limit)

    def get_columns(self):
        return list(_project_row({}).keys())


class LangSmithRunsTable(LangSmithBaseTable):
    pseudo_columns = {"project_name", "project_id", "is_root", "filter", "trace_filter", "tree_filter", "query"}

    def list(self, conditions=None, limit=None, sort=None, targets=None, force_scan=False):
        if force_scan and limit is None:
            limit = DEFAULT_SCAN_LIMIT
        elif force_scan:
            limit = max(int(limit), DEFAULT_SCAN_LIMIT)
        return self.handler._list_runs(conditions or [], limit)

    def get_columns(self):
        return list(_run_row({}).keys())


class LangSmithThreadsTable(LangSmithBaseTable):
    pseudo_columns = {"project_name", "project_id", "filter", "start_time"}

    def list(self, conditions=None, limit=None, sort=None, targets=None, force_scan=False):
        if force_scan and limit is None:
            limit = DEFAULT_SCAN_LIMIT
        elif force_scan:
            limit = max(int(limit), DEFAULT_SCAN_LIMIT)
        return self.handler._list_threads(conditions or [], limit)

    def get_columns(self):
        return ["thread_id", "run_count", "min_start_time", "max_start_time", "runs"]


class LangSmithThreadRunsTable(LangSmithBaseTable):
    pseudo_columns = {"project_name", "project_id", "filter", "is_root", "order"}

    def list(self, conditions=None, limit=None, sort=None, targets=None, force_scan=False):
        if force_scan and limit is None:
            limit = DEFAULT_SCAN_LIMIT
        elif force_scan:
            limit = max(int(limit), DEFAULT_SCAN_LIMIT)
        return self.handler._list_thread_runs(conditions or [], limit)

    def get_columns(self):
        return list(_thread_run_row({}, None).keys())
