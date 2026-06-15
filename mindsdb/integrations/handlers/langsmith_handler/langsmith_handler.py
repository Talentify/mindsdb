from mindsdb.integrations.libs.api_handler import APIHandler
from mindsdb.integrations.libs.response import HandlerStatusResponse
from mindsdb.integrations.utilities.sql_utils import FilterOperator
from .langsmith_tables import LangSmithRunsTable, LangSmithThreadsTable, LangSmithThreadRunsTable

DEFAULT_LIMIT = 100
DEFAULT_SCAN_LIMIT = 1000


class LangSmithHandler(APIHandler):
    name = "langsmith"

    def __init__(self, name, **kwargs):
        super().__init__(name)
        self.connection_data = kwargs.get("connection_data", {})
        self.client = None
        self.is_connected = False
        self._register_table("runs", LangSmithRunsTable(self))
        self._register_table("threads", LangSmithThreadsTable(self))
        self._register_table("thread_runs", LangSmithThreadRunsTable(self))

    def connect(self):
        if self.is_connected:
            return self.client
        import langsmith
        kw = {}
        for k in ("api_key", "api_url", "workspace_id"):
            if self.connection_data.get(k) is not None:
                kw[k] = self.connection_data[k]
        self.client = langsmith.Client(**kw)
        self.is_connected = True
        return self.client

    def check_connection(self):
        try:
            client = self.connect()
            kwargs = {"limit": 1}
            if self.connection_data.get("project_name"):
                kwargs["project_name"] = self.connection_data["project_name"]
            list(client.list_runs(**kwargs))
            return HandlerStatusResponse(True)
        except Exception as e:
            return HandlerStatusResponse(False, str(e))

    def _native_conditions(self, conditions, supported_ops, pseudo_columns=None):
        pseudo_columns = pseudo_columns or set()
        native = {}
        remaining = []
        for c in conditions:
            if c.column not in supported_ops:
                remaining.append(c)
                continue

            if c.op not in supported_ops[c.column]:
                if c.column in pseudo_columns:
                    raise ValueError(f"Unsupported operator {c.op.value} for LangSmith parameter '{c.column}'")
                remaining.append(c)
                continue

            if c.column in {"is_root", "error"} and not isinstance(c.value, bool):
                if c.column in pseudo_columns:
                    raise ValueError(f"LangSmith parameter '{c.column}' requires a boolean value")
                remaining.append(c)
                continue

            if c.column in native and native[c.column] != c.value:
                raise ValueError(f"Multiple values for LangSmith parameter '{c.column}' are not supported")

            if c.column in native:
                c.applied = True
            else:
                native[c.column] = c.value
                c.applied = True
        return native, remaining

    def _default_project_name(self, native):
        return native.get("project_name") or self.connection_data.get("project_name")

    def _fetch_limit(self, limit, has_unapplied):
        limit = DEFAULT_LIMIT if limit is None else int(limit)
        return max(limit, DEFAULT_SCAN_LIMIT) if has_unapplied else limit

    def _list_runs(self, conditions, limit):
        client = self.connect()
        eq = {FilterOperator.EQUAL}
        supported = {
            "project_name": eq,
            "project_id": eq,
            "run_type": eq,
            "trace_id": eq,
            "reference_example_id": eq,
            "parent_run_id": eq,
            "is_root": eq,
            "error": eq,
            "start_time": {FilterOperator.GREATER_THAN_OR_EQUAL},
            "filter": eq,
            "trace_filter": eq,
            "tree_filter": eq,
            "query": eq,
        }
        pseudo = {"project_name", "project_id", "is_root", "filter", "trace_filter", "tree_filter", "query"}
        native, remaining = self._native_conditions(conditions, supported, pseudo_columns=pseudo)
        if not native.get("project_name") and not native.get("project_id") and self.connection_data.get("project_name"):
            native["project_name"] = self.connection_data["project_name"]
        fetch_limit = self._fetch_limit(limit, bool(remaining))
        kwargs = {k: v for k, v in native.items() if k != "query"}
        if native.get("query") is not None:
            kwargs["query"] = native["query"]
        kwargs["limit"] = fetch_limit
        runs = list(client.list_runs(**kwargs))
        import pandas as pd
        rows = []
        from .langsmith_tables import _run_row
        for run in runs:
            rows.append(_run_row(run))
        return pd.DataFrame(rows, columns=self._tables["runs"].get_columns())

    def _list_threads(self, conditions, limit):
        client = self.connect()
        eq = {FilterOperator.EQUAL}
        supported = {
            "project_name": eq,
            "project_id": eq,
            "filter": eq,
            "start_time": {FilterOperator.GREATER_THAN_OR_EQUAL},
        }
        pseudo = {"project_name", "project_id", "filter", "start_time"}
        native, remaining = self._native_conditions(conditions, supported, pseudo_columns=pseudo)
        if not native.get("project_name") and not native.get("project_id") and self.connection_data.get("project_name"):
            native["project_name"] = self.connection_data["project_name"]
        fetch_limit = self._fetch_limit(limit, bool(remaining))
        kwargs = dict(native)
        kwargs["limit"] = fetch_limit
        threads = list(client.list_threads(**kwargs))
        import pandas as pd
        from .langsmith_tables import _get, _json_dumps
        rows = []
        for t in threads:
            rows.append({
                "thread_id": _get(t, "thread_id", "id"),
                "run_count": _get(t, "run_count", "count"),
                "min_start_time": _get(t, "min_start_time"),
                "max_start_time": _get(t, "max_start_time"),
                "runs": _json_dumps(_get(t, "runs")),
            })
        return pd.DataFrame(rows, columns=self._tables["threads"].get_columns())

    def _list_thread_runs(self, conditions, limit):
        client = self.connect()
        thread_id = next((c.value for c in conditions if c.column == "thread_id" and c.op == FilterOperator.EQUAL), None)
        if not thread_id:
            raise ValueError("WHERE thread_id = '...' is required for thread_runs")
        eq = {FilterOperator.EQUAL}
        supported = {"thread_id": eq, "project_name": eq, "project_id": eq, "filter": eq, "is_root": eq}
        unsupported_order = next((c for c in conditions if c.column == "order" and c.op != FilterOperator.EQUAL), None)
        if unsupported_order is not None:
            raise ValueError("thread_runs order only supports equality")
        order_condition = next((c for c in conditions if c.column == "order" and c.op == FilterOperator.EQUAL), None)
        order = None
        if order_condition is not None:
            if order_condition.value not in ("asc", "desc"):
                raise ValueError("thread_runs order must be 'asc' or 'desc'")
            order = order_condition.value
            order_condition.applied = True
        pseudo = {"thread_id", "project_name", "project_id", "filter", "is_root"}
        native, remaining = self._native_conditions(conditions, supported, pseudo_columns=pseudo)
        if order_condition in remaining:
            remaining.remove(order_condition)
        if not native.get("project_name") and not native.get("project_id") and self.connection_data.get("project_name"):
            native["project_name"] = self.connection_data["project_name"]
        fetch_limit = self._fetch_limit(limit, bool(remaining))
        kwargs = dict(native)
        if order is not None:
            kwargs["order"] = order
        kwargs["limit"] = fetch_limit
        runs = list(client.read_thread(**kwargs))
        import pandas as pd
        from .langsmith_tables import _thread_run_row
        return pd.DataFrame([_thread_run_row(r, thread_id) for r in runs], columns=self._tables["thread_runs"].get_columns())
