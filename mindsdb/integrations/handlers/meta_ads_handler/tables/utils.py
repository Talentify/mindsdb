from __future__ import annotations

from typing import Any, Iterable, List, Optional, Sequence

import pandas as pd
from mindsdb_sql_parser import ast

from mindsdb.integrations.utilities.sql_utils import FilterCondition, FilterOperator


def _get_condition_value(
    conditions: Optional[List[FilterCondition]],
    column: str,
    ops: Sequence[FilterOperator] = (FilterOperator.EQUAL,),
) -> Any:
    """Return the value of the first matching condition and mark it applied."""
    for condition in conditions or []:
        if condition.column != column or condition.op not in ops:
            continue
        condition.applied = True
        return condition.value
    return None


def _get_condition_values(
    conditions: Optional[List[FilterCondition]],
    column: str,
    ops: Sequence[FilterOperator] = (FilterOperator.EQUAL, FilterOperator.IN),
) -> Optional[List[Any]]:
    """Return a list of values for the first matching condition and mark it applied.

    EQUAL -> [value]; IN -> the list of values as-is.
    """
    for condition in conditions or []:
        if condition.column != column or condition.op not in ops:
            continue
        condition.applied = True
        if condition.op == FilterOperator.IN:
            value = condition.value
            return list(value) if isinstance(value, (list, tuple)) else [value]
        return [condition.value]
    return None


def _to_numeric(df: pd.DataFrame, cols: Iterable[str]) -> pd.DataFrame:
    """Coerce the given columns to numeric. Graph returns all metrics as strings."""
    for col in cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def _join_list(value: Any) -> Any:
    """Comma-join a list of scalar values (e.g. enum strings); pass through anything
    else unchanged.

    Encoding rule (applied consistently across campaigns/ad_sets/ads): a list of
    scalar enums/strings is comma-joined so `=`/`LIKE` behave the way a user expects
    in SQL, e.g. `special_ad_categories = 'HOUSING'`. A JSON-encoded list would make
    that comparison silently return zero rows instead. Lists of *objects* (or other
    nested structures) are a different case and stay JSON-encoded -- see the
    JSON_COLUMNS handling in each table.

    Elements are str()-cast before joining: some of these fields are documented as
    list<int> rather than list<string> (e.g. ad_sets' flattened `genders`, coded 1/2),
    and `",".join(...)` raises TypeError on a non-string element.
    """
    return ",".join(str(v) for v in value) if isinstance(value, list) else value


def _collect_identifiers(node) -> List[str]:
    """Recursively collect all Identifier column names from any AST node.

    Walks into CASE WHEN, Function args, BinaryOperation, etc. so that
    columns referenced inside complex expressions are not missed.
    """
    if node is None:
        return []
    if isinstance(node, ast.Identifier):
        return [str(node.parts[-1])]
    if isinstance(node, ast.Case):
        names = []
        for condition, result in node.rules:
            names.extend(_collect_identifiers(condition))
            names.extend(_collect_identifiers(result))
        names.extend(_collect_identifiers(node.default))
        return names
    if isinstance(node, ast.Function):
        names = []
        for arg in (node.args or []):
            names.extend(_collect_identifiers(arg))
        return names
    if isinstance(node, ast.BinaryOperation):
        return _collect_identifiers(node.args[0]) + _collect_identifiers(node.args[1])
    if isinstance(node, ast.UnaryOperation):
        return _collect_identifiers(node.args[0])
    if isinstance(node, ast.TypeCast):
        return _collect_identifiers(node.arg)
    return []
