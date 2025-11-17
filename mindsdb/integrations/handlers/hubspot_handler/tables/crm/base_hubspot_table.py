"""
Base class for HubSpot tables with shared search functionality.
"""
from typing import List, Dict
from mindsdb.utilities import log

logger = log.getLogger(__name__)


class HubSpotSearchMixin:
    """
    Mixin class providing shared search functionality for HubSpot tables.
    This class should be mixed into APITable subclasses for Companies, Contacts, and Deals.
    """

    @staticmethod
    def _map_operator_to_hubspot(sql_op: str) -> str:
        """
        Map SQL operator to HubSpot search API operator.

        Parameters
        ----------
        sql_op : str
            SQL operator (=, !=, >, <, etc.)

        Returns
        -------
        str
            HubSpot operator (EQ, NEQ, GT, LT, etc.) or None if not supported
        """
        mapping = {
            "=": "EQ",
            "!=": "NEQ",
            "<": "LT",
            "<=": "LTE",
            ">": "GT",
            ">=": "GTE",
            "in": "IN",
            "not in": "NOT_IN",
            "is null": "NOT_HAS_PROPERTY",
            "is not null": "HAS_PROPERTY",
            "between": "BETWEEN",
            "like": "CONTAINS_TOKEN",
            "not like": "NOT_CONTAINS_TOKEN",
        }
        return mapping.get(sql_op.lower())

    @staticmethod
    def _build_search_filters(where_conditions: List[List]) -> List[Dict]:
        """
        Convert WHERE conditions to HubSpot search API filters.

        Parameters
        ----------
        where_conditions : List[List]
            List of conditions in format [[operator, column, value], ...]

        Returns
        -------
        List[Dict]
            List of HubSpot filter dictionaries
        """
        hubspot_filters = []

        for condition in where_conditions:
            if len(condition) < 3:
                logger.warning(f"Invalid condition format: {condition}")
                continue

            op, column, value = condition[0], condition[1], condition[2]
            hubspot_op = HubSpotSearchMixin._map_operator_to_hubspot(op)

            if not hubspot_op:
                logger.warning(f"Unsupported operator '{op}' for HubSpot search, skipping condition")
                continue

            # Handle different operator types
            if op.lower() == "between":
                # BETWEEN: needs value and highValue
                if isinstance(value, (list, tuple)) and len(value) == 2:
                    hubspot_filters.append({
                        "propertyName": column,
                        "operator": "BETWEEN",
                        "value": str(value[0]),
                        "highValue": str(value[1])
                    })
                else:
                    logger.warning(f"Invalid BETWEEN value format: {value}")

            elif op.lower() == "not between":
                # NOT BETWEEN: HubSpot filters in same group are AND, so NOT BETWEEN needs special handling
                if isinstance(value, (list, tuple)) and len(value) == 2:
                    logger.warning("NOT BETWEEN not fully supported by HubSpot search API, skipping")
                else:
                    logger.warning(f"Invalid NOT BETWEEN value format: {value}")

            elif op.lower() in ["in", "not in"]:
                # IN/NOT IN: needs values array
                values_list = value if isinstance(value, list) else [value]
                hubspot_filters.append({
                    "propertyName": column,
                    "operator": hubspot_op,
                    "values": [str(v) for v in values_list]
                })

            elif op.lower() in ["is null", "is not null"]:
                # NULL checks: no value needed
                hubspot_filters.append({
                    "propertyName": column,
                    "operator": hubspot_op
                })

            elif op.lower() in ["like", "not like"]:
                # LIKE: extract search term by removing SQL wildcards
                search_term = str(value).replace('%', '').replace('_', '')
                hubspot_filters.append({
                    "propertyName": column,
                    "operator": hubspot_op,
                    "value": search_term
                })

            else:
                # Standard comparison operators (=, !=, >, <, >=, <=)
                hubspot_filters.append({
                    "propertyName": column,
                    "operator": hubspot_op,
                    "value": str(value)
                })

        return hubspot_filters
