"""
Base class for HubSpot tables with shared search functionality and rate limiting.
"""
from typing import List, Dict, Any, Callable
from mindsdb.utilities import log
from mindsdb.integrations.handlers.hubspot_handler.utils.rate_limiter import (
    with_retry,
    batch_operation_with_retry,
    chunk_list,
    handle_hubspot_error
)

logger = log.getLogger(__name__)


class HubSpotSearchMixin:
    """
    Mixin class providing shared search functionality and rate limiting for HubSpot tables.
    This class should be mixed into APITable subclasses for all HubSpot object types.

    Features:
    - SQL operator mapping to HubSpot search API
    - WHERE clause to HubSpot filters conversion
    - Automatic retry with exponential backoff
    - Batch operation chunking for large datasets
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

    def _execute_with_retry(self, operation: Callable[[], Any], operation_name: str = "") -> Any:
        """
        Execute a HubSpot API operation with automatic retry on rate limits.

        Parameters
        ----------
        operation : Callable
            Function that performs the API call
        operation_name : str
            Name of the operation for logging purposes

        Returns
        -------
        Any
            Result from the API operation

        Raises
        ------
        RateLimitError
            If rate limit is exceeded after all retries
        HubSpotAPIError
            If API call fails after all retries
        """
        @with_retry(max_retries=5)
        def execute():
            try:
                return operation()
            except Exception as e:
                error_message = handle_hubspot_error(e)
                logger.error(f"HubSpot API error in {operation_name}: {error_message}")
                raise

        return execute()

    def _batch_create_with_chunking(
        self,
        items: List[Dict[str, Any]],
        create_func: Callable[[List], Any],
        item_name: str = "items"
    ) -> None:
        """
        Create items in batches with automatic chunking and retry.

        Parameters
        ----------
        items : List[Dict[str, Any]]
            List of items to create
        create_func : Callable
            Function that creates a batch of items
        item_name : str
            Name of items for logging (e.g., "contacts", "deals")

        Raises
        ------
        Exception
            If any batch fails after retries
        """
        if not items:
            logger.info(f"No {item_name} to create")
            return

        if len(items) <= 100:
            # Small batch, execute directly with retry
            self._execute_with_retry(
                lambda: create_func(items),
                f"create_{item_name}"
            )
            logger.info(f"Created {len(items)} {item_name}")
        else:
            # Large batch, use chunking
            logger.info(f"Creating {len(items)} {item_name} in multiple batches...")

            result = batch_operation_with_retry(
                create_func,
                items,
                batch_size=100,
                max_retries=5
            )

            if result['failed_count'] > 0:
                logger.warning(
                    f"Created {result['succeeded_count']}/{result['total']} {item_name}. "
                    f"{result['failed_count']} failed."
                )
                raise Exception(
                    f"{item_name.capitalize()} creation partially failed: "
                    f"{result['failed_count']}/{result['total']} {item_name} failed to create"
                )
            else:
                logger.info(f"Successfully created all {result['total']} {item_name}")

    def _batch_update_with_chunking(
        self,
        item_ids: List[str],
        values_to_update: Dict[str, Any],
        update_func: Callable[[List], Any],
        item_name: str = "items"
    ) -> None:
        """
        Update items in batches with automatic chunking and retry.

        Parameters
        ----------
        item_ids : List[str]
            List of item IDs to update
        values_to_update : Dict[str, Any]
            Property values to update
        update_func : Callable
            Function that updates a batch of items
        item_name : str
            Name of items for logging

        Raises
        ------
        Exception
            If any batch fails after retries
        """
        if not item_ids:
            logger.info(f"No {item_name} to update")
            return

        if len(item_ids) <= 100:
            # Small batch, execute directly with retry
            self._execute_with_retry(
                lambda: update_func(item_ids, values_to_update),
                f"update_{item_name}"
            )
            logger.info(f"Updated {len(item_ids)} {item_name}")
        else:
            # Large batch, use chunking
            logger.info(f"Updating {len(item_ids)} {item_name} in multiple batches...")

            chunks = chunk_list(item_ids, chunk_size=100)
            failed_chunks = []

            for i, chunk in enumerate(chunks, 1):
                try:
                    self._execute_with_retry(
                        lambda: update_func(chunk, values_to_update),
                        f"update_{item_name}_batch_{i}"
                    )
                    logger.debug(f"Updated batch {i}/{len(chunks)} ({len(chunk)} {item_name})")
                except Exception as e:
                    logger.error(f"Failed to update batch {i}/{len(chunks)}: {e}")
                    failed_chunks.append(i)

            if failed_chunks:
                raise Exception(
                    f"{item_name.capitalize()} update partially failed: "
                    f"{len(failed_chunks)} batch(es) failed (batches: {failed_chunks})"
                )
            else:
                logger.info(f"Successfully updated all {len(item_ids)} {item_name}")

    def _batch_delete_with_chunking(
        self,
        item_ids: List[str],
        delete_func: Callable[[List], Any],
        item_name: str = "items"
    ) -> None:
        """
        Delete (archive) items in batches with automatic chunking and retry.

        Parameters
        ----------
        item_ids : List[str]
            List of item IDs to delete
        delete_func : Callable
            Function that deletes a batch of items
        item_name : str
            Name of items for logging

        Raises
        ------
        Exception
            If any batch fails after retries
        """
        if not item_ids:
            logger.info(f"No {item_name} to delete")
            return

        if len(item_ids) <= 100:
            # Small batch, execute directly with retry
            self._execute_with_retry(
                lambda: delete_func(item_ids),
                f"delete_{item_name}"
            )
            logger.info(f"Deleted {len(item_ids)} {item_name}")
        else:
            # Large batch, use chunking
            logger.info(f"Deleting {len(item_ids)} {item_name} in multiple batches...")

            chunks = chunk_list(item_ids, chunk_size=100)
            failed_chunks = []

            for i, chunk in enumerate(chunks, 1):
                try:
                    self._execute_with_retry(
                        lambda: delete_func(chunk),
                        f"delete_{item_name}_batch_{i}"
                    )
                    logger.debug(f"Deleted batch {i}/{len(chunks)} ({len(chunk)} {item_name})")
                except Exception as e:
                    logger.error(f"Failed to delete batch {i}/{len(chunks)}: {e}")
                    failed_chunks.append(i)

            if failed_chunks:
                raise Exception(
                    f"{item_name.capitalize()} deletion partially failed: "
                    f"{len(failed_chunks)} batch(es) failed (batches: {failed_chunks})"
                )
            else:
                logger.info(f"Successfully deleted all {len(item_ids)} {item_name}")
