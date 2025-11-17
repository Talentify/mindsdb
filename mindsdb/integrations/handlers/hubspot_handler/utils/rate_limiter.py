"""
Rate limiting and retry logic for HubSpot API calls.

This module provides decorators and utilities to handle HubSpot's rate limits gracefully:
- Exponential backoff on rate limit errors (429)
- Retry on temporary failures (502, 503, 504)
- Configurable retry attempts and backoff
- Batch chunking for operations exceeding API limits
"""

import time
import functools
from typing import Callable, Any, List, Dict
from mindsdb.utilities import log

logger = log.getLogger(__name__)


class RateLimitError(Exception):
    """Raised when rate limit is exceeded and retries are exhausted"""
    pass


class HubSpotAPIError(Exception):
    """Base exception for HubSpot API errors"""
    pass


def with_retry(max_retries: int = 5, backoff_factor: int = 2, retry_on_status: tuple = (429, 502, 503, 504)):
    """
    Decorator to retry HubSpot API calls with exponential backoff.

    Args:
        max_retries: Maximum number of retry attempts (default: 5)
        backoff_factor: Base for exponential backoff calculation (default: 2)
        retry_on_status: HTTP status codes to retry on (default: 429, 502, 503, 504)

    Usage:
        @with_retry(max_retries=5)
        def my_api_call():
            return hubspot.crm.contacts.get_all()
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            last_exception = None

            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)

                except Exception as e:
                    # Try to extract status code from the exception
                    status_code = getattr(e, 'status', None)

                    # Check if this is a retryable error
                    if status_code not in retry_on_status:
                        # Not a retryable error, re-raise immediately
                        raise

                    last_exception = e

                    # If we've exhausted retries, raise the exception
                    if attempt >= max_retries:
                        if status_code == 429:
                            logger.error(f"Rate limit exceeded after {max_retries} retries in {func.__name__}")
                            raise RateLimitError(
                                f"HubSpot rate limit exceeded after {max_retries} retries. "
                                f"Please wait before making more requests."
                            ) from e
                        else:
                            logger.error(f"API call failed after {max_retries} retries in {func.__name__}: {e}")
                            raise HubSpotAPIError(
                                f"HubSpot API call failed after {max_retries} retries: {e}"
                            ) from e

                    # Calculate wait time with exponential backoff
                    wait_time = backoff_factor ** attempt

                    logger.warning(
                        f"API call failed in {func.__name__} (attempt {attempt + 1}/{max_retries}), "
                        f"status: {status_code}, retrying in {wait_time}s: {e}"
                    )

                    time.sleep(wait_time)

            # This should never be reached, but just in case
            if last_exception:
                raise last_exception

        return wrapper
    return decorator


def chunk_list(items: List[Any], chunk_size: int = 100) -> List[List[Any]]:
    """
    Split a list into chunks of specified size.

    Args:
        items: List to be chunked
        chunk_size: Maximum size of each chunk (default: 100, HubSpot's batch limit)

    Returns:
        List of chunks, each containing up to chunk_size items

    Example:
        >>> chunk_list([1, 2, 3, 4, 5], chunk_size=2)
        [[1, 2], [3, 4], [5]]
    """
    if not items:
        return []

    chunks = []
    for i in range(0, len(items), chunk_size):
        chunks.append(items[i:i + chunk_size])

    return chunks


def batch_operation_with_retry(
    operation_func: Callable,
    items: List[Any],
    batch_size: int = 100,
    max_retries: int = 5
) -> Dict[str, Any]:
    """
    Execute a batch operation with automatic chunking and retry logic.

    Args:
        operation_func: Function that performs the batch operation (e.g., create, update, delete)
        items: List of items to process
        batch_size: Maximum items per batch (default: 100)
        max_retries: Maximum retry attempts per batch (default: 5)

    Returns:
        Dict containing:
            - 'success': List of successfully processed items
            - 'failed': List of items that failed processing
            - 'total': Total number of items
            - 'succeeded_count': Number of successful items
            - 'failed_count': Number of failed items

    Example:
        >>> def create_contacts_batch(batch):
        ...     return hubspot.crm.contacts.batch_api.create(batch)
        >>>
        >>> result = batch_operation_with_retry(
        ...     create_contacts_batch,
        ...     contacts_to_create,
        ...     batch_size=100
        ... )
    """
    if not items:
        return {
            'success': [],
            'failed': [],
            'total': 0,
            'succeeded_count': 0,
            'failed_count': 0
        }

    chunks = chunk_list(items, batch_size)
    total_items = len(items)

    logger.info(
        f"Processing {total_items} items in {len(chunks)} batch(es) of up to {batch_size} items each"
    )

    success_results = []
    failed_items = []

    for i, chunk in enumerate(chunks, 1):
        logger.debug(f"Processing batch {i}/{len(chunks)} ({len(chunk)} items)")

        # Wrap the operation with retry logic
        @with_retry(max_retries=max_retries)
        def execute_chunk():
            return operation_func(chunk)

        try:
            result = execute_chunk()

            # Collect successful results
            if hasattr(result, 'results'):
                success_results.extend(result.results)
            else:
                success_results.append(result)

            logger.debug(f"Batch {i}/{len(chunks)} completed successfully")

        except Exception as e:
            logger.error(f"Batch {i}/{len(chunks)} failed after retries: {e}")
            # Track failed items from this chunk
            failed_items.extend(chunk)

    succeeded_count = len(success_results)
    failed_count = len(failed_items)

    logger.info(
        f"Batch operation completed: {succeeded_count}/{total_items} succeeded, "
        f"{failed_count}/{total_items} failed"
    )

    return {
        'success': success_results,
        'failed': failed_items,
        'total': total_items,
        'succeeded_count': succeeded_count,
        'failed_count': failed_count
    }


def handle_hubspot_error(error: Exception) -> str:
    """
    Convert HubSpot API errors into user-friendly error messages.

    Args:
        error: Exception from HubSpot API

    Returns:
        Formatted error message with actionable information
    """
    status = getattr(error, 'status', None)

    error_messages = {
        400: "Bad Request - Check your data format and required fields",
        401: "Authentication Failed - Verify your access token is valid",
        403: "Forbidden - Your access token doesn't have permission for this operation",
        404: "Not Found - The requested resource doesn't exist",
        429: "Rate Limit Exceeded - Too many requests, please slow down",
        500: "HubSpot Server Error - Try again later",
        502: "Bad Gateway - HubSpot service temporarily unavailable",
        503: "Service Unavailable - HubSpot is temporarily down",
        504: "Gateway Timeout - Request took too long",
    }

    if status in error_messages:
        return f"{error_messages[status]}: {str(error)}"

    return f"HubSpot API Error: {str(error)}"
