import base64
import threading
from datetime import datetime, timedelta, timezone
from typing import Dict, Any, Optional

import requests
from hubspot import HubSpot

from mindsdb.integrations.handlers.hubspot_handler.hubspot_tables import (
    ContactsTable, CompaniesTable, DealsTable
)
from mindsdb.integrations.libs.api_handler import APIHandler
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
)
from mindsdb.utilities import log
from mindsdb_sql_parser import parse_sql

logger = log.getLogger(__name__)


class HubspotHandler(APIHandler):
    """
        A class for handling connections and interactions with the Hubspot API.

        Supports OAuth2 authentication with token injection and automatic token refresh.
    """

    name = 'hubspot'
    _refresh_lock = threading.Lock()

    def __init__(self, name: str, **kwargs):
        """
        Initialize the handler.
        Args:
            name (str): name of particular handler instance
            **kwargs: arbitrary keyword arguments.
        """
        super().__init__(name)

        connection_data = kwargs.get("connection_data", {})
        self.connection_data = connection_data
        self.kwargs = kwargs

        # OAuth parameters
        self.access_token = connection_data.get("access_token")
        self.refresh_token = connection_data.get("refresh_token")
        self.client_id = connection_data.get("client_id")
        self.client_secret = connection_data.get("client_secret")
        self.hub_id = connection_data.get("hub_id")
        self.code = connection_data.get("code")
        self.redirect_uri = connection_data.get("redirect_uri")

        # Handler storage for encrypted token storage
        self.handler_storage = kwargs.get("handler_storage")

        self.connection = None
        self.is_connected = False

        companies_data = CompaniesTable(self)
        self._register_table("companies", companies_data)

        contacts_data = ContactsTable(self)
        self._register_table("contacts", contacts_data)

        deals_data = DealsTable(self)
        self._register_table("deals", deals_data)

    def connect(self) -> HubSpot:
        """Creates a new Hubspot API client with OAuth2 support.

        Supports token injection with automatic refresh. The method:
        1. Checks for stored tokens first (most important for rotating refresh tokens)
        2. Falls back to provided tokens from connection_data
        3. Automatically refreshes expired tokens if refresh_token and credentials available
        4. Extracts and stores hub_id from token info

        Returns:
            HubSpot: Authenticated HubSpot API client
        """
        if self.is_connected is True:
            return self.connection

        try:
            # Get valid access token (with refresh if needed)
            token_data = self._get_valid_token()

            # Store hub_id if available
            if token_data.get("hub_id"):
                self.hub_id = token_data["hub_id"]

            # Create HubSpot API client with access token
            self.connection = HubSpot(access_token=token_data["access_token"])
            self.is_connected = True

        except Exception as e:
            logger.error(f'Error connecting to HubSpot: {e}')
            raise

        return self.connection

    def _get_valid_token(self) -> Dict[str, Any]:
        """
        Get a valid access token, refreshing if necessary.

        Returns:
            dict: Token data with access_token, refresh_token (if available), expires_at, hub_id
        """
        # Step 1: Try to load previously stored tokens first
        # This is CRITICAL for rotating refresh tokens - stored tokens have the latest refresh token
        stored_token_data = self._load_stored_tokens()

        if stored_token_data:
            token_data = stored_token_data
        else:
            # No stored tokens - use provided tokens from connection data
            if not self.access_token and not self.refresh_token:
                raise ValueError(
                    "At least access_token or refresh_token must be provided for authentication"
                )

            # Build initial token data
            token_data = {
                "access_token": self.access_token,
                "refresh_token": self.refresh_token,
                "expires_at": None,  # Will be populated on first refresh
                "hub_id": self.hub_id,
            }

            # Store initial tokens so next connection uses them
            self._store_tokens(token_data)

        # Step 2: Check if token needs refresh with race condition protection
        if self._is_token_expired(token_data) and token_data.get("refresh_token"):
            if self.client_id and self.client_secret:
                # Acquire lock to prevent concurrent token refresh attempts
                with self._refresh_lock:
                    # Double-check pattern: re-check stored tokens after acquiring lock
                    # Another thread may have already refreshed the token
                    stored_token_data = self._load_stored_tokens()
                    if stored_token_data and not self._is_token_expired(stored_token_data):
                        # Token was refreshed by another thread while we waited for the lock
                        token_data = stored_token_data
                    else:
                        # Proceed with refresh
                        token_data = self._refresh_tokens(token_data["refresh_token"])
                        self._store_tokens(token_data)
            else:
                logger.warning(
                    "Token is expired but client_id/client_secret not provided. "
                    "Attempting to use token as-is, but API calls may fail."
                )
        elif not token_data.get("access_token") and token_data.get("refresh_token"):
            # No access token but have refresh token - must refresh to get one
            if self.client_id and self.client_secret:
                with self._refresh_lock:
                    # Double-check after acquiring lock
                    stored_token_data = self._load_stored_tokens()
                    if stored_token_data and stored_token_data.get("access_token"):
                        token_data = stored_token_data
                    else:
                        token_data = self._refresh_tokens(token_data["refresh_token"])
                        self._store_tokens(token_data)
            else:
                raise ValueError(
                    "Cannot refresh token: access_token is missing and client credentials "
                    "(client_id/client_secret) are not provided. Please provide either a valid "
                    "access_token or both client credentials."
                )

        return token_data

    def _refresh_tokens(self, refresh_token: str) -> Dict[str, Any]:
        """
        Refresh the access token using the refresh token.

        HubSpot token refresh endpoint: https://api.hubapi.com/oauth/v1/token

        **CRITICAL: HubSpot Rotating Refresh Tokens**
        - HubSpot invalidates the refresh token after each use
        - The response ALWAYS includes a new refresh_token
        - We MUST extract and use this new token

        Args:
            refresh_token: OAuth2 refresh token

        Returns:
            dict: Updated token data with access_token, NEW refresh_token, expires_at, hub_id

        Raises:
            ValueError: If credentials are missing
            Exception: If token refresh fails
        """
        token_url = "https://api.hubapi.com/oauth/v1/token"

        # Validate that client credentials are available
        if not self.client_id or not self.client_secret:
            raise ValueError(
                "Client ID and Client Secret are required to refresh tokens. "
                "Please provide these credentials in your connection configuration."
            )

        # HubSpot uses Basic Authentication for token refresh
        auth_string = base64.b64encode(
            f"{self.client_id}:{self.client_secret}".encode()
        ).decode()

        headers = {
            "Authorization": f"Basic {auth_string}",
            "Content-Type": "application/x-www-form-urlencoded",
        }

        data = {
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
        }

        try:
            response = requests.post(token_url, headers=headers, data=data)
            response.raise_for_status()
            token_response = response.json()

            # Extract new tokens from response
            new_access_token = token_response.get("access_token")
            new_refresh_token = token_response.get("refresh_token")
            expires_in = token_response.get("expires_in", 1800)  # Default 30 minutes

            if not new_access_token:
                raise Exception("HubSpot token refresh response did not include access_token")

            if not new_refresh_token:
                logger.warning(
                    "HubSpot token refresh response did not include new refresh_token. "
                    "This may indicate an issue with token rotation."
                )
                # Use old refresh token as fallback
                new_refresh_token = refresh_token

            # Get hub_id from token info
            hub_id = self._get_hub_id_from_token(new_access_token)

            # Calculate expiration time
            expires_at = datetime.now(timezone.utc) + timedelta(seconds=expires_in)

            return {
                "access_token": new_access_token,
                "refresh_token": new_refresh_token,
                "expires_at": expires_at,
                "hub_id": hub_id,
            }

        except requests.exceptions.HTTPError as e:
            logger.error(f"HTTP error during token refresh: {e}")
            raise Exception(f"Failed to refresh HubSpot token: {str(e)}")
        except Exception as e:
            logger.error(f"Error refreshing HubSpot token: {e}")
            raise

    def _get_hub_id_from_token(self, access_token: str) -> Optional[str]:
        """
        Get the hub_id from HubSpot's token info endpoint.

        Args:
            access_token: OAuth2 access token

        Returns:
            str: Hub ID (Portal ID) or None if retrieval fails
        """
        # If hub_id was already provided in connection_data, use it
        if self.hub_id:
            return self.hub_id

        # Otherwise, try to get it from stored tokens
        stored_tokens = self._load_stored_tokens()
        if stored_tokens and stored_tokens.get("hub_id"):
            return stored_tokens["hub_id"]

        # Finally, query the token info endpoint
        try:
            token_info_url = f"https://api.hubapi.com/oauth/v1/access-tokens/{access_token}"
            response = requests.get(token_info_url)
            response.raise_for_status()
            token_info = response.json()
            return token_info.get("hub_id")
        except Exception as e:
            logger.warning(f"Failed to retrieve hub_id from token info: {e}")
            return None

    def _is_token_expired(self, token_data: Dict[str, Any]) -> bool:
        """
        Check if the access token is expired or about to expire.

        Tokens are considered expired if they expire within 5 minutes (grace period).
        Supports both ISO 8601 string format and datetime objects.

        Args:
            token_data: Token data dictionary

        Returns:
            bool: True if token is expired or expires within 5 minutes
        """
        if not token_data or "expires_at" not in token_data:
            # If no expiration info, assume token needs refresh
            return True

        expires_at = token_data["expires_at"]
        if not expires_at:
            return True

        # Parse expires_at to datetime
        if isinstance(expires_at, str):
            try:
                expires_at = datetime.fromisoformat(expires_at)
            except (ValueError, TypeError):
                # If parsing fails, assume expired
                return True
        elif isinstance(expires_at, (int, float)):
            # Unix timestamp
            try:
                expires_at = datetime.fromtimestamp(expires_at, tz=timezone.utc)
            except (ValueError, OSError):
                return True
        elif not isinstance(expires_at, datetime):
            # Unknown format, assume expired
            return True

        # Ensure timezone-aware datetime
        if expires_at.tzinfo is None:
            expires_at = expires_at.replace(tzinfo=timezone.utc)

        # Consider token expired if it expires within 5 minutes (grace period)
        buffer_time = datetime.now(timezone.utc) + timedelta(minutes=5)
        return buffer_time >= expires_at

    def _store_tokens(self, token_data: Dict[str, Any]) -> None:
        """
        Store tokens securely in encrypted handler storage.

        Args:
            token_data: Token data to store
        """
        if not self.handler_storage:
            logger.warning("Handler storage not available, tokens will not be persisted")
            return

        try:
            # Convert datetime to string for JSON serialization
            stored_data = token_data.copy()
            if isinstance(stored_data.get("expires_at"), datetime):
                stored_data["expires_at"] = stored_data["expires_at"].isoformat()

            self.handler_storage.encrypted_json_set("hubspot_tokens", stored_data)
            logger.debug("Successfully stored HubSpot tokens")
        except Exception as e:
            logger.error(f"Failed to store tokens: {e}")

    def _load_stored_tokens(self) -> Optional[Dict[str, Any]]:
        """
        Load stored tokens from encrypted handler storage.

        Returns:
            dict: Stored token data or None if not found
        """
        if not self.handler_storage:
            return None

        try:
            token_data = self.handler_storage.encrypted_json_get("hubspot_tokens")
            if token_data and isinstance(token_data.get("expires_at"), str):
                # Convert ISO format string back to datetime
                token_data["expires_at"] = datetime.fromisoformat(token_data["expires_at"])
            return token_data
        except Exception as e:
            logger.debug(f"No stored tokens found or failed to load: {e}")
            return None

    def check_connection(self) -> StatusResponse:
        """Checks whether the API client is connected to Hubspot.

        Returns:
            StatusResponse: A status response indicating whether the API client is connected to Hubspot.
        """

        response = StatusResponse(False)

        try:
            self.connect()
            response.success = True

        except Exception as e:
            logger.error(f'Error connecting to Hubspot: {e}')
            response.error_message = e

        self.is_connected = response.success
        return response

    def native_query(self, query: str = None) -> Response:
        """Receive and process a raw query.
        Parameters
        ----------
        query : str
            query in a native format
        Returns
        -------
        StatusResponse
            Request status
        """
        ast = parse_sql(query)
        return self.query(ast)
