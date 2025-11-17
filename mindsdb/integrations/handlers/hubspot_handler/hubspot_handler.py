import time
from hubspot import HubSpot

from mindsdb.integrations.handlers.hubspot_handler.tables.crm.companies_table import CompaniesTable
from mindsdb.integrations.handlers.hubspot_handler.tables.crm.contacts_table import ContactsTable
from mindsdb.integrations.handlers.hubspot_handler.tables.crm.deals_table import DealsTable
from mindsdb.integrations.handlers.hubspot_handler.tables.crm.properties_table import PropertiesTable

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
    """

    name = 'hubspot'

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

        self.connection = None
        self.is_connected = False

        # Properties cache (shared across all tables)
        # Format: {object_type: {'properties': [...], 'timestamp': float}}
        self._properties_cache = {}
        self._properties_cache_ttl = 3600  # 1 hour in seconds

        companies_data = CompaniesTable(self)
        self._register_table("companies", companies_data)

        contacts_data = ContactsTable(self)
        self._register_table("contacts", contacts_data)

        deals_data = DealsTable(self)
        self._register_table("deals", deals_data)

        properties_data = PropertiesTable(self)
        self._register_table("properties", properties_data)

    def connect(self) -> HubSpot:
        """Creates a new Hubspot API client if needed and sets it as the client to use for requests.

        Returns newly created Hubspot API client, or current client if already set.
        """
        if self.is_connected is True:
            return self.connection

        access_token = self.connection_data['access_token']

        self.connection = HubSpot(access_token=access_token)
        self.is_connected = True

        return self.connection

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

    def get_properties_cache(self, object_type: str) -> dict:
        """
        Get cached property definitions for a specific HubSpot object type.
        Caches for 1 hour to avoid repeated API calls.

        Args:
            object_type (str): The HubSpot object type ('contacts', 'companies', 'deals')

        Returns:
            dict: {
                'properties': list of property definitions with name, label, type, etc.,
                'property_names': set of property names for quick lookup,
                'timestamp': cache timestamp
            }
        """
        # Check if cache is valid
        current_time = time.time()
        if object_type in self._properties_cache:
            cache_entry = self._properties_cache[object_type]
            cache_age = current_time - cache_entry['timestamp']
            if cache_age < self._properties_cache_ttl:
                logger.info(f"Using cached properties for {object_type} (age: {cache_age:.0f}s)")
                return cache_entry

        # Fetch fresh metadata from API
        logger.info(f"Fetching properties for {object_type} from HubSpot API")
        try:
            hubspot = self.connect()

            # Use the HubSpot client to fetch properties
            # The API endpoint is: /crm/v3/properties/{object_type}
            properties_response = hubspot.crm.properties.core_api.get_all(
                object_type=object_type
            )

            # Extract property information
            properties = []
            property_names = set()

            for prop in properties_response.results:
                property_info = {
                    'name': prop.name,
                    'label': prop.label,
                    'type': prop.type,
                    'fieldType': prop.field_type,
                    'description': getattr(prop, 'description', ''),
                    'groupName': prop.group_name,
                    'hidden': getattr(prop, 'hidden', False),
                    'hubspotDefined': getattr(prop, 'hubspot_defined', True),
                }
                properties.append(property_info)
                property_names.add(prop.name)

            # Cache the results
            cache_entry = {
                'properties': properties,
                'property_names': property_names,
                'timestamp': current_time
            }
            self._properties_cache[object_type] = cache_entry

            logger.info(f"Cached {len(properties)} properties for {object_type}")
            return cache_entry

        except Exception as e:
            logger.error(f"Error fetching properties for {object_type}: {e}")
            # Return empty cache on error
            return {
                'properties': [],
                'property_names': set(),
                'timestamp': current_time
            }

    def invalidate_properties_cache(self, object_type: str = None):
        """
        Invalidate the properties cache for a specific object type or all types.

        Args:
            object_type (str, optional): The object type to invalidate. If None, invalidates all.
        """
        if object_type:
            if object_type in self._properties_cache:
                del self._properties_cache[object_type]
                logger.info(f"Invalidated properties cache for {object_type}")
        else:
            self._properties_cache = {}
            logger.info("Invalidated all properties cache")
