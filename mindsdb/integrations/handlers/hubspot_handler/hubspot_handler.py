import time
from hubspot import HubSpot

from mindsdb.integrations.handlers.hubspot_handler.utils.rate_limiter import (
    with_retry,
    handle_hubspot_error
)
from mindsdb.integrations.handlers.hubspot_handler.tables.crm.companies_table import CompaniesTable
from mindsdb.integrations.handlers.hubspot_handler.tables.crm.contacts_table import ContactsTable
from mindsdb.integrations.handlers.hubspot_handler.tables.crm.deals_table import DealsTable
from mindsdb.integrations.handlers.hubspot_handler.tables.crm.properties_table import PropertiesTable
from mindsdb.integrations.handlers.hubspot_handler.tables.crm.tickets_table import TicketsTable
from mindsdb.integrations.handlers.hubspot_handler.tables.crm.line_items_table import LineItemsTable
from mindsdb.integrations.handlers.hubspot_handler.tables.crm.quotes_table import QuotesTable
from mindsdb.integrations.handlers.hubspot_handler.tables.crm.products_table import ProductsTable
from mindsdb.integrations.handlers.hubspot_handler.tables.crm.calls_table import CallsTable
from mindsdb.integrations.handlers.hubspot_handler.tables.crm.emails_table import EmailsTable
from mindsdb.integrations.handlers.hubspot_handler.tables.crm.meetings_table import MeetingsTable
from mindsdb.integrations.handlers.hubspot_handler.tables.crm.notes_table import NotesTable
from mindsdb.integrations.handlers.hubspot_handler.tables.crm.tasks_table import TasksTable
from mindsdb.integrations.handlers.hubspot_handler.tables.crm.leads_table import LeadsTable
from mindsdb.integrations.handlers.hubspot_handler.tables.crm.owners_table import OwnersTable
from mindsdb.integrations.handlers.hubspot_handler.tables.crm.pipelines_table import PipelinesTable
from mindsdb.integrations.handlers.hubspot_handler.tables.crm.associations_table import AssociationsTable

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

        # Core CRM Objects
        companies_data = CompaniesTable(self)
        self._register_table("companies", companies_data)

        contacts_data = ContactsTable(self)
        self._register_table("contacts", contacts_data)

        deals_data = DealsTable(self)
        self._register_table("deals", deals_data)

        tickets_data = TicketsTable(self)
        self._register_table("tickets", tickets_data)

        leads_data = LeadsTable(self)
        self._register_table("leads", leads_data)

        # Commerce Objects
        line_items_data = LineItemsTable(self)
        self._register_table("line_items", line_items_data)

        quotes_data = QuotesTable(self)
        self._register_table("quotes", quotes_data)

        products_data = ProductsTable(self)
        self._register_table("products", products_data)

        # Activity Objects
        calls_data = CallsTable(self)
        self._register_table("calls", calls_data)

        emails_data = EmailsTable(self)
        self._register_table("emails", emails_data)

        meetings_data = MeetingsTable(self)
        self._register_table("meetings", meetings_data)

        notes_data = NotesTable(self)
        self._register_table("notes", notes_data)

        tasks_data = TasksTable(self)
        self._register_table("tasks", tasks_data)

        # Metadata and Configuration
        properties_data = PropertiesTable(self)
        self._register_table("properties", properties_data)

        owners_data = OwnersTable(self)
        self._register_table("owners", owners_data)

        pipelines_data = PipelinesTable(self)
        self._register_table("pipelines", pipelines_data)

        # Associations (Relationships)
        associations_data = AssociationsTable(self)
        self._register_table("associations", associations_data)

    def connect(self) -> HubSpot:
        """Creates a new Hubspot API client if needed and sets it as the client to use for requests.

        Returns newly created Hubspot API client, or current client if already set.

        Raises:
            Exception: If connection fails (invalid token, network issues, etc.)
        """
        if self.is_connected is True:
            return self.connection

        access_token = self.connection_data.get('access_token')

        if not access_token:
            raise ValueError("HubSpot access token is required. Please provide 'access_token' in connection data.")

        try:
            self.connection = HubSpot(access_token=access_token)
            self.is_connected = True
            logger.info("Successfully connected to HubSpot API")
        except Exception as e:
            self.is_connected = False
            error_message = handle_hubspot_error(e)
            logger.error(f"Failed to connect to HubSpot: {error_message}")
            raise Exception(f"HubSpot connection failed: {error_message}") from e

        return self.connection

    def check_connection(self) -> StatusResponse:
        """Checks whether the API client is connected to Hubspot with retry logic.

        Returns:
            StatusResponse: A status response indicating whether the API client is connected to Hubspot.
        """
        response = StatusResponse(False)

        @with_retry(max_retries=3, backoff_factor=2)
        def validate_connection():
            """Validate connection by making a simple API call"""
            hubspot = self.connect()
            # Make a simple API call to verify the connection works
            # Using contacts API as it's available to all access tokens
            hubspot.crm.contacts.basic_api.get_page(limit=1)

        try:
            validate_connection()
            response.success = True
            logger.info("HubSpot connection validated successfully")

        except Exception as e:
            error_message = handle_hubspot_error(e)
            logger.error(f'Error connecting to HubSpot: {error_message}')
            response.error_message = f"Connection failed: {error_message}"
            response.success = False

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
