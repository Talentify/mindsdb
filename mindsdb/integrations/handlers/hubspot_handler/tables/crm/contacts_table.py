from typing import List, Dict, Text, Any
import pandas as pd
from hubspot.crm.objects import (
    SimplePublicObjectId as HubSpotObjectId,
    SimplePublicObjectBatchInput as HubSpotObjectBatchInput,
    SimplePublicObjectInputForCreate as HubSpotObjectInputCreate,
    BatchInputSimplePublicObjectId as HubSpotBatchObjectIdInput,
    BatchInputSimplePublicObjectBatchInput as HubSpotBatchObjectBatchInput,
    BatchInputSimplePublicObjectInputForCreate as HubSpotBatchObjectInputCreate,
)

from mindsdb_sql_parser import ast
from mindsdb.integrations.libs.api_handler import APITable
from mindsdb.integrations.utilities.handlers.query_utilities import (
    INSERTQueryParser,
    SELECTQueryParser,
    UPDATEQueryParser,
    DELETEQueryParser,
    SELECTQueryExecutor,
    UPDATEQueryExecutor,
    DELETEQueryExecutor,
)
from mindsdb.utilities import log
from mindsdb.integrations.handlers.hubspot_handler.tables.crm.base_hubspot_table import HubSpotSearchMixin


logger = log.getLogger(__name__)


class ContactsTable(HubSpotSearchMixin, APITable):
    """Hubspot Contacts table."""

    # Default essential properties to fetch (to avoid overloading with 100+ properties)
    DEFAULT_PROPERTIES = [
        'email', 'firstname', 'lastname', 'phone', 'company', 'website',
        'jobtitle', 'city', 'state', 'country', 'lifecyclestage',
        'createdate', 'lastmodifieddate'
    ]

    def select(self, query: ast.Select) -> pd.DataFrame:
        """
        Pulls Hubspot Contacts data

        Parameters
        ----------
        query : ast.Select
            Given SQL SELECT query

        Returns
        -------
        pd.DataFrame
            Hubspot Contacts matching the query

        Raises
        ------
        ValueError
            If the query contains an unsupported condition

        """

        select_statement_parser = SELECTQueryParser(
            query,
            "contacts",
            self.get_columns()
        )
        selected_columns, where_conditions, order_by_conditions, result_limit = select_statement_parser.parse_query()

        # Determine which properties to fetch from HubSpot API
        requested_properties = None
        if selected_columns and len(selected_columns) > 0:
            requested_properties = [col for col in selected_columns if col != 'id']

        # Check if WHERE conditions exist - use search API if they do
        if where_conditions and len(where_conditions) > 0:
            hubspot_filters = self._build_search_filters(where_conditions)

            if hubspot_filters:
                logger.info(f"Using HubSpot search API with {len(hubspot_filters)} filter(s)")
                contacts_df = pd.json_normalize(
                    self.search_contacts(
                        filters=hubspot_filters,
                        properties=requested_properties,
                        limit=result_limit
                    )
                )
                where_conditions = []
            else:
                logger.info("No valid HubSpot filters, using get_all")
                contacts_df = pd.json_normalize(
                    self.get_contacts(limit=result_limit, properties=requested_properties)
                )
        else:
            contacts_df = pd.json_normalize(
                self.get_contacts(limit=result_limit, properties=requested_properties)
            )

        select_statement_executor = SELECTQueryExecutor(
            contacts_df,
            selected_columns,
            where_conditions,
            order_by_conditions
        )
        contacts_df = select_statement_executor.execute_query()

        return contacts_df

    def insert(self, query: ast.Insert) -> None:
        """
        Inserts data into HubSpot "POST /crm/v3/objects/contacts/batch/create" API endpoint.

        Parameters
        ----------
        query : ast.Insert
           Given SQL INSERT query

        Returns
        -------
        None

        Raises
        ------
        ValueError
            If the query contains an unsupported condition
        """
        # Get dynamic list of supported columns from properties cache
        try:
            properties_cache = self.handler.get_properties_cache('contacts')
            supported_columns = list(properties_cache['property_names'])
        except Exception as e:
            logger.warning(f"Failed to get dynamic columns for insert, using minimal set: {e}")
            supported_columns = ['email', 'firstname', 'lastname', 'phone', 'company', 'website']

        insert_statement_parser = INSERTQueryParser(
            query,
            supported_columns=supported_columns,
            mandatory_columns=['email'],
            all_mandatory=False,
        )
        contact_data = insert_statement_parser.parse_query()
        self.create_contacts(contact_data)

    def update(self, query: ast.Update) -> None:
        """
        Updates data from HubSpot "PATCH /crm/v3/objects/contacts/batch/update" API endpoint.

        Parameters
        ----------
        query : ast.Update
           Given SQL UPDATE query

        Returns
        -------
        None

        Raises
        ------
        ValueError
            If the query contains an unsupported condition
        """
        update_statement_parser = UPDATEQueryParser(query)
        values_to_update, where_conditions = update_statement_parser.parse_query()

        contacts_df = pd.json_normalize(self.get_contacts())
        update_query_executor = UPDATEQueryExecutor(
            contacts_df,
            where_conditions
        )

        contacts_df = update_query_executor.execute_query()
        contact_ids = contacts_df['id'].tolist()
        self.update_contacts(contact_ids, values_to_update)

    def delete(self, query: ast.Delete) -> None:
        """
        Deletes data from HubSpot "DELETE /crm/v3/objects/contacts/batch/archive" API endpoint.

        Parameters
        ----------
        query : ast.Delete
           Given SQL DELETE query

        Returns
        -------
        None

        Raises
        ------
        ValueError
            If the query contains an unsupported condition
        """
        delete_statement_parser = DELETEQueryParser(query)
        where_conditions = delete_statement_parser.parse_query()

        contacts_df = pd.json_normalize(self.get_contacts())
        delete_query_executor = DELETEQueryExecutor(
            contacts_df,
            where_conditions
        )

        contacts_df = delete_query_executor.execute_query()
        contact_ids = contacts_df['id'].tolist()
        self.delete_contacts(contact_ids)

    def get_columns(self) -> List[Text]:
        """
        Get column names for the table.
        Returns default essential properties to avoid overloading with 100+ properties.
        Users can still query specific custom properties explicitly in SELECT.
        """
        # Return id + default essential properties
        return ['id'] + self.DEFAULT_PROPERTIES

    def get_contacts(self, properties: List[Text] = None, **kwargs) -> List[Dict]:
        """
        Fetch contacts with specified properties.

        Parameters
        ----------
        properties : List[Text], optional
            List of property names to fetch. If None, fetches DEFAULT_PROPERTIES.
            To fetch ALL properties, pass an empty list [].
        **kwargs : dict
            Additional arguments to pass to the HubSpot API (e.g., limit)

        Returns
        -------
        List[Dict]
            List of contact dictionaries with requested properties
        """
        hubspot = self.handler.connect()

        # Determine which properties to request from HubSpot
        if properties is None:
            # Default: fetch only essential properties
            properties_to_fetch = self.DEFAULT_PROPERTIES
        elif len(properties) == 0:
            # Empty list means fetch ALL available properties
            properties_cache = self.handler.get_properties_cache('contacts')
            properties_to_fetch = list(properties_cache['property_names'])
        else:
            # Specific properties requested
            properties_to_fetch = properties

        # Add properties parameter to API call
        kwargs['properties'] = properties_to_fetch
        contacts = hubspot.crm.contacts.get_all(**kwargs)

        contacts_dict = []
        for contact in contacts:
            # Start with the ID
            contact_dict = {"id": contact.id}

            # Extract properties that were returned
            if hasattr(contact, 'properties') and contact.properties:
                for prop_name, prop_value in contact.properties.items():
                    contact_dict[prop_name] = prop_value

            contacts_dict.append(contact_dict)

        return contacts_dict

    def search_contacts(self, filters: List[Dict], properties: List[Text] = None, limit: int = None) -> List[Dict]:
        """
        Search contacts using HubSpot search API with filters.

        Parameters
        ----------
        filters : List[Dict]
            List of HubSpot filter dictionaries
        properties : List[Text], optional
            List of property names to fetch. If None, fetches DEFAULT_PROPERTIES.
        limit : int, optional
            Maximum number of results to return

        Returns
        -------
        List[Dict]
            List of contact dictionaries matching the filters
        """
        hubspot = self.handler.connect()

        # Determine which properties to request
        if properties is None:
            properties_to_fetch = self.DEFAULT_PROPERTIES
        elif len(properties) == 0:
            properties_cache = self.handler.get_properties_cache('contacts')
            properties_to_fetch = list(properties_cache['property_names'])
        else:
            properties_to_fetch = properties

        # Build search request
        search_request = {
            "filterGroups": [{"filters": filters}],
            "properties": properties_to_fetch,
            "limit": min(limit or 100, 100),
        }

        # Pagination to fetch all results
        all_contacts = []
        after = 0

        try:
            while True:
                if after > 0:
                    search_request["after"] = after

                # Call HubSpot search API
                response = hubspot.crm.contacts.search_api.do_search(
                    public_object_search_request=search_request
                )

                # Extract contacts from response
                for contact in response.results:
                    contact_dict = {"id": contact.id}
                    if hasattr(contact, 'properties') and contact.properties:
                        for prop_name, prop_value in contact.properties.items():
                            contact_dict[prop_name] = prop_value
                    all_contacts.append(contact_dict)

                # Check if we've reached the limit
                if limit and len(all_contacts) >= limit:
                    all_contacts = all_contacts[:limit]
                    break

                # Check if there are more results
                if not hasattr(response, 'paging') or not response.paging:
                    break

                if hasattr(response.paging, 'next') and response.paging.next:
                    after = response.paging.next.after
                else:
                    break

        except Exception as e:
            logger.error(f"Error searching contacts: {e}")
            raise Exception(f"Contact search failed: {e}")

        logger.info(f"Found {len(all_contacts)} contacts matching filters")
        return all_contacts

    def create_contacts(self, contacts_data: List[Dict[Text, Any]]) -> None:
        hubspot = self.handler.connect()
        contacts_to_create = [HubSpotObjectInputCreate(properties=contact) for contact in contacts_data]
        try:
            created_contacts = hubspot.crm.contacts.batch_api.create(
                HubSpotBatchObjectInputCreate(inputs=contacts_to_create)
            )
            logger.info(f"Contacts created with ID {[created_contact.id for created_contact in created_contacts.results]}")
        except Exception as e:
            raise Exception(f"Contacts creation failed {e}")

    def update_contacts(self, contact_ids: List[Text], values_to_update: Dict[Text, Any]) -> None:
        hubspot = self.handler.connect()
        contacts_to_update = [HubSpotObjectBatchInput(id=contact_id, properties=values_to_update) for contact_id in contact_ids]
        try:
            updated_contacts = hubspot.crm.contacts.batch_api.update(
                HubSpotBatchObjectBatchInput(inputs=contacts_to_update),
            )
            logger.info(f"Contacts with ID {[updated_contact.id for updated_contact in updated_contacts.results]} updated")
        except Exception as e:
            raise Exception(f"Contacts update failed {e}")

    def delete_contacts(self, contact_ids: List[Text]) -> None:
        hubspot = self.handler.connect()
        contacts_to_delete = [HubSpotObjectId(id=contact_id) for contact_id in contact_ids]
        try:
            hubspot.crm.contacts.batch_api.archive(
                HubSpotBatchObjectIdInput(inputs=contacts_to_delete),
            )
            logger.info("Contacts deleted")
        except Exception as e:
            raise Exception(f"Contacts deletion failed {e}")