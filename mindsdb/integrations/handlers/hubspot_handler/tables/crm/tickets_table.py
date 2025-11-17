from typing import List, Dict, Text, Any
import pandas as pd
from hubspot.crm.objects import (
    SimplePublicObjectId as HubSpotObjectId,
    SimplePublicObjectBatchInput as HubSpotObjectBatchInput,
    SimplePublicObjectInputForCreate as HubSpotObjectInputCreate,
    BatchInputSimplePublicObjectId as HubSpotBatchObjectIdInput,
    BatchInputSimplePublicObjectBatchInput as HubSpotBatchObjectBatchInput,
    BatchInputSimplePublicObjectBatchInputForCreate as HubSpotBatchObjectInputCreate,
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


class TicketsTable(HubSpotSearchMixin, APITable):
    """Hubspot Tickets table."""

    # Default essential properties to fetch (to avoid overloading with 100+ properties)
    DEFAULT_PROPERTIES = [
        'subject', 'content', 'hs_pipeline', 'hs_pipeline_stage', 'hs_ticket_priority',
        'hubspot_owner_id', 'hs_ticket_category', 'source_type', 'createdate',
        'hs_lastmodifieddate', 'closed_date'
    ]

    def select(self, query: ast.Select) -> pd.DataFrame:
        """
        Pulls Hubspot Tickets data

        Parameters
        ----------
        query : ast.Select
            Given SQL SELECT query

        Returns
        -------
        pd.DataFrame
            Hubspot Tickets matching the query

        Raises
        ------
        ValueError
            If the query contains an unsupported condition

        """

        select_statement_parser = SELECTQueryParser(
            query,
            "tickets",
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
                tickets_df = pd.json_normalize(
                    self.search_tickets(
                        filters=hubspot_filters,
                        properties=requested_properties,
                        limit=result_limit
                    )
                )
                where_conditions = []
            else:
                logger.info("No valid HubSpot filters, using get_all")
                tickets_df = pd.json_normalize(
                    self.get_tickets(limit=result_limit, properties=requested_properties)
                )
        else:
            tickets_df = pd.json_normalize(
                self.get_tickets(limit=result_limit, properties=requested_properties)
            )

        select_statement_executor = SELECTQueryExecutor(
            tickets_df,
            selected_columns,
            where_conditions,
            order_by_conditions
        )
        tickets_df = select_statement_executor.execute_query()

        return tickets_df

    def insert(self, query: ast.Insert) -> None:
        """
        Inserts data into HubSpot "POST /crm/v3/objects/tickets/batch/create" API endpoint.

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
            properties_cache = self.handler.get_properties_cache('tickets')
            supported_columns = list(properties_cache['property_names'])
        except Exception as e:
            logger.warning(f"Failed to get dynamic columns for insert, using minimal set: {e}")
            supported_columns = ['subject', 'content', 'hs_pipeline', 'hs_pipeline_stage', 'hs_ticket_priority']

        insert_statement_parser = INSERTQueryParser(
            query,
            supported_columns=supported_columns,
            mandatory_columns=['subject'],
            all_mandatory=False,
        )
        tickets_data = insert_statement_parser.parse_query()
        self.create_tickets(tickets_data)

    def update(self, query: ast.Update) -> None:
        """
        Updates data from HubSpot "PATCH /crm/v3/objects/tickets/batch/update" API endpoint.

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

        tickets_df = pd.json_normalize(self.get_tickets())
        update_query_executor = UPDATEQueryExecutor(
            tickets_df,
            where_conditions
        )

        tickets_df = update_query_executor.execute_query()
        ticket_ids = tickets_df['id'].tolist()
        self.update_tickets(ticket_ids, values_to_update)

    def delete(self, query: ast.Delete) -> None:
        """
        Deletes data from HubSpot "DELETE /crm/v3/objects/tickets/batch/archive" API endpoint.

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

        tickets_df = pd.json_normalize(self.get_tickets())
        delete_query_executor = DELETEQueryExecutor(
            tickets_df,
            where_conditions
        )

        tickets_df = delete_query_executor.execute_query()
        ticket_ids = tickets_df['id'].tolist()
        self.delete_tickets(ticket_ids)

    def get_columns(self) -> List[Text]:
        """
        Get column names for the table.
        Returns default essential properties to avoid overloading with 100+ properties.
        Users can still query specific custom properties explicitly in SELECT.
        """
        # Return id + default essential properties
        return ['id'] + self.DEFAULT_PROPERTIES

    def get_tickets(self, properties: List[Text] = None, **kwargs) -> List[Dict]:
        """
        Fetch tickets with specified properties.

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
            List of ticket dictionaries with requested properties
        """
        hubspot = self.handler.connect()

        # Determine which properties to request from HubSpot
        if properties is None:
            # Default: fetch only essential properties
            properties_to_fetch = self.DEFAULT_PROPERTIES
        elif len(properties) == 0:
            # Empty list means fetch ALL available properties
            properties_cache = self.handler.get_properties_cache('tickets')
            properties_to_fetch = list(properties_cache['property_names'])
        else:
            # Specific properties requested
            properties_to_fetch = properties

        # Add properties parameter to API call
        kwargs['properties'] = properties_to_fetch
        tickets = hubspot.crm.tickets.get_all(**kwargs)

        tickets_dict = []
        for ticket in tickets:
            # Start with the ID
            ticket_dict = {"id": ticket.id}

            # Extract properties that were returned
            if hasattr(ticket, 'properties') and ticket.properties:
                for prop_name, prop_value in ticket.properties.items():
                    ticket_dict[prop_name] = prop_value

            tickets_dict.append(ticket_dict)

        return tickets_dict

    def search_tickets(self, filters: List[Dict], properties: List[Text] = None, limit: int = None) -> List[Dict]:
        """
        Search tickets using HubSpot search API with filters.

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
            List of ticket dictionaries matching the filters
        """
        hubspot = self.handler.connect()

        # Determine which properties to request
        if properties is None:
            properties_to_fetch = self.DEFAULT_PROPERTIES
        elif len(properties) == 0:
            properties_cache = self.handler.get_properties_cache('tickets')
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
        all_tickets = []
        after = 0

        try:
            while True:
                if after > 0:
                    search_request["after"] = after

                # Call HubSpot search API
                response = hubspot.crm.tickets.search_api.do_search(
                    public_object_search_request=search_request
                )

                # Extract tickets from response
                for ticket in response.results:
                    ticket_dict = {"id": ticket.id}
                    if hasattr(ticket, 'properties') and ticket.properties:
                        for prop_name, prop_value in ticket.properties.items():
                            ticket_dict[prop_name] = prop_value
                    all_tickets.append(ticket_dict)

                # Check if we've reached the limit
                if limit and len(all_tickets) >= limit:
                    all_tickets = all_tickets[:limit]
                    break

                # Check if there are more results
                if not hasattr(response, 'paging') or not response.paging:
                    break

                if hasattr(response.paging, 'next') and response.paging.next:
                    after = response.paging.next.after
                else:
                    break

        except Exception as e:
            logger.error(f"Error searching tickets: {e}")
            raise Exception(f"Ticket search failed: {e}")

        logger.info(f"Found {len(all_tickets)} tickets matching filters")
        return all_tickets

    def create_tickets(self, tickets_data: List[Dict[Text, Any]]) -> None:
        hubspot = self.handler.connect()
        tickets_to_create = [HubSpotObjectInputCreate(properties=ticket) for ticket in tickets_data]
        try:
            created_tickets = hubspot.crm.tickets.batch_api.create(
                HubSpotBatchObjectInputCreate(inputs=tickets_to_create),
            )
            logger.info(f"Tickets created with ID's {[created_ticket.id for created_ticket in created_tickets.results]}")
        except Exception as e:
            raise Exception(f"Tickets creation failed {e}")

    def update_tickets(self, ticket_ids: List[Text], values_to_update: Dict[Text, Any]) -> None:
        hubspot = self.handler.connect()
        tickets_to_update = [HubSpotObjectBatchInput(id=ticket_id, properties=values_to_update) for ticket_id in ticket_ids]
        try:
            updated_tickets = hubspot.crm.tickets.batch_api.update(
                HubSpotBatchObjectBatchInput(inputs=tickets_to_update),
            )
            logger.info(f"Tickets with ID {[updated_ticket.id for updated_ticket in updated_tickets.results]} updated")
        except Exception as e:
            raise Exception(f"Tickets update failed {e}")

    def delete_tickets(self, ticket_ids: List[Text]) -> None:
        hubspot = self.handler.connect()
        tickets_to_delete = [HubSpotObjectId(id=ticket_id) for ticket_id in ticket_ids]
        try:
            hubspot.crm.tickets.batch_api.archive(
                HubSpotBatchObjectIdInput(inputs=tickets_to_delete),
            )
            logger.info("Tickets deleted")
        except Exception as e:
            raise Exception(f"Tickets deletion failed {e}")
