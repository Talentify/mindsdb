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


class DealsTable(HubSpotSearchMixin, APITable):
    """Hubspot Deals table."""

    # Default essential properties to fetch (to avoid overloading with 100+ properties)
    DEFAULT_PROPERTIES = [
        'dealname', 'amount', 'pipeline', 'dealstage', 'closedate',
        'hubspot_owner_id', 'dealtype', 'description',
        'createdate', 'hs_lastmodifieddate'
    ]

    def select(self, query: ast.Select) -> pd.DataFrame:
        """
        Pulls Hubspot Deals data

        Parameters
        ----------
        query : ast.Select
            Given SQL SELECT query

        Returns
        -------
        pd.DataFrame
            Hubspot Deals matching the query

        Raises
        ------
        ValueError
            If the query contains an unsupported condition

        """

        select_statement_parser = SELECTQueryParser(
            query,
            "deals",
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
                deals_df = pd.json_normalize(
                    self.search_deals(
                        filters=hubspot_filters,
                        properties=requested_properties,
                        limit=result_limit
                    )
                )
                where_conditions = []
            else:
                logger.info("No valid HubSpot filters, using get_all")
                deals_df = pd.json_normalize(
                    self.get_deals(limit=result_limit, properties=requested_properties)
                )
        else:
            deals_df = pd.json_normalize(
                self.get_deals(limit=result_limit, properties=requested_properties)
            )

        # Filter selected_columns to only include columns that actually exist in the dataframe
        # This handles cases where requested properties don't exist in HubSpot
        if not deals_df.empty and selected_columns:
            available_columns = [col for col in selected_columns if col in deals_df.columns]
            if len(available_columns) < len(selected_columns):
                missing = set(selected_columns) - set(available_columns)
                logger.warning(f"Some requested columns not available in deals data: {missing}")
            selected_columns = available_columns

        select_statement_executor = SELECTQueryExecutor(
            deals_df,
            selected_columns,
            where_conditions,
            order_by_conditions
        )
        deals_df = select_statement_executor.execute_query()

        return deals_df

    def insert(self, query: ast.Insert) -> None:
        """
        Inserts data into HubSpot "POST /crm/v3/objects/deals/batch/create" API endpoint.

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
            properties_cache = self.handler.get_properties_cache('deals')
            supported_columns = list(properties_cache['property_names'])
        except Exception as e:
            logger.warning(f"Failed to get dynamic columns for insert, using minimal set: {e}")
            supported_columns = ['amount', 'dealname', 'pipeline', 'closedate', 'dealstage', 'hubspot_owner_id']

        insert_statement_parser = INSERTQueryParser(
            query,
            supported_columns=supported_columns,
            mandatory_columns=['dealname'],
            all_mandatory=False,
        )
        deals_data = insert_statement_parser.parse_query()
        self.create_deals(deals_data)

    def update(self, query: ast.Update) -> None:
        """
        Updates data from HubSpot "PATCH /crm/v3/objects/deals/batch/update" API endpoint.

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

        deals_df = pd.json_normalize(self.get_deals())
        update_query_executor = UPDATEQueryExecutor(
            deals_df,
            where_conditions
        )

        deals_df = update_query_executor.execute_query()
        deal_ids = deals_df['id'].tolist()
        self.update_deals(deal_ids, values_to_update)

    def delete(self, query: ast.Delete) -> None:
        """
        Deletes data from HubSpot "DELETE /crm/v3/objects/deals/batch/archive" API endpoint.

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

        deals_df = pd.json_normalize(self.get_deals())
        delete_query_executor = DELETEQueryExecutor(
            deals_df,
            where_conditions
        )

        deals_df = delete_query_executor.execute_query()
        deal_ids = deals_df['id'].tolist()
        self.delete_deals(deal_ids)

    def get_columns(self) -> List[Text]:
        """
        Get column names for the table.
        Returns default essential properties to avoid overloading with 100+ properties.
        Users can still query specific custom properties explicitly in SELECT.
        """
        # Return id + default essential properties
        return ['id'] + self.DEFAULT_PROPERTIES

    def get_deals(self, properties: List[Text] = None, **kwargs) -> List[Dict]:
        """
        Fetch deals with specified properties.

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
            List of deal dictionaries with requested properties
        """
        hubspot = self.handler.connect()

        # Determine which properties to request from HubSpot
        if properties is None:
            # Default: fetch only essential properties
            properties_to_fetch = self.DEFAULT_PROPERTIES
        elif len(properties) == 0:
            # Empty list means fetch ALL available properties
            properties_cache = self.handler.get_properties_cache('deals')
            properties_to_fetch = list(properties_cache['property_names'])
        else:
            # Specific properties requested
            properties_to_fetch = properties

        # Add properties parameter to API call
        kwargs['properties'] = properties_to_fetch
        deals = hubspot.crm.deals.get_all(**kwargs)

        deals_dict = []
        for deal in deals:
            # Start with the ID
            deal_dict = {"id": deal.id}

            # Extract properties that were returned
            if hasattr(deal, 'properties') and deal.properties:
                for prop_name, prop_value in deal.properties.items():
                    deal_dict[prop_name] = prop_value

            deals_dict.append(deal_dict)

        return deals_dict

    def search_deals(self, filters: List[Dict], properties: List[Text] = None, limit: int = None) -> List[Dict]:
        """
        Search deals using HubSpot search API with filters.

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
            List of deal dictionaries matching the filters
        """
        hubspot = self.handler.connect()

        # Determine which properties to request
        if properties is None:
            properties_to_fetch = self.DEFAULT_PROPERTIES
        elif len(properties) == 0:
            properties_cache = self.handler.get_properties_cache('deals')
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
        all_deals = []
        after = 0

        try:
            while True:
                if after > 0:
                    search_request["after"] = after

                # Call HubSpot search API
                response = hubspot.crm.deals.search_api.do_search(
                    public_object_search_request=search_request
                )

                # Extract deals from response
                for deal in response.results:
                    deal_dict = {"id": deal.id}
                    if hasattr(deal, 'properties') and deal.properties:
                        for prop_name, prop_value in deal.properties.items():
                            deal_dict[prop_name] = prop_value
                    all_deals.append(deal_dict)

                # Check if we've reached the limit
                if limit and len(all_deals) >= limit:
                    all_deals = all_deals[:limit]
                    break

                # Check if there are more results
                if not hasattr(response, 'paging') or not response.paging:
                    break

                if hasattr(response.paging, 'next') and response.paging.next:
                    after = response.paging.next.after
                else:
                    break

        except Exception as e:
            logger.error(f"Error searching deals: {e}")
            raise Exception(f"Deal search failed: {e}")

        logger.info(f"Found {len(all_deals)} deals matching filters")
        return all_deals

    def create_deals(self, deals_data: List[Dict[Text, Any]]) -> None:
        hubspot = self.handler.connect()
        deals_to_create = [HubSpotObjectInputCreate(properties=deal) for deal in deals_data]
        try:
            created_deals = hubspot.crm.deals.batch_api.create(
                HubSpotBatchObjectInputCreate(inputs=deals_to_create),
            )
            logger.info(f"Deals created with ID's {[created_deal.id for created_deal in created_deals.results]}")
        except Exception as e:
            raise Exception(f"Deals creation failed {e}")

    def update_deals(self, deal_ids: List[Text], values_to_update: Dict[Text, Any]) -> None:
        hubspot = self.handler.connect()
        deals_to_update = [HubSpotObjectBatchInput(id=deal_id, properties=values_to_update) for deal_id in deal_ids]
        try:
            updated_deals = hubspot.crm.deals.batch_api.update(
                HubSpotBatchObjectBatchInput(inputs=deals_to_update),
            )
            logger.info(f"Deals with ID {[updated_deal.id for updated_deal in updated_deals.results]} updated")
        except Exception as e:
            raise Exception(f"Deals update failed {e}")

    def delete_deals(self, deal_ids: List[Text]) -> None:
        hubspot = self.handler.connect()
        deals_to_delete = [HubSpotObjectId(id=deal_id) for deal_id in deal_ids]
        try:
            hubspot.crm.deals.batch_api.archive(
                HubSpotBatchObjectIdInput(inputs=deals_to_delete),
            )
            logger.info("Deals deleted")
        except Exception as e:
            raise Exception(f"Deals deletion failed {e}")