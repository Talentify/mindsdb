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


class LineItemsTable(HubSpotSearchMixin, APITable):
    """Hubspot Line Items table."""

    # Default essential properties to fetch (to avoid overloading with 100+ properties)
    DEFAULT_PROPERTIES = [
        'name', 'description', 'quantity', 'price', 'amount', 'hs_product_id',
        'hs_sku', 'discount', 'tax', 'createdate', 'hs_lastmodifieddate'
    ]

    def select(self, query: ast.Select) -> pd.DataFrame:
        """
        Pulls Hubspot Line Items data

        Parameters
        ----------
        query : ast.Select
            Given SQL SELECT query

        Returns
        -------
        pd.DataFrame
            Hubspot Line Items matching the query

        Raises
        ------
        ValueError
            If the query contains an unsupported condition

        """

        select_statement_parser = SELECTQueryParser(
            query,
            "line_items",
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
                line_items_df = pd.json_normalize(
                    self.search_line_items(
                        filters=hubspot_filters,
                        properties=requested_properties,
                        limit=result_limit
                    )
                )
                where_conditions = []
            else:
                logger.info("No valid HubSpot filters, using get_all")
                line_items_df = pd.json_normalize(
                    self.get_line_items(limit=result_limit, properties=requested_properties)
                )
        else:
            line_items_df = pd.json_normalize(
                self.get_line_items(limit=result_limit, properties=requested_properties)
            )

        select_statement_executor = SELECTQueryExecutor(
            line_items_df,
            selected_columns,
            where_conditions,
            order_by_conditions
        )
        line_items_df = select_statement_executor.execute_query()

        return line_items_df

    def insert(self, query: ast.Insert) -> None:
        """
        Inserts data into HubSpot "POST /crm/v3/objects/line_items/batch/create" API endpoint.

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
            properties_cache = self.handler.get_properties_cache('line_items')
            supported_columns = list(properties_cache['property_names'])
        except Exception as e:
            logger.warning(f"Failed to get dynamic columns for insert, using minimal set: {e}")
            supported_columns = ['name', 'quantity', 'price', 'hs_product_id']

        insert_statement_parser = INSERTQueryParser(
            query,
            supported_columns=supported_columns,
            mandatory_columns=['quantity', 'price'],
            all_mandatory=False,
        )
        line_items_data = insert_statement_parser.parse_query()
        self.create_line_items(line_items_data)

    def update(self, query: ast.Update) -> None:
        """
        Updates data from HubSpot "PATCH /crm/v3/objects/line_items/batch/update" API endpoint.

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

        line_items_df = pd.json_normalize(self.get_line_items())
        update_query_executor = UPDATEQueryExecutor(
            line_items_df,
            where_conditions
        )

        line_items_df = update_query_executor.execute_query()
        line_item_ids = line_items_df['id'].tolist()
        self.update_line_items(line_item_ids, values_to_update)

    def delete(self, query: ast.Delete) -> None:
        """
        Deletes data from HubSpot "DELETE /crm/v3/objects/line_items/batch/archive" API endpoint.

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

        line_items_df = pd.json_normalize(self.get_line_items())
        delete_query_executor = DELETEQueryExecutor(
            line_items_df,
            where_conditions
        )

        line_items_df = delete_query_executor.execute_query()
        line_item_ids = line_items_df['id'].tolist()
        self.delete_line_items(line_item_ids)

    def get_columns(self) -> List[Text]:
        """
        Get column names for the table.
        Returns default essential properties to avoid overloading with 100+ properties.
        Users can still query specific custom properties explicitly in SELECT.
        """
        # Return id + default essential properties
        return ['id'] + self.DEFAULT_PROPERTIES

    def get_line_items(self, properties: List[Text] = None, **kwargs) -> List[Dict]:
        """
        Fetch line items with specified properties.

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
            List of line item dictionaries with requested properties
        """
        hubspot = self.handler.connect()

        # Determine which properties to request from HubSpot
        if properties is None:
            # Default: fetch only essential properties
            properties_to_fetch = self.DEFAULT_PROPERTIES
        elif len(properties) == 0:
            # Empty list means fetch ALL available properties
            properties_cache = self.handler.get_properties_cache('line_items')
            properties_to_fetch = list(properties_cache['property_names'])
        else:
            # Specific properties requested
            properties_to_fetch = properties

        # Add properties parameter to API call
        kwargs['properties'] = properties_to_fetch
        line_items = hubspot.crm.line_items.get_all(**kwargs)

        line_items_dict = []
        for line_item in line_items:
            # Start with the ID
            line_item_dict = {"id": line_item.id}

            # Extract properties that were returned
            if hasattr(line_item, 'properties') and line_item.properties:
                for prop_name, prop_value in line_item.properties.items():
                    line_item_dict[prop_name] = prop_value

            line_items_dict.append(line_item_dict)

        return line_items_dict

    def search_line_items(self, filters: List[Dict], properties: List[Text] = None, limit: int = None) -> List[Dict]:
        """
        Search line items using HubSpot search API with filters.

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
            List of line item dictionaries matching the filters
        """
        hubspot = self.handler.connect()

        # Determine which properties to request
        if properties is None:
            properties_to_fetch = self.DEFAULT_PROPERTIES
        elif len(properties) == 0:
            properties_cache = self.handler.get_properties_cache('line_items')
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
        all_line_items = []
        after = 0

        try:
            while True:
                if after > 0:
                    search_request["after"] = after

                # Call HubSpot search API
                response = hubspot.crm.line_items.search_api.do_search(
                    public_object_search_request=search_request
                )

                # Extract line items from response
                for line_item in response.results:
                    line_item_dict = {"id": line_item.id}
                    if hasattr(line_item, 'properties') and line_item.properties:
                        for prop_name, prop_value in line_item.properties.items():
                            line_item_dict[prop_name] = prop_value
                    all_line_items.append(line_item_dict)

                # Check if we've reached the limit
                if limit and len(all_line_items) >= limit:
                    all_line_items = all_line_items[:limit]
                    break

                # Check if there are more results
                if not hasattr(response, 'paging') or not response.paging:
                    break

                if hasattr(response.paging, 'next') and response.paging.next:
                    after = response.paging.next.after
                else:
                    break

        except Exception as e:
            logger.error(f"Error searching line items: {e}")
            raise Exception(f"Line item search failed: {e}")

        logger.info(f"Found {len(all_line_items)} line items matching filters")
        return all_line_items

    def create_line_items(self, line_items_data: List[Dict[Text, Any]]) -> None:
        hubspot = self.handler.connect()
        line_items_to_create = [HubSpotObjectInputCreate(properties=line_item) for line_item in line_items_data]
        try:
            created_line_items = hubspot.crm.line_items.batch_api.create(
                HubSpotBatchObjectInputCreate(inputs=line_items_to_create),
            )
            logger.info(f"Line items created with ID's {[created_line_item.id for created_line_item in created_line_items.results]}")
        except Exception as e:
            raise Exception(f"Line items creation failed {e}")

    def update_line_items(self, line_item_ids: List[Text], values_to_update: Dict[Text, Any]) -> None:
        hubspot = self.handler.connect()
        line_items_to_update = [HubSpotObjectBatchInput(id=line_item_id, properties=values_to_update) for line_item_id in line_item_ids]
        try:
            updated_line_items = hubspot.crm.line_items.batch_api.update(
                HubSpotBatchObjectBatchInput(inputs=line_items_to_update),
            )
            logger.info(f"Line items with ID {[updated_line_item.id for updated_line_item in updated_line_items.results]} updated")
        except Exception as e:
            raise Exception(f"Line items update failed {e}")

    def delete_line_items(self, line_item_ids: List[Text]) -> None:
        hubspot = self.handler.connect()
        line_items_to_delete = [HubSpotObjectId(id=line_item_id) for line_item_id in line_item_ids]
        try:
            hubspot.crm.line_items.batch_api.archive(
                HubSpotBatchObjectIdInput(inputs=line_items_to_delete),
            )
            logger.info("Line items deleted")
        except Exception as e:
            raise Exception(f"Line items deletion failed {e}")
