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


class ProductsTable(HubSpotSearchMixin, APITable):
    """Hubspot Products table."""

    # Default essential properties to fetch (to avoid overloading with 100+ properties)
    DEFAULT_PROPERTIES = [
        'name', 'description', 'price', 'hs_sku', 'hs_cost_of_goods_sold',
        'hs_recurring_billing_period', 'hs_product_type', 'createdate', 'hs_lastmodifieddate'
    ]

    def select(self, query: ast.Select) -> pd.DataFrame:
        """
        Pulls Hubspot Products data

        Parameters
        ----------
        query : ast.Select
            Given SQL SELECT query

        Returns
        -------
        pd.DataFrame
            Hubspot Products matching the query

        Raises
        ------
        ValueError
            If the query contains an unsupported condition

        """

        select_statement_parser = SELECTQueryParser(
            query,
            "products",
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
                products_df = pd.json_normalize(
                    self.search_products(
                        filters=hubspot_filters,
                        properties=requested_properties,
                        limit=result_limit
                    )
                )
                where_conditions = []
            else:
                logger.info("No valid HubSpot filters, using get_all")
                products_df = pd.json_normalize(
                    self.get_products(limit=result_limit, properties=requested_properties)
                )
        else:
            products_df = pd.json_normalize(
                self.get_products(limit=result_limit, properties=requested_properties)
            )

        select_statement_executor = SELECTQueryExecutor(
            products_df,
            selected_columns,
            where_conditions,
            order_by_conditions
        )
        products_df = select_statement_executor.execute_query()

        return products_df

    def insert(self, query: ast.Insert) -> None:
        """
        Inserts data into HubSpot "POST /crm/v3/objects/products/batch/create" API endpoint.

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
            properties_cache = self.handler.get_properties_cache('products')
            supported_columns = list(properties_cache['property_names'])
        except Exception as e:
            logger.warning(f"Failed to get dynamic columns for insert, using minimal set: {e}")
            supported_columns = ['name', 'description', 'price', 'hs_sku']

        insert_statement_parser = INSERTQueryParser(
            query,
            supported_columns=supported_columns,
            mandatory_columns=['name'],
            all_mandatory=False,
        )
        products_data = insert_statement_parser.parse_query()
        self.create_products(products_data)

    def update(self, query: ast.Update) -> None:
        """
        Updates data from HubSpot "PATCH /crm/v3/objects/products/batch/update" API endpoint.

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

        products_df = pd.json_normalize(self.get_products())
        update_query_executor = UPDATEQueryExecutor(
            products_df,
            where_conditions
        )

        products_df = update_query_executor.execute_query()
        product_ids = products_df['id'].tolist()
        self.update_products(product_ids, values_to_update)

    def delete(self, query: ast.Delete) -> None:
        """
        Deletes data from HubSpot "DELETE /crm/v3/objects/products/batch/archive" API endpoint.

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

        products_df = pd.json_normalize(self.get_products())
        delete_query_executor = DELETEQueryExecutor(
            products_df,
            where_conditions
        )

        products_df = delete_query_executor.execute_query()
        product_ids = products_df['id'].tolist()
        self.delete_products(product_ids)

    def get_columns(self) -> List[Text]:
        """
        Get column names for the table.
        Returns default essential properties to avoid overloading with 100+ properties.
        Users can still query specific custom properties explicitly in SELECT.
        """
        # Return id + default essential properties
        return ['id'] + self.DEFAULT_PROPERTIES

    def get_products(self, properties: List[Text] = None, **kwargs) -> List[Dict]:
        """
        Fetch products with specified properties.

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
            List of product dictionaries with requested properties
        """
        hubspot = self.handler.connect()

        # Determine which properties to request from HubSpot
        if properties is None:
            # Default: fetch only essential properties
            properties_to_fetch = self.DEFAULT_PROPERTIES
        elif len(properties) == 0:
            # Empty list means fetch ALL available properties
            properties_cache = self.handler.get_properties_cache('products')
            properties_to_fetch = list(properties_cache['property_names'])
        else:
            # Specific properties requested
            properties_to_fetch = properties

        # Add properties parameter to API call
        kwargs['properties'] = properties_to_fetch
        products = hubspot.crm.products.get_all(**kwargs)

        products_dict = []
        for product in products:
            # Start with the ID
            product_dict = {"id": product.id}

            # Extract properties that were returned
            if hasattr(product, 'properties') and product.properties:
                for prop_name, prop_value in product.properties.items():
                    product_dict[prop_name] = prop_value

            products_dict.append(product_dict)

        return products_dict

    def search_products(self, filters: List[Dict], properties: List[Text] = None, limit: int = None) -> List[Dict]:
        """
        Search products using HubSpot search API with filters.

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
            List of product dictionaries matching the filters
        """
        hubspot = self.handler.connect()

        # Determine which properties to request
        if properties is None:
            properties_to_fetch = self.DEFAULT_PROPERTIES
        elif len(properties) == 0:
            properties_cache = self.handler.get_properties_cache('products')
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
        all_products = []
        after = 0

        try:
            while True:
                if after > 0:
                    search_request["after"] = after

                # Call HubSpot search API
                response = hubspot.crm.products.search_api.do_search(
                    public_object_search_request=search_request
                )

                # Extract products from response
                for product in response.results:
                    product_dict = {"id": product.id}
                    if hasattr(product, 'properties') and product.properties:
                        for prop_name, prop_value in product.properties.items():
                            product_dict[prop_name] = prop_value
                    all_products.append(product_dict)

                # Check if we've reached the limit
                if limit and len(all_products) >= limit:
                    all_products = all_products[:limit]
                    break

                # Check if there are more results
                if not hasattr(response, 'paging') or not response.paging:
                    break

                if hasattr(response.paging, 'next') and response.paging.next:
                    after = response.paging.next.after
                else:
                    break

        except Exception as e:
            logger.error(f"Error searching products: {e}")
            raise Exception(f"Product search failed: {e}")

        logger.info(f"Found {len(all_products)} products matching filters")
        return all_products

    def create_products(self, products_data: List[Dict[Text, Any]]) -> None:
        hubspot = self.handler.connect()
        products_to_create = [HubSpotObjectInputCreate(properties=product) for product in products_data]
        try:
            created_products = hubspot.crm.products.batch_api.create(
                HubSpotBatchObjectInputCreate(inputs=products_to_create),
            )
            logger.info(f"Products created with ID's {[created_product.id for created_product in created_products.results]}")
        except Exception as e:
            raise Exception(f"Products creation failed {e}")

    def update_products(self, product_ids: List[Text], values_to_update: Dict[Text, Any]) -> None:
        hubspot = self.handler.connect()
        products_to_update = [HubSpotObjectBatchInput(id=product_id, properties=values_to_update) for product_id in product_ids]
        try:
            updated_products = hubspot.crm.products.batch_api.update(
                HubSpotBatchObjectBatchInput(inputs=products_to_update),
            )
            logger.info(f"Products with ID {[updated_product.id for updated_product in updated_products.results]} updated")
        except Exception as e:
            raise Exception(f"Products update failed {e}")

    def delete_products(self, product_ids: List[Text]) -> None:
        hubspot = self.handler.connect()
        products_to_delete = [HubSpotObjectId(id=product_id) for product_id in product_ids]
        try:
            hubspot.crm.products.batch_api.archive(
                HubSpotBatchObjectIdInput(inputs=products_to_delete),
            )
            logger.info("Products deleted")
        except Exception as e:
            raise Exception(f"Products deletion failed {e}")
