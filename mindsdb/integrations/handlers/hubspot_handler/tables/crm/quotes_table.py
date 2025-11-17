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


class QuotesTable(HubSpotSearchMixin, APITable):
    """Hubspot Quotes table."""

    # Default essential properties to fetch (to avoid overloading with 100+ properties)
    # Note: Quotes have unique property names, using only commonly available ones
    DEFAULT_PROPERTIES = [
        'hs_title', 'hs_expiration_date', 'hs_status', 'hs_quote_amount',
        'hs_currency', 'hs_public_url_key', 'hubspot_owner_id'
    ]

    def select(self, query: ast.Select) -> pd.DataFrame:
        """
        Pulls Hubspot Quotes data

        Parameters
        ----------
        query : ast.Select
            Given SQL SELECT query

        Returns
        -------
        pd.DataFrame
            Hubspot Quotes matching the query

        Raises
        ------
        ValueError
            If the query contains an unsupported condition

        """

        select_statement_parser = SELECTQueryParser(
            query,
            "quotes",
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
                quotes_df = pd.json_normalize(
                    self.search_quotes(
                        filters=hubspot_filters,
                        properties=requested_properties,
                        limit=result_limit
                    )
                )
                where_conditions = []
            else:
                logger.info("No valid HubSpot filters, using get_all")
                quotes_df = pd.json_normalize(
                    self.get_quotes(limit=result_limit, properties=requested_properties)
                )
        else:
            quotes_df = pd.json_normalize(
                self.get_quotes(limit=result_limit, properties=requested_properties)
            )

        # Filter selected_columns to only include columns that actually exist in the dataframe
        # This handles cases where requested properties don't exist in HubSpot
        if not quotes_df.empty and selected_columns:
            available_columns = [col for col in selected_columns if col in quotes_df.columns]
            if len(available_columns) < len(selected_columns):
                missing = set(selected_columns) - set(available_columns)
                logger.warning(f"Some requested columns not available in quotes data: {missing}")
            selected_columns = available_columns if available_columns else None

        select_statement_executor = SELECTQueryExecutor(
            quotes_df,
            selected_columns,
            where_conditions,
            order_by_conditions
        )
        quotes_df = select_statement_executor.execute_query()

        return quotes_df

    def insert(self, query: ast.Insert) -> None:
        """
        Inserts data into HubSpot "POST /crm/v3/objects/quotes/batch/create" API endpoint.

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
            properties_cache = self.handler.get_properties_cache('quotes')
            supported_columns = list(properties_cache['property_names'])
        except Exception as e:
            logger.warning(f"Failed to get dynamic columns for insert, using minimal set: {e}")
            supported_columns = ['hs_title', 'hs_expiration_date', 'hs_quote_amount']

        insert_statement_parser = INSERTQueryParser(
            query,
            supported_columns=supported_columns,
            mandatory_columns=['hs_title'],
            all_mandatory=False,
        )
        quotes_data = insert_statement_parser.parse_query()
        self.create_quotes(quotes_data)

    def update(self, query: ast.Update) -> None:
        """
        Updates data from HubSpot "PATCH /crm/v3/objects/quotes/batch/update" API endpoint.

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

        quotes_df = pd.json_normalize(self.get_quotes())
        update_query_executor = UPDATEQueryExecutor(
            quotes_df,
            where_conditions
        )

        quotes_df = update_query_executor.execute_query()
        quote_ids = quotes_df['id'].tolist()
        self.update_quotes(quote_ids, values_to_update)

    def delete(self, query: ast.Delete) -> None:
        """
        Deletes data from HubSpot "DELETE /crm/v3/objects/quotes/batch/archive" API endpoint.

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

        quotes_df = pd.json_normalize(self.get_quotes())
        delete_query_executor = DELETEQueryExecutor(
            quotes_df,
            where_conditions
        )

        quotes_df = delete_query_executor.execute_query()
        quote_ids = quotes_df['id'].tolist()
        self.delete_quotes(quote_ids)

    def get_columns(self) -> List[Text]:
        """
        Get column names for the table.
        Returns default essential properties to avoid overloading with 100+ properties.
        Users can still query specific custom properties explicitly in SELECT.
        """
        # Return id + default essential properties
        return ['id'] + self.DEFAULT_PROPERTIES

    def get_quotes(self, properties: List[Text] = None, **kwargs) -> List[Dict]:
        """
        Fetch quotes with specified properties.

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
            List of quote dictionaries with requested properties
        """
        hubspot = self.handler.connect()

        # Determine which properties to request from HubSpot
        if properties is None:
            # Default: fetch only essential properties
            properties_to_fetch = self.DEFAULT_PROPERTIES
        elif len(properties) == 0:
            # Empty list means fetch ALL available properties
            properties_cache = self.handler.get_properties_cache('quotes')
            properties_to_fetch = list(properties_cache['property_names'])
        else:
            # Specific properties requested
            properties_to_fetch = properties

        # Add properties parameter to API call
        kwargs['properties'] = properties_to_fetch
        quotes = hubspot.crm.quotes.get_all(**kwargs)

        quotes_dict = []
        for quote in quotes:
            # Start with the ID
            quote_dict = {"id": quote.id}

            # Extract properties that were returned
            if hasattr(quote, 'properties') and quote.properties:
                for prop_name, prop_value in quote.properties.items():
                    quote_dict[prop_name] = prop_value

            quotes_dict.append(quote_dict)

        return quotes_dict

    def search_quotes(self, filters: List[Dict], properties: List[Text] = None, limit: int = None) -> List[Dict]:
        """
        Search quotes using HubSpot search API with filters.

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
            List of quote dictionaries matching the filters
        """
        hubspot = self.handler.connect()

        # Determine which properties to request
        if properties is None:
            properties_to_fetch = self.DEFAULT_PROPERTIES
        elif len(properties) == 0:
            properties_cache = self.handler.get_properties_cache('quotes')
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
        all_quotes = []
        after = 0

        try:
            while True:
                if after > 0:
                    search_request["after"] = after

                # Call HubSpot search API
                response = hubspot.crm.quotes.search_api.do_search(
                    public_object_search_request=search_request
                )

                # Extract quotes from response
                for quote in response.results:
                    quote_dict = {"id": quote.id}
                    if hasattr(quote, 'properties') and quote.properties:
                        for prop_name, prop_value in quote.properties.items():
                            quote_dict[prop_name] = prop_value
                    all_quotes.append(quote_dict)

                # Check if we've reached the limit
                if limit and len(all_quotes) >= limit:
                    all_quotes = all_quotes[:limit]
                    break

                # Check if there are more results
                if not hasattr(response, 'paging') or not response.paging:
                    break

                if hasattr(response.paging, 'next') and response.paging.next:
                    after = response.paging.next.after
                else:
                    break

        except Exception as e:
            logger.error(f"Error searching quotes: {e}")
            raise Exception(f"Quote search failed: {e}")

        logger.info(f"Found {len(all_quotes)} quotes matching filters")
        return all_quotes

    def create_quotes(self, quotes_data: List[Dict[Text, Any]]) -> None:
        hubspot = self.handler.connect()
        quotes_to_create = [HubSpotObjectInputCreate(properties=quote) for quote in quotes_data]
        try:
            created_quotes = hubspot.crm.quotes.batch_api.create(
                HubSpotBatchObjectInputCreate(inputs=quotes_to_create),
            )
            logger.info(f"Quotes created with ID's {[created_quote.id for created_quote in created_quotes.results]}")
        except Exception as e:
            raise Exception(f"Quotes creation failed {e}")

    def update_quotes(self, quote_ids: List[Text], values_to_update: Dict[Text, Any]) -> None:
        hubspot = self.handler.connect()
        quotes_to_update = [HubSpotObjectBatchInput(id=quote_id, properties=values_to_update) for quote_id in quote_ids]
        try:
            updated_quotes = hubspot.crm.quotes.batch_api.update(
                HubSpotBatchObjectBatchInput(inputs=quotes_to_update),
            )
            logger.info(f"Quotes with ID {[updated_quote.id for updated_quote in updated_quotes.results]} updated")
        except Exception as e:
            raise Exception(f"Quotes update failed {e}")

    def delete_quotes(self, quote_ids: List[Text]) -> None:
        hubspot = self.handler.connect()
        quotes_to_delete = [HubSpotObjectId(id=quote_id) for quote_id in quote_ids]
        try:
            hubspot.crm.quotes.batch_api.archive(
                HubSpotBatchObjectIdInput(inputs=quotes_to_delete),
            )
            logger.info("Quotes deleted")
        except Exception as e:
            raise Exception(f"Quotes deletion failed {e}")
