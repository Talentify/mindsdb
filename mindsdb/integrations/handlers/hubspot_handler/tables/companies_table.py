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
from mindsdb.integrations.handlers.hubspot_handler.tables.base_hubspot_table import HubSpotSearchMixin


logger = log.getLogger(__name__)


class CompaniesTable(HubSpotSearchMixin, APITable):
    """Hubspot Companies table."""

    # Default essential properties to fetch (to avoid overloading with 100+ properties)
    DEFAULT_PROPERTIES = [
        'name', 'domain', 'city', 'state', 'country', 'phone', 'industry',
        'website', 'description', 'numberofemployees', 'annualrevenue',
        'createdate', 'hs_lastmodifieddate'
    ]

    def select(self, query: ast.Select) -> pd.DataFrame:
        """
        Pulls Hubspot Companies data

        Parameters
        ----------
        query : ast.Select
            Given SQL SELECT query

        Returns
        -------
        pd.DataFrame
            Hubspot Companies matching the query

        Raises
        ------
        ValueError
            If the query contains an unsupported condition

        """

        select_statement_parser = SELECTQueryParser(
            query,
            "companies",
            self.get_columns()
        )
        selected_columns, where_conditions, order_by_conditions, result_limit = select_statement_parser.parse_query()

        # Determine which properties to fetch from HubSpot API
        # If specific columns are requested, fetch only those (+ id)
        # If SELECT * is used, fetch only default essential properties
        requested_properties = None
        if selected_columns and len(selected_columns) > 0:
            # User requested specific columns - fetch only those
            requested_properties = [col for col in selected_columns if col != 'id']
        # else: Will use default properties in get_companies()

        # Check if WHERE conditions exist - use search API if they do
        if where_conditions and len(where_conditions) > 0:
            # Convert WHERE conditions to HubSpot search filters
            hubspot_filters = self._build_search_filters(where_conditions)

            if hubspot_filters:
                # Use search API with filters
                logger.info(f"Using HubSpot search API with {len(hubspot_filters)} filter(s)")
                companies_df = pd.json_normalize(
                    self.search_companies(
                        filters=hubspot_filters,
                        properties=requested_properties,
                        limit=result_limit
                    )
                )
                # Filters already applied at API level
                where_conditions = []
            else:
                # No valid filters, fall back to get_all
                logger.info("No valid HubSpot filters, using get_all")
                companies_df = pd.json_normalize(
                    self.get_companies(limit=result_limit, properties=requested_properties)
                )
        else:
            # No WHERE clause, use get_all
            companies_df = pd.json_normalize(
                self.get_companies(limit=result_limit, properties=requested_properties)
            )

        # Apply column selection and ORDER BY
        select_statement_executor = SELECTQueryExecutor(
            companies_df,
            selected_columns,
            where_conditions,  # Empty if already applied via search API
            order_by_conditions
        )
        companies_df = select_statement_executor.execute_query()

        return companies_df

    def insert(self, query: ast.Insert) -> None:
        """
        Inserts data into HubSpot "POST /crm/v3/objects/companies/batch/create" API endpoint.

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
            properties_cache = self.handler.get_properties_cache('companies')
            supported_columns = list(properties_cache['property_names'])
        except Exception as e:
            logger.warning(f"Failed to get dynamic columns for insert, using minimal set: {e}")
            supported_columns = ['name', 'city', 'phone', 'state', 'domain', 'industry']

        insert_statement_parser = INSERTQueryParser(
            query,
            supported_columns=supported_columns,
            mandatory_columns=['name'],
            all_mandatory=False,
        )
        company_data = insert_statement_parser.parse_query()
        self.create_companies(company_data)

    def update(self, query: ast.Update) -> None:
        """
        Updates data from HubSpot "PATCH /crm/v3/objects/companies/batch/update" API endpoint.

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

        companies_df = pd.json_normalize(self.get_companies())
        update_query_executor = UPDATEQueryExecutor(
            companies_df,
            where_conditions
        )

        companies_df = update_query_executor.execute_query()
        company_ids = companies_df['id'].tolist()
        self.update_companies(company_ids, values_to_update)

    def delete(self, query: ast.Delete) -> None:
        """
        Deletes data from HubSpot "DELETE /crm/v3/objects/companies/batch/archive" API endpoint.

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

        companies_df = pd.json_normalize(self.get_companies())
        delete_query_executor = DELETEQueryExecutor(
            companies_df,
            where_conditions
        )

        companies_df = delete_query_executor.execute_query()
        company_ids = companies_df['id'].tolist()
        self.delete_companies(company_ids)

    def get_columns(self) -> List[Text]:
        """
        Get column names for the table.
        Returns default essential properties to avoid overloading with 100+ properties.
        Users can still query specific custom properties explicitly in SELECT.
        """
        # Return id + default essential properties
        return ['id'] + self.DEFAULT_PROPERTIES

    def get_companies(self, properties: List[Text] = None, **kwargs) -> List[Dict]:
        """
        Fetch companies with specified properties.

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
            List of company dictionaries with requested properties
        """
        hubspot = self.handler.connect()

        # Determine which properties to request from HubSpot
        if properties is None:
            # Default: fetch only essential properties
            properties_to_fetch = self.DEFAULT_PROPERTIES
        elif len(properties) == 0:
            # Empty list means fetch ALL available properties
            properties_cache = self.handler.get_properties_cache('companies')
            properties_to_fetch = list(properties_cache['property_names'])
        else:
            # Specific properties requested
            properties_to_fetch = properties

        # Add properties parameter to API call
        kwargs['properties'] = properties_to_fetch
        companies = hubspot.crm.companies.get_all(**kwargs)

        companies_dict = []
        for company in companies:
            # Start with the ID
            company_dict = {"id": company.id}

            # Extract properties that were returned
            if hasattr(company, 'properties') and company.properties:
                for prop_name, prop_value in company.properties.items():
                    company_dict[prop_name] = prop_value

            companies_dict.append(company_dict)

        return companies_dict

    def search_companies(self, filters: List[Dict], properties: List[Text] = None, limit: int = None) -> List[Dict]:
        """
        Search companies using HubSpot search API with filters.

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
            List of company dictionaries matching the filters
        """
        hubspot = self.handler.connect()

        # Determine which properties to request
        if properties is None:
            properties_to_fetch = self.DEFAULT_PROPERTIES
        elif len(properties) == 0:
            properties_cache = self.handler.get_properties_cache('companies')
            properties_to_fetch = list(properties_cache['property_names'])
        else:
            properties_to_fetch = properties

        # Build search request
        search_request = {
            "filterGroups": [{"filters": filters}],
            "properties": properties_to_fetch,
            "limit": min(limit or 100, 100),  # HubSpot max is 100 per page
        }

        # Pagination to fetch all results
        all_companies = []
        after = 0

        try:
            while True:
                if after > 0:
                    search_request["after"] = after

                # Call HubSpot search API
                response = hubspot.crm.companies.search_api.do_search(
                    public_object_search_request=search_request
                )

                # Extract companies from response
                for company in response.results:
                    company_dict = {"id": company.id}
                    if hasattr(company, 'properties') and company.properties:
                        for prop_name, prop_value in company.properties.items():
                            company_dict[prop_name] = prop_value
                    all_companies.append(company_dict)

                # Check if we've reached the limit
                if limit and len(all_companies) >= limit:
                    all_companies = all_companies[:limit]
                    break

                # Check if there are more results
                if not hasattr(response, 'paging') or not response.paging:
                    break

                if hasattr(response.paging, 'next') and response.paging.next:
                    after = response.paging.next.after
                else:
                    break

        except Exception as e:
            logger.error(f"Error searching companies: {e}")
            raise Exception(f"Company search failed: {e}")

        logger.info(f"Found {len(all_companies)} companies matching filters")
        return all_companies

    def create_companies(self, companies_data: List[Dict[Text, Any]]) -> None:
        hubspot = self.handler.connect()
        companies_to_create = [HubSpotObjectInputCreate(properties=company) for company in companies_data]
        try:
            created_companies = hubspot.crm.companies.batch_api.create(
                HubSpotBatchObjectInputCreate(inputs=companies_to_create),
            )
            logger.info(f"Companies created with ID's {[created_company.id for created_company in created_companies.results]}")
        except Exception as e:
            raise Exception(f"Companies creation failed {e}")

    def update_companies(self, company_ids: List[Text], values_to_update: Dict[Text, Any]) -> None:
        hubspot = self.handler.connect()
        companies_to_update = [HubSpotObjectBatchInput(id=company_id, properties=values_to_update) for company_id in company_ids]
        try:
            updated_companies = hubspot.crm.companies.batch_api.update(
                HubSpotBatchObjectBatchInput(inputs=companies_to_update),
            )
            logger.info(f"Companies with ID {[updated_company.id for updated_company in updated_companies.results]} updated")
        except Exception as e:
            raise Exception(f"Companies update failed {e}")

    def delete_companies(self, company_ids: List[Text]) -> None:
        hubspot = self.handler.connect()
        companies_to_delete = [HubSpotObjectId(id=company_id) for company_id in company_ids]
        try:
            hubspot.crm.companies.batch_api.archive(
                HubSpotBatchObjectIdInput(inputs=companies_to_delete),
            )
            logger.info("Companies deleted")
        except Exception as e:
            raise Exception(f"Companies deletion failed {e}")