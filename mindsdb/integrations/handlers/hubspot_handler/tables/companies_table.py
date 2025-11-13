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


logger = log.getLogger(__name__)


class CompaniesTable(APITable):
    """Hubspot Companies table."""

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

        companies_df = pd.json_normalize(self.get_companies(limit=result_limit))
        select_statement_executor = SELECTQueryExecutor(
            companies_df,
            selected_columns,
            where_conditions,
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
        insert_statement_parser = INSERTQueryParser(
            query,
            supported_columns=['name', 'city', 'phone', 'state', 'domain', 'industry'],
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
        return pd.json_normalize(self.get_companies(limit=1)).columns.tolist()

    def get_companies(self, **kwargs) -> List[Dict]:
        hubspot = self.handler.connect()
        companies = hubspot.crm.companies.get_all(**kwargs)
        companies_dict = [
            {
                "id": company.id,
                "name": company.properties.get("name", None),
                "city": company.properties.get("city", None),
                "phone": company.properties.get("phone", None),
                "state": company.properties.get("state", None),
                "domain": company.properties.get("company", None),
                "industry": company.properties.get("industry", None),
                "createdate": company.properties["createdate"],
                "lastmodifieddate": company.properties["hs_lastmodifieddate"],
            }
            for company in companies
        ]
        return companies_dict

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