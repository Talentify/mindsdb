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


class DealsTable(APITable):
    """Hubspot Deals table."""

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

        deals_df = pd.json_normalize(self.get_deals(limit=result_limit))
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
        insert_statement_parser = INSERTQueryParser(
            query,
            supported_columns=['amount', 'dealname', 'pipeline', 'closedate', 'dealstage', 'hubspot_owner_id'],
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
        return pd.json_normalize(self.get_deals(limit=1)).columns.tolist()

    def get_deals(self, **kwargs) -> List[Dict]:
        hubspot = self.handler.connect()
        deals = hubspot.crm.deals.get_all(**kwargs)
        deals_dict = [
            {
                "id": deal.id,
                "dealname": deal.properties["dealname"],
                "amount": deal.properties.get("amount", None),
                "pipeline": deal.properties.get("pipeline", None),
                "closedate": deal.properties.get("closedate", None),
                "dealstage": deal.properties.get("dealstage", None),
                "hubspot_owner_id": deal.properties.get("hubspot_owner_id", None),
                "createdate": deal.properties["createdate"],
                "hs_lastmodifieddate": deal.properties["hs_lastmodifieddate"],
            }
            for deal in deals
        ]
        return deals_dict

    def create_deals(self, deals_data: List[Dict[Text, Any]]) -> None:
        hubspot = self.handler.connect()
        deals_to_create = [HubSpotObjectInputCreate(properties=deal) for deal in deals_data]
        try:
            created_deals = hubspot.crm.deals.batch_api.create(
                HubSpotBatchObjectBatchInput(inputs=deals_to_create),
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