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


class CallsTable(HubSpotSearchMixin, APITable):
    """Hubspot Calls table (Activity)."""

    DEFAULT_PROPERTIES = [
        'hs_timestamp', 'hs_call_title', 'hs_call_body', 'hs_call_duration',
        'hs_call_from_number', 'hs_call_to_number', 'hs_call_status',
        'hs_call_direction', 'hs_call_disposition', 'hubspot_owner_id',
        'createdate', 'hs_lastmodifieddate'
    ]

    def select(self, query: ast.Select) -> pd.DataFrame:
        """Pulls Hubspot Calls data"""
        select_statement_parser = SELECTQueryParser(
            query,
            "calls",
            self.get_columns()
        )
        selected_columns, where_conditions, order_by_conditions, result_limit = select_statement_parser.parse_query()

        requested_properties = None
        if selected_columns and len(selected_columns) > 0:
            requested_properties = [col for col in selected_columns if col != 'id']

        if where_conditions and len(where_conditions) > 0:
            hubspot_filters = self._build_search_filters(where_conditions)
            if hubspot_filters:
                logger.info(f"Using HubSpot search API with {len(hubspot_filters)} filter(s)")
                calls_df = pd.json_normalize(
                    self.search_calls(
                        filters=hubspot_filters,
                        properties=requested_properties,
                        limit=result_limit
                    )
                )
                where_conditions = []
            else:
                logger.info("No valid HubSpot filters, using get_all")
                calls_df = pd.json_normalize(
                    self.get_calls(limit=result_limit, properties=requested_properties)
                )
        else:
            calls_df = pd.json_normalize(
                self.get_calls(limit=result_limit, properties=requested_properties)
            )

        # Filter selected_columns to only include columns that actually exist in the dataframe
        # This handles cases where requested properties don't exist in HubSpot
        if not calls_df.empty and selected_columns:
            available_columns = [col for col in selected_columns if col in calls_df.columns]
            if len(available_columns) < len(selected_columns):
                missing = set(selected_columns) - set(available_columns)
                logger.warning(f"Some requested columns not available in calls data: {missing}")
            selected_columns = available_columns

        select_statement_executor = SELECTQueryExecutor(
            calls_df,
            selected_columns,
            where_conditions,
            order_by_conditions
        )
        return select_statement_executor.execute_query()

    def insert(self, query: ast.Insert) -> None:
        """Inserts data into HubSpot Calls"""
        try:
            properties_cache = self.handler.get_properties_cache('calls')
            supported_columns = list(properties_cache['property_names'])
        except Exception as e:
            logger.warning(f"Failed to get dynamic columns for insert: {e}")
            supported_columns = ['hs_timestamp', 'hs_call_title', 'hs_call_duration']

        insert_statement_parser = INSERTQueryParser(
            query,
            supported_columns=supported_columns,
            mandatory_columns=['hs_timestamp'],
            all_mandatory=False,
        )
        calls_data = insert_statement_parser.parse_query()
        self.create_calls(calls_data)

    def update(self, query: ast.Update) -> None:
        """Updates HubSpot Calls"""
        update_statement_parser = UPDATEQueryParser(query)
        values_to_update, where_conditions = update_statement_parser.parse_query()

        calls_df = pd.json_normalize(self.get_calls())
        update_query_executor = UPDATEQueryExecutor(calls_df, where_conditions)
        calls_df = update_query_executor.execute_query()
        call_ids = calls_df['id'].tolist()
        self.update_calls(call_ids, values_to_update)

    def delete(self, query: ast.Delete) -> None:
        """Deletes HubSpot Calls"""
        delete_statement_parser = DELETEQueryParser(query)
        where_conditions = delete_statement_parser.parse_query()

        calls_df = pd.json_normalize(self.get_calls())
        delete_query_executor = DELETEQueryExecutor(calls_df, where_conditions)
        calls_df = delete_query_executor.execute_query()
        call_ids = calls_df['id'].tolist()
        self.delete_calls(call_ids)

    def get_columns(self) -> List[Text]:
        """Get column names for the table"""
        return ['id'] + self.DEFAULT_PROPERTIES

    def get_calls(self, properties: List[Text] = None, **kwargs) -> List[Dict]:
        """Fetch calls with specified properties"""
        hubspot = self.handler.connect()

        if properties is None:
            properties_to_fetch = self.DEFAULT_PROPERTIES
        elif len(properties) == 0:
            properties_cache = self.handler.get_properties_cache('calls')
            properties_to_fetch = list(properties_cache['property_names'])
        else:
            properties_to_fetch = properties

        kwargs['properties'] = properties_to_fetch

        # Use basic_api.get_page for activity objects
        response = hubspot.crm.objects.basic_api.get_page(
            object_type="calls",
            **kwargs
        )

        calls_dict = []
        for call in response.results:
            call_dict = {"id": call.id}
            if hasattr(call, 'properties') and call.properties:
                for prop_name, prop_value in call.properties.items():
                    call_dict[prop_name] = prop_value
            calls_dict.append(call_dict)

        return calls_dict

    def search_calls(self, filters: List[Dict], properties: List[Text] = None, limit: int = None) -> List[Dict]:
        """Search calls using HubSpot search API"""
        hubspot = self.handler.connect()

        if properties is None:
            properties_to_fetch = self.DEFAULT_PROPERTIES
        elif len(properties) == 0:
            properties_cache = self.handler.get_properties_cache('calls')
            properties_to_fetch = list(properties_cache['property_names'])
        else:
            properties_to_fetch = properties

        search_request = {
            "filterGroups": [{"filters": filters}],
            "properties": properties_to_fetch,
            "limit": min(limit or 100, 100),
        }

        all_calls = []
        after = 0

        try:
            while True:
                if after > 0:
                    search_request["after"] = after

                response = hubspot.crm.objects.search_api.do_search(
                    object_type="calls",
                    public_object_search_request=search_request
                )

                for call in response.results:
                    call_dict = {"id": call.id}
                    if hasattr(call, 'properties') and call.properties:
                        for prop_name, prop_value in call.properties.items():
                            call_dict[prop_name] = prop_value
                    all_calls.append(call_dict)

                if limit and len(all_calls) >= limit:
                    all_calls = all_calls[:limit]
                    break

                if not hasattr(response, 'paging') or not response.paging:
                    break

                if hasattr(response.paging, 'next') and response.paging.next:
                    after = response.paging.next.after
                else:
                    break

        except Exception as e:
            logger.error(f"Error searching calls: {e}")
            raise Exception(f"Call search failed: {e}")

        logger.info(f"Found {len(all_calls)} calls matching filters")
        return all_calls

    def create_calls(self, calls_data: List[Dict[Text, Any]]) -> None:
        """Create calls"""
        hubspot = self.handler.connect()
        calls_to_create = [HubSpotObjectInputCreate(properties=call) for call in calls_data]
        try:
            created_calls = hubspot.crm.objects.batch_api.create(
                object_type="calls",
                batch_input_simple_public_object_input_for_create=HubSpotBatchObjectInputCreate(inputs=calls_to_create)
            )
            logger.info(f"Calls created with IDs {[call.id for call in created_calls.results]}")
        except Exception as e:
            raise Exception(f"Calls creation failed: {e}")

    def update_calls(self, call_ids: List[Text], values_to_update: Dict[Text, Any]) -> None:
        """Update calls"""
        hubspot = self.handler.connect()
        calls_to_update = [HubSpotObjectBatchInput(id=call_id, properties=values_to_update) for call_id in call_ids]
        try:
            updated_calls = hubspot.crm.objects.batch_api.update(
                object_type="calls",
                batch_input_simple_public_object_batch_input=HubSpotBatchObjectBatchInput(inputs=calls_to_update)
            )
            logger.info(f"Calls with IDs {[call.id for call in updated_calls.results]} updated")
        except Exception as e:
            raise Exception(f"Calls update failed: {e}")

    def delete_calls(self, call_ids: List[Text]) -> None:
        """Delete calls"""
        hubspot = self.handler.connect()
        calls_to_delete = [HubSpotObjectId(id=call_id) for call_id in call_ids]
        try:
            hubspot.crm.objects.batch_api.archive(
                object_type="calls",
                batch_input_simple_public_object_id=HubSpotBatchObjectIdInput(inputs=calls_to_delete)
            )
            logger.info("Calls deleted")
        except Exception as e:
            raise Exception(f"Calls deletion failed: {e}")
