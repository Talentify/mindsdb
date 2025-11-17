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


class MeetingsTable(HubSpotSearchMixin, APITable):
    """Hubspot Meetings table (Activity)."""

    DEFAULT_PROPERTIES = [
        'hs_timestamp', 'hs_meeting_title', 'hs_meeting_body', 'hs_meeting_start_time',
        'hs_meeting_end_time', 'hs_meeting_outcome', 'hs_meeting_location',
        'hubspot_owner_id', 'createdate', 'hs_lastmodifieddate'
    ]

    def select(self, query: ast.Select) -> pd.DataFrame:
        """Pulls Hubspot Meetings data"""
        select_statement_parser = SELECTQueryParser(
            query,
            "meetings",
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
                meetings_df = pd.json_normalize(
                    self.search_meetings(
                        filters=hubspot_filters,
                        properties=requested_properties,
                        limit=result_limit
                    )
                )
                where_conditions = []
            else:
                logger.info("No valid HubSpot filters, using get_all")
                meetings_df = pd.json_normalize(
                    self.get_meetings(limit=result_limit, properties=requested_properties)
                )
        else:
            meetings_df = pd.json_normalize(
                self.get_meetings(limit=result_limit, properties=requested_properties)
            )

        # Filter selected_columns to only include columns that actually exist in the dataframe
        # This handles cases where requested properties don't exist in HubSpot
        if not meetings_df.empty and selected_columns:
            available_columns = [col for col in selected_columns if col in meetings_df.columns]
            if len(available_columns) < len(selected_columns):
                missing = set(selected_columns) - set(available_columns)
                logger.warning(f"Some requested columns not available in meetings data: {missing}")
            selected_columns = available_columns if available_columns else None

        select_statement_executor = SELECTQueryExecutor(
            meetings_df,
            selected_columns,
            where_conditions,
            order_by_conditions
        )
        return select_statement_executor.execute_query()

    def insert(self, query: ast.Insert) -> None:
        """Inserts data into HubSpot Meetings"""
        try:
            properties_cache = self.handler.get_properties_cache('meetings')
            supported_columns = list(properties_cache['property_names'])
        except Exception as e:
            logger.warning(f"Failed to get dynamic columns for insert: {e}")
            supported_columns = ['hs_timestamp', 'hs_meeting_title', 'hs_meeting_start_time', 'hs_meeting_end_time']

        insert_statement_parser = INSERTQueryParser(
            query,
            supported_columns=supported_columns,
            mandatory_columns=['hs_timestamp'],
            all_mandatory=False,
        )
        meetings_data = insert_statement_parser.parse_query()
        self.create_meetings(meetings_data)

    def update(self, query: ast.Update) -> None:
        """Updates HubSpot Meetings"""
        update_statement_parser = UPDATEQueryParser(query)
        values_to_update, where_conditions = update_statement_parser.parse_query()

        meetings_df = pd.json_normalize(self.get_meetings())
        update_query_executor = UPDATEQueryExecutor(meetings_df, where_conditions)
        meetings_df = update_query_executor.execute_query()
        meeting_ids = meetings_df['id'].tolist()
        self.update_meetings(meeting_ids, values_to_update)

    def delete(self, query: ast.Delete) -> None:
        """Deletes HubSpot Meetings"""
        delete_statement_parser = DELETEQueryParser(query)
        where_conditions = delete_statement_parser.parse_query()

        meetings_df = pd.json_normalize(self.get_meetings())
        delete_query_executor = DELETEQueryExecutor(meetings_df, where_conditions)
        meetings_df = delete_query_executor.execute_query()
        meeting_ids = meetings_df['id'].tolist()
        self.delete_meetings(meeting_ids)

    def get_columns(self) -> List[Text]:
        """Get column names for the table"""
        return ['id'] + self.DEFAULT_PROPERTIES

    def get_meetings(self, properties: List[Text] = None, **kwargs) -> List[Dict]:
        """Fetch meetings with specified properties"""
        hubspot = self.handler.connect()

        if properties is None:
            properties_to_fetch = self.DEFAULT_PROPERTIES
        elif len(properties) == 0:
            properties_cache = self.handler.get_properties_cache('meetings')
            properties_to_fetch = list(properties_cache['property_names'])
        else:
            properties_to_fetch = properties

        kwargs['properties'] = properties_to_fetch
        response = hubspot.crm.objects.basic_api.get_page(object_type="meetings", **kwargs)

        meetings_dict = []
        for meeting in response.results:
            meeting_dict = {"id": meeting.id}
            if hasattr(meeting, 'properties') and meeting.properties:
                for prop_name, prop_value in meeting.properties.items():
                    meeting_dict[prop_name] = prop_value
            meetings_dict.append(meeting_dict)

        return meetings_dict

    def search_meetings(self, filters: List[Dict], properties: List[Text] = None, limit: int = None) -> List[Dict]:
        """Search meetings using HubSpot search API"""
        hubspot = self.handler.connect()

        if properties is None:
            properties_to_fetch = self.DEFAULT_PROPERTIES
        elif len(properties) == 0:
            properties_cache = self.handler.get_properties_cache('meetings')
            properties_to_fetch = list(properties_cache['property_names'])
        else:
            properties_to_fetch = properties

        search_request = {
            "filterGroups": [{"filters": filters}],
            "properties": properties_to_fetch,
            "limit": min(limit or 100, 100),
        }

        all_meetings = []
        after = 0

        try:
            while True:
                if after > 0:
                    search_request["after"] = after

                response = hubspot.crm.objects.search_api.do_search(object_type="meetings", 
                    public_object_search_request=search_request
                )

                for meeting in response.results:
                    meeting_dict = {"id": meeting.id}
                    if hasattr(meeting, 'properties') and meeting.properties:
                        for prop_name, prop_value in meeting.properties.items():
                            meeting_dict[prop_name] = prop_value
                    all_meetings.append(meeting_dict)

                if limit and len(all_meetings) >= limit:
                    all_meetings = all_meetings[:limit]
                    break

                if not hasattr(response, 'paging') or not response.paging:
                    break

                if hasattr(response.paging, 'next') and response.paging.next:
                    after = response.paging.next.after
                else:
                    break

        except Exception as e:
            logger.error(f"Error searching meetings: {e}")
            raise Exception(f"Meeting search failed: {e}")

        logger.info(f"Found {len(all_meetings)} meetings matching filters")
        return all_meetings

    def create_meetings(self, meetings_data: List[Dict[Text, Any]]) -> None:
        """Create meetings"""
        hubspot = self.handler.connect()
        meetings_to_create = [HubSpotObjectInputCreate(properties=meeting) for meeting in meetings_data]
        try:
            created_meetings = hubspot.crm.objects.batch_api.create(object_type="meetings", 
                batch_input_simple_public_object_input_for_create=HubSpotBatchObjectInputCreate(inputs=meetings_to_create)
            )
            logger.info(f"Meetings created with IDs {[meeting.id for meeting in created_meetings.results]}")
        except Exception as e:
            raise Exception(f"Meetings creation failed: {e}")

    def update_meetings(self, meeting_ids: List[Text], values_to_update: Dict[Text, Any]) -> None:
        """Update meetings"""
        hubspot = self.handler.connect()
        meetings_to_update = [HubSpotObjectBatchInput(id=meeting_id, properties=values_to_update) for meeting_id in meeting_ids]
        try:
            updated_meetings = hubspot.crm.objects.batch_api.update(object_type="meetings", 
                batch_input_simple_public_object_batch_input=HubSpotBatchObjectBatchInput(inputs=meetings_to_update)
            )
            logger.info(f"Meetings with IDs {[meeting.id for meeting in updated_meetings.results]} updated")
        except Exception as e:
            raise Exception(f"Meetings update failed: {e}")

    def delete_meetings(self, meeting_ids: List[Text]) -> None:
        """Delete meetings"""
        hubspot = self.handler.connect()
        meetings_to_delete = [HubSpotObjectId(id=meeting_id) for meeting_id in meeting_ids]
        try:
            hubspot.crm.objects.batch_api.archive(object_type="meetings", 
                batch_input_simple_public_object_id=HubSpotBatchObjectIdInput(inputs=meetings_to_delete)
            )
            logger.info("Meetings deleted")
        except Exception as e:
            raise Exception(f"Meetings deletion failed: {e}")
