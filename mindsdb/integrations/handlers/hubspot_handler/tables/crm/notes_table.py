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


class NotesTable(HubSpotSearchMixin, APITable):
    """Hubspot Notes table (Activity)."""

    DEFAULT_PROPERTIES = [
        'hs_timestamp', 'hs_note_body', 'hubspot_owner_id',
        'createdate', 'hs_lastmodifieddate'
    ]

    def select(self, query: ast.Select) -> pd.DataFrame:
        """Pulls Hubspot Notes data"""
        select_statement_parser = SELECTQueryParser(
            query,
            "notes",
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
                notes_df = pd.json_normalize(
                    self.search_notes(
                        filters=hubspot_filters,
                        properties=requested_properties,
                        limit=result_limit
                    )
                )
                where_conditions = []
            else:
                logger.info("No valid HubSpot filters, using get_all")
                notes_df = pd.json_normalize(
                    self.get_notes(limit=result_limit, properties=requested_properties)
                )
        else:
            notes_df = pd.json_normalize(
                self.get_notes(limit=result_limit, properties=requested_properties)
            )

        # Filter selected_columns to only include columns that actually exist in the dataframe
        # This handles cases where requested properties don't exist in HubSpot
        if not notes_df.empty and selected_columns:
            available_columns = [col for col in selected_columns if col in notes_df.columns]
            if len(available_columns) < len(selected_columns):
                missing = set(selected_columns) - set(available_columns)
                logger.warning(f"Some requested columns not available in notes data: {missing}")
            selected_columns = available_columns if available_columns else None

        select_statement_executor = SELECTQueryExecutor(
            notes_df,
            selected_columns,
            where_conditions,
            order_by_conditions
        )
        return select_statement_executor.execute_query()

    def insert(self, query: ast.Insert) -> None:
        """Inserts data into HubSpot Notes"""
        try:
            properties_cache = self.handler.get_properties_cache('notes')
            supported_columns = list(properties_cache['property_names'])
        except Exception as e:
            logger.warning(f"Failed to get dynamic columns for insert: {e}")
            supported_columns = ['hs_timestamp', 'hs_note_body']

        insert_statement_parser = INSERTQueryParser(
            query,
            supported_columns=supported_columns,
            mandatory_columns=['hs_timestamp', 'hs_note_body'],
            all_mandatory=False,
        )
        notes_data = insert_statement_parser.parse_query()
        self.create_notes(notes_data)

    def update(self, query: ast.Update) -> None:
        """Updates HubSpot Notes"""
        update_statement_parser = UPDATEQueryParser(query)
        values_to_update, where_conditions = update_statement_parser.parse_query()

        notes_df = pd.json_normalize(self.get_notes())
        update_query_executor = UPDATEQueryExecutor(notes_df, where_conditions)
        notes_df = update_query_executor.execute_query()
        note_ids = notes_df['id'].tolist()
        self.update_notes(note_ids, values_to_update)

    def delete(self, query: ast.Delete) -> None:
        """Deletes HubSpot Notes"""
        delete_statement_parser = DELETEQueryParser(query)
        where_conditions = delete_statement_parser.parse_query()

        notes_df = pd.json_normalize(self.get_notes())
        delete_query_executor = DELETEQueryExecutor(notes_df, where_conditions)
        notes_df = delete_query_executor.execute_query()
        note_ids = notes_df['id'].tolist()
        self.delete_notes(note_ids)

    def get_columns(self) -> List[Text]:
        """Get column names for the table"""
        return ['id'] + self.DEFAULT_PROPERTIES

    def get_notes(self, properties: List[Text] = None, **kwargs) -> List[Dict]:
        """Fetch notes with specified properties"""
        hubspot = self.handler.connect()

        if properties is None:
            properties_to_fetch = self.DEFAULT_PROPERTIES
        elif len(properties) == 0:
            properties_cache = self.handler.get_properties_cache('notes')
            properties_to_fetch = list(properties_cache['property_names'])
        else:
            properties_to_fetch = properties

        kwargs['properties'] = properties_to_fetch
        response = hubspot.crm.objects.basic_api.get_page(object_type="notes", **kwargs)

        notes_dict = []
        for note in response.results:
            note_dict = {"id": note.id}
            if hasattr(note, 'properties') and note.properties:
                for prop_name, prop_value in note.properties.items():
                    note_dict[prop_name] = prop_value
            notes_dict.append(note_dict)

        return notes_dict

    def search_notes(self, filters: List[Dict], properties: List[Text] = None, limit: int = None) -> List[Dict]:
        """Search notes using HubSpot search API"""
        hubspot = self.handler.connect()

        if properties is None:
            properties_to_fetch = self.DEFAULT_PROPERTIES
        elif len(properties) == 0:
            properties_cache = self.handler.get_properties_cache('notes')
            properties_to_fetch = list(properties_cache['property_names'])
        else:
            properties_to_fetch = properties

        search_request = {
            "filterGroups": [{"filters": filters}],
            "properties": properties_to_fetch,
            "limit": min(limit or 100, 100),
        }

        all_notes = []
        after = 0

        try:
            while True:
                if after > 0:
                    search_request["after"] = after

                response = hubspot.crm.objects.search_api.do_search(object_type="notes", 
                    public_object_search_request=search_request
                )

                for note in response.results:
                    note_dict = {"id": note.id}
                    if hasattr(note, 'properties') and note.properties:
                        for prop_name, prop_value in note.properties.items():
                            note_dict[prop_name] = prop_value
                    all_notes.append(note_dict)

                if limit and len(all_notes) >= limit:
                    all_notes = all_notes[:limit]
                    break

                if not hasattr(response, 'paging') or not response.paging:
                    break

                if hasattr(response.paging, 'next') and response.paging.next:
                    after = response.paging.next.after
                else:
                    break

        except Exception as e:
            logger.error(f"Error searching notes: {e}")
            raise Exception(f"Note search failed: {e}")

        logger.info(f"Found {len(all_notes)} notes matching filters")
        return all_notes

    def create_notes(self, notes_data: List[Dict[Text, Any]]) -> None:
        """Create notes"""
        hubspot = self.handler.connect()
        notes_to_create = [HubSpotObjectInputCreate(properties=note) for note in notes_data]
        try:
            created_notes = hubspot.crm.objects.batch_api.create(object_type="notes", 
                batch_input_simple_public_object_input_for_create=HubSpotBatchObjectInputCreate(inputs=notes_to_create)
            )
            logger.info(f"Notes created with IDs {[note.id for note in created_notes.results]}")
        except Exception as e:
            raise Exception(f"Notes creation failed: {e}")

    def update_notes(self, note_ids: List[Text], values_to_update: Dict[Text, Any]) -> None:
        """Update notes"""
        hubspot = self.handler.connect()
        notes_to_update = [HubSpotObjectBatchInput(id=note_id, properties=values_to_update) for note_id in note_ids]
        try:
            updated_notes = hubspot.crm.objects.batch_api.update(object_type="notes", 
                batch_input_simple_public_object_batch_input=HubSpotBatchObjectBatchInput(inputs=notes_to_update)
            )
            logger.info(f"Notes with IDs {[note.id for note in updated_notes.results]} updated")
        except Exception as e:
            raise Exception(f"Notes update failed: {e}")

    def delete_notes(self, note_ids: List[Text]) -> None:
        """Delete notes"""
        hubspot = self.handler.connect()
        notes_to_delete = [HubSpotObjectId(id=note_id) for note_id in note_ids]
        try:
            hubspot.crm.objects.batch_api.archive(object_type="notes", 
                batch_input_simple_public_object_id=HubSpotBatchObjectIdInput(inputs=notes_to_delete)
            )
            logger.info("Notes deleted")
        except Exception as e:
            raise Exception(f"Notes deletion failed: {e}")
