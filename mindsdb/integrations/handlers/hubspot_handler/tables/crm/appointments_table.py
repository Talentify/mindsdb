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


class AppointmentsTable(HubSpotSearchMixin, APITable):
    """Hubspot Appointments table."""

    # Default essential properties to fetch
    DEFAULT_PROPERTIES = [
        'hs_timestamp', 'hs_meeting_title', 'hs_meeting_body', 'hs_meeting_start_time',
        'hs_meeting_end_time', 'hs_meeting_outcome', 'hubspot_owner_id',
        'createdate', 'hs_lastmodifieddate'
    ]

    def select(self, query: ast.Select) -> pd.DataFrame:
        """Pulls Hubspot Appointments data"""
        select_statement_parser = SELECTQueryParser(
            query,
            "appointments",
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
                df = pd.json_normalize(
                    self.search_objects(
                        filters=hubspot_filters,
                        properties=requested_properties,
                        limit=result_limit
                    )
                )
                where_conditions = []
            else:
                logger.info("No valid HubSpot filters, using get_all")
                df = pd.json_normalize(
                    self.get_objects(limit=result_limit, properties=requested_properties)
                )
        else:
            df = pd.json_normalize(
                self.get_objects(limit=result_limit, properties=requested_properties)
            )

        select_statement_executor = SELECTQueryExecutor(
            df,
            selected_columns,
            where_conditions,
            order_by_conditions
        )
        return select_statement_executor.execute_query()

    def insert(self, query: ast.Insert) -> None:
        """Inserts data into HubSpot Appointments"""
        try:
            properties_cache = self.handler.get_properties_cache('appointments')
            supported_columns = list(properties_cache['property_names'])
        except Exception as e:
            logger.warning(f"Failed to get dynamic columns for insert: {e}")
            supported_columns = ['hs_timestamp', 'hs_meeting_title', 'hs_meeting_start_time', 'hs_meeting_end_time']

        insert_statement_parser = INSERTQueryParser(
            query,
            supported_columns=supported_columns,
            mandatory_columns=[],
            all_mandatory=False,
        )
        data = insert_statement_parser.parse_query()
        self.create_objects(data)

    def update(self, query: ast.Update) -> None:
        """Updates HubSpot Appointments"""
        update_statement_parser = UPDATEQueryParser(query)
        values_to_update, where_conditions = update_statement_parser.parse_query()

        df = pd.json_normalize(self.get_objects())
        update_query_executor = UPDATEQueryExecutor(df, where_conditions)
        df = update_query_executor.execute_query()
        ids = df['id'].tolist()
        self.update_objects(ids, values_to_update)

    def delete(self, query: ast.Delete) -> None:
        """Deletes HubSpot Appointments"""
        delete_statement_parser = DELETEQueryParser(query)
        where_conditions = delete_statement_parser.parse_query()

        df = pd.json_normalize(self.get_objects())
        delete_query_executor = DELETEQueryExecutor(df, where_conditions)
        df = delete_query_executor.execute_query()
        ids = df['id'].tolist()
        self.delete_objects(ids)

    def get_columns(self) -> List[Text]:
        """Get column names for the table"""
        return ['id'] + self.DEFAULT_PROPERTIES

    def get_objects(self, properties: List[Text] = None, **kwargs) -> List[Dict]:
        """Fetch appointments with specified properties"""
        hubspot = self.handler.connect()

        if properties is None:
            properties_to_fetch = self.DEFAULT_PROPERTIES
        elif len(properties) == 0:
            properties_cache = self.handler.get_properties_cache('appointments')
            properties_to_fetch = list(properties_cache['property_names'])
        else:
            properties_to_fetch = properties

        kwargs['properties'] = properties_to_fetch
        objects = hubspot.crm.objects.basic_api.get_page(
            object_type="appointments",
            **kwargs
        )

        objects_dict = []
        for obj in objects.results:
            obj_dict = {"id": obj.id}
            if hasattr(obj, 'properties') and obj.properties:
                for prop_name, prop_value in obj.properties.items():
                    obj_dict[prop_name] = prop_value
            objects_dict.append(obj_dict)

        return objects_dict

    def search_objects(self, filters: List[Dict], properties: List[Text] = None, limit: int = None) -> List[Dict]:
        """Search appointments using HubSpot search API"""
        hubspot = self.handler.connect()

        if properties is None:
            properties_to_fetch = self.DEFAULT_PROPERTIES
        elif len(properties) == 0:
            properties_cache = self.handler.get_properties_cache('appointments')
            properties_to_fetch = list(properties_cache['property_names'])
        else:
            properties_to_fetch = properties

        search_request = {
            "filterGroups": [{"filters": filters}],
            "properties": properties_to_fetch,
            "limit": min(limit or 100, 100),
        }

        all_objects = []
        after = 0

        try:
            while True:
                if after > 0:
                    search_request["after"] = after

                response = hubspot.crm.objects.search_api.do_search(
                    object_type="appointments",
                    public_object_search_request=search_request
                )

                for obj in response.results:
                    obj_dict = {"id": obj.id}
                    if hasattr(obj, 'properties') and obj.properties:
                        for prop_name, prop_value in obj.properties.items():
                            obj_dict[prop_name] = prop_value
                    all_objects.append(obj_dict)

                if limit and len(all_objects) >= limit:
                    all_objects = all_objects[:limit]
                    break

                if not hasattr(response, 'paging') or not response.paging:
                    break

                if hasattr(response.paging, 'next') and response.paging.next:
                    after = response.paging.next.after
                else:
                    break

        except Exception as e:
            logger.error(f"Error searching appointments: {e}")
            raise Exception(f"Appointment search failed: {e}")

        logger.info(f"Found {len(all_objects)} appointments matching filters")
        return all_objects

    def create_objects(self, objects_data: List[Dict[Text, Any]]) -> None:
        """Create appointments"""
        hubspot = self.handler.connect()
        objects_to_create = [HubSpotObjectInputCreate(properties=obj) for obj in objects_data]
        try:
            created = hubspot.crm.objects.batch_api.create(
                object_type="appointments",
                batch_input_simple_public_object_input_for_create=HubSpotBatchObjectInputCreate(inputs=objects_to_create)
            )
            logger.info(f"Appointments created with IDs {[obj.id for obj in created.results]}")
        except Exception as e:
            raise Exception(f"Appointments creation failed: {e}")

    def update_objects(self, object_ids: List[Text], values_to_update: Dict[Text, Any]) -> None:
        """Update appointments"""
        hubspot = self.handler.connect()
        objects_to_update = [HubSpotObjectBatchInput(id=obj_id, properties=values_to_update) for obj_id in object_ids]
        try:
            updated = hubspot.crm.objects.batch_api.update(
                object_type="appointments",
                batch_input_simple_public_object_batch_input=HubSpotBatchObjectBatchInput(inputs=objects_to_update)
            )
            logger.info(f"Appointments with IDs {[obj.id for obj in updated.results]} updated")
        except Exception as e:
            raise Exception(f"Appointments update failed: {e}")

    def delete_objects(self, object_ids: List[Text]) -> None:
        """Delete appointments"""
        hubspot = self.handler.connect()
        objects_to_delete = [HubSpotObjectId(id=obj_id) for obj_id in object_ids]
        try:
            hubspot.crm.objects.batch_api.archive(
                object_type="appointments",
                batch_input_simple_public_object_id=HubSpotBatchObjectIdInput(inputs=objects_to_delete)
            )
            logger.info("Appointments deleted")
        except Exception as e:
            raise Exception(f"Appointments deletion failed: {e}")
