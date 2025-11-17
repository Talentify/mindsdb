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


class TasksTable(HubSpotSearchMixin, APITable):
    """Hubspot Tasks table (Activity)."""

    DEFAULT_PROPERTIES = [
        'hs_timestamp', 'hs_task_subject', 'hs_task_body', 'hs_task_status',
        'hs_task_priority', 'hs_task_type', 'hubspot_owner_id',
        'createdate', 'hs_lastmodifieddate'
    ]

    def select(self, query: ast.Select) -> pd.DataFrame:
        """Pulls Hubspot Tasks data"""
        select_statement_parser = SELECTQueryParser(
            query,
            "tasks",
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
                tasks_df = pd.json_normalize(
                    self.search_tasks(
                        filters=hubspot_filters,
                        properties=requested_properties,
                        limit=result_limit
                    )
                )
                where_conditions = []
            else:
                logger.info("No valid HubSpot filters, using get_all")
                tasks_df = pd.json_normalize(
                    self.get_tasks(limit=result_limit, properties=requested_properties)
                )
        else:
            tasks_df = pd.json_normalize(
                self.get_tasks(limit=result_limit, properties=requested_properties)
            )

        # Filter selected_columns to only include columns that actually exist in the dataframe
        # This handles cases where requested properties don't exist in HubSpot
        if not tasks_df.empty and selected_columns:
            available_columns = [col for col in selected_columns if col in tasks_df.columns]
            if len(available_columns) < len(selected_columns):
                missing = set(selected_columns) - set(available_columns)
                logger.warning(f"Some requested columns not available in tasks data: {missing}")
            selected_columns = available_columns

        select_statement_executor = SELECTQueryExecutor(
            tasks_df,
            selected_columns,
            where_conditions,
            order_by_conditions
        )
        return select_statement_executor.execute_query()

    def insert(self, query: ast.Insert) -> None:
        """Inserts data into HubSpot Tasks"""
        try:
            properties_cache = self.handler.get_properties_cache('tasks')
            supported_columns = list(properties_cache['property_names'])
        except Exception as e:
            logger.warning(f"Failed to get dynamic columns for insert: {e}")
            supported_columns = ['hs_timestamp', 'hs_task_subject', 'hs_task_body', 'hs_task_status']

        insert_statement_parser = INSERTQueryParser(
            query,
            supported_columns=supported_columns,
            mandatory_columns=['hs_timestamp', 'hs_task_subject'],
            all_mandatory=False,
        )
        tasks_data = insert_statement_parser.parse_query()
        self.create_tasks(tasks_data)

    def update(self, query: ast.Update) -> None:
        """Updates HubSpot Tasks"""
        update_statement_parser = UPDATEQueryParser(query)
        values_to_update, where_conditions = update_statement_parser.parse_query()

        tasks_df = pd.json_normalize(self.get_tasks())
        update_query_executor = UPDATEQueryExecutor(tasks_df, where_conditions)
        tasks_df = update_query_executor.execute_query()
        task_ids = tasks_df['id'].tolist()
        self.update_tasks(task_ids, values_to_update)

    def delete(self, query: ast.Delete) -> None:
        """Deletes HubSpot Tasks"""
        delete_statement_parser = DELETEQueryParser(query)
        where_conditions = delete_statement_parser.parse_query()

        tasks_df = pd.json_normalize(self.get_tasks())
        delete_query_executor = DELETEQueryExecutor(tasks_df, where_conditions)
        tasks_df = delete_query_executor.execute_query()
        task_ids = tasks_df['id'].tolist()
        self.delete_tasks(task_ids)

    def get_columns(self) -> List[Text]:
        """Get column names for the table"""
        return ['id'] + self.DEFAULT_PROPERTIES

    def get_tasks(self, properties: List[Text] = None, **kwargs) -> List[Dict]:
        """Fetch tasks with specified properties"""
        hubspot = self.handler.connect()

        if properties is None:
            properties_to_fetch = self.DEFAULT_PROPERTIES
        elif len(properties) == 0:
            properties_cache = self.handler.get_properties_cache('tasks')
            properties_to_fetch = list(properties_cache['property_names'])
        else:
            properties_to_fetch = properties

        kwargs['properties'] = properties_to_fetch
        response = hubspot.crm.objects.basic_api.get_page(object_type="tasks", **kwargs)

        tasks_dict = []
        for task in response.results:
            task_dict = {"id": task.id}
            if hasattr(task, 'properties') and task.properties:
                for prop_name, prop_value in task.properties.items():
                    task_dict[prop_name] = prop_value
            tasks_dict.append(task_dict)

        return tasks_dict

    def search_tasks(self, filters: List[Dict], properties: List[Text] = None, limit: int = None) -> List[Dict]:
        """Search tasks using HubSpot search API"""
        hubspot = self.handler.connect()

        if properties is None:
            properties_to_fetch = self.DEFAULT_PROPERTIES
        elif len(properties) == 0:
            properties_cache = self.handler.get_properties_cache('tasks')
            properties_to_fetch = list(properties_cache['property_names'])
        else:
            properties_to_fetch = properties

        search_request = {
            "filterGroups": [{"filters": filters}],
            "properties": properties_to_fetch,
            "limit": min(limit or 100, 100),
        }

        all_tasks = []
        after = 0

        try:
            while True:
                if after > 0:
                    search_request["after"] = after

                response = hubspot.crm.objects.search_api.do_search(object_type="tasks", 
                    public_object_search_request=search_request
                )

                for task in response.results:
                    task_dict = {"id": task.id}
                    if hasattr(task, 'properties') and task.properties:
                        for prop_name, prop_value in task.properties.items():
                            task_dict[prop_name] = prop_value
                    all_tasks.append(task_dict)

                if limit and len(all_tasks) >= limit:
                    all_tasks = all_tasks[:limit]
                    break

                if not hasattr(response, 'paging') or not response.paging:
                    break

                if hasattr(response.paging, 'next') and response.paging.next:
                    after = response.paging.next.after
                else:
                    break

        except Exception as e:
            logger.error(f"Error searching tasks: {e}")
            raise Exception(f"Task search failed: {e}")

        logger.info(f"Found {len(all_tasks)} tasks matching filters")
        return all_tasks

    def create_tasks(self, tasks_data: List[Dict[Text, Any]]) -> None:
        """Create tasks"""
        hubspot = self.handler.connect()
        tasks_to_create = [HubSpotObjectInputCreate(properties=task) for task in tasks_data]
        try:
            created_tasks = hubspot.crm.objects.batch_api.create(object_type="tasks", 
                batch_input_simple_public_object_input_for_create=HubSpotBatchObjectInputCreate(inputs=tasks_to_create)
            )
            logger.info(f"Tasks created with IDs {[task.id for task in created_tasks.results]}")
        except Exception as e:
            raise Exception(f"Tasks creation failed: {e}")

    def update_tasks(self, task_ids: List[Text], values_to_update: Dict[Text, Any]) -> None:
        """Update tasks"""
        hubspot = self.handler.connect()
        tasks_to_update = [HubSpotObjectBatchInput(id=task_id, properties=values_to_update) for task_id in task_ids]
        try:
            updated_tasks = hubspot.crm.objects.batch_api.update(object_type="tasks", 
                batch_input_simple_public_object_batch_input=HubSpotBatchObjectBatchInput(inputs=tasks_to_update)
            )
            logger.info(f"Tasks with IDs {[task.id for task in updated_tasks.results]} updated")
        except Exception as e:
            raise Exception(f"Tasks update failed: {e}")

    def delete_tasks(self, task_ids: List[Text]) -> None:
        """Delete tasks"""
        hubspot = self.handler.connect()
        tasks_to_delete = [HubSpotObjectId(id=task_id) for task_id in task_ids]
        try:
            hubspot.crm.objects.batch_api.archive(object_type="tasks", 
                batch_input_simple_public_object_id=HubSpotBatchObjectIdInput(inputs=tasks_to_delete)
            )
            logger.info("Tasks deleted")
        except Exception as e:
            raise Exception(f"Tasks deletion failed: {e}")
