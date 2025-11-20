"""
HubSpot Pipeline Stages Table

This table provides access to individual pipeline stages by flattening the nested
stages array from the pipelines table into queryable rows.

Each stage includes its parent pipeline context (pipeline_id, object_type, etc.)
and object-type-specific metadata (probability for deals, ticket_state for tickets).

API Endpoint: /crm/v3/pipelines/{objectType} (via pipelines table)
Documentation: https://developers.hubspot.com/docs/api/crm/pipelines

Note: This is a READ-ONLY table. Pipeline stages are managed through HubSpot's pipeline settings.
"""

from typing import List, Dict, Text, Any
import pandas as pd
import json
from mindsdb_sql_parser import ast
from mindsdb.integrations.libs.api_handler import APITable
from mindsdb.integrations.utilities.handlers.query_utilities import SELECTQueryParser
from mindsdb.utilities import log
from mindsdb.integrations.handlers.hubspot_handler.tables.crm.base_hubspot_table import HubSpotSearchMixin

logger = log.getLogger(__name__)


class PipelineStagesTable(HubSpotSearchMixin, APITable):
    """
    HubSpot Pipeline Stages table - expands nested stages from pipelines.

    This table flattens the stages array from pipelines into individual rows,
    making it easier to query, filter, and join with deals/tickets.

    Example queries:
        -- Get all deal stages
        SELECT * FROM hubspot.pipeline_stages WHERE object_type = 'deals'

        -- Find closed/won stages
        SELECT pipeline_label, stage_label, probability
        FROM hubspot.pipeline_stages
        WHERE is_closed = true

        -- Get stages for specific pipeline
        SELECT * FROM hubspot.pipeline_stages
        WHERE pipeline_id = '123456'
        ORDER BY display_order
    """

    def select(self, query: ast.Select) -> pd.DataFrame:
        """
        Get pipeline stages from HubSpot.

        Supports:
        - Column selection (SELECT specific columns)
        - Filtering (WHERE object_type = 'deals', pipeline_id = '...')
        - Ordering (ORDER BY display_order)
        - Limit (LIMIT 100)

        Parameters
        ----------
        query : ast.Select
            SQL SELECT query

        Returns
        -------
        pd.DataFrame
            Pipeline stages data
        """
        # Use SELECTQueryParser to properly parse the query
        select_statement_parser = SELECTQueryParser(
            query,
            "pipeline_stages",
            self.get_columns()
        )
        selected_columns, where_conditions, order_by_conditions, result_limit = select_statement_parser.parse_query()

        # Extract filter parameters from WHERE clause
        object_type = None
        pipeline_id = None

        for condition in where_conditions:
            if len(condition) < 3:
                continue

            op, column, value = condition[0], condition[1], condition[2]

            if column == 'object_type' and op == '=':
                object_type = value
            elif column == 'pipeline_id' and op == '=':
                pipeline_id = value

        # Fetch pipeline stages (flattened from pipelines)
        stages = self.get_pipeline_stages(object_type=object_type, pipeline_id=pipeline_id)

        # Convert to DataFrame
        if not stages:
            logger.info("No pipeline stages found")
            return pd.DataFrame()

        stages_df = pd.DataFrame(stages)

        # Apply additional WHERE conditions (local filtering)
        if where_conditions and not stages_df.empty:
            stages_df = self._apply_conditions(stages_df, where_conditions)

        # Apply column selection
        if selected_columns and not stages_df.empty:
            # Filter to only available columns
            available_columns = [col for col in selected_columns if col in stages_df.columns]
            if len(available_columns) < len(selected_columns):
                missing = set(selected_columns) - set(available_columns)
                logger.warning(f"Some requested columns not available in pipeline stages data: {missing}")
            if available_columns:
                stages_df = stages_df[available_columns]

        # Apply limit
        if result_limit and not stages_df.empty:
            stages_df = stages_df.head(result_limit)

        logger.info(f"Returning {len(stages_df)} pipeline stages")
        return stages_df

    def get_pipeline_stages(
        self,
        object_type: str = None,
        pipeline_id: str = None
    ) -> List[Dict]:
        """
        Get pipeline stages by flattening stages from pipelines.

        Parameters
        ----------
        object_type : str, optional
            Filter by object type ('deals' or 'tickets')
        pipeline_id : str, optional
            Filter by specific pipeline ID

        Returns
        -------
        List[Dict]
            List of stage dictionaries with parent pipeline context
        """
        # Import PipelinesTable to reuse its get_pipelines method
        from mindsdb.integrations.handlers.hubspot_handler.tables.crm.pipelines_table import PipelinesTable

        pipelines_table = PipelinesTable(self.handler)
        all_stages = []

        # Determine which object types to query
        if object_type:
            object_types = [object_type]
        else:
            object_types = ['deals', 'tickets']

        for obj_type in object_types:
            try:
                # Get pipelines for this object type
                pipelines = pipelines_table.get_pipelines(object_type=obj_type)

                # Flatten stages from each pipeline
                for pipeline in pipelines:
                    # Skip if filtering by pipeline_id and this isn't it
                    if pipeline_id and pipeline.get('id') != pipeline_id:
                        continue

                    # Parse stages from JSON string
                    stages_json = pipeline.get('stages', '[]')
                    if isinstance(stages_json, str):
                        try:
                            stages_list = json.loads(stages_json)
                        except json.JSONDecodeError:
                            logger.warning(f"Failed to parse stages JSON for pipeline {pipeline.get('id')}")
                            stages_list = []
                    else:
                        stages_list = stages_json if isinstance(stages_json, list) else []

                    # Create a row for each stage
                    for stage in stages_list:
                        stage_dict = {
                            'stage_id': stage.get('id'),
                            'stage_label': stage.get('label'),
                            'display_order': stage.get('displayOrder'),
                            'pipeline_id': pipeline.get('id'),
                            'pipeline_label': pipeline.get('label'),
                            'object_type': obj_type,
                            'archived': pipeline.get('archived', False),
                        }

                        # Add metadata fields (object-type specific)
                        metadata = stage.get('metadata', {})
                        if obj_type == 'deals':
                            stage_dict['probability'] = metadata.get('probability')
                            stage_dict['is_closed'] = metadata.get('isClosed')
                            stage_dict['ticket_state'] = None
                        elif obj_type == 'tickets':
                            stage_dict['probability'] = None
                            stage_dict['is_closed'] = None
                            stage_dict['ticket_state'] = metadata.get('ticketState')
                        else:
                            stage_dict['probability'] = None
                            stage_dict['is_closed'] = None
                            stage_dict['ticket_state'] = None

                        all_stages.append(stage_dict)

            except Exception as e:
                logger.error(f"Error fetching stages for {obj_type}: {e}")
                continue

        logger.info(f"Retrieved {len(all_stages)} pipeline stages")
        return all_stages

    def get_columns(self) -> List[Text]:
        """
        Get list of available columns.

        Returns
        -------
        List[Text]
            Column names
        """
        return [
            'stage_id',
            'stage_label',
            'display_order',
            'pipeline_id',
            'pipeline_label',
            'object_type',
            'archived',
            'probability',
            'is_closed',
            'ticket_state',
        ]

    def insert(self, query: ast.Insert) -> None:
        """Not supported - pipeline stages are read-only."""
        raise NotImplementedError("INSERT not supported for pipeline_stages table. "
                                "Manage stages through HubSpot's pipeline settings.")

    def update(self, query: ast.Update) -> None:
        """Not supported - pipeline stages are read-only."""
        raise NotImplementedError("UPDATE not supported for pipeline_stages table. "
                                "Manage stages through HubSpot's pipeline settings.")

    def delete(self, query: ast.Delete) -> None:
        """Not supported - pipeline stages are read-only."""
        raise NotImplementedError("DELETE not supported for pipeline_stages table. "
                                "Manage stages through HubSpot's pipeline settings.")

    @staticmethod
    def _apply_conditions(df: pd.DataFrame, conditions: List[List]) -> pd.DataFrame:
        """Apply WHERE conditions to DataFrame (local filtering)"""
        if df.empty:
            return df

        for condition in conditions:
            if len(condition) < 3:
                continue

            op, column, value = condition[0], condition[1], condition[2]

            if column not in df.columns:
                logger.debug(f"Column '{column}' not found in pipeline stages data, skipping condition")
                continue

            # Apply filter based on operator
            if op == '=':
                df = df[df[column] == value]
            elif op == '!=':
                df = df[df[column] != value]
            elif op == '>':
                df = df[df[column] > value]
            elif op == '>=':
                df = df[df[column] >= value]
            elif op == '<':
                df = df[df[column] < value]
            elif op == '<=':
                df = df[df[column] <= value]
            elif op == 'in':
                if isinstance(value, list):
                    df = df[df[column].isin(value)]
            elif op == 'like':
                # Convert SQL LIKE to pandas regex
                pattern = value.replace('%', '.*').replace('_', '.')
                df = df[df[column].str.match(pattern, case=False, na=False)]

        return df
