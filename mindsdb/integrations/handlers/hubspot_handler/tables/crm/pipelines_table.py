"""
HubSpot Pipelines Table

This table provides access to HubSpot pipelines and their stages for deals, tickets, and other objects.
Pipelines define the workflow stages that records progress through (e.g., sales pipeline stages).

API Endpoint: /crm/v3/pipelines/{objectType}
Documentation: https://developers.hubspot.com/docs/api/crm/pipelines

Note: This is a READ-ONLY table. Pipelines are managed through HubSpot's pipeline settings.
"""

from typing import List, Dict, Text, Any
import pandas as pd
import json
from mindsdb.integrations.libs.api_handler import APITable
from mindsdb.integrations.handlers.hubspot_handler.tables.crm.base_hubspot_table import HubSpotSearchMixin
from mindsdb.utilities import log
from mindsdb_sql.parser import ast

logger = log.getLogger(__name__)


class PipelinesTable(HubSpotSearchMixin, APITable):
    """
    HubSpot Pipelines table for querying pipeline definitions and stages.

    Pipelines organize records into workflow stages. This is critical for understanding
    deal stages, ticket statuses, and other object progressions.

    Example queries:
        SELECT * FROM hubspot.pipelines WHERE object_type = 'deals'
        SELECT * FROM hubspot.pipelines WHERE archived = false
        SELECT id, label, stages FROM hubspot.pipelines WHERE object_type = 'tickets'
    """

    # Supported object types that have pipelines
    SUPPORTED_OBJECT_TYPES = ['deals', 'tickets']

    def select(self, query: ast.Select) -> pd.DataFrame:
        """
        Get pipelines from HubSpot.

        Supports:
        - Column selection (SELECT specific columns)
        - Filtering (WHERE object_type = 'deals', archived = false)
        - Limit (LIMIT 100)

        Parameters
        ----------
        query : ast.Select
            SQL SELECT query

        Returns
        -------
        pd.DataFrame
            Pipelines data
        """
        # Parse query conditions
        conditions = self._extract_where_conditions(query.where) if query.where else []
        limit = query.limit.value if query.limit else None

        # Get requested columns
        selected_columns = []
        if query.targets:
            for target in query.targets:
                if isinstance(target, ast.Star):
                    selected_columns = None  # SELECT * - get all columns
                    break
                elif isinstance(target, ast.Identifier):
                    selected_columns.append(target.parts[-1])

        # Check if object_type is specified in WHERE clause
        object_types_to_fetch = []
        for condition in conditions:
            if len(condition) >= 3 and condition[1] == 'object_type':
                op, _, value = condition[0], condition[1], condition[2]
                if op == '=':
                    object_types_to_fetch = [value]
                elif op == 'in':
                    object_types_to_fetch = value if isinstance(value, list) else [value]

        # If no object_type specified, fetch all supported types
        if not object_types_to_fetch:
            object_types_to_fetch = self.SUPPORTED_OBJECT_TYPES

        # Fetch pipelines for each object type
        all_pipelines = []
        for object_type in object_types_to_fetch:
            if object_type not in self.SUPPORTED_OBJECT_TYPES:
                logger.warning(f"Object type '{object_type}' does not support pipelines, skipping")
                continue

            pipelines = self.get_pipelines(object_type)
            all_pipelines.extend(pipelines)

        # Convert to DataFrame
        if not all_pipelines:
            logger.info("No pipelines found")
            return pd.DataFrame()

        pipelines_df = pd.DataFrame(all_pipelines)

        # Apply WHERE conditions (local filtering)
        if conditions and not pipelines_df.empty:
            pipelines_df = self._apply_conditions(pipelines_df, conditions)

        # Apply column selection
        if selected_columns and not pipelines_df.empty:
            # Filter to only available columns
            available_columns = [col for col in selected_columns if col in pipelines_df.columns]
            if len(available_columns) < len(selected_columns):
                missing = set(selected_columns) - set(available_columns)
                logger.warning(f"Some requested columns not available in pipelines data: {missing}")
            if available_columns:
                pipelines_df = pipelines_df[available_columns]

        # Apply limit
        if limit and not pipelines_df.empty:
            pipelines_df = pipelines_df.head(limit)

        logger.info(f"Returning {len(pipelines_df)} pipelines")
        return pipelines_df

    def get_pipelines(self, object_type: str) -> List[Dict]:
        """
        Get all pipelines for a specific object type from HubSpot.

        Parameters
        ----------
        object_type : str
            Object type to get pipelines for ('deals', 'tickets', etc.)

        Returns
        -------
        List[Dict]
            List of pipeline dictionaries with fields:
            - id: Pipeline ID (string)
            - object_type: Object type this pipeline applies to
            - label: Pipeline name/label
            - displayOrder: Display order (integer)
            - archived: Whether pipeline is archived
            - stages: List of stage dictionaries (id, label, displayOrder, metadata)
        """
        if object_type not in self.SUPPORTED_OBJECT_TYPES:
            logger.warning(f"Object type '{object_type}' does not support pipelines")
            return []

        hubspot = self.handler.connect()

        try:
            # Get pipelines with retry logic
            response = self._execute_with_retry(
                lambda: hubspot.crm.pipelines.pipelines_api.get_all(object_type=object_type),
                f"get_pipelines_{object_type}"
            )

            pipelines = []
            for pipeline in response.results:
                pipeline_dict = {
                    'id': pipeline.id,
                    'object_type': object_type,
                    'label': pipeline.label,
                    'displayOrder': pipeline.display_order if hasattr(pipeline, 'display_order') else None,
                    'archived': pipeline.archived if hasattr(pipeline, 'archived') else False,
                }

                # Add stage information
                if hasattr(pipeline, 'stages') and pipeline.stages:
                    stages = []
                    for stage in pipeline.stages:
                        stage_dict = {
                            'id': stage.id,
                            'label': stage.label,
                            'displayOrder': stage.display_order if hasattr(stage, 'display_order') else None,
                            'metadata': {}
                        }

                        # Add metadata if available (varies by object type)
                        if hasattr(stage, 'metadata') and stage.metadata:
                            metadata_dict = {}
                            # Deal stages have probability, isClosed
                            if hasattr(stage.metadata, 'probability'):
                                metadata_dict['probability'] = stage.metadata.probability
                            if hasattr(stage.metadata, 'is_closed'):
                                metadata_dict['isClosed'] = stage.metadata.is_closed
                            # Ticket stages have ticketState
                            if hasattr(stage.metadata, 'ticket_state'):
                                metadata_dict['ticketState'] = stage.metadata.ticket_state

                            stage_dict['metadata'] = metadata_dict

                        stages.append(stage_dict)

                    # Store stages as JSON string for easier querying
                    pipeline_dict['stages'] = json.dumps(stages)
                    pipeline_dict['stage_count'] = len(stages)
                else:
                    pipeline_dict['stages'] = json.dumps([])
                    pipeline_dict['stage_count'] = 0

                pipelines.append(pipeline_dict)

            logger.info(f"Retrieved {len(pipelines)} pipelines for {object_type} from HubSpot")
            return pipelines

        except Exception as e:
            logger.error(f"Error fetching pipelines for {object_type}: {e}")
            raise Exception(f"Failed to fetch pipelines for {object_type}: {e}")

    def insert(self, query: ast.Insert) -> None:
        """Pipelines cannot be created via API - managed through HubSpot UI"""
        raise NotImplementedError(
            "Pipelines cannot be created through the API. "
            "Please manage pipelines through HubSpot's pipeline settings."
        )

    def update(self, query: ast.Update) -> None:
        """Pipelines cannot be updated via API - managed through HubSpot UI"""
        raise NotImplementedError(
            "Pipelines cannot be updated through the API. "
            "Please manage pipelines through HubSpot's pipeline settings."
        )

    def delete(self, query: ast.Delete) -> None:
        """Pipelines cannot be deleted via API - managed through HubSpot UI"""
        raise NotImplementedError(
            "Pipelines cannot be deleted through the API. "
            "Please manage pipelines through HubSpot's pipeline settings."
        )

    @staticmethod
    def _extract_where_conditions(where_clause) -> List[List]:
        """Extract WHERE conditions from SQL query for local filtering"""
        conditions = []

        if not where_clause:
            return conditions

        def parse_condition(node):
            if isinstance(node, ast.BinaryOperation):
                if node.op in ['and', 'or']:
                    # Handle AND/OR - recursively parse both sides
                    parse_condition(node.args[0])
                    parse_condition(node.args[1])
                else:
                    # Handle comparison operators
                    if isinstance(node.args[0], ast.Identifier) and isinstance(node.args[1], (ast.Constant, ast.Parameter)):
                        column = node.args[0].parts[-1]
                        value = node.args[1].value
                        conditions.append([node.op, column, value])
            elif isinstance(node, ast.UnaryOperation):
                # Handle IS NULL, IS NOT NULL, etc.
                if isinstance(node.args[0], ast.Identifier):
                    column = node.args[0].parts[-1]
                    conditions.append([node.op, column, None])

        parse_condition(where_clause)
        return conditions

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
                logger.warning(f"Column '{column}' not found in pipelines data, skipping condition")
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
            elif op == 'like':
                # Convert SQL LIKE to pandas string contains
                search_term = str(value).replace('%', '')
                df = df[df[column].astype(str).str.contains(search_term, case=False, na=False)]
            elif op == 'in':
                values = value if isinstance(value, list) else [value]
                df = df[df[column].isin(values)]
            elif op == 'not in':
                values = value if isinstance(value, list) else [value]
                df = df[~df[column].isin(values)]

        return df

    def get_columns(self) -> List[Text]:
        """
        Get list of columns for the pipelines table.

        Returns
        -------
        List[str]
            Column names
        """
        return [
            'id',           # Pipeline ID (string)
            'object_type',  # Object type ('deals', 'tickets')
            'label',        # Pipeline name
            'displayOrder', # Display order (integer)
            'archived',     # Whether pipeline is archived (boolean)
            'stages',       # JSON string of stage information
            'stage_count',  # Number of stages (integer)
        ]
