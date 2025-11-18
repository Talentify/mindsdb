"""
HubSpot Associations Table

This table provides access to associations (relationships) between HubSpot CRM objects.
Associations connect records like Contacts to Companies, Deals to Contacts, etc.

API Endpoint: /crm/v4/associations/{fromObjectType}/{toObjectType}/batch/read
Documentation: https://developers.hubspot.com/docs/api/crm/associations

Supports full CRUD operations for managing associations between CRM objects.
"""

from typing import List, Dict, Text, Any, Tuple, Optional
import pandas as pd
from mindsdb_sql_parser import ast
from mindsdb.integrations.libs.api_handler import APITable
from mindsdb.integrations.utilities.handlers.query_utilities import SELECTQueryParser, DELETEQueryParser
from mindsdb.utilities import log
from mindsdb.integrations.handlers.hubspot_handler.tables.crm.base_hubspot_table import HubSpotSearchMixin
from hubspot.crm.associations.v4 import (
    BatchInputPublicFetchAssociationsBatchRequest,
    PublicFetchAssociationsBatchRequest
)

logger = log.getLogger(__name__)


class AssociationsTable(HubSpotSearchMixin, APITable):
    """
    HubSpot Associations table for managing relationships between CRM objects.

    Associations define relationships like:
    - Contact to Company
    - Deal to Contact
    - Ticket to Contact
    - etc.

    Example queries:
        -- Get all associations for a contact
        SELECT * FROM hubspot.associations
        WHERE from_object_type='contacts' AND from_object_id='12345'

        -- Get all companies associated with a contact
        SELECT * FROM hubspot.associations
        WHERE from_object_type='contacts' AND to_object_type='companies'
        AND from_object_id='12345'

        -- Create association between contact and company
        INSERT INTO hubspot.associations (from_object_type, from_object_id, to_object_type, to_object_id, association_type_id)
        VALUES ('contacts', '12345', 'companies', '67890', 1)

        -- Delete association
        DELETE FROM hubspot.associations
        WHERE from_object_type='contacts' AND from_object_id='12345'
        AND to_object_type='companies' AND to_object_id='67890'
    """

    # Common association type IDs
    # Full list: https://developers.hubspot.com/docs/api/crm/associations#association-type-id-values
    ASSOCIATION_TYPES = {
        # Contact associations
        ('contacts', 'companies'): {'default': 1, 'primary': 1},
        ('contacts', 'deals'): {'default': 3, 'primary': 3},
        ('contacts', 'tickets'): {'default': 16, 'primary': 16},
        # Company associations
        ('companies', 'contacts'): {'default': 2, 'primary': 2},
        ('companies', 'deals'): {'default': 5, 'primary': 5},
        ('companies', 'tickets'): {'default': 26, 'primary': 26},
        # Deal associations
        ('deals', 'contacts'): {'default': 4, 'primary': 4},
        ('deals', 'companies'): {'default': 6, 'primary': 6},
        ('deals', 'line_items'): {'default': 19, 'primary': 19},
        ('deals', 'tickets'): {'default': 28, 'primary': 28},
        # Ticket associations
        ('tickets', 'contacts'): {'default': 15, 'primary': 15},
        ('tickets', 'companies'): {'default': 25, 'primary': 25},
        ('tickets', 'deals'): {'default': 27, 'primary': 27},
    }

    def select(self, query: ast.Select) -> pd.DataFrame:
        """
        Get associations from HubSpot.

        Requires from_object_type and from_object_id in WHERE clause.

        Parameters
        ----------
        query : ast.Select
            SQL SELECT query

        Returns
        -------
        pd.DataFrame
            Associations data
        """
        # Use SELECTQueryParser to properly parse the query
        select_statement_parser = SELECTQueryParser(
            query,
            "associations",
            self.get_columns()
        )
        selected_columns, where_conditions, order_by_conditions, result_limit = select_statement_parser.parse_query()

        # Extract required parameters from WHERE clause
        from_object_type = None
        from_object_ids = []
        to_object_type = None
        to_object_ids = []

        for condition in where_conditions:
            if len(condition) < 3:
                continue

            op, column, value = condition[0], condition[1], condition[2]

            if column == 'from_object_type' and op == '=':
                from_object_type = value
            elif column == 'from_object_id':
                if op == '=':
                    from_object_ids = [value]
                elif op == 'in':
                    from_object_ids = value if isinstance(value, list) else [value]
            elif column == 'to_object_type' and op == '=':
                to_object_type = value
            elif column == 'to_object_id':
                if op == '=':
                    to_object_ids = [value]
                elif op == 'in':
                    to_object_ids = value if isinstance(value, list) else [value]

        # Validate required parameters
        if not from_object_type:
            raise ValueError(
                "from_object_type is required in WHERE clause. "
                "Example: WHERE from_object_type='contacts' AND from_object_id='12345'"
            )

        if not from_object_ids:
            raise ValueError(
                "from_object_id is required in WHERE clause. "
                "Example: WHERE from_object_type='contacts' AND from_object_id='12345'"
            )

        # Fetch associations
        associations = self.get_associations(
            from_object_type=from_object_type,
            from_object_ids=from_object_ids,
            to_object_type=to_object_type
        )

        # Convert to DataFrame
        if not associations:
            logger.info("No associations found")
            # Return empty DataFrame with correct column schema
            return pd.DataFrame(columns=self.get_columns())

        associations_df = pd.DataFrame(associations)

        # Apply additional WHERE conditions that weren't used in the API query
        # We need to exclude conditions already applied at the API level:
        # - from_object_type (always used)
        # - from_object_id (always used)
        # - to_object_type (used if specified)
        if where_conditions and not associations_df.empty:
            # Filter out conditions already applied at API level
            conditions_to_apply = []
            for condition in where_conditions:
                if len(condition) < 3:
                    continue

                op, column, value = condition[0], condition[1], condition[2]

                # Skip conditions already used for API filtering
                if column == 'from_object_type' or column == 'from_object_id':
                    continue  # Already filtered by API
                if column == 'to_object_type' and to_object_type:
                    continue  # Already filtered by API

                # Keep other conditions for local filtering (e.g., to_object_id, association_type_id)
                conditions_to_apply.append(condition)

            if conditions_to_apply:
                associations_df = self._apply_conditions(associations_df, conditions_to_apply)

        # Apply column selection
        if selected_columns and not associations_df.empty:
            # Filter to only available columns
            available_columns = [col for col in selected_columns if col in associations_df.columns]
            if len(available_columns) < len(selected_columns):
                missing = set(selected_columns) - set(available_columns)
                logger.warning(f"Some requested columns not available in associations data: {missing}")
            if available_columns:
                associations_df = associations_df[available_columns]

        # Apply limit
        if result_limit and not associations_df.empty:
            associations_df = associations_df.head(result_limit)

        logger.info(f"Returning {len(associations_df)} associations")
        return associations_df

    def get_associations(
        self,
        from_object_type: str,
        from_object_ids: List[str],
        to_object_type: Optional[str] = None
    ) -> List[Dict]:
        """
        Get associations for specified objects.

        Parameters
        ----------
        from_object_type : str
            Source object type (e.g., 'contacts', 'deals')
        from_object_ids : List[str]
            List of source object IDs
        to_object_type : str, optional
            Filter by destination object type

        Returns
        -------
        List[Dict]
            List of association dictionaries
        """
        if not from_object_ids:
            return []

        hubspot = self.handler.connect()
        all_associations = []

        try:
            # Determine which object types to fetch associations for
            if to_object_type:
                to_object_types = [to_object_type]
            else:
                # Fetch all possible associations
                to_object_types = ['contacts', 'companies', 'deals', 'tickets', 'line_items']

            for to_type in to_object_types:
                # Skip if trying to associate with self
                if to_type == from_object_type:
                    continue

                try:
                    # Batch read associations using get_page
                    # Note: HubSpot v4 associations API uses get_page method for batch reads
                    # Create proper batch request with PublicFetchAssociationsBatchRequest objects
                    inputs = [
                        PublicFetchAssociationsBatchRequest(id=str(obj_id))
                        for obj_id in from_object_ids
                    ]
                    batch_read_input = BatchInputPublicFetchAssociationsBatchRequest(inputs=inputs)

                    response = self._execute_with_retry(
                        lambda: hubspot.crm.associations.v4.batch_api.get_page(
                            from_object_type=from_object_type,
                            to_object_type=to_type,
                            batch_input_public_fetch_associations_batch_request=batch_read_input
                        ),
                        f"get_associations_{from_object_type}_to_{to_type}"
                    )

                    # Process results
                    if hasattr(response, 'results'):
                        for result in response.results:
                            from_id = result.from_.id if hasattr(result, 'from_') else None

                            if hasattr(result, 'to') and result.to:
                                for to_obj in result.to:
                                    association_dict = {
                                        'from_object_type': from_object_type,
                                        'from_object_id': str(from_id),
                                        'to_object_type': to_type,
                                        'to_object_id': str(to_obj.to_object_id) if hasattr(to_obj, 'to_object_id') else str(to_obj.id),
                                    }

                                    # Add association type information
                                    if hasattr(to_obj, 'association_types') and to_obj.association_types:
                                        # Get first association type (there can be multiple)
                                        assoc_type = to_obj.association_types[0]
                                        association_dict['association_type_id'] = assoc_type.type_id if hasattr(assoc_type, 'type_id') else None
                                        association_dict['association_label'] = assoc_type.label if hasattr(assoc_type, 'label') else None
                                    else:
                                        association_dict['association_type_id'] = None
                                        association_dict['association_label'] = None

                                    all_associations.append(association_dict)

                except Exception as e:
                    # Log but continue - some object type combinations may not be valid
                    logger.debug(f"No associations found from {from_object_type} to {to_type}: {e}")
                    continue

            logger.info(f"Retrieved {len(all_associations)} associations")
            return all_associations

        except Exception as e:
            logger.error(f"Error fetching associations: {e}")
            raise Exception(f"Failed to fetch associations: {e}")

    def insert(self, query: ast.Insert) -> None:
        """
        Create associations between HubSpot objects.

        Required columns:
        - from_object_type
        - from_object_id
        - to_object_type
        - to_object_id

        Optional columns:
        - association_type_id (defaults to standard type for object pair)

        Parameters
        ----------
        query : ast.Insert
            SQL INSERT query
        """
        # Extract column names and values
        columns = [col.parts[-1] for col in query.columns] if query.columns else []

        associations_to_create = []

        if isinstance(query.values, list):
            # Multiple rows
            for value_list in query.values:
                values = [v.value if hasattr(v, 'value') else v for v in value_list]
                row_data = dict(zip(columns, values))
                associations_to_create.append(row_data)
        else:
            # Single row
            values = [v.value if hasattr(v, 'value') else v for v in query.values]
            row_data = dict(zip(columns, values))
            associations_to_create.append(row_data)

        # Validate and create associations
        for assoc_data in associations_to_create:
            # Validate required fields
            required_fields = ['from_object_type', 'from_object_id', 'to_object_type', 'to_object_id']
            for field in required_fields:
                if field not in assoc_data or not assoc_data[field]:
                    raise ValueError(f"Missing required field: {field}")

        self.create_associations(associations_to_create)

    def create_associations(self, associations_data: List[Dict[Text, Any]]) -> None:
        """
        Create associations between objects.

        Parameters
        ----------
        associations_data : List[Dict]
            List of association data dictionaries
        """
        if not associations_data:
            logger.info("No associations to create")
            return

        hubspot = self.handler.connect()

        # Group associations by (from_type, to_type) pair for batch operations
        grouped_associations = {}
        for assoc in associations_data:
            key = (assoc['from_object_type'], assoc['to_object_type'])
            if key not in grouped_associations:
                grouped_associations[key] = []
            grouped_associations[key].append(assoc)

        # Create associations for each group
        for (from_type, to_type), group in grouped_associations.items():
            try:
                # Prepare batch create input
                inputs = []
                for assoc in group:
                    # Determine association type ID
                    association_type_id = assoc.get('association_type_id')
                    if not association_type_id:
                        # Use default for this object pair
                        type_key = (from_type, to_type)
                        if type_key in self.ASSOCIATION_TYPES:
                            association_type_id = self.ASSOCIATION_TYPES[type_key]['default']
                        else:
                            raise ValueError(
                                f"No default association type for {from_type} -> {to_type}. "
                                f"Please specify association_type_id."
                            )

                    inputs.append({
                        'from': {'id': str(assoc['from_object_id'])},
                        'to': {'id': str(assoc['to_object_id'])},
                        'types': [{'associationTypeId': int(association_type_id)}]
                    })

                # Create associations with retry and chunking
                def create_batch(batch):
                    batch_input = {'inputs': batch}
                    return hubspot.crm.associations.v4.batch_api.create(
                        from_object_type=from_type,
                        to_object_type=to_type,
                        batch_input_public_association=batch_input
                    )

                self._batch_create_with_chunking(
                    inputs,
                    create_batch,
                    f"associations_{from_type}_to_{to_type}"
                )

                logger.info(f"Created {len(group)} associations from {from_type} to {to_type}")

            except Exception as e:
                logger.error(f"Error creating associations from {from_type} to {to_type}: {e}")
                raise Exception(f"Failed to create associations from {from_type} to {to_type}: {e}")

    def update(self, query: ast.Update) -> None:
        """
        Update not supported for associations.
        To change an association, delete and recreate it.
        """
        raise NotImplementedError(
            "Associations cannot be updated. To change an association, delete it and create a new one."
        )

    def delete(self, query: ast.Delete) -> None:
        """
        Delete associations between HubSpot objects.

        Requires WHERE clause with:
        - from_object_type
        - from_object_id
        - to_object_type
        - to_object_id

        Parameters
        ----------
        query : ast.Delete
            SQL DELETE query
        """
        # Parse WHERE clause
        if not query.where:
            raise ValueError(
                "DELETE requires WHERE clause with from_object_type, from_object_id, "
                "to_object_type, and to_object_id"
            )

        delete_statement_parser = DELETEQueryParser(query)
        where_conditions = delete_statement_parser.parse_query()

        # Extract parameters
        from_object_type = None
        from_object_ids = []
        to_object_type = None
        to_object_ids = []

        for condition in where_conditions:
            if len(condition) < 3:
                continue

            op, column, value = condition[0], condition[1], condition[2]

            if column == 'from_object_type' and op == '=':
                from_object_type = value
            elif column == 'from_object_id':
                if op == '=':
                    from_object_ids = [value]
                elif op == 'in':
                    from_object_ids = value if isinstance(value, list) else [value]
            elif column == 'to_object_type' and op == '=':
                to_object_type = value
            elif column == 'to_object_id':
                if op == '=':
                    to_object_ids = [value]
                elif op == 'in':
                    to_object_ids = value if isinstance(value, list) else [value]

        # Validate required parameters
        if not all([from_object_type, from_object_ids, to_object_type, to_object_ids]):
            raise ValueError(
                "DELETE requires from_object_type, from_object_id, to_object_type, "
                "and to_object_id in WHERE clause"
            )

        # Type assertions for mypy - we've validated these are not None above
        assert from_object_type is not None
        assert to_object_type is not None

        self.delete_associations(from_object_type, from_object_ids, to_object_type, to_object_ids)

    def delete_associations(
        self,
        from_object_type: str,
        from_object_ids: List[str],
        to_object_type: str,
        to_object_ids: List[str]
    ) -> None:
        """
        Delete associations between objects.

        Parameters
        ----------
        from_object_type : str
            Source object type
        from_object_ids : List[str]
            Source object IDs
        to_object_type : str
            Destination object type
        to_object_ids : List[str]
            Destination object IDs
        """
        if not from_object_ids or not to_object_ids:
            logger.info("No associations to delete")
            return

        hubspot = self.handler.connect()

        # Prepare batch delete input (pairs of from/to IDs)
        inputs = []
        for from_id in from_object_ids:
            for to_id in to_object_ids:
                inputs.append({
                    'from': {'id': str(from_id)},
                    'to': {'id': str(to_id)}
                })

        # Delete associations with retry and chunking
        def delete_batch(batch):
            batch_input = {'inputs': batch}
            return hubspot.crm.associations.v4.batch_api.archive(
                from_object_type=from_object_type,
                to_object_type=to_object_type,
                batch_input_public_association=batch_input
            )

        self._batch_delete_with_chunking(
            inputs,
            delete_batch,
            f"associations_{from_object_type}_to_{to_object_type}"
        )

        logger.info(f"Deleted associations from {from_object_type} to {to_object_type}")

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
                logger.debug(f"Column '{column}' not found in associations data, skipping condition")
                continue

            # Apply filter based on operator
            if op == '=':
                df = df[df[column] == value]
            elif op == '!=':
                df = df[df[column] != value]
            elif op == 'in':
                values = value if isinstance(value, list) else [value]
                df = df[df[column].isin(values)]
            elif op == 'not in':
                values = value if isinstance(value, list) else [value]
                df = df[~df[column].isin(values)]

        return df

    def get_columns(self) -> List[Text]:
        """
        Get list of columns for the associations table.

        Returns
        -------
        List[str]
            Column names
        """
        return [
            'from_object_type',      # Source object type (e.g., 'contacts')
            'from_object_id',        # Source object ID
            'to_object_type',        # Destination object type (e.g., 'companies')
            'to_object_id',          # Destination object ID
            'association_type_id',   # Association type ID (integer)
            'association_label',     # Association label (e.g., 'Contact to Company')
        ]
