"""
HubSpot Owners Table

This table provides access to HubSpot owners (users who can be assigned to records).
Owners are referenced throughout HubSpot CRM objects via hubspot_owner_id properties.

API Endpoint: /crm/v3/owners
Documentation: https://developers.hubspot.com/docs/api/crm/owners

Note: This is a READ-ONLY table. Owners are managed through HubSpot's user management interface.
"""

from typing import List, Dict, Text, Any
import pandas as pd
from mindsdb.integrations.libs.api_handler import APITable
from mindsdb.integrations.handlers.hubspot_handler.tables.crm.base_hubspot_table import HubSpotSearchMixin
from mindsdb.utilities import log
from mindsdb_sql.parser import ast

logger = log.getLogger(__name__)


class OwnersTable(HubSpotSearchMixin, APITable):
    """
    HubSpot Owners table for querying user/owner information.

    Owners can be assigned to contacts, companies, deals, tickets, and other CRM objects.
    Use this table to get owner details for filtering and joining with other tables.

    Example queries:
        SELECT * FROM hubspot.owners WHERE archived = false
        SELECT * FROM hubspot.owners WHERE email LIKE '%@company.com'
        SELECT id, email, firstName, lastName FROM hubspot.owners
    """

    def select(self, query: ast.Select) -> pd.DataFrame:
        """
        Get owners from HubSpot.

        Supports:
        - Column selection (SELECT specific columns)
        - Filtering (WHERE email = '...', archived = false)
        - Limit (LIMIT 100)

        Parameters
        ----------
        query : ast.Select
            SQL SELECT query

        Returns
        -------
        pd.DataFrame
            Owners data
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

        # Fetch owners
        owners = self.get_owners()

        # Convert to DataFrame
        if not owners:
            logger.info("No owners found")
            return pd.DataFrame()

        owners_df = pd.DataFrame(owners)

        # Apply WHERE conditions (local filtering since Owners API doesn't support search)
        if conditions and not owners_df.empty:
            owners_df = self._apply_conditions(owners_df, conditions)

        # Apply column selection
        if selected_columns and not owners_df.empty:
            # Filter to only available columns
            available_columns = [col for col in selected_columns if col in owners_df.columns]
            if len(available_columns) < len(selected_columns):
                missing = set(selected_columns) - set(available_columns)
                logger.warning(f"Some requested columns not available in owners data: {missing}")
            if available_columns:
                owners_df = owners_df[available_columns]

        # Apply limit
        if limit and not owners_df.empty:
            owners_df = owners_df.head(limit)

        logger.info(f"Returning {len(owners_df)} owners")
        return owners_df

    def get_owners(self, email: str = None, archived: bool = None) -> List[Dict]:
        """
        Get all owners from HubSpot.

        Parameters
        ----------
        email : str, optional
            Filter by email address
        archived : bool, optional
            Filter by archived status

        Returns
        -------
        List[Dict]
            List of owner dictionaries with fields:
            - id: Owner ID (string)
            - email: Email address
            - firstName: First name
            - lastName: Last name
            - userId: User ID (integer)
            - type: Owner type (e.g., "PERSON")
            - archived: Whether owner is archived
            - teams: List of team memberships
        """
        hubspot = self.handler.connect()

        try:
            # Get owners with retry logic
            response = self._execute_with_retry(
                lambda: hubspot.crm.owners.get_page(
                    email=email,
                    archived=archived
                ),
                "get_owners"
            )

            owners = []
            for owner in response.results:
                owner_dict = {
                    'id': owner.id,
                    'email': owner.email,
                    'firstName': owner.first_name if hasattr(owner, 'first_name') else None,
                    'lastName': owner.last_name if hasattr(owner, 'last_name') else None,
                    'userId': owner.user_id if hasattr(owner, 'user_id') else None,
                    'type': owner.type if hasattr(owner, 'type') else None,
                    'archived': owner.archived if hasattr(owner, 'archived') else False,
                }

                # Add team information if available
                if hasattr(owner, 'teams') and owner.teams:
                    owner_dict['teams'] = [{'id': team.id, 'name': team.name if hasattr(team, 'name') else None}
                                          for team in owner.teams]
                else:
                    owner_dict['teams'] = []

                owners.append(owner_dict)

            logger.info(f"Retrieved {len(owners)} owners from HubSpot")
            return owners

        except Exception as e:
            logger.error(f"Error fetching owners: {e}")
            raise Exception(f"Failed to fetch owners: {e}")

    def insert(self, query: ast.Insert) -> None:
        """Owners cannot be created via API - managed through HubSpot UI"""
        raise NotImplementedError(
            "Owners cannot be created through the API. "
            "Please manage owners through HubSpot's user management interface."
        )

    def update(self, query: ast.Update) -> None:
        """Owners cannot be updated via API - managed through HubSpot UI"""
        raise NotImplementedError(
            "Owners cannot be updated through the API. "
            "Please manage owners through HubSpot's user management interface."
        )

    def delete(self, query: ast.Delete) -> None:
        """Owners cannot be deleted via API - managed through HubSpot UI"""
        raise NotImplementedError(
            "Owners cannot be deleted through the API. "
            "Please manage owners through HubSpot's user management interface."
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
                logger.warning(f"Column '{column}' not found in owners data, skipping condition")
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
        Get list of columns for the owners table.

        Returns
        -------
        List[str]
            Column names
        """
        return [
            'id',           # Owner ID (string)
            'email',        # Email address
            'firstName',    # First name
            'lastName',     # Last name
            'userId',       # User ID (integer)
            'type',         # Owner type (e.g., "PERSON")
            'archived',     # Whether owner is archived (boolean)
            'teams',        # Team memberships (list of dicts)
        ]
