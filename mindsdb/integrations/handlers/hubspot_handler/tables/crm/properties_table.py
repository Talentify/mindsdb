from typing import List, Dict, Text
import pandas as pd

from mindsdb_sql_parser import ast
from mindsdb.integrations.libs.api_handler import APITable
from mindsdb.integrations.utilities.handlers.query_utilities import SELECTQueryParser, SELECTQueryExecutor
from mindsdb.utilities import log

logger = log.getLogger(__name__)


class PropertiesTable(APITable):
    """HubSpot Properties metadata table.

    This table allows users to discover available properties for each object type.

    Usage examples:
        SELECT * FROM hubspot.properties WHERE object_type = 'contacts'
        SELECT * FROM hubspot.properties WHERE object_type = 'companies'
        SELECT name, label, type FROM hubspot.properties WHERE object_type = 'deals' AND hubspotDefined = false
    """

    def select(self, query: ast.Select) -> pd.DataFrame:
        """
        Pulls HubSpot Properties metadata.

        Parameters
        ----------
        query : ast.Select
            Given SQL SELECT query

        Returns
        -------
        pd.DataFrame
            HubSpot properties matching the query

        Raises
        ------
        ValueError
            If the query contains an unsupported condition
        """
        select_statement_parser = SELECTQueryParser(
            query,
            "properties",
            self.get_columns()
        )
        selected_columns, where_conditions, order_by_conditions, result_limit = select_statement_parser.parse_query()

        # Get properties for all object types or filtered by where conditions
        properties_df = pd.json_normalize(self.get_properties())

        select_statement_executor = SELECTQueryExecutor(
            properties_df,
            selected_columns,
            where_conditions,
            order_by_conditions
        )
        properties_df = select_statement_executor.execute_query()

        return properties_df

    def get_columns(self) -> List[Text]:
        """
        Returns the column names for the properties metadata table.
        """
        return [
            'object_type',
            'name',
            'label',
            'type',
            'fieldType',
            'description',
            'groupName',
            'hidden',
            'hubspotDefined'
        ]

    def get_properties(self, object_type: str = None) -> List[Dict]:
        """
        Fetch property metadata for HubSpot object types.

        Parameters
        ----------
        object_type : str, optional
            If provided, only fetch properties for this object type.
            Otherwise, fetch for all object types (contacts, companies, deals).

        Returns
        -------
        List[Dict]
            List of property metadata dictionaries.
        """
        object_types = [object_type] if object_type else ['contacts', 'companies', 'deals']

        all_properties = []
        for obj_type in object_types:
            try:
                properties_cache = self.handler.get_properties_cache(obj_type)

                for prop in properties_cache['properties']:
                    property_data = {
                        'object_type': obj_type,
                        'name': prop['name'],
                        'label': prop['label'],
                        'type': prop['type'],
                        'fieldType': prop['fieldType'],
                        'description': prop.get('description', ''),
                        'groupName': prop.get('groupName', ''),
                        'hidden': prop.get('hidden', False),
                        'hubspotDefined': prop.get('hubspotDefined', True)
                    }
                    all_properties.append(property_data)

            except Exception as e:
                logger.error(f"Failed to fetch properties for {obj_type}: {e}")
                continue

        return all_properties

    def insert(self, query: ast.Insert) -> None:
        """Properties table is read-only."""
        raise NotImplementedError("Properties table is read-only. You cannot insert data.")

    def update(self, query: ast.Update) -> None:
        """Properties table is read-only."""
        raise NotImplementedError("Properties table is read-only. You cannot update data.")

    def delete(self, query: ast.Delete) -> None:
        """Properties table is read-only."""
        raise NotImplementedError("Properties table is read-only. You cannot delete data.")
