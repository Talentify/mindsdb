import pandas as pd
from mindsdb_sql_parser import ast
from pandas import DataFrame

from mindsdb.integrations.libs.api_handler import APITable
from mindsdb.integrations.utilities.sql_utils import extract_comparison_conditions
from mindsdb.utilities import log

logger = log.getLogger("mindsdb")

class SearchAnalyticsTable(APITable):
    """
    Table class for the Google Search Console Search Analytics table.
    """

    FILTERABLE_DIMENSIONS = ['country', 'device', 'page', 'query', 'searchAppearance']

    def select(self, query: ast.Select) -> DataFrame:
        """
        Gets all traffic data from the Search Console.

        Args:
            query (ast.Select): SQL query to parse.

        Returns:
            Response: Response object containing the results.
        """
        # Parse the query to get the conditions.
        conditions = extract_comparison_conditions(query.where) if query.where else []
        # Get the start and end times from the conditions.
        params = {}

        if 'start_date' not in [arg1 for _, arg1, _ in conditions]:
            raise ValueError('start_date is required in WHERE clause (e.g., WHERE start_date = "2023-01-01")')

        if 'end_date' not in [arg1 for _, arg1, _ in conditions]:
            raise ValueError('end_date is required in WHERE clause (e.g., WHERE end_date = "2023-01-01")')

        accepted_params = ['site_url', 'type', 'data_state', 'start_row']
        accepted_dimensions = ['date', "hour", 'query', 'page', 'country', 'device', 'searchAppearance']
        dimension_filters = {}
        for op, arg, val in conditions:
            # Validate the conditions and convert them into API parameters.
            # Validate start_date and end_date conditions
            if arg in ['start_date']:
                if op in ['=']:
                    params['start_date'] = val
                else:
                    raise NotImplementedError(f"Operator '{op}' not supported for start_date. Use '=', '>=', or '>'")
            elif arg in ['end_date']:
                if op in ['=']:
                    params['end_date'] = val
                else:
                    raise NotImplementedError(f"Operator '{op}' not supported for end_date. Use '=', '<=', or '<'")
            # Validate dimensions condition
            elif arg in ['dimensions']:
                # Dimensions can be one or a list
                if op not in ['=', "in"]:
                    raise NotImplementedError(f"Operator '{op}' not supported for dimension. Use '=' or 'IN'")
                if isinstance(val, str):
                    val = [val]
                if not isinstance(val, list):
                    raise ValueError("Dimensions must be provided as a list or a single string value.")
                # Validate that all dimensions are accepted and that hour and date are not used together
                for v in val:
                    if v not in accepted_dimensions:
                        raise ValueError(f"Invalid dimension '{v}'. Accepted dimensions are: {accepted_dimensions}")
                    if 'hour' in val and 'date' in val:
                        raise ValueError("Cannot use 'hour' dimension with 'date' dimension.")
                params['dimensions'] = val
            # Validate data state condition
            elif arg in ['data_state']:
                if op != '=':
                    raise NotImplementedError(f"Operator '{op}' not supported for data_state. Use '='")
                # (all is to include fresh data that may not be fully processed, final is to include only 
                # fully processed data, hourly_all is to include all hourly data regardless of processing state)
                if val not in ['all', 'final', 'hourly_all']:
                    raise ValueError("Invalid data_state value. Accepted values are: 'all', 'final', 'hourly_all'")
                params['data_state'] = val
            # Validate type
            elif arg in ['type']:
                if op != '=':
                    raise NotImplementedError(f"Operator '{op}' not supported for type. Use '='")
                if val not in ['discover', 'googleNews', 'news', 'web', 'image', 'video']:
                    raise ValueError("Invalid type value. Accepted values are: 'discover', 'googleNews', 'news', 'web', 'image', 'video'")
                params['type'] = val
            # Validate start_row
            elif arg in ['start_row']:
                if op != '=':
                    raise NotImplementedError(f"Operator '{op}' not supported for start_row. Use '='")
                if not isinstance(val, int) or val < 0:
                    raise ValueError("start_row must be a non-negative integer.")
                params['start_row'] = val
            # Other accepted parameters
            elif arg in accepted_params:
                if op != '=':
                    raise NotImplementedError
                params[arg] = val
            # Handle dimension filters
            elif arg in self.FILTERABLE_DIMENSIONS:
                if op not in ['=', '!=', 'like', 'not like']:
                    raise NotImplementedError(
                        f"Operator '{op}' not supported for dimension filters. "
                        f"Supported operators: =, !=, LIKE, NOT LIKE"
                    )
                dimension_filters[arg] = (op, val)
            else:
                raise NotImplementedError(
                    f"Unknown parameter '{arg}'. Valid: start_date, end_date, dimensions, "
                    f"data_state, type, start_row, site_url, and filterable dimensions "
                    f"({', '.join(self.FILTERABLE_DIMENSIONS)})"
                )
            
        if 'hour' in params.get('dimensions', []) and params.get('data_state') != 'hourly_all':
            raise ValueError("When using 'hour' dimension, 'data_state' must be set to 'hourly_all'")

        if query.limit is not None:
            params['row_limit'] = query.limit.value

        # Build dimension filter groups if any filters specified
        if dimension_filters:
            params['dimensionFilterGroups'] = self._build_dimension_filters(dimension_filters)

        # Get the traffic data from the Google Search Console API.
        traffic_data = self.handler. \
            call_application_api(method_name='get_traffic_data', params=params)

        # Get dimensions from params for dynamic column handling
        dimensions = params.get('dimensions', [])

        selected_columns = []
        for target in query.targets:
            if isinstance(target, ast.Star):
                selected_columns = self.get_columns(dimensions)
                break
            elif isinstance(target, ast.Identifier):
                selected_columns.append(target.parts[-1])
            else:
                raise ValueError(f"Unknown query target {type(target)}")
        if len(traffic_data) == 0:
            traffic_data = pd.DataFrame([], columns=selected_columns)
        else:
            # Traffic data already has correct columns from get_traffic_data transformation
            # Only drop columns that weren't selected
            for col in set(traffic_data.columns).difference(set(selected_columns)):
                traffic_data = traffic_data.drop(col, axis=1)
        return traffic_data

    def get_columns(self, dimensions=None) -> list:
        """Gets all columns to be returned in pandas DataFrame responses

        Args:
            dimensions (list, optional): List of dimension names. If provided, returns
                dimension columns followed by metric columns. If None, returns only
                metric columns for aggregated data.

        Returns:
            list: Column names for the DataFrame
        """
        metrics = ['clicks', 'impressions', 'ctr', 'position']

        if dimensions:
            # Return dimension columns + metric columns
            return dimensions + metrics
        else:
            # Return 'keys' + metrics for backward compatibility when dimensions not specified
            return ['keys'] + metrics

    def _map_operator_to_gsc(self, sql_op: str, value: str, dimension: str) -> tuple:
        """
        Map SQL operator to Google Search Console API operator.

        Args:
            sql_op: SQL operator (=, !=, like, not like)
            value: Filter value
            dimension: Dimension name (for error messages)

        Returns:
            tuple: (gsc_operator, expression_value)

        Raises:
            NotImplementedError: If operator is not supported
        """
        sql_op_lower = sql_op.lower().strip()

        if sql_op_lower == '=':
            return ('equals', str(value))

        elif sql_op_lower == '!=':
            return ('notEquals', str(value))

        elif sql_op_lower == 'like':
            # Handle LIKE patterns
            str_value = str(value)
            if str_value.startswith('%') and str_value.endswith('%'):
                # %pattern% -> contains
                pattern = str_value.strip('%')
                return ('contains', pattern)
            elif '%' not in str_value:
                # No wildcards -> exact match
                return ('equals', str_value)
            else:
                # Unsupported pattern (e.g., 'term%' or '%term')
                raise NotImplementedError(
                    f"LIKE pattern '{value}' not fully supported for '{dimension}'. "
                    f"Use '%term%' for substring or 'term' for exact match."
                )

        elif sql_op_lower == 'not like':
            # Handle NOT LIKE patterns
            str_value = str(value)
            if str_value.startswith('%') and str_value.endswith('%'):
                # %pattern% -> notContains
                pattern = str_value.strip('%')
                return ('notContains', pattern)
            elif '%' not in str_value:
                # No wildcards -> not equals
                return ('notEquals', str_value)
            else:
                raise NotImplementedError(
                    f"NOT LIKE pattern '{value}' not fully supported for '{dimension}'. "
                    f"Use '%term%' for substring or 'term' for exact match."
                )

        else:
            raise NotImplementedError(
                f"Operator '{sql_op}' not supported for dimension filters. "
                f"Supported: =, !=, LIKE, NOT LIKE"
            )

    def _build_dimension_filters(self, dimension_filters: dict) -> list:
        """
        Build dimension filter groups for Google Search Console API.

        Args:
            dimension_filters: Dict mapping dimension names to (operator, value) tuples

        Returns:
            list: dimensionFilterGroups structure for API

        Example:
            Input: {'query': ('like', '%mindsdb%'), 'country': ('=', 'USA')}
            Output: [{
                "groupType": "and",
                "filters": [
                    {"dimension": "query", "operator": "contains", "expression": "mindsdb"},
                    {"dimension": "country", "operator": "equals", "expression": "USA"}
                ]
            }]
        """
        if not dimension_filters:
            return []

        filters = []
        for dimension, (sql_op, value) in dimension_filters.items():
            # Map SQL operator to GSC API operator
            gsc_operator, expression = self._map_operator_to_gsc(sql_op, value, dimension)

            filters.append({
                "dimension": dimension,
                "operator": gsc_operator,
                "expression": expression
            })

        # Return single filter group with AND logic
        return [{
            "groupType": "and",
            "filters": filters
        }]


class SiteMapsTable(APITable):
    """
    Table class for the Google Search Console Site Maps table.
    """

    def select(self, query: ast.Select) -> DataFrame:
        """
        Gets all traffic data from the Search Console.

        Args:
            query (ast.Select): SQL query to parse.

        Returns:
            Response: Response object containing the results.
        """

        # Parse the query to get the conditions.
        conditions = extract_comparison_conditions(query.where)
        # Get the start and end times from the conditions.
        params = {}
        # Note: siteUrl is now optional in WHERE clause (taken from connection if not specified)
        accepted_params = ['site_url', 'sitemap_index']
        for op, arg1, arg2 in conditions:
            if op != '=':
                raise NotImplementedError
            if arg1 in accepted_params:
                params[arg1] = arg2
            else:
                raise NotImplementedError

        if query.limit is not None:
            params['row_limit'] = query.limit.value

        # Get the traffic data from the Google Search Console API.
        sitemaps = self.handler. \
            call_application_api(method_name='get_sitemaps', params=params)

        selected_columns = []
        for target in query.targets:
            if isinstance(target, ast.Star):
                selected_columns = self.get_columns()
                break
            elif isinstance(target, ast.Identifier):
                selected_columns.append(target.parts[-1])
            else:
                raise ValueError(f"Unknown query target {type(target)}")

        if len(sitemaps) == 0:
            sitemaps = pd.DataFrame([], columns=selected_columns)
        else:
            sitemaps.columns = self.get_columns()
            for col in set(sitemaps.columns).difference(set(selected_columns)):
                sitemaps = sitemaps.drop(col, axis=1)
        return sitemaps

    def insert(self, query: ast.Insert):
        """
        Submits a sitemap for a site.

        Args:
            query (ast.Insert): SQL query to parse.

        Returns:
            Response: Response object containing the results.
        """

        # Get the values from the query.
        values = query.values[0]
        params = {}
        # Get the event data from the values.
        # Note: siteUrl is optional (taken from connection if not specified)
        for col, val in zip(query.columns, values):
            if col == 'siteUrl' or col == 'feedpath':
                params[col] = val
            else:
                raise NotImplementedError

        # Insert the event into the Google Calendar API.
        self.handler.call_application_api(method_name='submit_sitemap', params=params)

    def delete(self, query: ast.Delete):
        """
        Deletes a sitemap for a site.

        Args:
            query (ast.Delete): SQL query to parse.

        Returns:
            Response: Response object containing the results.
        """

        # Parse the query to get the conditions.
        conditions = extract_comparison_conditions(query.where)
        # Get the start and end times from the conditions.
        params = {}
        # Note: siteUrl is optional in WHERE clause (taken from connection if not specified)
        for op, arg1, arg2 in conditions:
            if op != '=':
                raise NotImplementedError
            if arg1 == 'siteUrl' or arg1 == 'feedpath':
                params[arg1] = arg2
            else:
                raise NotImplementedError

        # Delete the events in the Google Calendar API.
        self.handler.call_application_api(method_name='delete_sitemap', params=params)

    def get_columns(self) -> list:
        """Gets all columns to be returned in pandas DataFrame responses"""
        return [
            'path',
            'lastSubmitted',
            'isPending',
            'isSitemapsIndex',
            'type',
            'lastDownloaded',
            'warnings',
            'errors',
            'contents'
        ]


class UrlInspectionTable(APITable):
    """
    Table class for the Google Search Console URL Inspection API.
    """

    def select(self, query: ast.Select) -> DataFrame:
        """
        Inspects URLs to check indexing status, mobile usability, AMP, rich results, etc.

        Args:
            query (ast.Select): SQL query to parse.

        Returns:
            Response: Response object containing the results.
        """

        # Parse the query to get the conditions.
        conditions = extract_comparison_conditions(query.where)
        params = {}
        # Note: siteUrl is optional in WHERE clause (taken from connection if not specified)
        accepted_params = ['site_url', 'inspection_url', 'language_code']
        for op, arg1, arg2 in conditions:
            if op != '=':
                raise NotImplementedError('Only = operator is supported')
            if arg1 in accepted_params:
                params[arg1] = arg2
            else:
                raise NotImplementedError(f'Unsupported parameter: {arg1}')

        # inspectionUrl is required
        if 'inspection_url' not in params:
            raise ValueError('inspection_url is required in WHERE clause (e.g., WHERE inspection_url = "https://example.com/page")')

        # Get the URL inspection data from the Google Search Console API.
        inspection_data = self.handler.call_application_api(
            method_name='inspect_url',
            params=params
        )

        selected_columns = []
        for target in query.targets:
            if isinstance(target, ast.Star):
                selected_columns = self.get_columns()
                break
            elif isinstance(target, ast.Identifier):
                selected_columns.append(target.parts[-1])
            else:
                raise ValueError(f"Unknown query target {type(target)}")

        if len(inspection_data) == 0:
            inspection_data = pd.DataFrame([], columns=selected_columns)
        else:
            inspection_data.columns = self.get_columns()
            for col in set(inspection_data.columns).difference(set(selected_columns)):
                inspection_data = inspection_data.drop(col, axis=1)
        return inspection_data

    def get_columns(self) -> list:
        """Gets all columns to be returned in pandas DataFrame responses"""
        return [
            'inspectionUrl',
            'indexStatusVerdict',
            'coverageState',
            'robotsTxtState',
            'indexingState',
            'lastCrawlTime',
            'pageFetchState',
            'googleCanonical',
            'userCanonical',
            'crawledAs',
            'mobileUsabilityVerdict',
            'mobileUsabilityIssues',
            'ampInspectionResult',
            'richResultsResult'
        ]
