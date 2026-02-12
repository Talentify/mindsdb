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

        accepted_params = ['site_url', 'type', 'row_limit', 'data_state']
        accepted_dimensions = ['date', "hour", 'query', 'page', 'country', 'device']
        for op, arg, val in conditions:
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
            elif arg in ['dimensions']:
                if op not in ['=', "in"]:
                    raise NotImplementedError(f"Operator '{op}' not supported for dimension. Use '=' or 'IN'")
                if isinstance(val, str):
                    val = [val]
                if not isinstance(val, list):
                    raise ValueError("Dimensions must be provided as a list or a single string value.")
                for v in val:
                    if v not in accepted_dimensions:
                        raise ValueError(f"Invalid dimension '{v}'. Accepted dimensions are: {accepted_dimensions}")
                    if 'hour' in val and 'date' in val:
                        raise ValueError("Cannot use 'hour' dimension with 'date' dimension.")
                params['dimensions'] = val
            elif arg in ['data_state']:
                if op != '=':
                    raise NotImplementedError(f"Operator '{op}' not supported for data_state. Use '='")
                if val not in ['all', 'final', 'hourly_all']:
                    raise ValueError("Invalid data_state value. Accepted values are: 'all', 'final', 'hourly_all'")
                params['data_state'] = val
            elif arg in accepted_params:
                if op != '=':
                    raise NotImplementedError
                params[arg] = val
            else:
                raise NotImplementedError
            
        if 'hour' in params.get('dimensions', []) and params.get('data_state') != 'hourly_all':
            raise ValueError("When using 'hour' dimension, 'data_state' must be set to 'hourly_all'")

        # Get the order by from the query.
        if query.order_by is not None:
            if query.order_by[0].value == 'start_time':
                params['orderBy'] = 'startTime'
            elif query.order_by[0].value == 'updated':
                params['orderBy'] = 'updated'
            else:
                raise NotImplementedError

        if query.limit is not None:
            params['row_limit'] = query.limit.value

        # Get the traffic data from the Google Search Console API.
        traffic_data = self.handler. \
            call_application_api(method_name='get_traffic_data', params=params)

        selected_columns = []
        for target in query.targets:
            if isinstance(target, ast.Star):
                selected_columns = self.get_columns()
                break
            elif isinstance(target, ast.Identifier):
                selected_columns.append(target.parts[-1])
            else:
                raise ValueError(f"Unknown query target {type(target)}")
        if len(traffic_data) == 0:
            traffic_data = pd.DataFrame([], columns=selected_columns)
        else:
            traffic_data.columns = self.get_columns()
            for col in set(traffic_data.columns).difference(set(selected_columns)):
                traffic_data = traffic_data.drop(col, axis=1)
        return traffic_data

    def get_columns(self) -> list:
        """Gets all columns to be returned in pandas DataFrame responses"""
        return [
            'keys',
            'clicks',
            'impressions',
            'ctr',
            'position'
        ]


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
        accepted_params = ['siteUrl', 'sitemapIndex']
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
        accepted_params = ['siteUrl', 'inspectionUrl', 'languageCode']
        for op, arg1, arg2 in conditions:
            if op != '=':
                raise NotImplementedError('Only = operator is supported')
            if arg1 in accepted_params:
                params[arg1] = arg2
            else:
                raise NotImplementedError(f'Unsupported parameter: {arg1}')

        # inspectionUrl is required
        if 'inspectionUrl' not in params:
            raise ValueError('inspectionUrl is required in WHERE clause (e.g., WHERE inspectionUrl = "https://example.com/page")')

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


class MobileFriendlyTestTable(APITable):
    """
    Table class for the Google Search Console Mobile-Friendly Test API.
    """

    def select(self, query: ast.Select) -> DataFrame:
        """
        Tests URLs for mobile-friendliness.

        Args:
            query (ast.Select): SQL query to parse.

        Returns:
            Response: Response object containing the results.
        """

        # Parse the query to get the conditions.
        conditions = extract_comparison_conditions(query.where)
        params = {}
        accepted_params = ['url', 'requestScreenshot']
        for op, arg1, arg2 in conditions:
            if op != '=':
                raise NotImplementedError('Only = operator is supported')
            if arg1 in accepted_params:
                params[arg1] = arg2
            else:
                raise NotImplementedError(f'Unsupported parameter: {arg1}')

        # url is required
        if 'url' not in params:
            raise ValueError('url is required in WHERE clause (e.g., WHERE url = "https://example.com/page")')

        # Get the mobile-friendly test data from the Google Search Console API.
        test_data = self.handler.call_application_api(
            method_name='mobile_friendly_test',
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

        if len(test_data) == 0:
            test_data = pd.DataFrame([], columns=selected_columns)
        else:
            test_data.columns = self.get_columns()
            for col in set(test_data.columns).difference(set(selected_columns)):
                test_data = test_data.drop(col, axis=1)
        return test_data

    def get_columns(self) -> list:
        """Gets all columns to be returned in pandas DataFrame responses"""
        return [
            'url',
            'mobileFriendliness',
            'mobileFriendlyIssues',
            'resourceIssues',
            'testStatus',
            'screenshot'
        ]
