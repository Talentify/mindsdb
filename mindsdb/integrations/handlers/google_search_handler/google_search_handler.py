from datetime import datetime, timedelta
import pandas as pd

from pandas import DataFrame
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials as OAuthCredentials
from googleapiclient.discovery import build
from mindsdb.api.executor.data_types.response_type import RESPONSE_TYPE
from .google_search_tables import SearchAnalyticsTable, SiteMapsTable, UrlInspectionTable
from mindsdb.integrations.libs.api_handler import APIHandler, FuncParser
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
)
from mindsdb.utilities import log
from mindsdb.utilities.config import Config
from mindsdb.integrations.utilities.handlers.auth_utilities.google import GoogleUserOAuth2Manager
from mindsdb.integrations.utilities.handlers.auth_utilities.exceptions import AuthException

DEFAULT_SCOPES = [
    "https://www.googleapis.com/auth/webmasters.readonly"
]

logger = log.getLogger(__name__)


class GoogleSearchConsoleHandler(APIHandler):
    """
    A class for handling connections and interactions with the Google Search Console API.
    """

    name = "google_search"

    def __init__(self, name: str, **kwargs):
        """
        Initialize the Google Search Console API handler.
        Args:
            name (str): name of the handler
            kwargs (dict): additional arguments
        """
        super().__init__(name)
        self.service = None
        self.search_console_service = None  # For URL Inspection API (searchconsole v1)
        self.connection_data = kwargs.get("connection_data", {})
        self.is_connected = False

        self.handler_storage = kwargs["handler_storage"]

        # Get site_url from connection data
        self.site_url = self.connection_data.get("site_url", None)
        if not self.site_url:
            raise ValueError("site_url is required for Google Search Console handler")

        self.credentials_url = self.connection_data.get("credentials_url", None)
        self.credentials_file = self.connection_data.get("credentials_file", None)
        if self.connection_data.get("credentials"):
            self.credentials_file = self.connection_data.pop("credentials")
        if not self.credentials_file and not self.credentials_url:
            # try to get from config
            gsearch_config = Config().get("handlers", {}).get("google_search", {})
            secret_file = gsearch_config.get("credentials_file")
            secret_url = gsearch_config.get("credentials_url")
            if secret_file:
                self.credentials_file = secret_file
            elif secret_url:
                self.credentials_url = secret_url

        self.scopes = self.connection_data.get("scopes", DEFAULT_SCOPES)
        if isinstance(self.scopes, str):
            self.scopes = [scope.strip() for scope in self.scopes.split(',') if scope.strip()]

        analytics = SearchAnalyticsTable(self)
        self.analytics = analytics
        self._register_table("Analytics", analytics)
        sitemaps = SiteMapsTable(self)
        self.sitemaps = sitemaps
        self._register_table("Sitemaps", sitemaps)
        url_inspection = UrlInspectionTable(self)
        self.url_inspection = url_inspection
        self._register_table("UrlInspection", url_inspection)

    def connect(self, **kwargs):
        """
        Set up any connections required by the handler
        Should return output of check_connection() method after attempting
        connection. Should switch self.is_connected.
        Returns:
            HandlerStatusResponse
        """
        if self.is_connected and self.service is not None and self.search_console_service is not None:
            return self.service

        params = dict(self.connection_data) if self.connection_data else {}

        # Merge optional parameters passed at call time without mutating the cached args
        override_params = kwargs.get('parameters') or {}
        params.update(override_params)

        # Allow nested "parameters" key (e.g. when provided through CREATE DATABASE ... PARAMETERS = {...})
        nested_params = params.get('parameters')
        if isinstance(nested_params, dict):
            params.update(nested_params)

        if 'refresh_token' in params:
            client_id = params.get('client_id')
            client_secret = params.get('client_secret')
            refresh_token = params['refresh_token']
            token_uri = params.get('token_uri', 'https://oauth2.googleapis.com/token')
            scopes = params.get('scopes') or self.scopes or DEFAULT_SCOPES
            if isinstance(scopes, str):
                scopes = [scope.strip() for scope in scopes.split(',') if scope.strip()]

            if not client_id or not client_secret:
                raise Exception('google_search_handler: client_id and client_secret are required when refresh_token is provided')

            creds = OAuthCredentials(
                token=None,
                refresh_token=refresh_token,
                token_uri=token_uri,
                client_id=client_id,
                client_secret=client_secret,
                scopes=scopes
            )

            creds.refresh(Request())
            self.service = build('webmasters', 'v3', credentials=creds)
            self.search_console_service = build('searchconsole', 'v1', credentials=creds)
            self.is_connected = True
            return self.service

        google_oauth2_manager = GoogleUserOAuth2Manager(
            self.handler_storage,
            self.scopes,
            self.credentials_file,
            self.credentials_url,
            self.connection_data.get('code')
        )
        creds = google_oauth2_manager.get_oauth2_credentials()

        self.service = build('webmasters', 'v3', credentials=creds)
        self.search_console_service = build('searchconsole', 'v1', credentials=creds)

        self.is_connected = True
        return self.service

    def check_connection(self) -> StatusResponse:
        """
        Check connection to the handler
        Returns:
            HandlerStatusResponse
        """
        response = StatusResponse(False)

        try:
            self.connect()
            response.success = True
            response.copy_storage = True

        except AuthException as error:
            response.error_message = str(error)
            response.redirect_url = error.auth_url
            return response

        except Exception as e:
            logger.error(f"Error connecting to Google Search Console API: {e}!")
            response.error_message = e

        self.is_connected = response.success
        return response

    def native_query(self, query: str = None) -> Response:
        """
        Receive raw query and act upon it somehow.
        Args:
            query (Any): query in native format (str for sql databases,
                api's json etc)
        Returns:
            HandlerResponse
        """
        method_name, params = FuncParser().from_string(query)

        df = self.call_application_api(method_name, params)

        return Response(RESPONSE_TYPE.TABLE, data_frame=df)

    def get_traffic_data(self, params: dict = None) -> DataFrame:
        """
        Get traffic data from Google Search Console API
        Args:
            params (dict): query parameters
        Returns:
            DataFrame
        """
        service = self.connect()
        accepted_params = ["start_date", "end_date", "dimensions", "row_limit", "aggregation_type", "data_state", "dimensionFilterGroups"]
        search_analytics_query_request = {
            key: value for key, value in params.items() if key in accepted_params and value is not None
        }
        # Use site_url from connection if not provided in params
        site_url = params.get("site_url", self.site_url)

        # Default start and end_dates, if provided, replace these defaults (last 30 days)
        if search_analytics_query_request.get('start_date') is None:
            search_analytics_query_request['start_date'] = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')  
        if search_analytics_query_request.get('end_date') is None:
            search_analytics_query_request['end_date'] = datetime.now().strftime('%Y-%m-%d')

        response = (
            service.searchanalytics().query(siteUrl=site_url, body=search_analytics_query_request).execute()
        )

        # Get dimensions from params to determine column structure
        dimensions = params.get("dimensions", [])

        if "rows" not in response:
            return pd.DataFrame(columns=self.analytics.get_columns(dimensions))

        df = pd.DataFrame(response["rows"], columns=self.analytics.get_columns())

        # Expand keys list into separate dimension columns
        if dimensions and 'keys' in df.columns and len(df) > 0:
            keys_data = df['keys'].tolist()
            for i, dim_name in enumerate(dimensions):
                df[dim_name] = [keys[i] if keys and len(keys) > i else None for keys in keys_data]
            df = df.drop('keys', axis=1)

        return df

    def get_sitemaps(self, params: dict = None) -> DataFrame:
        """
        Get sitemaps data from Google Search Console API
        Args:
            params (dict): query parameters
        Returns:
            DataFrame
        """
        service = self.connect()
        # Use site_url from connection if not provided in params
        site_url = params.get("site_url", self.site_url)
        if params.get("sitemap_index"):
            response = service.sitemaps().list(siteUrl=site_url, sitemapIndex=params["sitemap_index"]).execute()
        else:
            response = service.sitemaps().list(siteUrl=site_url).execute()
        if "sitemap" not in response:
            return pd.DataFrame(columns=self.sitemaps.get_columns())
        df = pd.DataFrame(response["sitemap"], columns=self.sitemaps.get_columns())

        # Get as many sitemaps as indicated by the row_limit parameter
        if params.get("row_limit"):
            if params["row_limit"] > len(df):
                row_limit = len(df)
            else:
                row_limit = params["row_limit"]

            df = df[:row_limit]

        return df

    def submit_sitemap(self, params: dict = None) -> DataFrame:
        """
        Submit sitemap to Google Search Console API
        Args:
            params (dict): query parameters
        Returns:
            DataFrame
        """
        service = self.connect()
        # Use site_url from connection if not provided in params
        site_url = params.get("siteUrl", self.site_url)
        response = service.sitemaps().submit(siteUrl=site_url, feedpath=params["feedpath"]).execute()
        df = pd.DataFrame(response, columns=self.sitemaps.get_columns())
        return df

    def delete_sitemap(self, params: dict = None) -> DataFrame:
        """
        Delete sitemap from Google Search Console API
        Args:
            params (dict): query parameters
        Returns:
            DataFrame
        """
        service = self.connect()
        # Use site_url from connection if not provided in params
        site_url = params.get("siteUrl", self.site_url)
        response = service.sitemaps().delete(siteUrl=site_url, feedpath=params["feedpath"]).execute()
        df = pd.DataFrame(response, columns=self.sitemaps.get_columns())
        return df

    def inspect_url(self, params: dict = None) -> DataFrame:
        """
        Inspect a URL using Google Search Console URL Inspection API
        Args:
            params (dict): query parameters including inspectionUrl, siteUrl (optional), languageCode (optional)
        Returns:
            DataFrame
        """
        import json

        self.connect()  # Ensures both services are initialized
        if not self.search_console_service:
            raise Exception(
                "Search Console v1 service not available. "
                "URL Inspection requires the searchconsole v1 API."
            )

        # Use site_url from connection if not provided in params
        site_url = params.get("site_url", self.site_url)
        inspection_url = params["inspection_url"]
        language_code = params.get("language_code", "en-US")

        if not inspection_url:
            if site_url.startswith("sc-domain"):
                inspection_url = f"https://{site_url.split(':')[1]}"
            else:
                inspection_url = site_url

        body = {
            "inspectionUrl": inspection_url,
            "siteUrl": site_url,
            "languageCode": language_code
        }

        response = self.search_console_service.urlInspection().index().inspect(body=body).execute()

        # Extract key data from the nested response structure
        inspection_result = response.get('inspectionResult', {})
        index_status = inspection_result.get('indexStatusResult', {})
        mobile_usability = inspection_result.get('mobileUsabilityResult', {})
        amp_result = inspection_result.get('ampResult', {})
        rich_results = inspection_result.get('richResultsResult', {})

        # Build flattened result
        result = {
            'inspectionUrl': inspection_url,
            'indexStatusVerdict': index_status.get('verdict'),
            'coverageState': index_status.get('coverageState'),
            'robotsTxtState': index_status.get('robotsTxtState'),
            'indexingState': index_status.get('indexingState'),
            'lastCrawlTime': index_status.get('lastCrawlTime'),
            'pageFetchState': index_status.get('pageFetchState'),
            'googleCanonical': index_status.get('googleCanonical'),
            'userCanonical': index_status.get('userCanonical'),
            'crawledAs': index_status.get('crawledAs'),
            'mobileUsabilityVerdict': mobile_usability.get('verdict'),
            'mobileUsabilityIssues': json.dumps(mobile_usability.get('issues', [])),
            'ampInspectionResult': json.dumps(amp_result),
            'richResultsResult': json.dumps(rich_results)
        }
        
        df = pd.DataFrame([result], columns=self.url_inspection.get_columns())
        return df

    def call_application_api(self, method_name: str = None, params: dict = None) -> DataFrame:
        """
        Call Google Search Console API and map the data to pandas DataFrame
        Args:
            method_name (str): method name
            params (dict): query parameters
        Returns:
            DataFrame
        """
        if method_name == "get_traffic_data":
            return self.get_traffic_data(params)
        elif method_name == "get_sitemaps":
            return self.get_sitemaps(params)
        elif method_name == "submit_sitemap":
            return self.submit_sitemap(params)
        elif method_name == "delete_sitemap":
            return self.delete_sitemap(params)
        elif method_name == "inspect_url":
            return self.inspect_url(params)
        else:
            raise NotImplementedError(f"Unknown method {method_name}")
