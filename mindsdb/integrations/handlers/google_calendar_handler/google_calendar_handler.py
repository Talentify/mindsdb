from datetime import datetime, timedelta
import pandas as pd
import re
from googleapiclient.discovery import build
from google.oauth2.credentials import Credentials as OAuthCredentials
from google.auth.transport.requests import Request

from mindsdb.api.executor.data_types.response_type import RESPONSE_TYPE
from mindsdb.integrations.libs.api_handler import APIHandler, FuncParser
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
)
from mindsdb.utilities.config import Config
from mindsdb.utilities import log
from mindsdb.integrations.utilities.handlers.auth_utilities.google import GoogleUserOAuth2Manager
from mindsdb.integrations.utilities.handlers.auth_utilities.exceptions import AuthException

from .google_calendar_tables import (
    GoogleCalendarEventsTable,
    GoogleCalendarListTable,
    GoogleCalendarFreeBusyTable,
)

DEFAULT_SCOPES = [
    "https://www.googleapis.com/auth/calendar.readonly"
]

logger = log.getLogger(__name__)


def camel_to_snake(name):
    """Convert camelCase to snake_case"""
    # Handle special cases
    if name == "iCalUID":
        return "ical_uid"
    # Insert underscore before capital letters and convert to lowercase
    name = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', name).lower()


def snake_to_camel(name):
    """Convert snake_case to camelCase"""
    # Handle special cases
    if name == "ical_uid":
        return "iCalUID"
    if name == "html_link":
        return "htmlLink"
    # Split by underscore and capitalize each word except first
    components = name.split('_')
    return components[0] + ''.join(x.title() for x in components[1:])


def convert_dict_keys_to_snake(data):
    """Recursively convert dictionary keys from camelCase to snake_case"""
    if isinstance(data, dict):
        return {camel_to_snake(k): convert_dict_keys_to_snake(v) for k, v in data.items()}
    elif isinstance(data, list):
        return [convert_dict_keys_to_snake(item) for item in data]
    else:
        return data


def flatten_event_fields(event):
    """
    Flatten nested object fields in event data for easier DataFrame handling.
    Expands simple nested objects (creator, organizer, start, end, etc.) into flat fields.
    """
    flattened = dict(event)

    # Flatten creator object
    if 'creator' in flattened and isinstance(flattened['creator'], dict):
        creator = flattened.pop('creator')
        flattened['creator_id'] = creator.get('id')
        flattened['creator_email'] = creator.get('email')
        flattened['creator_display_name'] = creator.get('display_name')
        flattened['creator_self'] = creator.get('self')

    # Flatten organizer object
    if 'organizer' in flattened and isinstance(flattened['organizer'], dict):
        organizer = flattened.pop('organizer')
        flattened['organizer_id'] = organizer.get('id')
        flattened['organizer_email'] = organizer.get('email')
        flattened['organizer_display_name'] = organizer.get('display_name')
        flattened['organizer_self'] = organizer.get('self')

    # Flatten start object
    if 'start' in flattened and isinstance(flattened['start'], dict):
        start = flattened.pop('start')
        flattened['start_date'] = start.get('date')
        flattened['start_date_time'] = start.get('date_time')
        flattened['start_time_zone'] = start.get('time_zone')

    # Flatten end object
    if 'end' in flattened and isinstance(flattened['end'], dict):
        end = flattened.pop('end')
        flattened['end_date'] = end.get('date')
        flattened['end_date_time'] = end.get('date_time')
        flattened['end_time_zone'] = end.get('time_zone')

    # Flatten original_start_time object
    if 'original_start_time' in flattened and isinstance(flattened['original_start_time'], dict):
        original = flattened.pop('original_start_time')
        flattened['original_start_time_date'] = original.get('date')
        flattened['original_start_time_date_time'] = original.get('date_time')
        flattened['original_start_time_time_zone'] = original.get('time_zone')

    # Flatten source object
    if 'source' in flattened and isinstance(flattened['source'], dict):
        source = flattened.pop('source')
        flattened['source_url'] = source.get('url')
        flattened['source_title'] = source.get('title')

    return flattened


class GoogleCalendarHandler(APIHandler):
    """
    A class for handling connections and interactions with the Google Calendar API.
    """

    name = "google_calendar"

    def __init__(self, name: str, **kwargs):
        """constructor
        Args:
            name (str): the handler name
            credentials_file (str): The path to the credentials file.
            scopes (list): The list of scopes to use for authentication.
            is_connected (bool): Whether the API client is connected to Google Calendar.
            events (GoogleCalendarEventsTable): The `GoogleCalendarEventsTable` object for interacting with the events table.
        """
        super().__init__(name)
        self.connection_data = kwargs.get("connection_data", {})

        self.service = None
        self.is_connected = False

        self.handler_storage = kwargs["handler_storage"]

        self.credentials_url = self.connection_data.get("credentials_url", None)
        self.credentials_file = self.connection_data.get("credentials_file", None)
        if self.connection_data.get("credentials"):
            self.credentials_file = self.connection_data.pop("credentials")
        if not self.credentials_file and not self.credentials_url:
            # try to get from config
            gcalendar_config = Config().get("handlers", {}).get("google_calendar", {})
            secret_file = gcalendar_config.get("credentials_file")
            secret_url = gcalendar_config.get("credentials_url")
            if secret_file:
                self.credentials_file = secret_file
            elif secret_url:
                self.credentials_url = secret_url

        self.scopes = self.connection_data.get("scopes", DEFAULT_SCOPES)
        if isinstance(self.scopes, str):
            self.scopes = [scope.strip() for scope in self.scopes.split(',') if scope.strip()]

        self.default_calendar_id = self.connection_data.get("calendar_id", "primary")

        events = GoogleCalendarEventsTable(self)
        self.events = events
        self._register_table("events", events)

        calendar_list = GoogleCalendarListTable(self)
        self.calendar_list = calendar_list
        self._register_table("calendar_list", calendar_list)

        free_busy = GoogleCalendarFreeBusyTable(self)
        self.free_busy = free_busy
        self._register_table("free_busy", free_busy)

    def connect(self, **kwargs):
        """
        Set up any connections required by the handler
        Should return output of check_connection() method after attempting
        connection. Should switch self.is_connected.
        Returns:
            HandlerStatusResponse
        """
        if self.is_connected and self.service is not None:
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
                raise Exception('google_calendar_handler: client_id and client_secret are required when refresh_token is provided')

            creds = OAuthCredentials(
                token=None,
                refresh_token=refresh_token,
                token_uri=token_uri,
                client_id=client_id,
                client_secret=client_secret,
                scopes=scopes
            )

            creds.refresh(Request())
            self.service = build('calendar', 'v3', credentials=creds)
            self.is_connected = True
            return self.service

        google_oauth2_manager = GoogleUserOAuth2Manager(
            self.handler_storage,
            self.scopes,
            self.credentials_file,
            self.credentials_url,
            self.connection_data.get("code"),
        )
        creds = google_oauth2_manager.get_oauth2_credentials()

        self.service = build("calendar", "v3", credentials=creds)

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
            logger.error(f"Error connecting to Google Calendar API: {e}!")
            response.error_message = e

        self.is_connected = response.success
        return response

    def _normalize_calendar_ids(self, calendar_id_param=None):
        """
        Normalize calendar_id parameter to a list of calendar IDs.

        Args:
            calendar_id_param: Single calendar ID, comma-separated string, or list

        Returns:
            list: List of calendar IDs
        """
        if calendar_id_param is None:
            calendar_id_param = self.default_calendar_id

        if isinstance(calendar_id_param, list):
            return calendar_id_param
        elif isinstance(calendar_id_param, str):
            return [cid.strip() for cid in calendar_id_param.split(',') if cid.strip()]
        else:
            return [str(calendar_id_param)]

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

    def get_events(self, params: dict = None) -> pd.DataFrame:
        """
        Get events from Google Calendar API
        Args:
            params (dict): query parameters, may include 'calendar_id'
        Returns:
            DataFrame
        """
        service = self.connect()

        # Extract and normalize calendar IDs from params or use default
        calendar_ids = self._normalize_calendar_ids(params.get("calendar_id"))
        if not calendar_ids:
            raise ValueError("calendar_id is required for FreeBusy queries")            

        # Defaults for timeMin and timeMax can be set here if desired
        if params.get("time_min") is None:
            params["time_min"] = datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ") # Default to now
        if params.get("time_max") is None:
            params["time_max"] = (datetime.now() + timedelta(days=7)).strftime("%Y-%m-%dT23:59:59Z") # Default to 7 days
        # Defaults to user's timezone
        if params.get("time_zone") is None:
            params["time_zone"] = service.settings().get(setting="timezone").execute().get("value", "UTC")

        all_events = pd.DataFrame(columns=self.events.get_columns())
        
        body = {
            "timeMin": params.get("time_min"),
            "timeMax": params.get("time_max"),
            "timeZone": params.get("time_zone", "UTC"),
        }

        # Fetch events from each calendar
        for calendar_id in calendar_ids:
            try:
                page_token = None
                while True:
                    logger.info(f"Fetching events for calendar {calendar_id} with params: {body} and page_token: {page_token}")
                    events_result = service.events().list(
                        calendarId=calendar_id,
                        **(body or {})
                    ).execute()

                    items = events_result.get("items", [])
                    if items:
                        # Convert camelCase keys to snake_case
                        items_snake = [convert_dict_keys_to_snake(item) for item in items]
                        # Flatten nested object fields
                        items_flattened = [flatten_event_fields(item) for item in items_snake]
                        events_df = pd.DataFrame(items_flattened, columns=self.events.get_columns())
                        # Add calendar_id column for clarity
                        events_df["calendar_id"] = calendar_id
                        all_events = pd.concat([all_events, events_df], ignore_index=True)

                    page_token = events_result.get("nextPageToken")
                    if not page_token:
                        break

            except Exception as e:
                logger.error(f"Error fetching events from calendar {calendar_id}: {e}")
                # Continue with next calendar instead of failing completely
                continue

        return all_events

    def get_calendar_list(self, params: dict = None) -> pd.DataFrame:
        """
        Get list of calendars accessible to the user.
        Filters by calendar_id from connection params if specified.

        Args:
            params (dict): query parameters
        Returns:
            DataFrame with calendar metadata
        """
        service = self.connect()

        # Get all calendars from API
        try:
            calendar_list_result = service.calendarList().list().execute()
            items = calendar_list_result.get("items", [])

            if not items:
                return pd.DataFrame([], columns=self.calendar_list.get_columns())

            # Filter by calendar_id if specified in connection params
            calendar_ids = self._normalize_calendar_ids(self.default_calendar_id)

            # If default is "primary", show all calendars
            # Otherwise, filter to only specified calendars
            if calendar_ids != ["primary"]:
                items = [item for item in items if item.get("id") in calendar_ids]

            # Convert camelCase keys to snake_case
            items_snake = [convert_dict_keys_to_snake(item) for item in items]

            # Create DataFrame
            calendars_df = pd.DataFrame(items_snake, columns=self.calendar_list.get_columns())

            return calendars_df

        except Exception as e:
            logger.error(f"Error fetching calendar list: {e}")
            return pd.DataFrame([], columns=self.calendar_list.get_columns())

    def get_free_busy(self, params: dict = None) -> pd.DataFrame:
        """
        Query free/busy information for specified calendars.

        Args:
            params (dict)
        Returns:
            DataFrame with busy time blocks for each calendar
        """
        service = self.connect()

        # Extract and normalize calendar IDs from params or use default
        calendar_ids = self._normalize_calendar_ids(params.get("calendar_id"))
        if not calendar_ids:
            raise ValueError("calendar_id is required for FreeBusy queries")            

        # Defaults for timeMin and timeMax can be set here if desired
        if params.get("time_min") is None:
            params["time_min"] = datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ") # Default to now
        if params.get("time_max") is None:
            params["time_max"] = (datetime.now() + timedelta(days=7)).strftime("%Y-%m-%dT23:59:59Z") # Default to 7 days
        # Defaults to user's timezone
        if params.get("time_zone") is None:
            params["time_zone"] = service.settings().get(setting="timezone").execute().get("value", "UTC")
        
        # Build request body
        body = {
            "items": [{"id": cal_id} for cal_id in calendar_ids],
            "timeMin": params.get("time_min"),
            "timeMax": params.get("time_max"),
            "timeZone": params.get("time_zone", "UTC"),
        }
        try:
            # Call freebusy API
            freebusy_result = service.freebusy().query(body=body).execute()

            # Extract busy periods for each calendar
            calendars_data = freebusy_result.get("calendars", {})

            all_busy_times = []
            for calendar_id, calendar_data in calendars_data.items():
                busy_periods = calendar_data.get("busy", [])
                for period in busy_periods:
                    all_busy_times.append({
                        "calendar_id": calendar_id,
                        "status": "busy",
                        "start": period.get("start"),
                        "end": period.get("end"),
                        "time_zone": params.get("time_zone", "UTC")
                    })

            if not all_busy_times:
                return pd.DataFrame([], columns=self.free_busy.get_columns())

            return pd.DataFrame(all_busy_times, columns=self.free_busy.get_columns())

        except Exception as e:
            logger.error(f"Error fetching free/busy information: {e}")
            return pd.DataFrame([], columns=self.free_busy.get_columns())

    def create_event(self, params: dict = None) -> pd.DataFrame:
        """
        Create an event in the calendar.
        Args:
            params (dict): query parameters, may include 'calendar_id'
        Returns:
            DataFrame
        """
        service = self.connect()

        # Extract and validate calendar ID for write operation
        calendar_id_param = params.pop("calendar_id", None) if params else None
        calendar_ids = self._normalize_calendar_ids(calendar_id_param)

        if len(calendar_ids) > 1:
            raise ValueError("INSERT operations can only target a single calendar. Please specify one calendar_id.")

        calendar_id = calendar_ids[0]

        # Check if 'attendees' is a string and split it into a list
        if params and isinstance(params.get("attendees"), str):
            params["attendees"] = params["attendees"].split(",")

        event = {
            "summary": params["summary"],
            "location": params["location"],
            "description": params["description"],
            "start": {
                "dateTime": params["start"]["dateTime"],
                "timeZone": params["start"]["timeZone"],
            },
            "end": {
                "dateTime": params["end"]["dateTime"],
                "timeZone": params["end"]["timeZone"],
            },
            "recurrence": ["RRULE:FREQ=DAILY;COUNT=1"],
            "attendees": [
                {"email": attendee["email"]}
                for attendee in (
                    params["attendees"] if isinstance(params["attendees"], list) else [params["attendees"]]
                )
            ],
            "reminders": {
                "useDefault": False,
                "overrides": [
                    {"method": "email", "minutes": 24 * 60},
                    {"method": "popup", "minutes": 10},
                ],
            },
        }

        event = service.events().insert(calendarId=calendar_id, body=event).execute()
        # Convert camelCase keys to snake_case and flatten nested objects
        event_snake = convert_dict_keys_to_snake(event)
        event_flattened = flatten_event_fields(event_snake)
        result_df = pd.DataFrame([event_flattened], columns=self.events.get_columns())
        result_df["calendar_id"] = calendar_id
        return result_df

    def update_event(self, params: dict = None) -> pd.DataFrame:
        """
        Update event or events in the calendar.
        Args:
            params (dict): query parameters, may include 'calendar_id'
        Returns:
            DataFrame
        """
        service = self.connect()

        # Extract and validate calendar ID for write operation
        calendar_id_param = params.pop("calendar_id", None) if params else None
        calendar_ids = self._normalize_calendar_ids(calendar_id_param)

        if len(calendar_ids) > 1:
            raise ValueError("UPDATE operations can only target a single calendar. Please specify one calendar_id.")

        calendar_id = calendar_ids[0]

        df = pd.DataFrame(columns=["eventId", "status", "calendar_id"])

        if params.get("event_id"):
            start_id = int(params["event_id"])
            end_id = start_id + 1
        elif not params.get("start_id"):
            start_id = int(params["end_id"]) - 10
            end_id = int(params["end_id"])
        elif not params.get("end_id"):
            start_id = int(params["start_id"])
            end_id = start_id + 10
        else:
            start_id = int(params["start_id"])
            end_id = int(params["end_id"])

        for i in range(start_id, end_id):
            try:
                event = service.events().get(calendarId=calendar_id, eventId=str(i)).execute()
                if params.get("summary"):
                    event["summary"] = params["summary"]
                if params.get("location"):
                    event["location"] = params["location"]
                if params.get("description"):
                    event["description"] = params["description"]
                if params.get("start"):
                    event["start"]["dateTime"] = params["start"]["dateTime"]
                    event["start"]["timeZone"] = params["start"]["timeZone"]
                if params.get("end"):
                    event["end"]["dateTime"] = params["end"]["dateTime"]
                    event["end"]["timeZone"] = params["end"]["timeZone"]
                if params.get("attendees"):
                    event["attendees"] = [{"email": attendee} for attendee in params["attendees"].split(",")]

                updated_event = service.events().update(
                    calendarId=calendar_id,
                    eventId=event["id"],
                    body=event
                ).execute()

                df = pd.concat(
                    [df, pd.DataFrame([{
                        "eventId": updated_event["id"],
                        "status": "updated",
                        "calendar_id": calendar_id
                    }])],
                    ignore_index=True
                )
            except Exception as e:
                logger.error(f"Error updating event {i} in calendar {calendar_id}: {e}")
                continue

        return df

    def delete_event(self, params):
        """
        Delete event or events in the calendar.
        Args:
            params (dict): query parameters, may include 'calendar_id'
        Returns:
            DataFrame
        """
        service = self.connect()

        # Extract and validate calendar ID for write operation
        calendar_id_param = params.pop("calendar_id", None) if params else None
        calendar_ids = self._normalize_calendar_ids(calendar_id_param)
        
        if len(calendar_ids) > 1:
            raise ValueError("DELETE operations can only target a single calendar. Please specify one calendar_id.")

        calendar_id = calendar_ids[0]

        if params.get("event_id"):
            try:
                service.events().delete(calendarId=calendar_id, eventId=params["event_id"]).execute()
                return pd.DataFrame([{
                    "eventId": params["event_id"],
                    "status": "deleted",
                    "calendar_id": calendar_id
                }])
            except Exception as e:
                logger.error(f"Error deleting event {params['event_id']} from calendar {calendar_id}: {e}")
                raise
        else:
            df = pd.DataFrame(columns=["eventId", "status", "calendar_id"])

            if not params.get("start_id"):
                start_id = int(params["end_id"]) - 10
                end_id = int(params["end_id"])
            elif not params.get("end_id"):
                start_id = int(params["start_id"])
                end_id = start_id + 10
            else:
                start_id = int(params["start_id"])
                end_id = int(params["end_id"])

            for i in range(start_id, end_id):
                try:
                    service.events().delete(calendarId=calendar_id, eventId=str(i)).execute()
                    df = pd.concat([df, pd.DataFrame([{
                        "eventId": str(i),
                        "status": "deleted",
                        "calendar_id": calendar_id
                    }])], ignore_index=True)
                except Exception as e:
                    logger.error(f"Error deleting event {i} from calendar {calendar_id}: {e}")
                    continue

            return df

    def call_application_api(self, method_name: str = None, params: dict = None) -> pd.DataFrame:
        """
        Call Google Calendar API and map the data to pandas DataFrame
        Args:
            method_name (str): method name
            params (dict): query parameters
        Returns:
            DataFrame
        """
        if method_name == "get_events":
            return self.get_events(params)
        elif method_name == "create_event":
            return self.create_event(params)
        elif method_name == "update_event":
            return self.update_event(params)
        elif method_name == "delete_event":
            return self.delete_event(params)
        elif method_name == "get_calendar_list":
            return self.get_calendar_list(params)
        elif method_name == "get_free_busy":
            return self.get_free_busy(params)
        else:
            raise NotImplementedError(f"Unknown method {method_name}")
