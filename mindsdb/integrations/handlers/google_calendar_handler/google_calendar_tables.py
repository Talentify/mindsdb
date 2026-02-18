import pandas as pd
from datetime import datetime, time
from mindsdb_sql_parser import ast
from pandas import DataFrame
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from mindsdb.integrations.libs.api_handler import APITable
from mindsdb.integrations.utilities.date_utils import utc_date_str_to_timestamp_ms, parse_local_date
from mindsdb.integrations.utilities.sql_utils import extract_comparison_conditions

def format_time_min(date_str: str) -> str:
    """Converts a date string to UTC ISO format expected by Google Calendar API."""
    dt = parse_local_date(date_str)
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")

def format_time_max(date_str: str) -> str:
    """Converts a date string to UTC ISO format expected by Google Calendar API."""
    dt = parse_local_date(date_str)
    if dt.time() == time(0, 0):
        dt = dt.replace(hour=23, minute=59, second=59)
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def is_valid_timezone(tz: str) -> bool:
    try:
        ZoneInfo(tz)
        return True
    except ZoneInfoNotFoundError:
        return False

class GoogleCalendarEventsTable(APITable):
    def select(self, query: ast.Select) -> DataFrame:
        """
        Gets all events from the calendar.

        Args:
            query (ast.Select): SQL query to parse.

        Returns:
            Response: Response object containing the results.
        """

        # Parse the query to get the conditions.
        conditions = extract_comparison_conditions(query.where)
        # Get the start and end times from the conditions.
        params = {}
        for op, arg1, arg2 in conditions:
            # Handle both snake_case and camelCase for backward compatibility
            if arg1 in ["time_max"]:
                date = format_time_max(arg2)
                if op == "=":
                    params["time_max"] = date
                else:
                    raise NotImplementedError(f"Operator {op} not supported for {arg1}. Use only '=' operator.")
            elif arg1 in ["time_min"]:
                date = format_time_min(arg2)
                if op == "=":
                    params["time_min"] = date
                else:
                    raise NotImplementedError(f"Operator {op} not supported for {arg1}. Use only '=' operator.")
            elif arg1 in ["time_zone"]:
                if op == "=":
                    if not is_valid_timezone(arg2):
                        raise ValueError(f"Invalid timezone: {arg2}")
                    params["time_zone"] = arg2
                else:
                    raise NotImplementedError(f"Operator {op} not supported for {arg1}. Use only '=' operator.")
            elif arg1 == "q":
                params["q"] = arg2
            elif arg1 == "calendar_id":
                if op in ["=", "in"]:
                    params["calendar_id"] = arg2
                else:
                    raise NotImplementedError(f"Operator {op} not supported for calendar_id. Use only '=' or 'IN' operator.")

        # Get the order by from the query.
        if query.order_by is not None:
            order_col = query.order_by[0].value
            if order_col in ["start_time", "start"]:
                params["order_by"] = "startTime"
            elif order_col == "updated":
                params["order_by"] = "updated"
            else:
                raise NotImplementedError(f"ORDER BY {order_col} not supported")

        if query.limit is not None:
            params["max_results"] = query.limit.value

        # Get the events from the Google Calendar API.
        events = self.handler.call_application_api(method_name="get_events", params=params)

        selected_columns = []
        for target in query.targets:
            if isinstance(target, ast.Star):
                selected_columns = self.get_columns()
                break
            elif isinstance(target, ast.Identifier):
                selected_columns.append(target.parts[-1])
            else:
                # Complex expression (CASE WHEN, SUM, etc.) — the outer DuckDB
                # layer handles the computation; return all raw columns so it can.
                selected_columns = self.get_columns()
                break
        if not selected_columns:
            selected_columns = self.get_columns()

        if len(events) == 0:
            events = pd.DataFrame([], columns=selected_columns)
        else:
            events.columns = self.get_columns()
            for col in set(events.columns).difference(set(selected_columns)):
                events = events.drop(col, axis=1)
        return events

    def insert(self, query: ast.Insert):
        """
        Inserts an event into the calendar.

        Args:
            query (ast.Insert): SQL query to parse.

        Returns:
            Response: Response object containing the results.
        """

        # Get the values from the query.
        values = query.values[0]
        # Get the event data from the values.
        event_data = {}
        timestamp_columns = {"start_time", "end_time", "created", "updated"}
        regular_columns = {
            "summary",
            "description",
            "location",
            "status",
            "html_link",
            "creator",
            "organizer",
            "reminders",
            "time_zone",
            "calendar_id",
            "attendees",
        }

        # TODO: check why query.columns is None
        for col, val in zip(query.columns, values):
            if col.name in timestamp_columns:
                event_data[col.name] = utc_date_str_to_timestamp_ms(val)
            elif col.name in regular_columns:
                event_data[col.name] = val
            else:
                raise NotImplementedError

        # st = datetime.datetime.fromtimestamp(event_data["start_time"] / 1000, datetime.timezone.utc).isoformat() + "Z"
        # et = datetime.datetime.fromtimestamp(event_data["end_time"] / 1000, datetime.timezone.utc).isoformat() + "Z"

        # event_data["start"] = {"dateTime": st, "timeZone": event_data["timeZone"]}

        # event_data["end"] = {"dateTime": et, "timeZone": event_data["timeZone"]}

        # event_data["attendees"] = event_data["attendees"].split(",")
        # event_data["attendees"] = [{"email": attendee} for attendee in event_data["attendees"]]

        # Insert the event into the Google Calendar API.
        # self.handler.call_application_api(method_name="create_event", params=event_data)

    def update(self, query: ast.Update):
        """
        Updates an event or events in the calendar.

        Args:
            query (ast.Update): SQL query to parse.

        Returns:
            Response: Response object containing the results.
        """

        # Get the values from the query.
        values = query.values[0]
        # Get the event data from the values.
        event_data = {}
        for col, val in zip(query.update_columns, values):
            if col == "start_time" or col == "end_time" or col == "created" or col == "updated":
                event_data[col] = utc_date_str_to_timestamp_ms(val)
            elif (
                col == "summary"
                or col == "description"
                or col == "location"
                or col == "status"
                or col == "html_link"
                or col == "creator"
                or col == "organizer"
                or col == "reminders"
                or col == "timeZone"
                or col == "calendar_id"
                or col == "attendees"
            ):
                event_data[col] = val
            else:
                raise NotImplementedError

        # event_data["start"] = {"dateTime": event_data["start_time"], "timeZone": event_data["timeZone"]}

        # event_data["end"] = {"dateTime": event_data["end_time"], "timeZone": event_data["timeZone"]}

        # event_data["attendees"] = event_data.get("attendees").split(",")
        # event_data["attendees"] = [{"email": attendee} for attendee in event_data["attendees"]]

        conditions = extract_comparison_conditions(query.where)
        for op, arg1, arg2 in conditions:
            if arg1 == "event_id":
                if op == "=":
                    event_data["event_id"] = arg2
                elif op == ">":
                    event_data["start_id"] = arg2
                elif op == "<":
                    event_data["end_id"] = arg2
                else:
                    raise NotImplementedError
            else:
                raise NotImplementedError

        # Update the event in the Google Calendar API.
        self.handler.call_application_api(method_name="update_event", params=event_data)

    def delete(self, query: ast.Delete):
        """
        Deletes an event or events in the calendar.

        Args:
            query (ast.Delete): SQL query to parse.

        Returns:
            Response: Response object containing the results.
        """

        # Parse the query to get the conditions.
        conditions = extract_comparison_conditions(query.where)
        # Get the start and end times from the conditions.
        params = {}
        for op, arg1, arg2 in conditions:
            if arg1 == "event_id":
                if op == "=":
                    params[arg1] = arg2
                elif op == ">":
                    params["start_id"] = arg2
                elif op == "<":
                    params["end_id"] = arg2
                else:
                    raise NotImplementedError

        # Delete the events in the Google Calendar API.
        self.handler.call_application_api(method_name="delete_event", params=params)

    def get_columns(self) -> list:
        """Gets all columns to be returned in pandas DataFrame responses"""
        return [
            # Identifiers
            "kind",
            "etag",
            "id",
            "ical_uid",

            # Basic info
            "status",
            "event_type",
            "summary",
            "description",
            "location",

            # Links
            "html_link",
            "hangout_link",

            # Timestamps
            "created",
            "updated",

            # Creator (expanded from nested object)
            "creator_id",
            "creator_email",
            "creator_display_name",
            "creator_self",

            # Organizer (expanded from nested object)
            "organizer_id",
            "organizer_email",
            "organizer_display_name",
            "organizer_self",

            # Attendees (kept as array)
            "attendees",

            # Start time (expanded from nested object)
            "start_date",
            "start_date_time",
            "start_time_zone",

            # End time (expanded from nested object)
            "end_date",
            "end_date_time",
            "end_time_zone",

            # Original start time (expanded from nested object)
            "original_start_time_date",
            "original_start_time_date_time",
            "original_start_time_time_zone",

            # Time metadata
            "end_time_unspecified",

            # Recurrence
            "recurrence",
            "recurring_event_id",

            # Display & behavior
            "color_id",
            "visibility",
            "transparency",

            # Conferencing (kept as nested - too complex)
            "conference_data",

            # Metadata
            "sequence",
            "reminders",
            "attachments",
            "extended_properties",

            # Source (expanded from nested object)
            "source_url",
            "source_title",

            "locked",
            "attendees_omitted",

            # Permissions
            "anyone_can_add_self",
            "guests_can_invite_others",
            "guests_can_modify",
            "guests_can_see_other_guests",
            "private_copy",

            # Special event types
            "working_location_properties",
            "out_of_office_properties",
            "focus_time_properties",
            "birthday_properties",

            # Deprecated (included for completeness)
            "gadget",

            # Handler-added fields (not from API)
            "calendar_id",
        ]


class GoogleCalendarListTable(APITable):
    """Table for querying calendar list metadata"""

    def select(self, query: ast.Select) -> DataFrame:
        """
        Gets list of calendars accessible to the user.

        Supports filtering by calendar_id from connection params.
        """
        # Parse WHERE conditions (if any)
        conditions = extract_comparison_conditions(query.where) if query.where else []
        params = {}

        # Extract selected columns
        selected_columns = []
        for target in query.targets:
            if isinstance(target, ast.Star):
                selected_columns = self.get_columns()
                break
            elif isinstance(target, ast.Identifier):
                selected_columns.append(target.parts[-1])
            else:
                # Complex expression — return all raw columns for DuckDB to process.
                selected_columns = self.get_columns()
                break
        if not selected_columns:
            selected_columns = self.get_columns()

        # Call handler method
        calendars = self.handler.call_application_api(
            method_name="get_calendar_list",
            params=params
        )

        # Format results
        if len(calendars) == 0:
            calendars = pd.DataFrame([], columns=selected_columns)
        else:
            calendars.columns = self.get_columns()
            for col in set(calendars.columns).difference(set(selected_columns)):
                calendars = calendars.drop(col, axis=1)

        return calendars

    def get_columns(self) -> list:
        """Gets all columns for calendar list"""
        return [
            "kind",
            "etag",
            "id",
            "summary",
            "description",
            "time_zone",
            "color_id",
            "background_color",
            "foreground_color",
            "hidden",
            "selected",
            "access_role",
            "primary",
            "deleted",
        ]

    def insert(self, query: ast.Insert):
        raise NotImplementedError("CalendarList table is read-only")

    def update(self, query: ast.Update):
        raise NotImplementedError("CalendarList table is read-only")

    def delete(self, query: ast.Delete):
        raise NotImplementedError("CalendarList table is read-only")


class GoogleCalendarFreeBusyTable(APITable):
    """Table for querying free/busy information"""

    def select(self, query: ast.Select) -> DataFrame:
        """
        Gets free/busy information for specified calendars.

        Required WHERE conditions:
        - calendar_id (single or comma-separated list)
        - time_min (start time)
        - time_max (end time)

        Optional:
        - time_zone (defaults to UTC)
        """
        # Parse WHERE conditions
        conditions = extract_comparison_conditions(query.where) if query.where else []
        params = {}

        for op, arg1, arg2 in conditions:
            # Handle both snake_case and camelCase for backward compatibility
            if arg1 in ["time_min"]:
                if op == "=":
                    params["time_min"] = format_time_min(arg2)
                else:
                    raise NotImplementedError(f"Operator {op} not supported for {arg1}. Use only '=' operator.")
            elif arg1 in ["time_max"]:
                if op == "=":
                    params["time_max"] = format_time_max(arg2)
                else:
                    raise NotImplementedError(f"Operator {op} not supported for {arg1}. Use only '=' operator.")
            elif arg1 == "calendar_id":
                if op in ("=", "in"):
                    params["calendar_id"] = arg2
                else:
                    raise NotImplementedError(f"Operator {op} not supported for calendar_id. Use only '=' or 'IN' operator.")
            elif arg1 in ["time_zone"]:
                if op == "=":
                    if not is_valid_timezone(arg2):
                        raise ValueError(f"Invalid timezone: {arg2}")
                    params["time_zone"] = arg2
                else:
                    raise NotImplementedError(f"Operator {op} not supported for time_zone. Use only '=' operator.")

        # Extract selected columns
        selected_columns = []
        for target in query.targets:
            if isinstance(target, ast.Star):
                selected_columns = self.get_columns()
                break
            elif isinstance(target, ast.Identifier):
                selected_columns.append(target.parts[-1])
            else:
                # Complex expression — return all raw columns for DuckDB to process.
                selected_columns = self.get_columns()
                break
        if not selected_columns:
            selected_columns = self.get_columns()

        # Call handler method
        busy_times = self.handler.call_application_api(
            method_name="get_free_busy",
            params=params
        )

        # Format results
        if len(busy_times) == 0:
            busy_times = pd.DataFrame([], columns=selected_columns)
        else:
            busy_times.columns = self.get_columns()
            for col in set(busy_times.columns).difference(set(selected_columns)):
                busy_times = busy_times.drop(col, axis=1)

        return busy_times

    def get_columns(self) -> list:
        """Gets all columns for free/busy data"""
        return [
            "calendar_id",
            "status",
            "start",
            "end",
            "time_zone"
        ]

    def insert(self, query: ast.Insert):
        raise NotImplementedError("FreeBusy table is read-only")

    def update(self, query: ast.Update):
        raise NotImplementedError("FreeBusy table is read-only")

    def delete(self, query: ast.Delete):
        raise NotImplementedError("FreeBusy table is read-only")
