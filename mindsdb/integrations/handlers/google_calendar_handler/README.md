# Google Calendar API Integration

This handler integrates with the [Google Calendar API](https://developers.google.com/calendar/api/guides/overview)
to make event data available to use for model training and predictions.

## Example: Automate your Calendar

To see how the Google Calendar handler is used, let's walk through a simple example to create a model to predict
future events in your calendar.

## Connect to the Google Calendar API

We start by creating a database to connect to the Google Calendar API. Currently, there is no need for an API key:

However, you will need to have a Google account and have enabled the Google Calendar API.
Also, you will need to have a calendar created in your Google account and the credentials for that calendar
in a json file. You can find more information on how to do
this [here](https://developers.google.com/calendar/quickstart/python).

**Optional:**  The credentials file can be stored in the google_calendar handler folder in
the [mindsdb/integrations/google_calendar_handler](mindsdb/integrations/handlers/google_calendar_handler) directory.

~~~~sql
CREATE
DATABASE my_calendar
WITH  ENGINE = 'google_calendar',
parameters = {
    'credentials': 'C:\\Users\\panagiotis\\Desktop\\GitHub\\mindsdb\\mindsdb\\integrations\\handlers\\google_calendar_handler\\credentials.json'
};
~~~~

**Optional:** You can specify a default calendar ID to query. This is useful when you want to query a specific calendar or multiple calendars:

~~~~sql
CREATE
DATABASE my_calendar
WITH  ENGINE = 'google_calendar',
parameters = {
    'credentials': 'C:\\Users\\panagiotis\\Desktop\\GitHub\\mindsdb\\mindsdb\\integrations\\handlers\\google_calendar_handler\\credentials.json',
    'calendar_id': 'primary'  -- Or use a specific calendar ID like 'work@example.com' or multiple calendars 'primary,work@example.com'
};
~~~~

This creates a database called my_calendar. This database ships with a table called events that we can use to search for
events as well as to process events.

## Searching for events in SQL

Let's get a list of events in our calendar.

~~~~sql
SELECT id,
       created,
       creator,
       summary
FROM my_calendar.events
WHERE start_time > '2023-02-16'
  AND end_time < '2023-04-09' LIMIT 20;
~~~~

## Querying Multiple Calendars

You can query events from multiple calendars in a single query by specifying a comma-separated list of calendar IDs:

~~~~sql
SELECT id, summary, created, calendar_id
FROM my_calendar.events
WHERE calendar_id = 'primary,work@example.com,personal@example.com'
  AND start_time > '2023-02-16'
  AND end_time < '2023-04-09'
LIMIT 20;
~~~~

The `calendar_id` column in the results will indicate which calendar each event belongs to. This is particularly useful for analyzing schedules across multiple people or resources.

## Querying a Specific Calendar

You can also query events from a specific calendar instead of the default:

~~~~sql
SELECT id, summary, created
FROM my_calendar.events
WHERE calendar_id = 'work@example.com'
  AND start_time > '2023-02-16'
LIMIT 10;
~~~~

## Listing Available Calendars

To see all calendars you have access to:

~~~~sql
SELECT id, summary, access_role, time_zone, primary
FROM my_calendar.calendar_list;
~~~~

This will show calendar IDs, names, access levels, and whether each is the primary calendar. Use the `id` column values when querying specific calendars.

## Checking Free/Busy Status

To check availability across multiple calendars:

~~~~sql
SELECT calendar_id, start, end
FROM my_calendar.free_busy
WHERE calendar_id = 'gabriel@talentify.io,othamar@talentify.io'
  AND time_min = '2026-02-13 09:00:00'
  AND time_max = '2026-02-13 18:00:00'
  AND time_zone = 'UTC-4';
~~~~

This returns busy time blocks for each specified calendar, making it easy to find mutual availability for scheduling.

**Note:** Both `calendar_list` and `free_busy` are read-only tables.

## Creating Events using SQL

Let's test by creating an event in our calendar.

~~~~sql
INSERT INTO my_calendar.events (start_time, end_time, summary, description, location, attendees, calendar_id)
VALUES ('2023-02-16 10:00:00', '2023-02-16 11:00:00', 'MindsDB Meeting', 'Discussing the future of MindsDB',
        'MindsDB HQ', '', 'primary')

~~~~

**Note:** INSERT, UPDATE, and DELETE operations can only target a single calendar at a time. If you need to modify events in multiple calendars, execute separate statements for each calendar.

## Updating Events using SQL

Let's update the event we just created.

~~~~sql
UPDATE my_calendar.events
SET summary     = 'MindsDB Meeting',
    description = 'Discussing the future of MindsDB',
    location    = 'MindsDB HQ',
    attendees   = '',
    reminders   = ''
~~~~

Or you can update all events in a given id range.

~~~~sql
UPDATE my_calendar.events
SET summary     = 'MindsDB Meeting',
    description = 'Discussing the future of MindsDB',
    location    = 'MindsDB HQ',
    attendees   = '',
    reminders   = ''
WHERE event_id > 1
  AND event_id < 10
~~~~

If you have specified only one aspect of the comparison (`>` or `<`), then the `start_id` will be `end_id` - 10 (
if `start_id` is
not defined) and the `end_id` will be `start_id` + 10 (if `end_id` is defined).

## Deleting Events using SQL

Let's delete the event we just created.

~~~~sql
DELETE
FROM my_calendar.events
WHERE id = '1'
~~~~

Or you can delete all events in a given id range.

~~~~sql
DELETE
FROM my_calendar.events
WHERE event_id > 1
  AND event_id < 10
~~~~

If you have specified only one aspect of the comparison (`>` or `<`), then the `start_id` will be `end_id` - 10 (
if `start_id` is
not defined) and the `end_id` will be `start_id` + 10 (if `end_id` is defined).

## Creating a model to predict future events

Now that we have some data in our calendar, we can do smarter scheduling, event recommendations, and other automations.

~~~~sql
CREATE
PREDICTOR predict_future_events
FROM my_calendar.events
PREDICT start_time, end_time, summary, description, location, attendees, reminders
WHERE timeMin = '2023-02-16'
  AND timeMax = '2023-04-09'
~~~~