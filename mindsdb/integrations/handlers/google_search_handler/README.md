# Google Search Console API Integration

This handler integrates with the [Google Search API](https://developers.google.com/webmaster-tools)
to allow you to use Google Search data in your SQL queries.

## Example: Predicting clicks based on Google Search activity

To see how the Google Search Console handler is used, let's walk through a simple example to create a model to predict
our clicks on a site based on Google Search activity.

## Connect to the Google Search API

We start by creating a database to connect to the Google Search Console API. Currently, there is no need for an API key:

However, you will need to have a Google account and have enabled the Google Search Console API.
Also, you will need to have the credentials in a json file. 
You can find more information on how to do
this [here](https://developers.google.com/webmaster-tools/v1/prereqs).

**Optional:**  The credentials file can be stored in the google_search handler folder in
the [mindsdb/integrations/google_search_handler](mindsdb/integrations/handlers/google_search_handler) directory.

~~~~sql
CREATE
DATABASE my_Search
WITH  ENGINE = 'google_search',
parameters = {
    'credentials': '/path/to/credentials.json'
};    
~~~~

This creates a database called my_Search. This database ships with a table called Analytics and with a table called Sitemaps that we can use to search for
Google Search data as well as to process Google Search data.

## Query your search traffic data with filters and parameters that you define

Let's get traffic data for a specific site.

~~~~sql
SELECT *
FROM my_console.Analytics
WHERE siteUrl = 'https://www.mindsdb.com'
  AND start_date = '2020-10-01'
  AND end_date = '2020-10-31'
  AND dimensions = 'query'
~~~~

This will return data with columns: `query`, `clicks`, `impressions`, `ctr`, `position`

### Using Multiple Dimensions

You can specify multiple dimensions to break down your data:

~~~~sql
SELECT *
FROM my_console.Analytics
WHERE siteUrl = 'https://www.mindsdb.com'
  AND start_date = '2020-10-01'
  AND end_date = '2020-10-31'
  AND dimensions IN ('date', 'query', 'country')
LIMIT 100
~~~~

This will return data with columns: `date`, `query`, `country`, `clicks`, `impressions`, `ctr`, `position`

**Note**: The dimension values are automatically expanded into separate columns for easy querying and analysis. Previously, these values were stored in a `keys` array column.

### Available Dimensions

- `date` - The date of the data
- `hour` - The hour of the data (requires `data_state = 'hourly_all'`)
- `query` - The search query
- `page` - The URL of the page
- `country` - The country code
- `device` - The device type (mobile, desktop, tablet)
- `searchAppearance` - The search appearance type

### Filtering by Dimensions

You can filter results by dimension values without including them in grouping:

~~~~sql
-- Filter by query (contains match)
SELECT date, clicks, impressions
FROM my_console.Analytics
WHERE start_date = '2024-01-01'
  AND end_date = '2024-01-31'
  AND dimensions = 'date'
  AND query LIKE '%mindsdb%'
LIMIT 100
~~~~

~~~~sql
-- Filter by country (exact match)
SELECT query, clicks, impressions
FROM my_console.Analytics
WHERE start_date = '2024-01-01'
  AND end_date = '2024-01-31'
  AND dimensions = 'query'
  AND country = 'USA'
LIMIT 100
~~~~

~~~~sql
-- Multiple filters (AND logic)
SELECT page, clicks, impressions
FROM my_console.Analytics
WHERE start_date = '2024-01-01'
  AND end_date = '2024-01-31'
  AND dimensions = 'page'
  AND query LIKE '%tutorial%'
  AND country = 'USA'
  AND device = 'MOBILE'
LIMIT 100
~~~~

#### Supported Filter Operators

- `=` - Exact match (e.g., `country = 'USA'`)
- `!=` - Not equal (e.g., `device != 'DESKTOP'`)
- `LIKE '%term%'` - Contains substring (e.g., `query LIKE '%mindsdb%'`)
- `NOT LIKE '%term%'` - Does not contain (e.g., `query NOT LIKE '%spam%'`)

#### Filterable Dimensions

- `country` - Country code (ISO 3166-1 alpha-3)
- `device` - Device type (DESKTOP, MOBILE, TABLET)
- `page` - Page URL
- `query` - Search query string
- `searchAppearance` - Search appearance type

**Note:** You can filter by a dimension without including it in the `dimensions` parameter for grouping. Multiple filters are combined with AND logic.

## Submit a sitemap to Google Search Console

Let's test by submitting a sitemap to Google Search Console.

~~~~sql
INSERT INTO my_console.Sitemaps (siteUrl, sitemapUrl)
VALUES ('https://www.mindsdb.com', 'https://www.mindsdb.com/sitemap.xml')
~~~~

## Delete a sitemap from Google Search Console

Let's test by deleting a sitemap from Google Search Console.

~~~~sql
DELETE FROM my_console.Sitemaps
WHERE siteUrl = 'https://www.mindsdb.com'
  AND feedpath = 'https://www.mindsdb.com/sitemap.xml'
~~~~



## Creating a model to predict future clicks

Now we can incorporate external data from Google Search within our ML models.

~~~~sql
CREATE
PREDICTOR my_search_clicks
FROM my_search.Analytics
PREDICT
clicks
~~~~