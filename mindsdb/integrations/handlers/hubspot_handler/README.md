# HubSpot Handler

HubSpot handler for MindsDB provides interfaces to connect to HubSpot via APIs and pull store data into MindsDB.

---

## Table of Contents

- [HubSpot Handler](#hubspot-handler)
  - [Table of Contents](#table-of-contents)
  - [About HubSpot](#about-hubspot)
  - [HubSpot Handler Implementation](#hubspot-handler-implementation)
  - [HubSpot Handler Initialization](#hubspot-handler-initialization)
  - [Implemented Features](#implemented-features)
  - [Example Usage](#example-usage)

---

## About HubSpot

HubSpot is a CRM platform with all the software, integrations, and resources you need to connect your marketing, sales, content management, and customer service.
<br>
https://www.hubspot.com/products?hubs_content=www.hubspot.com%2F&hubs_content-cta=All%20Products%20and%20Features

## HubSpot Handler Implementation

This handler was implemented using [hubspot-api-client
](https://github.com/HubSpot/hubspot-api-python), the Python library for the HubSpot API.

## HubSpot Handler Initialization

The HubSpot handler supports OAuth2 authentication with token injection and automatic token refresh.

### Authentication Parameters

#### Required (at least one must be provided):
- `access_token`: HubSpot OAuth2 access token for API authentication
- `refresh_token`: HubSpot OAuth2 refresh token for automatic token refresh

#### Optional (recommended for token refresh):
- `client_id`: OAuth2 application client ID (required for automatic token refresh)
- `client_secret`: OAuth2 application client secret (required for automatic token refresh)
- `hub_id`: HubSpot Hub ID (Portal ID). If not provided, will be automatically extracted from token info

#### OAuth Application Setup
To use OAuth authentication, you need to create an OAuth app in HubSpot:
1. Go to your HubSpot account settings
2. Navigate to "Integrations" > "Private Apps" or create an OAuth app
3. Obtain your `client_id`, `client_secret`, `access_token`, and `refresh_token`
4. [Read more about HubSpot OAuth](https://developers.hubspot.com/docs/api/oauth-quickstart-guide)

## Implemented Features

- [x] HubSpot Companies Table for a given account
  - [x] Support SELECT
    - [x] Support LIMIT
    - [x] Support WHERE
    - [x] Support ORDER BY
    - [x] Support column selection
  - [x] Support INSERT
  - [x] Support UPDATE
  - [x] Support DELETE
- [x] HubSpot Contacts Table for a given account
  - [x] Support SELECT
    - [x] Support LIMIT
    - [x] Support WHERE
    - [x] Support ORDER BY
    - [x] Support column selection
  - [x] Support INSERT
  - [x] Support UPDATE
  - [x] Support DELETE
- [x] HubSpot Deals Intents Table for a given account
  - [x] Support SELECT
    - [x] Support LIMIT
    - [x] Support WHERE
    - [x] Support ORDER BY
    - [x] Support column selection
  - [x] Support INSERT
  - [x] Support UPDATE
  - [x] Support DELETE

## TODO

- [ ] HubSpot Leads table
- [ ] HubSpot Products table
- [ ] Many more

## Example Usage

### Method 1: OAuth with Automatic Token Refresh (Recommended)

Create a database connection with OAuth tokens and automatic refresh capability:

~~~~sql
CREATE DATABASE hubspot_datasource
WITH ENGINE = 'hubspot',
PARAMETERS = {
  "access_token": "your_access_token",
  "refresh_token": "your_refresh_token",
  "client_id": "your_client_id",
  "client_secret": "your_client_secret",
  "hub_id": "your_hub_id"  -- Optional
};
~~~~

**Benefits:**
- Tokens are automatically refreshed when they expire
- Tokens are securely stored and persisted across sessions
- No manual token management required

### Method 2: Access Token Only (Legacy)

Create a database connection with just an access token:

~~~~sql
CREATE DATABASE hubspot_datasource
WITH ENGINE = 'hubspot',
PARAMETERS = {
  "access_token": "your_access_token"
};
~~~~

**Note:** Without `refresh_token` and client credentials, you'll need to manually update the access token when it expires.

Use the established connection to query your database:

### Querying the Companies Data
~~~~sql
SELECT * FROM hubspot_datasource.companies
~~~~

or, for the `contacts` table
~~~~sql
SELECT * FROM hubspot_datasource.contacts
~~~~

or, for the `deals` table
~~~~sql
SELECT * FROM hubspot_datasource.deals
~~~~

Run more advanced queries:

~~~~sql
SELECT name, industry
FROM hubspot_datasource.companies
WHERE city = 'bangalore'
ORDER BY name
LIMIT 5
~~~~

~~~~sql
INSERT INTO hubspot_datasource.companies(name)
VALUES('company_name')
~~~~

~~~~sql
UPDATE hubspot_datasource.companies
SET name = 'company_name_updated'
WHERE name = 'company_name'
~~~~

~~~~sql
DELETE FROM hubspot_datasource.companies
WHERE name = 'company_name_updated'
~~~~

~~~~sql
SELECT email, company
FROM hubspot_datasource.contacts
WHERE company = 'company_name'
ORDER BY email
LIMIT 5
~~~~

~~~~sql
INSERT INTO hubspot_datasource.contacts(email)
VALUES('contact_email')
~~~~

~~~~sql
UPDATE hubspot_datasource.contacts
SET email = 'contact_email_updated'
WHERE email = 'contact_email'
~~~~

~~~~sql
DELETE FROM hubspot_datasource.contacts
WHERE email = 'contact_email_updated'
~~~~

~~~~sql
SELECT dealname, amount
FROM hubspot_datasource.deals
WHERE dealstage = 'deal_stage_name'
ORDER BY dealname
LIMIT 5
~~~~

~~~~sql
INSERT INTO hubspot_datasource.deals(dealname)
VALUES('deal_name')
~~~~

~~~~sql
UPDATE hubspot_datasource.deals
SET dealname = 'deal_name_updated'
WHERE dealname = 'deal_name'
~~~~

~~~~sql
DELETE FROM hubspot_datasource.deals
WHERE dealname = 'deal_name_updated'
~~~~
