# Sentry Handler

Sentry application handler for MindsDB.

V1 scope:

- `projects` table for organization-scoped project discovery
- `issues` table for project-scoped operational issue inspection
- `logs` table for Explore-backed log inspection
  - includes curated columns plus `extra_json` for raw additional event context
- `logs_timeseries` table for Explore-backed log volume over time
- read-only `SELECT` support

Internal organization:

- `IssueSentryHandler` owns the current `projects` and `issues` flow
- `ExploreSentryHandler` owns the Explore-backed `logs` and `logs_timeseries` flow
- `sentry_handler.py` remains the public compatibility entrypoint
- this layout prepares the package for a future Explore-focused handler without mixing responsibilities

Example connection:

```sql
CREATE DATABASE sentry_datasource
WITH ENGINE = 'sentry',
PARAMETERS = {
  "auth_token": "sntrys_xxx",
  "organization_slug": "talentify",
  "project_slug": "mktplace",
  "environment": "production"
};
```
