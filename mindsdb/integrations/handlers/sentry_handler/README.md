# Sentry Handler

Sentry application handler for MindsDB.

V1 scope:

- `projects` table for organization-scoped project discovery
- `issues` table for project-scoped operational issue inspection
- read-only `SELECT` support

Internal organization:

- `IssueSentryHandler` owns the current `projects` and `issues` flow
- `sentry_handler.py` remains the public compatibility entrypoint
- this layout prepares the package for a future Explore-focused handler without mixing responsibilities
