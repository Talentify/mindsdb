---
title: Amazon S3
sidebarTitle: Amazon S3
---

This documentation describes the integration of MindsDB with [Amazon S3](https://docs.aws.amazon.com/AmazonS3/latest/userguide/Welcome.html), an object storage service that offers industry-leading scalability, data availability, security, and performance.

## Prerequisites

Before proceeding, ensure that MindsDB is installed locally via [Docker](/setup/self-hosted/docker) or [Docker Desktop](/setup/self-hosted/docker-desktop).

## Connection

Establish a connection to your Amazon S3 bucket from MindsDB by executing the following SQL command:

```sql
CREATE DATABASE s3_datasource
WITH
    engine = 's3',
    parameters = {
      "aws_access_key_id": "AQAXEQK89OX07YS34OP",
      "aws_secret_access_key": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
      "bucket": "my-bucket",
      "path_prefix": "rules/country=US/",
      "list_cache_ttl_seconds": 300,
      "include_metadata": false
    };
```

<Note>
Note that sample parameter values are provided here for reference, and you should replace them with your connection parameters.
</Note>

Required connection parameters include the following:

* `aws_access_key_id`: The AWS access key that identifies the user or IAM role.
* `aws_secret_access_key`: The AWS secret access key that identifies the user or IAM role.

Optional connection parameters include the following:

* `aws_session_token`: The AWS session token that identifies the user or IAM role. This becomes necessary when using temporary security credentials.
* `bucket`: The name of the Amazon S3 bucket. If not provided, all available buckets can be queried, however, this can affect performance, especially when listing all of the available objects.
* `path_prefix`: An optional S3 key prefix used to pre-filter objects returned by the `files` table.
* `list_cache_ttl_seconds`: Optional cache TTL, in seconds, for S3 object listings used by the `files` table. Defaults to 300 seconds.
* `include_metadata`: Whether to include custom S3 object metadata in the `files` table. Defaults to false. When enabled, the handler calls `HeadObject` for each listed file.

## Usage

Retrieve data from a specified object (file) in a S3 bucket by providing the integration name and the object key:

```sql
SELECT *
FROM s3_datasource.`my-file.csv`;
LIMIT 10;
```

<Tip>
If a bucket name is provided in the `CREATE DATABASE` command, querying will be limited to that bucket and the bucket name can be ommitted from the object key as shown in the example above. However, if the bucket name is not provided, the object key must include the bucket name, such as `s3_datasource.`my-bucket/my-folder/my-file.csv`.

Wrap the object key in backticks (\`) to avoid any issues parsing the SQL statements provided. This is especially important when the object key contains spaces, special characters or prefixes, such as `my-folder/my-file.csv`.

At the moment, the supported file formats are CSV, TSV, JSON, and Parquet. 
</Tip>

<Note>
The above examples utilize `s3_datasource` as the datasource name, which is defined in the `CREATE DATABASE` command.
</Note>

The special `files` table can be used to list all objects available in the specified bucket or all buckets if the bucket name is not provided:

```sql
SELECT *
FROM s3_datasource.files LIMIT 10
```

The `files` table includes standard object details returned by S3 listing operations: `path`, `name`, `extension`, `bucket`, `size`, `last_modified`, `etag`, and `storage_class`. These fields do not require per-object metadata requests.

Use path filters to list a subset of objects. Prefix-like filters can be pushed down to S3 listing, and the cached listing can then be filtered by DuckDB:

```sql
SELECT path, size, last_modified
FROM s3_datasource.files
WHERE path LIKE 'rules/country=US/state=CA/%'
  AND path LIKE '%dimension=pay_transparency/%';
```

Use exact path filters when you already know which documents to read. This avoids listing the bucket and directly checks the requested objects:

```sql
SELECT path, content
FROM s3_datasource.files
WHERE path IN (
  'rules/country=US/scope=federal/dimension=eeo_aap/rule_id=eeoc-eeo/version=2026-01-01/source.md',
  'rules/country=US/state=CA/scope=state/dimension=pay_transparency/rule_id=ca-pay-transparency/version=2026-01-01/source.md'
);
```

The content of files can also be retrieved by explicitly requesting the `content` column. This column is empty by default to avoid unnecessary data transfer:

```sql
SELECT path, content
FROM s3_datasource.files LIMIT 10
```

Custom S3 object metadata can be returned in the `metadata` column when `include_metadata` is set to true. This is intended for inspection and enrichment workflows, not high-volume filtering, because S3 requires one `HeadObject` request per file to retrieve custom metadata.

<Tip>
This table will return all objects regardless of the file format, however, only the supported file formats mentioned above can be queried.
</Tip>

## Troubleshooting Guide

<Warning>
`Database Connection Error`

* **Symptoms**: Failure to connect MindsDB with the Amazon S3 bucket.
* **Checklist**:
    1. Make sure the Amazon S3 bucket exists.
    2. Confirm that provided AWS credentials are correct. Try making a direct connection to the S3 bucket using the AWS CLI.
    3. Ensure a stable network between MindsDB and AWS.
</Warning>

<Warning>
`SQL statement cannot be parsed by mindsdb_sql`

* **Symptoms**: SQL queries failing or not recognizing object names containing spaces, special characters or prefixes.
* **Checklist**:
    1. Ensure object names with spaces, special characters or prefixes are enclosed in backticks.
    2. Examples:
        * Incorrect: SELECT * FROM integration.travel/travel_data.csv
        * Incorrect: SELECT * FROM integration.'travel/travel_data.csv'
        * Correct: SELECT * FROM integration.\`travel/travel_data.csv\`
</Warning>
