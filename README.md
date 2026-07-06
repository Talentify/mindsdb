<div align="center">

  # MindsDB Query Engine

**Semantic search over all your data — entirely in SQL.**

  <a href="https://pypi.org/project/MindsDB/" target="_blank">
    <img src="https://badge.fury.io/py/MindsDB.svg" alt="PyPI version" />
  </a>
  <a href="https://www.python.org/downloads/" target="_blank">
    <img src="https://img.shields.io/badge/python-3.10.x%7C%203.11.x%7C%203.12.x%7C%203.13.x-brightgreen.svg" alt="Supported Python versions" />
  </a>
  <a href="https://hub.docker.com/r/mindsdb/mindsdb" target="_blank">
    <img src="https://img.shields.io/docker/pulls/mindsdb/mindsdb.svg?logo=docker&label=Docker%20pulls&cacheSeconds=86400" alt="Docker pulls" />
  </a>

[**Docs**](https://mindsdb.github.io/engine) · [**Website**](https://mindshub.ai) · [**Discord**](https://mindshub.ai/discord) · [**Contact**](https://mindshub.ai/contact)
</div>

---

MindsDB Query Engine connects to 200+ data sources — databases, warehouses, applications, files — and lets you query them live in one SQL dialect, with no ETL. Index unstructured content into [knowledge bases](https://mindsdb.github.io/engine#kb-overview), then search it by meaning, by keyword, or both at once, with plain SQL filters on top. Everything is reachable from any MySQL- or PostgreSQL-compatible client.

> **Where this fits:** MindsDB now builds [MindsHub](https://mindshub.ai) — a hub for open AI agents. The Query Engine remains a standalone open-source project, and it pairs well with MindsHub agents: connect it to give an agent live, SQL-queryable access to your data and semantic search. The full story: [MindsHub vs MindsDB](https://mindshub.ai/mindshub-vs-mindsdb).

## How it works

```
   MySQL clients · PostgreSQL clients · BI tools · ORMs · HTTP API
                                  │
                   ┌──────────────▼───────────────┐
                   │     MindsDB Query Engine     │
                   │     one SQL dialect over     │
                   │  a federated query planner   │
                   └──────────────┬───────────────┘
                                  │
            ┌─────────────────────┼─────────────────────┐
            │                     │                     │
  ┌─────────▼─────────┐ ┌─────────▼─────────┐ ┌─────────▼─────────┐
  │     Databases     │ │    Apps & files   │ │  Knowledge bases  │
  │ Postgres, MySQL,  │ │ Slack, web crawler│ │   embeddings +    │
  │ MongoDB, Snowflake│ │ docs, sheets,     │ │  vector store +   │
  │ BigQuery, S3, …   │ │ email, calendars… │ │    BM25 index     │
  └───────────────────┘ └───────────────────┘ └───────────────────┘
           queried live, in place — data is never copied
```

- **One server, three interfaces.** The engine ships a built-in SQL editor on HTTP (`:47334`) and speaks the MySQL (`:47335`) and PostgreSQL (`:47336`) wire protocols — so `mysql`, `psql`, DBeaver, SQLAlchemy, or any BI tool [connects directly](https://mindsdb.github.io/engine#setup-clients).
- **Federated queries, no pipelines.** [`CREATE DATABASE`](https://mindsdb.github.io/engine#db-create) attaches a live data source through an integration handler. The planner translates each query, pushes work down to the source, and streams results back — your data stays where it is. Source-specific syntax is still available via [native queries](https://mindsdb.github.io/engine#native-queries).
- **Knowledge bases are the semantic layer.** A [knowledge base](https://mindsdb.github.io/engine#kb-overview) combines an embedding model, an optional reranking model, and a vector store (e.g. pgvector). `INSERT INTO` it to chunk, embed, and index content; `SELECT` from it to retrieve by meaning, filtered by metadata columns like any other table.
- **Hybrid retrieval.** [Hybrid search](https://mindsdb.github.io/engine#kb-hybrid) runs vector similarity and BM25 keyword matching in parallel and merges the results — for queries that mix natural language with exact identifiers, codes, or acronyms.
- **Organize and automate.** [Projects](https://mindsdb.github.io/engine#proj-create) namespace your work, [views](https://mindsdb.github.io/engine#view-create) save cross-source transformations, and [jobs](https://mindsdb.github.io/engine#job-create) schedule any SQL to run on an interval — e.g. to keep knowledge bases fresh.

## Quick start

Run with [Docker](https://mindsdb.github.io/engine#setup-docker):

```bash
docker run --name mindsdb_container \
  -e MINDSDB_APIS=http,mysql \
  -p 47334:47334 -p 47335:47335 \
  mindsdb/mindsdb
```

Or install from [PyPI](https://mindsdb.github.io/engine#setup-pip):

```bash
pip install mindsdb            # add extras as needed, e.g. mindsdb[pgvector,openai,postgres]
python -m mindsdb
```

Then open the editor at `http://127.0.0.1:47334`, or connect any MySQL client to port `47335`. The [quickstart](https://mindsdb.github.io/engine#quickstart) walks through the rest.

## From zero to semantic search

Six SQL statements, start to finish. Full syntax for every statement is in the [SQL reference](https://mindsdb.github.io/engine).

**1. Attach your data sources** ([docs](https://mindsdb.github.io/engine#db-create)) — they are queried live, nothing is imported:

```sql
CREATE DATABASE my_pg
WITH ENGINE = 'postgres',
PARAMETERS = {
  "host": "localhost", "port": 5432,
  "user": "user", "password": "pass",
  "database": "mydb"
};

CREATE DATABASE my_mongo
WITH ENGINE = 'mongodb',
PARAMETERS = {
  "host": "mongodb+srv://user:pass@cluster.example.net",
  "database": "support"
};
```

**2. Query across sources in one dialect** ([docs](https://mindsdb.github.io/engine#sql-join)) — even non-SQL stores like MongoDB, and save the result as a [view](https://mindsdb.github.io/engine#view-create):

```sql
CREATE VIEW open_tickets_by_product AS (
  SELECT p.name, COUNT(t.ticket_id) AS open_tickets
  FROM my_mongo.support_tickets AS t
  JOIN my_pg.products AS p
    ON t.product_id = p.id
  WHERE t.status = 'open'
  GROUP BY p.name
);
```

**3. Create a knowledge base** ([docs](https://mindsdb.github.io/engine#kb-create)) — an embedding model plus a vector store, addressable as a table:

```sql
CREATE KNOWLEDGE_BASE support_kb
USING
  embedding_model = {
    "provider":   "openai",
    "model_name": "text-embedding-3-large",
    "api_key":    "sk-..."
  },
  storage          = my_pgvector.support_kb_store,  -- a pgvector connection
  content_columns  = ['subject', 'body'],
  metadata_columns = ['product_name', 'priority', 'created_at'],
  id_column        = 'ticket_id';
```

**4. Index your content** ([docs](https://mindsdb.github.io/engine#kb-insert)) — rows are chunked, embedded, and upserted:

```sql
INSERT INTO support_kb
  SELECT ticket_id, subject, body, product_name, priority, created_at
  FROM my_mongo.support_tickets;
```

**5. Search by meaning, filter by metadata** ([docs](https://mindsdb.github.io/engine#kb-query)):

```sql
SELECT chunk_content, product_name, relevance
FROM support_kb
WHERE content = 'cannot connect after the latest update'
  AND priority <= 2
  AND relevance >= 0.5
LIMIT 10;

-- hybrid search: blend vector similarity with BM25 keyword matching
SELECT *
FROM support_kb
WHERE content = 'error ERR-4421'
  AND hybrid_search = true;
```

▶ [How to use semantic search with metadata filters](https://www.youtube.com/watch?v=HN4fHtS4mvo) — a good explainer of this feature.

**6. Keep the index fresh with a job** ([docs](https://mindsdb.github.io/engine#job-create)):

```sql
CREATE JOB refresh_support_kb (
  INSERT INTO support_kb
    SELECT ticket_id, subject, body, product_name, priority, created_at
    FROM my_mongo.support_tickets
    WHERE created_at > LAST
)
EVERY hour;
```

## Help and support

| You need | Go to |
| --- | --- |
| Ask a question | [Discord](https://mindshub.ai/discord) |
| Report a bug | [GitHub Issues](https://github.com/mindsdb/engine/issues) — please include reproduction steps |
| Commercial support | [Contact the team](https://mindshub.ai/contact) |

**Security note:** if you find a vulnerability, please do not open a public issue — follow our [security policy](https://github.com/mindsdb/engine/security) instead.

## Contributing

Contributions are welcome — code, integrations, docs, and bug reports alike. We follow the fork-and-pull workflow: see the [contribution guide](CONTRIBUTING.md) to get set up, and browse the [open issues](https://github.com/mindsdb/engine/issues) for somewhere to start. Good first areas are new integration handlers, bug fixes, and documentation improvements.

## Resources

- [Documentation](https://mindsdb.github.io/engine)
- [MindsHub — open AI agents, from the same team](https://mindshub.ai)
- [MindsHub vs MindsDB — how the product evolved](https://mindshub.ai/mindshub-vs-mindsdb)
- [Discord](https://mindshub.ai/discord)
- [Contact](https://mindshub.ai/contact)

## License

MindsDB Core is licensed under the [Elastic License 2.0](LICENSE); some directories carry their own license — see the [LICENSE](LICENSE) file for the full structure.
