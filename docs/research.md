# Research

Date: 2026-10-31

## 1. Data source: Wikimedia EventStreams

- **Endpoint:** `https://stream.wikimedia.org/v2/stream/recentchange`
- **Protocol:** Server-Sent Events (SSE) over HTTPS. It is free and needs no API key.
- **Schema:** `/mediawiki/recentchange/1.0.0`. It is published at schema.wikimedia.org.
- **Resume:** each SSE message has an `id`, which encodes topic, partition and offset/timestamp. Send it back as the `Last-Event-ID` header to resume. You can also start from a time with `?since=<ISO-8601>`.
- **Connection limit:** "WMF's HTTP connection termination layer enforces a connection timeout of 15 minutes." The client must reconnect automatically and resume from `Last-Event-ID`.
- **History:** "between 7 and 31 days of history". It "is not kept indefinitely".
- **Etiquette:** send a descriptive `User-Agent` (see mw:API:Etiquette). Don't replay large amounts of history.

### Measured sample (2026-10-31, 10 s capture)

The capture used `curl --max-time 10` and received 504 events, about 50 events/s.

**Event types:**

| type | count |
|---|---|
| `categorize` | 268 |
| `edit` | 207 |
| `new` | 19 |
| `log` | 10 |

**Bot flag:** `bot=true` 171, `bot=false` 333.

**Top-level fields seen:** `$schema`, `meta`, `id`, `type`, `namespace`, `title`, `title_url`, `comment`, `parsedcomment`, `timestamp`, `user`, `bot`, `minor`, `patrolled`, `length{old,new}`, `revision{old,new}`, `server_url`, `server_name`, `server_script_path`, `wiki`, `notify_url`, `log_id`, `log_type`, `log_action`, `log_params`, `log_action_comment`.

**What this means for the pipeline:**
- `meta.id` (a UUID) is unique per event. It is the dedup key.
- `meta.dt` (ISO-8601 with milliseconds, UTC) is the event time. `timestamp` only has second precision.
- `length` and `revision` exist only for `edit` and `new`. `log_*` fields exist only for `log`. Most fields are optional, so the schema must allow nulls.
- `user` is a username, a temporary account name or, historically, an IP address. Treat it as personal data: hash it in silver and show only aggregates on the dashboard.
- About 50 events/s is a light load. A single-broker Redpanda and a local Spark in `local[2]` mode can handle it.

## 2. Platform: Databricks Free Edition

From the [Free Edition limitations](https://docs.databricks.com/aws/en/getting-started/free-edition-limitations) page:
- Compute is serverless only: "Free Edition users only have access to serverless compute resources."
- Outbound network access is limited to a short list of approved domains.
- "Max of 5 concurrent job tasks per account."
- "One SQL warehouse, limited to a `2X-Small` cluster size."
- There is a fair-usage quota. When it is exceeded, compute stops until the quota resets, and data is kept.
- "One workspace and one metastore per account." Custom storage locations aren't supported.
- Commercial use isn't allowed. That is fine for a portfolio project.

From the [Serverless compute limitations](https://docs.databricks.com/aws/en/compute/serverless/limitations) page:
- The supported streaming trigger is `Trigger.AvailableNow()`. `Trigger.Once()` also works but is deprecated.
- "`Trigger.ProcessingTime(interval)` is not supported." An unsupported trigger raises an error.
- Use Unity Catalog volumes or workspace files. DBFS access is limited.
- Kafka is a supported source, but the broker must be reachable from Databricks over IPv4.

### Decision

Option (b) is accepted. Option (a) was rejected.

- **Live path (local, $0):** Wikimedia SSE → producer → Redpanda → Spark Structured Streaming → Delta bronze/silver/gold → Streamlit.
- **Databricks path (replay):** the producer also writes raw events to NDJSON files. These files are uploaded to a Unity Catalog volume. A Databricks job then runs the same bronze → silver → gold code with `Trigger.AvailableNow()`. This gives a real Databricks job run and Delta `DESCRIBE HISTORY` without exposing a broker to the internet.
- **Why not (a):** a cloud Kafka free tier adds an account, credentials and network setup. It also isn't needed, because serverless can't run a continuous trigger anyway.
- **Why not AWS:** MSK and Kinesis are paid services. The user chose a $0 setup.

## 3. Sources

- Wikimedia EventStreams: https://wikitech.wikimedia.org/wiki/Event_Platform/EventStreams_HTTP_Service
- Recentchange schema: https://schema.wikimedia.org/#!/primary/jsonschema/mediawiki/recentchange
- Databricks Free Edition limitations: https://docs.databricks.com/aws/en/getting-started/free-edition-limitations
- Databricks serverless limitations: https://docs.databricks.com/aws/en/compute/serverless/limitations
