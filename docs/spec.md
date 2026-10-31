# Spec: Wikipedia live edits streaming pipeline

Status: draft v0.1, 2026-10-31. See `docs/research.md` for sources.

## 1. Goal

Stream every Wikipedia edit worldwide, as it happens, into a medallion lakehouse:
- Delta bronze (raw)
- Delta silver (clean)
- Delta gold (per minute)

Show live per-minute metrics on a Streamlit dashboard. Then replay the same data on Databricks Free Edition to prove the pipeline runs there.

## 2. Architecture

```
Wikimedia SSE ──> producer ──> Redpanda topic wiki.edits.raw ──> Spark bronze ──> silver ──> gold ──> Streamlit
                     │                └─> wiki.edits.dlq (invalid events)
                     └─> NDJSON replay files ──> UC volume ──> Databricks job (AvailableNow)
```

Everything local runs in Docker Compose. No cloud account is needed except Databricks Free Edition for the replay.

## 3. Components

### 3.1 Producer (`producer/`)

The producer is written in Python 3.12 with `confluent-kafka`.

1. **Connect:** open the SSE stream with a descriptive `User-Agent`.
2. **Reconnect:** on disconnect or the 15-minute server timeout, reconnect with exponential backoff (1 s up to 60 s, with jitter).
3. **Resume:** save the last SSE `id` to a state file. Send it back as `Last-Event-ID` when reconnecting.
4. **Validate:** check each event against the v1 schema (section 4).
   - Valid events go to `wiki.edits.raw`. The key is `wiki`. Headers are `schema_version=1` and `source=eventstreams`.
   - Invalid events go to `wiki.edits.dlq` with header `error=<reason>`, with the original payload unchanged.
5. **Replay files:** optionally write raw valid events to rotating NDJSON files under `data/replay/`. Rotate every 10 000 events or 5 minutes.
6. **Offline mode:** `--from-file <ndjson>` replays a file instead of the live feed. Tests and benchmarks use this mode.
7. **Producer settings:** `acks=all` and `enable.idempotence=true`.

### 3.2 Redpanda (`infra/`)

- One broker with a pinned image tag, plus Redpanda Console.
- **Topics:**

  | Topic | Partitions | Retention |
  |---|---|---|
  | `wiki.edits.raw` | 3 | 24 h |
  | `wiki.edits.dlq` | 1 | 7 days |

### 3.3 Spark jobs (`spark/`)

These are PySpark Structured Streaming jobs with Delta Lake: `pyspark==4.2.0` and `delta-spark==4.4.1`, running on Java 21. Spark runs on the host in `local[4]` mode (chosen by benchmark), not in Docker, to save memory. One shared module holds the transforms, so the local jobs and the Databricks notebooks run the same code.

| Layer | Input | Logic | Output mode |
|---|---|---|---|
| bronze | Kafka `wiki.edits.raw`, or NDJSON files on Databricks | Keep the raw `value` string plus the Kafka `topic`/`partition`/`offset`/`timestamp` and an `ingest_ts` column. | append |
| silver | bronze (Delta stream) | Parse the JSON with an explicit schema and set `event_ts` = `meta.dt`. Hash `user` as `sha256(salt + user)` into `user_hash` and drop `user`, `comment` and `parsedcomment`. Compute `bytes_delta = length.new - length.old`. Deduplicate on `meta.id` with an insert-only `foreachBatch` MERGE (no watermark, so late events are never dropped from silver). Partition by `event_date`. | insert-only MERGE |
| gold | silver (Delta stream) | 1-minute tumbling windows on `event_ts` with a **2-minute watermark**. Group by `window`, `wiki` and `is_bot`. | update via `foreachBatch` MERGE |

**Silver events that fail to parse** go to a Delta table, `silver_rejects`.

**Gold table `gold_edits_per_minute`:**
- **Key:** `window_start`, `wiki`, `is_bot`.
- **Columns:** `window_end`, `edit_count` (`type=edit`), `new_page_count` (`type=new`), `event_count` (all types), `bytes_added` (sum of positive `bytes_delta`), `bytes_removed` (sum of negative `bytes_delta`, stored as a positive number), `unique_editors` (`approx_count_distinct(user_hash)`) and `updated_at`.

**Gold table `gold_page_edits_per_minute`:**
- **Key:** `window_start`, `wiki`, `title`.
- **Columns:** `edit_count` and `updated_at`.
- The dashboard computes the top-N pages at read time, because streaming top-N over an aggregate isn't supported.

**Why MERGE in update mode:** append mode emits a window only after the watermark passes it, which is about 3 minutes and breaks the latency target. Update mode with MERGE makes rows visible after each micro-batch and corrects them as late events arrive.

**Idempotence:** the MERGE is keyed on the gold key, so replaying a batch gives the same result. Each table has its own checkpoint.

**Late events:** events older than the watermark are dropped from gold. They stay in bronze and silver. Their count is logged as a metric.

**Triggers:**
- Locally, use `processingTime='10 seconds'`.
- On Databricks, use `availableNow=True`.

The trigger is selected by config, never hard-coded.

### 3.4 Dashboard (`dashboard/`)

The dashboard is a Streamlit app built with the `developing-with-streamlit` skill. It reads the gold Delta tables with `deltalake` (delta-rs), so it doesn't need Spark, and refreshes every 10 s.

**Views:**
- edits per minute (line chart), split by bot vs human
- top 10 wikis
- top 10 pages in the last 15 minutes
- bytes added vs removed
- pipeline freshness: now minus the latest `updated_at`

The dashboard shows aggregates only and never displays a user identifier.

### 3.5 Databricks replay (`spark/databricks/`)

1. A Databricks Asset Bundle (`databricks.yml`) creates the schema `workspace.wiki_stream`, the volumes `raw` and `checkpoints`, and the job. `producer/` and `spark/` ship as a wheel installed in the job's serverless environment, so `foreachBatch` code can import them on Spark Connect.
2. Upload `data/replay/*.ndjson` to the `raw` volume with the Databricks CLI (`make db-upload`).
3. The job (`spark/databricks/replay_job.py`) runs bronze (Auto Loader, text format) → silver → gold with `Trigger.AvailableNow()`, using the shared modules. Tables are Unity Catalog managed tables addressed by name (`spark/storage.py`); locally they are paths.
4. The user-hash salt comes from the secret scope `wiki-stream`, key `user-hash-salt`. If the scope is missing, the job logs a warning and uses the dev salt.
3. Collect the evidence: a job run screenshot and `DESCRIBE HISTORY` for each gold table.

## 4. Event schema v1

These fields are required. If any is missing, the event goes to the DLQ.

| Field | Type | Rule |
|---|---|---|
| `meta.id` | string | UUID |
| `meta.dt` | string | ISO-8601 UTC |
| `meta.domain` | string | non-empty |
| `type` | string | one of `edit`, `new`, `log`, `categorize`, `external` |
| `wiki` | string | non-empty |
| `title` | string | non-empty |
| `bot` | boolean | |
| `namespace` | integer | |

These fields are optional and may be null: `id`, `user`, `minor`, `patrolled`, `length.old`, `length.new`, `revision.old`, `revision.new`, `server_name`, `comment`, `log_type`, `log_action`.

Unknown extra fields are allowed and kept in bronze.

**Schema changes:** a new optional field is a minor change and keeps `schema_version=1`. Removing or retyping a required field needs `schema_version=2`, and silver must handle both versions. The SOP gives the steps.

## 5. Acceptance criteria

| # | Criterion | How it is checked |
|---|---|---|
| AC1 | The producer sends valid events to `wiki.edits.raw` and invalid ones to `wiki.edits.dlq`. | unit tests with fixture events, plus an integration test |
| AC2 | The producer resumes after a disconnect using `Last-Event-ID`, with no gap. | unit test with a mocked SSE server |
| AC3 | Bronze holds every Kafka message, so the bronze row count equals the produced count. | integration test |
| AC4 | Silver has no duplicate `meta.id`, even when the same events are produced twice. | Spark test and integration test |
| AC5 | Gold counts are correct for a known synthetic input, including a late event inside the watermark (counted) and one outside it (dropped). | local-mode PySpark test |
| AC6 | Restarting any job from its checkpoint doesn't change the gold totals. | integration test |
| AC7 | p95 latency from event `meta.dt` to the gold `updated_at` is under 60 s locally at the live rate. | benchmark |
| AC8 | No raw `user`, `comment` or IP-like value appears in silver, gold or the dashboard. | test plus `aidefence_has_pii` |
| AC9 | The Databricks job runs bronze → silver → gold from replay files with `AvailableNow` and succeeds. | screenshot plus `DESCRIBE HISTORY` |
| AC10 | `make up && make test` passes on a clean clone. No secrets are committed, image tags are pinned, and CI is green. | CI plus security review |

## 6. Benchmarks (`bench/`)

- **Metrics:**
  - producer messages/s
  - p95 latency from event to gold
  - micro-batch duration, from `StreamingQueryProgress`
  - Delta file count before and after `OPTIMIZE`
- **Baseline:** the first green run, saved as `bench/baseline.json` with the commit SHA.
- **Load test:** replay a recorded NDJSON file at 10× the live rate (about 500 events/s).

## 7. Out of scope (v1)

- Cloud Kafka
- AWS
- Live streaming on Databricks
- Alerting
- Authentication on the dashboard
- Schema registry: the JSON schema lives in the repo instead

## 8. Repo layout

```
producer/        SSE client, validation, Kafka producer
spark/           shared transforms, local jobs, databricks/ notebooks
dashboard/       Streamlit app
infra/           docker-compose.yml, topic setup
tests/           unit, spark, integration, fixtures/
bench/           benchmark scripts, baseline.json, results.json
docs/            research, spec, architecture, adr/, SOP
Makefile, .env.example, .github/workflows/ci.yml
```
