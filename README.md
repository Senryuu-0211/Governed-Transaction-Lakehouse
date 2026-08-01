# Governed Transaction Lakehouse

A near-real-time banking transaction pipeline and **governed lakehouse**, built
correctness-first and governance-first. It captures every transaction change via
**Change Data Capture (CDC)** with no lost events, lands it in an **ACID lakehouse**
with audit and time-travel, and enforces data quality, lineage, and PII controls.

> **Designed for bank-scale, demoed at small scale — and the boundary is stated explicitly.**
> The architecture is built to scale to billions of transactions; the demo runs at a modest
> rate on a single host. Where a choice is "designed to scale to X" vs "demo runs at Y", the
> code says so.

---

## Architecture (compute local, storage on AWS S3)

```
Postgres (CDC source)
   │  logical replication (WAL)
   ▼
Debezium ──► Kafka (KRaft) ──► Spark Structured Streaming
                                   │  Debezium envelope (op/before/after)
                                   ▼
                        Bronze  ──►  Silver  ──►  Gold      (Apache Iceberg on AWS S3)
                        (raw CDC)   (clean+PII)  (star schema)
                                   │
                        governance (dbt tests · lineage · reconciliation · PII masking)
                                   │
                        Airflow (orchestration)  ·  Prometheus/Grafana/Loki (observability)
```

**Cloud-ready by design — and proven:** storage runs on **real AWS S3**. The claim below was
tested by actually doing it: the pipeline started on MinIO and moved to S3 by changing an endpoint
and credentials — `dbt build` came back **82 PASS / 0 ERROR** with **no model or job code changed**.
Because all storage access goes through the **S3 API**, moving to the cloud (AWS S3 + Glue +
Athena) is an endpoint change with **no code rewrite** — one codebase serves both on-premise and
cloud, with no vendor lock-in.

---

## Tech stack (and why, not just what)

| Layer | Tool | Rationale |
|---|---|---|
| Source OLTP | **PostgreSQL 16** (`wal_level=logical`) | Clean logical-replication slot for CDC |
| CDC | **Debezium** | Reads the WAL — captures INSERT/UPDATE/**DELETE** in commit order; timestamp-polling misses deletes |
| Log / streaming bus | **Kafka (KRaft)** | Industry standard (MSK); KRaft removes ZooKeeper |
| Stream + batch compute | **Apache Spark** | Scales past single-machine RAM to bank-size data |
| Transform modelling | **dbt** (dbt-spark) | Structure, tests, docs, lineage over Spark execution |
| Lake storage | **AWS S3** (was MinIO) | Storage is reached only through the S3 API, so the move off MinIO was an endpoint swap, not a rewrite |
| Table format | **Apache Iceberg** | ACID, time-travel (audit), schema evolution, engine-agnostic |
| Catalog | **Iceberg REST** → Glue (Part 2) | |
| Orchestration | **Airflow** (LocalExecutor) | |
| Observability | **Prometheus + Grafana + Loki** | |

**Correctness rules baked in:** money is `DECIMAL`, never float; `FAILED` transactions are not
real money; `REVERSED` transactions are never double-counted; Bronze total reconciles to Gold
each run.

---

## Storage cost control (learned the hard way)

Streaming into a lakehouse writes a new file every micro-batch. Left alone, that filled a 468 GB
disk with **397 GB** of small and orphaned files. On object storage the same failure mode is a
*bill*, not just a full disk — every file written is a paid PUT request. Four controls, all in
the repo rather than in someone's memory:

- **Slower trigger** — 30 s instead of 5 s, roughly 6× fewer files and PUTs for the same data.
- **Compaction** (`rewrite_data_files`) — small files merged to ~128 MB.
- **Orphan cleanup** (`remove_orphan_files`) — the control that was *missing*. `expire_snapshots`
  only deletes files that were once committed; a job killed mid-write leaves files no snapshot
  ever referenced, and those accumulate forever. Retention is 72 h, deliberately: a file still
  being written looks exactly like an orphan, so a shorter window would delete live data.
- **A least-privilege IAM user scoped to one bucket, plus a budget alert** — blast radius and
  spend are both bounded.

`python scripts/s3_admin.py usage` reports size, object count, average object size (a small
average *is* the small-files problem), and estimated monthly cost.

---

## Roadmap

| Phase | Goal |
|---|---|
| **1 — CDC pipeline** | CDC flows end-to-end → Bronze Iceberg captures INSERT/UPDATE/DELETE |
| 2 — Medallion transform | Gold star schema + dbt structural tests (quality as a gate), correct REVERSAL |
| 3 — Orchestration | Airflow-driven, idempotent, backfillable |
| 4 — Advanced governance | Time-travel audit, lineage, reconciliation, Great Expectations, PII masking |
| 5 — Observability | Lag/freshness dashboards + alerting |
| 6 — CI/CD + LocalStack | Automated deploy; cloud code runs $0 on LocalStack |
| Part 2 — Real AWS | S3 + Glue + Athena (endpoint swap) |

---

## Status

- ✅ **Phase 1 · Step 1a** — Postgres source + logical replication + data faker
- ✅ **Phase 1 · Step 1b** — Debezium + Kafka KRaft (CDC → topics)
- ✅ **Phase 1 · Step 1c** — Spark Structured Streaming → Bronze Iceberg on **AWS S3**
- ✅ **Phase 2** — Medallion transform on **dbt-on-Spark**: Silver current-state (typed, PII-masked,
  soft-delete), Gold Kimball star schema, 71 dbt tests as a gate, lineage docs; **Superset**
  dashboards over a Postgres serving copy of the marts
- ✅ **Phase 2.5** — **Schema Registry (Avro)**: Debezium emits Avro through Apicurio; incompatible
  source schema changes are **rejected at the Kafka gate** instead of silently nulling downstream
- ✅ **Phase 3** — Airflow orchestration: two DAGs (`gtl_transform` hourly, `gtl_maintenance`
  daily) driven from the existing containerized Airflow via **`SSHOperator` to host-side
  Spark/dbt** (Airflow orchestrates, the host computes); `dbt_test` **gates** `push_marts` so bad
  data never reaches Superset; `verify_3.sh` 6/6 and a full end-to-end run green (dbt 11 models,
  71 tests, marts refreshed)

### Step 1a highlights
- `wal_level=logical` with replication slots ready for Debezium
- Schema: `accounts` (PII-tagged), `transactions` (money `DECIMAL`, status lifecycle
  `PENDING → COMPLETED/FAILED → REVERSED`), `merchants`; `REPLICA IDENTITY FULL` for CDC before-images
- **Least-privilege CDC role** (`REPLICATION` + `SELECT` only) with a scoped publication
- `updated_at` maintained by a DB trigger; secrets kept in `.env` (never committed)
- Faker mixes INSERT + status UPDATE + REVERSAL and mutates balances on completion

### Step 1b highlights
- **Kafka KRaft** single broker (no ZooKeeper); **Kafka Connect + Debezium** Postgres connector
- Connector uses the least-privilege `debezium` role + pre-created `dbz_publication` (autocreate disabled)
- `pgoutput` plugin; one topic per table (`gtl.public.*`); versioned connector config, idempotent registration
- **`decimal.handling.mode=string`** — money stays exact through Kafka (never float)
- **CDC safety:** `max_slot_wal_keep_size=10GB` — a lagging replication slot gets invalidated
  instead of filling disk and crashing the source DB (slot-lag = the #1 metric to watch, Phase 5)
- Verified full Debezium envelope: `r` (snapshot), `c` (insert), `u` (update with before/after
  status), `d` (delete with before + null after + tombstone)

Run: `docker compose up -d --build` → `bash scripts/register-connector.sh` → `bash scripts/verify_1b.sh`.
Inspect topics/messages in Kafka UI at `localhost:8092`.

### Step 1c highlights

CDC now lands in an **ACID lakehouse**: three concurrent Spark Structured Streaming queries read
the Debezium topics into Iceberg tables on **AWS S3**, reached over the **S3 API**.

- **Bronze mirrors the source, one table per source table.** The fact table is partitioned by
  ingestion day; the two dimensions are not, because partitioning ~100 rows by day only scatters
  them into tiny files. Separate tables are also what makes Silver's `MERGE INTO` coherent later:
  `transactions` keys on `txn_id`, `accounts` on `account_id`.
- **Bronze stays raw.** The whole Debezium envelope is kept verbatim in one column, with only
  `op`, source timestamp, and Kafka coordinates lifted out for partitioning and traceability.
  A source schema change cannot break ingestion, and any disputed figure is traceable to the
  bytes the source actually emitted. Typing, PII masking, and deduplication belong to Silver.
- **Independent queries, independent checkpoints** — the busy fact stream cannot stall the
  dimensions, and any one table can be restarted or backfilled alone.
- **S3 access through Iceberg's `S3FileIO`** (`iceberg-aws-bundle`) rather than
  `hadoop-aws` + `aws-java-sdk-bundle`, which removes the Hadoop/AWS SDK version conflict that
  makes this step fail for most people. Client jars and the REST catalog image are pinned to the
  same Iceberg version, and a smoke test proves the write path before any stream starts.
- **Verified end to end:** ~16.9M Bronze rows with `op` values `c`, `u`, **and** `d`, money still
  exact as a string, and a steady lag of tens of messages behind the source.

Run: `PYTHONPATH=spark python spark/bronze_layer/bronze_stream.py` → `bash scripts/verify_1c.sh`.
Inspect storage usage and cost with `python scripts/s3_admin.py usage`.

---

## CDC operational safety

Two mechanisms that separate "operated CDC in production" from "followed a tutorial":

- **Replication-slot protection.** Debezium holds a replication slot, so Postgres cannot recycle
  un-consumed WAL — a stalled or dead consumer could fill the source disk and crash the core DB.
  `max_slot_wal_keep_size=10GB` invalidates a lagging slot (recoverable via re-snapshot) instead
  of filling disk: **sacrifice the pipeline before the source system.** Slot lag (`retained_wal`)
  is the **#1 metric to monitor** — ahead of consumer lag (Phase 5).
- **Only committed transactions reach Bronze.** Debezium reads via **logical decoding** (not raw
  WAL), so it emits only committed changes, in commit order; rolled-back transactions never appear.
  Bronze is clean by construction — no in-flight/dirty rows to filter out.
- **Kafka is a buffer with an expiry date, not an archive.** Bronze is the archive — but only if
  it consumes before retention deletes. This pipeline hit exactly that during development: the
  broker ran on its 7-day default while capture was live and no durable consumer existed yet, so
  the oldest change events were dropped, silently. `startingOffsets=earliest` does not save you —
  "earliest" means the oldest message *still present*, not the oldest ever written. The fix is
  `retention.ms` sized to **the longest consumer outage worth surviving** (30 days here, measured
  at ~3 GB/day) plus a `retention.bytes` ceiling, on the same principle as the replication-slot
  limit: bound the damage. Retention only buys time, though — the real control is **alerting when
  the consumed offset approaches the oldest available one** (Phase 5, alongside slot lag).

---

## Quick start (Step 1a)

```bash
cp .env.example .env          # set credentials
docker compose up -d --build  # Postgres + faker
docker compose ps             # postgres healthy, faker up

bash scripts/verify.sh        # automated 11-point checkpoint (exit != 0 on failure)
```

Source DB: `postgresql://<user>@localhost:5433/banking`

---

## Repo layout

```
docker-compose.yml            # Step 1a stack (Postgres + faker)
postgres/init/                # 01_schema.sql · 02_cdc_role.sh (runs on empty volume)
faker/                        # synthetic transaction generator (never real PII)
scripts/verify.sh             # automated checkpoint
issues/                       # tracked design issues / follow-ups
```

## Design notes
See [`docs/design-notes.md`](docs/design-notes.md) for the **ingestion pattern**, **delivery
semantics** (why at-least-once), and the **idempotency / dedup strategy** that makes each
transaction count exactly once without chasing exactly-once delivery.

## Design philosophy & planned extensions

A governed data platform must serve **decision-makers, not just the data team** — governance only
has value when a non-technical user can answer *"what is this table, where's it from, can I trust
it?"* through a UI. Planned once the core (Phase 1–4) is solid:

- **Lineage visualization** for business users — DataHub / OpenMetadata (rich discovery, heavier),
  or lightweight OpenLineage + Marquez (native to Airflow/Spark/dbt).
- **AI year-end report** — an LLM writes the *narrative* around numbers that are **computed and
  fixed by code** (the LLM never touches the numbers; charts are code-rendered). Hallucination-safe
  by construction — essential for financial figures.

## Notes
- Postgres init scripts run only on an **empty volume** — changing schema needs `docker compose down -v`.
- `down -v` deletes data; confirm you're on the throwaway/test host first.
- Ports are tracked in `issues/PORTS.md`.
- Synthetic data only (Faker) — no real personal data anywhere.
