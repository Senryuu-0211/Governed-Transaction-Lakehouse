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

## Architecture (Part 1 — fully local, $0)

```
Postgres (CDC source)
   │  logical replication (WAL)
   ▼
Debezium ──► Kafka (KRaft) ──► Spark Structured Streaming
                                   │  Debezium envelope (op/before/after)
                                   ▼
                        Bronze  ──►  Silver  ──►  Gold      (Apache Iceberg on MinIO / S3 API)
                        (raw CDC)   (clean+PII)  (star schema)
                                   │
                        governance (dbt tests · lineage · reconciliation · PII masking)
                                   │
                        Airflow (orchestration)  ·  Prometheus/Grafana/Loki (observability)
```

**On-premise now, cloud-ready by design:** the whole stack runs locally at $0 (MinIO speaks S3).
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
| Lake storage | **MinIO** (S3 API) | S3-compatible → one endpoint change to reach AWS S3 |
| Table format | **Apache Iceberg** | ACID, time-travel (audit), schema evolution, engine-agnostic |
| Catalog | **Iceberg REST** → Glue (Part 2) | |
| Orchestration | **Airflow** (LocalExecutor) | |
| Observability | **Prometheus + Grafana + Loki** | |

**Correctness rules baked in:** money is `DECIMAL`, never float; `FAILED` transactions are not
real money; `REVERSED` transactions are never double-counted; Bronze total reconciles to Gold
each run.

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
- ⬜ Phase 1 · Step 1c — Spark Streaming → Bronze Iceberg on MinIO (S3 API)

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
