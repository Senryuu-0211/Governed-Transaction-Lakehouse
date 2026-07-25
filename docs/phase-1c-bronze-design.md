# Phase 1c — Bronze ingestion design (Kafka CDC → Iceberg on MinIO)

Scope: land the Debezium CDC stream into an ACID lakehouse Bronze layer, near-real-time.

## Goal and latency target

This is a **transaction processing system** (transfers, payments). The requirement at this
stage is **near-real-time**: seconds of end-to-end lag from a committed row change to a
queryable Bronze record.

**Spark Structured Streaming is sufficient here** — micro-batch gives second-level latency and
keeps one ecosystem (Spark + Iceberg + dbt). **Flink would be considered** only when the
workload needs sub-second per-event processing, in-flight complex event processing (e.g. fraud
detection before landing), or large stateful event-time joins/windows. Ingest-to-lakehouse does
not need that, so adopting Flink now would add a component without a matching requirement.

## Architecture

```
Postgres → Debezium → Kafka (keyed by PK)      [already built: Step 1a/1b]
                        │  localhost:9093 (EXTERNAL listener)
                        ▼
        Spark Structured Streaming (host venv, local[*])
                        │  3 concurrent queries, one per topic
                        ▼
        Iceberg REST catalog (localhost:8181) ──► MinIO (localhost:9001, S3 API)
                        s3://warehouse/bronze/...
```

### Why Spark runs on the host, outside Docker

Kafka already advertises `EXTERNAL://localhost:9093` — a listener created for host-side tools.
A host-side Spark reaches Kafka, MinIO, and the Iceberg REST catalog through published ports,
so **no container network bridging is required** and no shared network is coupled to this
project. The driver runs `local[*]`, matching the convention that analysis and benchmarking use
local mode rather than a standalone cluster.

## Bronze layout: mirror the source, one table per source table

| Bronze table | Source | Profile |
|---|---|---|
| `bronze.transactions` | `public.transactions` | fact — high volume, partitioned by `days(ingest_ts)` |
| `bronze.accounts` | `public.accounts` | dimension — small, unpartitioned |
| `bronze.merchants` | `public.merchants` | dimension — small, unpartitioned |

**Why mirrored rather than one combined table:**

- **Fact and dimension have different physical profiles.** The fact table grows continuously and
  benefits from date partitioning; the dimensions hold ~100 and ~50 rows and would be scattered
  into tiny files across date partitions, which harms compaction.
- **Downstream current-state maintenance requires it.** Silver collapses the CDC log into
  current state by `MERGE INTO` on the primary key. `transactions` keys on `txn_id`,
  `accounts` on `account_id`, with different slowly-changing-dimension semantics. A combined
  table mixes primary keys, making a merge-by-key incoherent.
- **Lineage is 1:1 with the source**, which matters for a platform meant to be legible to
  non-engineers, and Debezium already emits one topic per table.
- Separate tables allow independent retention, compaction, and (later) independent scheduling.

## Bronze is raw, not flattened

Each row keeps the **entire Debezium envelope** in a single `value` string column. A few fields
are extracted alongside it for partitioning and traceability only.

| Column | Source | Purpose |
|---|---|---|
| `op` | parsed from envelope | `r`/`c`/`u`/`d` — proves UPDATE/DELETE capture |
| `source_ts_ms` | parsed from envelope | source commit time |
| `key` | Kafka key | primary key of the changed row |
| `value` | Kafka value | **full envelope, unmodified** |
| `kafka_partition`, `kafka_offset`, `kafka_ts` | Kafka metadata | traceability, reconciliation, dedup key |
| `ingest_ts` | processing time | partition key |

Reasons to keep it raw:

1. **Schema evolution is safe.** A new or changed source column cannot break Bronze.
2. **The original bytes remain auditable.** Any disputed figure can be traced to what the source
   actually emitted.
3. **It respects the layer's job.** Bronze records; Silver interprets — typing, PII masking, and
   deduplication belong there. Flattening in Bronze would duplicate that work and remove a layer
   of defence.

Bronze performs **no deduplication**. It is an append-only log.

## Streaming semantics

- **Three concurrent queries in one `SparkSession`** (one driver), each subscribing to one topic
  and writing one table with its **own checkpoint**. Isolation means lag on the fact stream does
  not stall the dimensions, and each table can be restarted or backfilled independently.
- `startingOffsets=earliest` — the full CDC history already in Kafka is captured, consistent with
  the promise that no event is lost.
- `maxOffsetsPerTrigger` throttles the initial backfill (~15M messages) into bounded micro-batches
  so the first run cannot exhaust memory, then the query settles into near-real-time.
- Checkpoints are stored on the **local host filesystem**, which is more reliable for a
  single-driver setup than object-store checkpointing. Bronze data itself lives on MinIO.
- Each micro-batch commits atomically to Iceberg, so a restart cannot double-write a committed
  batch.

## Dependency versions

The main failure mode in this step is mismatched jars. Two rules:

- `iceberg-spark-runtime-3.5_2.12` and `iceberg-aws-bundle` **must be the same Iceberg version**
  (pinned to `1.9.2`), and the REST catalog image runs that same version.
- S3 access goes through Iceberg's own `S3FileIO` via `iceberg-aws-bundle`, **not** through
  `hadoop-aws` + `aws-java-sdk-bundle`. This removes the Hadoop/AWS SDK version conflict entirely.

```
org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0
org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.9.2
org.apache.iceberg:iceberg-aws-bundle:1.9.2
```

**How this version was chosen.** The usable versions are the intersection of two independently
published sets: tags released for the `apache/iceberg-rest-fixture` image (`1.8.1` and up) and
versions of the Spark 3.5 runtime jars on Maven Central (`1.7.0` and up). The overlap is
`1.8.1`–`1.10.1`; `1.9.2` sits inside it as a patch release, new enough to carry the REST and
`S3FileIO` fixes and old enough to be well exercised against Spark 3.5. An earlier attempt to
pin `1.6.1` failed at image pull because no such image tag exists — check both sets before
picking, rather than assuming a version is available everywhere. `1.10.1` is the fallback.

A smoke import verifies the jars resolve before any stream is started. If a class is missing,
the real error is reported and the version adjusted — versions are not guessed.

## New services

| Service | Ports | Memory |
|---|---|---|
| MinIO | `9001` API, `9002` console | 512M |
| Iceberg REST catalog | `8181` | 512M |

Credentials live in `.env`, never committed. Bucket: `warehouse`.

## Checkpoint for this step

`scripts/verify_1c.sh` must confirm:

1. MinIO is reachable and the `warehouse` bucket exists
2. The Iceberg REST catalog responds and the `bronze` namespace exists
3. All three Bronze tables exist in the catalog
4. Row counts are greater than zero and increasing between two samples
5. `bronze.transactions` contains `op` values `c`, `u`, **and** `d` — the Phase 1 goal is proving
   INSERT/UPDATE/**DELETE** are all captured
6. Checkpoint directories are non-empty (offsets are being committed)

## Deferred

- **Current-state tables** (`MERGE INTO` by primary key), typing, PII masking, and deduplication
  are Silver work — Phase 2.
- **Orchestration.** These are continuous streaming queries, not scheduled jobs. When Airflow
  takes over in Phase 3, it supervises long-running queries rather than triggering daily runs,
  and how a containerised Airflow supervises a host-side driver is decided there.
