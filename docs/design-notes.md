# Design Notes — Ingestion Pattern, Delivery Semantics & Idempotency

Reference notes for how this pipeline ingests data and how it guarantees each
transaction is counted **exactly once in the final results** without chasing the
(impractical) goal of exactly-once *delivery*.

---

## 1. Ingestion pattern

In one line: **log-based CDC → streaming via Kafka → append-only Bronze (ELT) → Medallion refine.**

| Axis | Choice | Why (vs the alternative it beats) |
|---|---|---|
| Extraction | **Log-based CDC** (Debezium reads the WAL) | Captures INSERT/UPDATE/**DELETE** in commit order with near-zero load on the OLTP. Query/timestamp-polling misses deletes and intermediate states, and re-scans load the source. |
| Cadence | **Streaming / micro-batch** (Spark Structured Streaming) | Seconds-to-minutes latency, event-driven — not scheduled batch pulls. |
| Transport | **Log/queue buffer** (Kafka) | Decouples source from sink; **replayable** from any offset; survives sink downtime. |
| Landing | **Append-only, immutable Bronze** (keeps full CDC envelope: `op`/`before`/`after`/`ts`) | Event-log / event-sourcing style — the raw change log can always be replayed to rebuild state (auditability). No dedup/transform here. |
| Load shape | **ELT** (land raw first, transform later) | Bronze → Silver → Gold in-lake, vs transforming before load. |

Bootstrap is **initial snapshot + CDC tail**: Debezium takes a consistent snapshot of each
table, then streams incremental changes from the WAL.

---

## 2. Delivery semantics — the honest position

- **Debezium is at-least-once.** On connector restart it re-emits from the last committed
  offset → **duplicates are possible**. This is by design, not a bug.
- **End-to-end exactly-once *delivery* across heterogeneous systems is not achievable** in
  the general case. What the industry calls "exactly-once" is really
  **effectively-once = at-least-once delivery + idempotent processing / dedup**.
- **Kafka EOS** (idempotent producer + transactions) is exactly-once only *within* Kafka↔Kafka
  boundaries (Kafka Streams). Crossing to an external sink (Iceberg/S3, a DB) requires the sink
  to participate transactionally.
- **Spark Structured Streaming + Iceberg** can be exactly-once *into the table*: Iceberg commits
  are atomic and Spark tracks Kafka offsets in its checkpoint. Edge cases remain around the
  ordering of data-commit vs offset-commit.

**Conclusion:** we do **not** pursue exactly-once transport. We engineer **effectively-once**.

---

## 3. Do transactions *need* exactly-once?

The real requirement for money is **not** "exactly-once delivery". It is:

1. **Never lose a transaction** → never use *at-most-once*.
2. **Count each transaction exactly once in Gold** → a *processing/result* guarantee.

Both are met by **at-least-once transport + idempotent dedup + reconciliation**, which is what
real banks/fintech ledgers do. (At the OLTP source of truth, write-time exactly-once is handled
separately by DB ACID + application idempotency keys — upstream of this analytics pipeline.)

---

## 4. Idempotency & dedup strategy

| Concern | Mechanism |
|---|---|
| Don't lose events | At-least-once (Debezium + durable Kafka) |
| Correct ordering + before-image for dedup | `REPLICA IDENTITY FULL` + DB-set `updated_at` trigger |
| "Exactly once" in results | Dedup at **Silver** by natural key `txn_id` + CDC sequence (LSN / `updated_at`), keep latest |
| Reversals not double-counted | Gold business logic (`FAILED` ≠ real money; `REVERSED` nets out) |
| No double-write into Bronze | Iceberg atomic commit + Spark checkpoint |
| Safety net | **Reconciliation**: independently compare source totals vs lakehouse totals each run to *detect* drift |

Dedup is deliberately **pushed downstream** (Silver), not done at ingestion — Bronze stays a
faithful, replayable at-least-once change log.

---

## 5. Phase mapping

- **Phase 1 (1a–1c):** WAL + `REPLICA IDENTITY FULL` + `updated_at` (done in 1a) → Debezium/Kafka (1b) → append-only Bronze on Iceberg (1c).
- **Phase 2:** Silver dedup (key + LSN), Gold star schema with correct reversal netting.
- **Phase 4:** reconciliation (Bronze total = Gold total) as the correctness safety net.

---

## References
- CDC at Airbnb / Netflix / Uber — https://venturebeat.com/data-infrastructure/change-data-capture-the-critical-link-for-airbnb-netflix-and-uber
- Apache Hudi at Uber — https://hudi.apache.org/blog/2025/06/30/uber-hudi/
- Exactly-once patterns with Kafka + Debezium — https://www.linkedin.com/pulse/event-driven-data-architecture-patterns-exactly-once-delivery-singh
- Extending Kafka exactly-once to external systems — https://medium.com/@raviatadobe/extending-kafkas-exactly-once-semantics-to-external-systems-c395267935bd
