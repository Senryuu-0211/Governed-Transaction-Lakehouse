"""Spark-side checks for the Phase 1c checkpoint.

Counting rows and reading the op distribution needs a Spark session, so those
checks live here. Everything that can be answered from Kafka or the filesystem
stays in scripts/verify_1c.sh, which is far cheaper to run.

A table is only required to hold rows if Kafka actually has messages available
for it. An empty Bronze table is a failure when the source has data waiting and
a correct result when retention has already removed everything -- reporting both
as the same failure would train us to ignore the check.

The caller passes the tables that genuinely have upstream data:
    EXPECT_DATA=transactions,accounts ... python spark/verify_bronze.py
"""

import json
import os
import sys

from gtl_session import CATALOG, get_spark

TABLES = ["transactions", "accounts", "merchants"]

passed = 0
failed = 0


def ok(message):
    global passed
    print(f"  [OK]   {message}")
    passed += 1


def no(message):
    global failed
    print(f"  [FAIL] {message}")
    failed += 1


def note(message):
    print(f"  [NOTE] {message}")


def main() -> int:
    raw = os.environ.get("EXPECT_DATA", ",".join(TABLES))
    expect_data = {name for name in raw.split(",") if name}

    spark = get_spark("gtl-verify-1c", master="local[2]", driver_memory="2g")

    counts = {}
    for name in TABLES:
        table = f"{CATALOG}.bronze.{name}"
        try:
            # Spark caches Iceberg table metadata for the life of a session, so
            # without this a count can silently report an old snapshot.
            spark.sql(f"REFRESH TABLE {table}")
            counts[name] = spark.table(table).count()
        except Exception as exc:  # noqa: BLE001 - show the real error, do not guess
            no(f"cannot read {table}: {exc}")
            continue

        if name not in expect_data:
            note(
                f"bronze.{name}: {counts[name]:,} rows "
                "(no messages available upstream, so this is not a failure)"
            )
        elif counts[name] > 0:
            ok(f"bronze.{name} has {counts[name]:,} rows")
        else:
            no(f"bronze.{name} is empty but Kafka has messages waiting")

    # --- INSERT, UPDATE and DELETE are all captured --------------------------
    # This is the point of Phase 1. Timestamp-polling ingestion would show
    # inserts and updates but miss every delete, so op=d is the evidence that
    # log-based CDC is really working end to end.
    if counts.get("transactions", 0) > 0:
        ops = {
            row["op"]: row["n"]
            for row in spark.sql(
                f"SELECT op, count(*) AS n FROM {CATALOG}.bronze.transactions GROUP BY op"
            ).collect()
        }
        print(f"         op distribution: {ops}")
        for op, label in [("c", "INSERT"), ("u", "UPDATE"), ("d", "DELETE")]:
            if ops.get(op, 0) > 0:
                ok(f"op={op} ({label}) captured: {ops[op]:,}")
            else:
                no(f"op={op} ({label}) missing")

        # --- The envelope survived intact -----------------------------------
        # Bronze is only useful as an audit record if `value` really holds the
        # whole Debezium envelope, and money must never have passed through a
        # float on the way here.
        sample = spark.sql(
            f"SELECT value FROM {CATALOG}.bronze.transactions "
            "WHERE op = 'u' AND value IS NOT NULL LIMIT 1"
        ).collect()
        if sample:
            envelope = json.loads(sample[0]["value"])
            if all(k in envelope for k in ("op", "before", "after", "source")):
                ok("raw envelope intact (op/before/after/source present)")
            else:
                no(f"envelope missing keys, got: {list(envelope)}")

            amount = (envelope.get("after") or {}).get("amount")
            if isinstance(amount, str):
                ok(f"money kept exact as string (amount={amount!r})")
            else:
                no(f"amount is {type(amount).__name__}, expected str")
        else:
            no("no update row available to inspect")
    else:
        note("skipping op/envelope checks: bronze.transactions has no rows")

    print(f"\n== spark checks: PASS={passed} FAIL={failed} ==")
    spark.stop()
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
