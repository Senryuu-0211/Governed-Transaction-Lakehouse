"""Prove the jar set and the storage path work before any streaming is attempted.

Mismatched Iceberg / AWS SDK / Hadoop jars are the usual reason this step fails,
and the failure surfaces as a ClassNotFoundException in the middle of a stream,
where it is expensive to diagnose. This script exercises the same wiring in a
few seconds: resolve the jars, reach the REST catalog, create a table, write to
MinIO, read it back, and clean up.

Run:  ~/working/gtl-spark-venv/bin/python spark/smoke_test.py
"""

import sys

from gtl_session import CATALOG, get_spark

TEST_TABLE = f"{CATALOG}.bronze.__smoke_test"


def main() -> int:
    spark = get_spark("gtl-smoke-test", master="local[2]", driver_memory="2g")
    print(f"\n[1] Spark {spark.version} started, jars resolved")

    namespaces = [row[0] for row in spark.sql(f"SHOW NAMESPACES IN {CATALOG}").collect()]
    print(f"[2] REST catalog reachable, namespaces: {namespaces}")
    if "bronze" not in namespaces:
        print("    FAIL: namespace 'bronze' missing")
        return 1

    # A real write proves S3FileIO can authenticate to MinIO and that the
    # catalog can commit metadata -- listing namespaces alone would not.
    spark.sql(f"DROP TABLE IF EXISTS {TEST_TABLE}")
    spark.sql(f"CREATE TABLE {TEST_TABLE} (id BIGINT, note STRING) USING iceberg")
    spark.sql(f"INSERT INTO {TEST_TABLE} VALUES (1, 'hello'), (2, 'lakehouse')")
    print("[3] table created and rows written to S3")

    rows = spark.sql(f"SELECT * FROM {TEST_TABLE} ORDER BY id").collect()
    print(f"[4] read back: {[(r['id'], r['note']) for r in rows]}")
    if len(rows) != 2:
        print(f"    FAIL: expected 2 rows, got {len(rows)}")
        return 1

    # Snapshot history is what makes the audit and time-travel work in the
    # governance phase; confirm the table really is Iceberg, not just a path.
    snapshots = spark.sql(f"SELECT snapshot_id FROM {TEST_TABLE}.snapshots").count()
    print(f"[5] iceberg snapshots recorded: {snapshots}")

    spark.sql(f"DROP TABLE {TEST_TABLE} PURGE")
    print("[6] test table dropped\n")
    print("SMOKE TEST PASSED - jars, REST catalog, and S3 write path all work")

    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
