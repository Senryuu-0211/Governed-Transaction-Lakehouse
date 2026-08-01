"""Land the Debezium CDC stream into Bronze Iceberg tables, near-real-time.

One streaming query per source table, each with its own checkpoint, so the busy
fact stream cannot stall the two small dimension streams and any one table can
be restarted or backfilled on its own.

Phase 2.5: Debezium now emits **Avro** (Confluent wire format) via the Apicurio
schema registry, not raw JSON. This stream decodes the Avro key and value back to
JSON text and stores that in Bronze, so the layer stays human-readable and
auditable and every downstream Silver model keeps working unchanged. The
byte-exact original is Avro; Bronze holds the faithfully-decoded JSON (a conscious
trade of byte-exact audit for an enforced schema contract at the Kafka gate).

Run:  PYTHONPATH=spark ~/working/gtl-spark-venv/bin/python spark/bronze_layer/bronze_stream.py
Stop: Ctrl-C (checkpoints make the next start resume where this one left off)
"""

import sys
import time
import urllib.request

from gtl_session import CATALOG, KAFKA_BOOTSTRAP, PROJECT_ROOT, REGISTRY_V2, get_spark
from pyspark.sql import functions as F
from pyspark.sql.avro.functions import from_avro

# Confluent wire format (Apicurio as-confluent=true): 1 magic byte + 4-byte schema
# id, then the Avro payload. Skip those 5 bytes before handing bytes to from_avro.
CONFLUENT_HEADER_BYTES = 5


def fetch_avro_schema(subject: str, retries: int = 30) -> str:
    """Lấy schema Avro TỰ CHỨA (đã dereference reference) của một subject.

    `?dereference=true` inline type ...Source (Debezium tách ra artifact riêng) nên
    from_avro nhận được schema đầy đủ. Body trả về CHÍNH LÀ schema JSON (không bọc).

    Poll có chờ: sau reset, artifact chỉ tồn tại SAU khi connector snapshot phát
    message đầu của bảng -> stream có thể khởi động trước, nên đợi.
    """
    url = f"{REGISTRY_V2}/groups/default/artifacts/{subject}?dereference=true"
    for attempt in range(retries):
        try:
            with urllib.request.urlopen(url, timeout=5) as resp:
                return resp.read().decode("utf-8")
        except Exception:  # noqa: BLE001 - artifact chưa có -> chờ connector
            if attempt == retries - 1:
                raise
            time.sleep(3)

CHECKPOINT_ROOT = PROJECT_ROOT / "_checkpoints"

# Each source table gets its own Bronze table mirroring it 1:1.
#
# The fact table is partitioned by ingestion day because it grows continuously.
# The dimensions hold roughly 100 and 50 rows, so partitioning them by day would
# only scatter a handful of rows into a new tiny file every day and give the
# compaction nothing useful to do.
SOURCES = [
    {
        "name": "transactions",
        "topic": "gtl.public.transactions",
        "table": f"{CATALOG}.bronze.transactions",
        "partition_by": "PARTITIONED BY (days(ingest_ts))",
        # ~15M messages have accumulated in Kafka. Without a cap, the first
        # micro-batch would try to take all of them at once; this splits the
        # catch-up into bounded batches and then becomes irrelevant once the
        # query is keeping pace with the source.
        "max_offsets_per_trigger": 200_000,
    },
    {
        "name": "accounts",
        "topic": "gtl.public.accounts",
        "table": f"{CATALOG}.bronze.accounts",
        "partition_by": "",
        "max_offsets_per_trigger": 50_000,
    },
    {
        "name": "merchants",
        "topic": "gtl.public.merchants",
        "table": f"{CATALOG}.bronze.merchants",
        "partition_by": "",
        "max_offsets_per_trigger": 50_000,
    },
]

BRONZE_COLUMNS = """
    op            STRING,
    source_ts_ms  BIGINT,
    key           STRING,
    value         STRING,
    kafka_partition INT,
    kafka_offset  BIGINT,
    kafka_ts      TIMESTAMP,
    ingest_ts     TIMESTAMP
"""


# Iceberg ghi MỘT metadata.json mới cho MỖI commit, và mỗi file chứa TOÀN BỘ lịch
# sử snapshot -> file sau to hơn file trước. Mặc định Iceberg GIỮ TẤT CẢ mãi mãi.
# Với stream trigger 30s (2.880 commit/ngày) điều này bùng nổ: đo thật ngày 01-08
# thấy metadata.json chiếm 1.217MB = 82% dung lượng bảng, trong khi DATA thật chỉ
# 190MB = 13%. Tệ hơn: MỖI lần mở bảng đều phải đọc metadata.json mới nhất (760KB
# và phình dần) -> trên S3 là egress TÍNH TIỀN cho mọi truy vấn.
# ⚠️ `expire_snapshots` KHÔNG dọn loại file này — chỉ hai property dưới mới dọn.
TABLE_PROPERTIES = {
    "write.metadata.delete-after-commit.enabled": "true",
    "write.metadata.previous-versions-max": "10",
}


def ensure_table(spark, source):
    """Create the Bronze table if it does not exist yet.

    The schema is declared here rather than inferred so that partitioning is
    explicit and a restart can never quietly land on a different layout.
    """
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {source['table']} ({BRONZE_COLUMNS}) "
        f"USING iceberg {source['partition_by']}"
    )
    # Áp cả cho bảng đã tồn tại (ALTER idempotent) — bảng cũ tạo trước khi có
    # property này vẫn phải được vá, không chỉ bảng tạo mới.
    for key, value in TABLE_PROPERTIES.items():
        spark.sql(f"ALTER TABLE {source['table']} SET TBLPROPERTIES ('{key}'='{value}')")


def build_stream(spark, source):
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", source["topic"])
        # Start from the beginning so the CDC history already sitting in Kafka
        # is captured too -- the pipeline promises no lost events, and skipping
        # to the newest offset would silently drop everything before now.
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", source["max_offsets_per_trigger"])
        # A topic disappearing should be loud, not silently skipped.
        .option("failOnDataLoss", "true")
        .load()
    )

    # Lấy schema Avro của key + value từ registry, bỏ 5-byte header Confluent rồi
    # from_avro -> struct -> to_json. Kết quả `value` là JSON text y như trước khi
    # đổi sang Avro, nên toàn bộ Silver (get_json_object) không phải sửa gì.
    key_schema = fetch_avro_schema(f"{source['topic']}-key")
    value_schema = fetch_avro_schema(f"{source['topic']}-value")

    payload = F.expr(f"substring(value, {CONFLUENT_HEADER_BYTES + 1}, "
                     f"length(value) - {CONFLUENT_HEADER_BYTES})")
    key_payload = F.expr(f"substring(key, {CONFLUENT_HEADER_BYTES + 1}, "
                         f"length(key) - {CONFLUENT_HEADER_BYTES})")

    # Tombstone: value=null -> giữ null (bản thân tombstone là event thật, không bỏ).
    # Key luôn có kể cả tombstone -> giải mã bình thường để có PK ổn định cho dedup.
    value_json = F.when(
        F.col("value").isNotNull(),
        F.to_json(from_avro(payload, value_schema)),
    )
    key_json = F.to_json(from_avro(key_payload, key_schema))

    decoded = raw.select(
        value_json.alias("value"),
        key_json.alias("key"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_ts"),
    )
    # op/source_ts_ms trích từ value đã-là-JSON: một tombstone có value null -> op
    # null, dòng vẫn giữ (đúng như bản JSON trước đây).
    return decoded.select(
        F.get_json_object("value", "$.op").alias("op"),
        F.get_json_object("value", "$.source.ts_ms").cast("bigint").alias("source_ts_ms"),
        F.col("key"),
        F.col("value"),
        F.col("kafka_partition"),
        F.col("kafka_offset"),
        F.col("kafka_ts"),
        F.current_timestamp().alias("ingest_ts"),
    )


def main() -> int:
    # local[3]: streaming append nhẹ, chạy 24/7 -> cấp ít core để chừa chỗ cho
    # dbt transform chạy đồng thời (tránh oversubscription trên 16 luồng của host).
    spark = get_spark("gtl-bronze-stream", master="local[3]")

    # After a full reset the catalog is empty, so the namespace has to be
    # recreated before any table can be placed in it. Doing it here keeps the
    # job self-sufficient instead of depending on a remembered manual step.
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.bronze")

    queries = []
    for source in SOURCES:
        ensure_table(spark, source)
        checkpoint = CHECKPOINT_ROOT / source["name"]
        checkpoint.mkdir(parents=True, exist_ok=True)

        query = (
            build_stream(spark, source)
            .writeStream.format("iceberg")
            .outputMode("append")
            # Checkpoints stay on the local disk. They are small, they are
            # written on every batch, and a single driver does not need them on
            # object storage -- where rename semantics make them fragile. The
            # Bronze data itself lives on S3.
            .option("checkpointLocation", str(checkpoint))
            .option("fanout-enabled", "true")
            # 28-07: 5s -> 30s. Mỗi micro-batch đẻ file mới, và trên S3 THẬT mỗi
            # file ghi = 1 PUT request TÍNH TIỀN (chưa kể small-files làm dbt quét
            # chậm và từng phình 397GB trên MinIO). 30s vẫn là near-real-time cho
            # workload này nhưng giảm ~6x số file/PUT. Đổi số này = đổi hoá đơn.
            .trigger(processingTime="30 seconds")
            .toTable(source["table"])
        )
        queries.append((source["name"], query))
        print(f"started query: {source['name']} -> {source['table']}")

    print("\nstreaming; Ctrl-C to stop\n")
    try:
        spark.streams.awaitAnyTermination()
    except KeyboardInterrupt:
        print("\nstopping queries...")
        for name, query in queries:
            query.stop()
            print(f"  stopped {name}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
