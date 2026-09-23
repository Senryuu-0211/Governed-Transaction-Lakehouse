"""Đẩy mart + dimension từ Iceberg sang Postgres db `marts` cho Superset và agent đọc.

Vì sao có bước này (đường A trong OPTIMIZATION):
  - Iceberg = KHO SỰ THẬT; nhưng BI click từng giây, Spark-đọc-Iceberg mất 5-30s/câu
    → Superset đọc BẢN SAO Postgres (bảng aggregate nhỏ) trả lời trong ms.
  - Agent hỏi-đáp còn gắt hơn: ngân sách cả câu trả lời ~12s, mà chỉ riêng khởi động
    SparkSession đã ~40s. Không có bản sao này thì không có agent.
  - Kỷ luật chống lệch bản sao: bảng ở đây CHỈ do script này ghi (không ai UPDATE
    tay); `_refreshed_at` cho biết số liệu tính đến lúc nào, `_kafka_offset_max` cho
    biết nó gồm những event nào — số thứ hai mới là thứ truy nguồn được.

VÌ SAO ĐẨY CẢ DIMENSION (23-09): agent trả lời cho người KHÔNG rành kỹ thuật, nên
"Merchant 47" là vô nghĩa — phải có TÊN. dim_* nhỏ (200 / 25k / ~90 dòng) nên chi phí
gần như bằng 0, và có sẵn ở đây thì agent không bao giờ phải chạm dữ liệu thô.

Chạy:  PYTHONPATH=spark ~/working/gtl-spark-venv/bin/python spark/gold_layer/push_marts.py
       ... --recreate   # khi mart ĐỔI CỘT (xem ghi chú dưới)
"""

import sys

from gtl_session import CATALOG, PROJECT_ROOT, get_spark, load_env

# (schema Iceberg, tên bảng) — thứ tự này cũng là thứ tự đẩy.
MARTS = [
    ("marts", "mart_daily_volume"),
    ("marts", "mart_channel_daily"),
    ("marts", "mart_category_daily"),
    # Khối rộng: bảng agent truy vấn nhiều nhất (~300k dòng, 6 chiều).
    ("marts", "mart_txn_daily"),
    ("marts", "mart_merchant_daily"),
    ("marts", "mart_fraud_daily"),
]

# Dimension sống ở schema `gold` nhưng vẫn sang db `marts`: tầng phục vụ cần tên,
# không cần khoá. Đều là bảng nhỏ và ĐÃ QUA MASK PII ở Silver.
DIMS = [
    ("gold", "dim_merchant"),
    ("gold", "dim_account"),
    ("gold", "dim_date"),
    ("gold", "dim_channel"),
]

# Sổ đối soát nằm ở schema `gold`, không phải `marts` — nhưng vẫn đẩy sang Postgres
# vì Phase 5: exporter Prometheus cần đọc chênh lệch mỗi 2 phút. Đọc thẳng Iceberg
# thì phải dựng SparkSession ~40s + tốn egress S3 mỗi lần đo -> vô lý. Ở đây đã có
# sẵn một Spark session đang chạy nên đẩy kèm là gần như miễn phí.
AUDIT = [("gold", "audit_reconciliation")]

POSTGRES_JAR = PROJECT_ROOT / "jars" / "postgresql-42.7.4.jar"
# Chạy từ host nên nói chuyện với Postgres qua cổng published 5433.
JDBC_URL = "jdbc:postgresql://localhost:5433/marts"


def main() -> int:
    # --recreate: DROP rồi CREATE thay vì TRUNCATE + INSERT.
    #
    # Steady-state dùng truncate=true vì nó giữ nguyên bảng nên không có khoảnh khắc
    # nào bảng biến mất khỏi Superset. NHƯNG truncate chỉ thay RUỘT — nếu model vừa
    # thêm/bớt cột thì INSERT sẽ vỡ vì bảng Postgres còn cột cũ. Lúc đó cần --recreate.
    #
    # An toàn về quyền: init đã đặt `ALTER DEFAULT PRIVILEGES FOR ROLE <bank> ...
    # GRANT SELECT ON TABLES TO marts_ro`, nên bảng tạo mới TỰ ĐỘNG có quyền đọc cho
    # Superset. Không cần cấp quyền lại bằng tay — chỗ này từng là lý do phải tránh
    # drop/create, giờ không còn.
    recreate = "--recreate" in sys.argv

    env = load_env()
    props = {
        "user": env["POSTGRES_USER"],
        "password": env["POSTGRES_PASSWORD"],
        "driver": "org.postgresql.Driver",
    }

    spark = get_spark(
        "gtl-push-marts",
        master="local[2]",
        driver_memory="2g",
        extra_conf={"spark.jars": str(POSTGRES_JAR)},
    )

    if recreate:
        print(">> chế độ --recreate: DROP + CREATE (dùng khi mart đổi cột)")

    failed = []
    for schema, name in MARTS + DIMS + AUDIT:
        try:
            df = spark.table(f"{CATALOG}.{schema}.{name}")
            writer = df.write.mode("overwrite")
            if not recreate:
                writer = writer.option("truncate", "true")
            writer.jdbc(JDBC_URL, name, properties=props)
            print(f"pushed {schema}.{name}: {df.count():,} rows")
        except Exception as exc:  # noqa: BLE001 — cô lập lỗi theo TỪNG bảng
            # Một bảng hỏng không được kéo theo cả mẻ: các bảng còn lại vẫn nên sang
            # Postgres, và log phải nói RÕ bảng nào. Bài học từ maintenance.py (006).
            print(f"FAILED {schema}.{name}: {type(exc).__name__}: {exc}")
            failed.append(f"{schema}.{name}")

    spark.stop()

    if failed:
        print(f"\n❌ {len(failed)} bảng KHÔNG sang được: {', '.join(failed)}")
        if not recreate:
            print("   Nếu vừa đổi cột model, chạy lại với --recreate")
        return 1

    print("DONE — marts + dimension đã sang Postgres")
    return 0


if __name__ == "__main__":
    sys.exit(main())
