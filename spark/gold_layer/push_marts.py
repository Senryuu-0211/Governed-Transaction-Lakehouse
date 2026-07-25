"""Đẩy các bảng mart từ Iceberg sang Postgres db `marts` cho Superset đọc.

Vì sao có bước này (đường A trong OPTIMIZATION):
  - Iceberg = KHO SỰ THẬT; nhưng BI click từng giây, Spark-đọc-Iceberg mất 5-30s/câu
    → Superset đọc BẢN SAO Postgres (bảng aggregate nhỏ) trả lời trong ms.
  - Kỷ luật chống lệch bản sao: bảng ở đây CHỈ do script này ghi (không ai UPDATE
    tay); cột `_refreshed_at` sinh từ dbt cho business thấy "số liệu tính đến lúc nào".
  - Phase 3 sẽ đưa bước này vào DAG Airflow ngay sau `dbt build`.

Chạy:  PYTHONPATH=spark ~/working/gtl-spark-venv/bin/python spark/gold_layer/push_marts.py
"""

import sys
from pathlib import Path

from gtl_session import CATALOG, PROJECT_ROOT, get_spark, load_env

MARTS = ["mart_daily_volume", "mart_channel_daily", "mart_category_daily"]
POSTGRES_JAR = PROJECT_ROOT / "jars" / "postgresql-42.7.4.jar"
# Chạy từ host nên nói chuyện với Postgres qua cổng published 5433.
JDBC_URL = "jdbc:postgresql://localhost:5433/marts"


def main() -> int:
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

    for name in MARTS:
        df = spark.table(f"{CATALOG}.marts.{name}")
        # overwrite + truncate=true: giữ nguyên bảng (và quyền SELECT của marts_ro),
        # chỉ thay ruột — thay vì drop/create làm rớt grant.
        (
            df.write.mode("overwrite")
            .option("truncate", "true")
            .jdbc(JDBC_URL, name, properties=props)
        )
        print(f"pushed {name}: {df.count():,} rows")

    spark.stop()
    print("DONE — marts đã sang Postgres cho Superset")
    return 0


if __name__ == "__main__":
    sys.exit(main())
