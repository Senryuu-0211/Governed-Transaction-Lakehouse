"""Bảo trì bảng Iceberg — nén small file + dọn snapshot cũ.

VÌ SAO CẦN (small-files problem — hệ quả tất yếu của streaming):
  Bronze stream commit mỗi 5s -> mỗi micro-batch đẻ 1 file parquet tí hon. Sau vài
  ngày: bronze.transactions ~4400 file × ~2000 dòng. Đọc incremental ở Silver phải
  quét hàng nghìn file -> bùng nổ request S3 -> MinIO single-node reset connection.
  Đây KHÔNG phải bug: mọi lakehouse streaming đều gặp, và đều xử bằng compaction
  định kỳ. Production auto-compact (hoặc chạy job này theo lịch — Airflow Phase 3).

LÀM 2 VIỆC:
  1. rewrite_data_files — gộp nhiều file nhỏ thành ít file lớn (~128MB), giảm số
     lần đọc, giữ NGUYÊN dữ liệu (chỉ sắp xếp lại vật lý).
  2. expire_snapshots — xoá snapshot + file mồ côi cũ (giữ time-travel gần đây).
     Không dọn thì metadata phình vô hạn.

An toàn chạy song song với stream (Iceberg compaction serializable — xung đột thì
báo, không hỏng data). Nhưng chạy lúc stream rảnh/dừng là nhẹ nhất.

Chạy:  PYTHONPATH=spark ~/working/gtl-spark-venv/bin/python spark/maintenance.py
"""

import sys

from gtl_session import CATALOG, get_spark

# Nén cả Bronze (nguồn small file) lẫn Silver (MERGE cũng đẻ file). Gold=table
# rebuild mỗi run nên không tích file — bỏ qua.
TABLES = [
    "bronze.transactions", "bronze.accounts", "bronze.merchants",
    "silver.transactions", "silver.accounts", "silver.merchants",
]
TARGET_FILE_BYTES = 128 * 1024 * 1024  # 128MB — chuẩn cho query analytics
RETAIN_SNAPSHOTS = 10                   # giữ 10 snapshot gần nhất cho time-travel


def main() -> int:
    spark = get_spark(
        "gtl-maintenance", master="local[4]", driver_memory="6g",
        # Nén 4400+ file một lượt làm OOM khi broadcast — tắt broadcast join cho job này.
        extra_conf={"spark.sql.autoBroadcastJoinThreshold": "-1"},
    )

    for table in TABLES:
        fqn = f"{CATALOG}.{table}"
        # Số file TRƯỚC (chứng minh hiệu quả nén)
        before = spark.sql(f"SELECT count(*) n FROM {fqn}.files").collect()[0]["n"]

        # 1) Nén small file. min-input-files=2 để bảng đã gọn thì bỏ qua (no-op).
        rc = spark.sql(
            f"CALL {CATALOG}.system.rewrite_data_files("
            f"  table => '{table}',"
            f"  options => map("
            f"    'target-file-size-bytes','{TARGET_FILE_BYTES}',"
            f"    'min-input-files','5',"
            # Commit theo TỪNG NHÓM file thay vì gom cả bảng vào một plan khổng lồ
            # (tránh OOM); nhóm dở vẫn giữ được tiến độ nếu một nhóm lỗi.
            f"    'partial-progress.enabled','true',"
            f"    'max-file-group-size-bytes','{256 * 1024 * 1024}'"
            f"  ))"
        ).collect()[0]
        rewritten = rc["rewritten_data_files_count"]
        added = rc["added_data_files_count"]

        # 2) Dọn snapshot cũ + file mồ côi (chỉ giữ RETAIN_SNAPSHOTS gần nhất).
        spark.sql(
            f"CALL {CATALOG}.system.expire_snapshots("
            f"  table => '{table}',"
            f"  retain_last => {RETAIN_SNAPSHOTS})"
        )

        after = spark.sql(f"SELECT count(*) n FROM {fqn}.files").collect()[0]["n"]
        print(f"{table:24s} files {before:>6,} -> {after:>4,}  "
              f"(rewrote {rewritten:,} -> {added:,})")

    spark.stop()
    print("\nDONE — compaction + snapshot expiry xong")
    return 0


if __name__ == "__main__":
    sys.exit(main())
