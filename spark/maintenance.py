"""Bảo trì bảng Iceberg — nén small file + dọn snapshot cũ.

VÌ SAO CẦN (small-files problem — hệ quả tất yếu của streaming):
  Bronze stream commit mỗi 5s -> mỗi micro-batch đẻ 1 file parquet tí hon. Sau vài
  ngày: bronze.transactions ~4400 file × ~2000 dòng. Đọc incremental ở Silver phải
  quét hàng nghìn file -> bùng nổ request S3 -> MinIO single-node reset connection.
  Đây KHÔNG phải bug: mọi lakehouse streaming đều gặp, và đều xử bằng compaction
  định kỳ. Production auto-compact (hoặc chạy job này theo lịch — Airflow Phase 3).

LÀM 3 VIỆC:
  1. rewrite_data_files — gộp nhiều file nhỏ thành ít file lớn (~128MB), giảm số
     lần đọc, giữ NGUYÊN dữ liệu (chỉ sắp xếp lại vật lý).
  2. expire_snapshots — xoá snapshot cũ + file KHÔNG CÒN THAM CHIẾU (giữ
     time-travel gần đây). Không dọn thì metadata phình vô hạn.
  3. remove_orphan_files — xoá file MỒ CÔI: file đã ghi lên storage nhưng commit
     THẤT BẠI (job OOM/bị kill giữa chừng) nên KHÔNG snapshot nào trỏ tới.
     ⚠️ expire_snapshots KHÔNG dọn được loại này (nó chỉ biết file từng được
     commit). Thiếu bước 3 chính là nguyên nhân MinIO phình 397GB làm đầy ổ
     (sự cố 28-07) — stream crash rất nhiều lần, mỗi lần để lại rác vĩnh viễn.
     Trên S3 THẬT rác này còn là TIỀN hàng tháng, không chỉ đĩa.

An toàn chạy song song với stream (Iceberg compaction serializable — xung đột thì
báo, không hỏng data). Nhưng chạy lúc stream rảnh/dừng là nhẹ nhất.

Chạy:  PYTHONPATH=spark ~/working/gtl-spark-venv/bin/python spark/maintenance.py
"""

import sys
from datetime import datetime, timedelta

from botocore.config import Config
from gtl_session import CATALOG, get_spark, s3_client

# Timeout cho client boto3 — CÙNG BÀI HỌC với S3FileIO, nhưng đây là một client
# KHÁC nên phải cấu hình RIÊNG. Tôi đã sót đúng chỗ này: sửa timeout cho S3FileIO
# rồi tưởng xong, trong khi `build_file_list_view()` vẫn gọi S3 bằng boto3 mặc định.
# Job vẫn treo, và mất thêm một vòng debug mới nhận ra có HAI đường ra S3.
#
# Máy này ra Internet qua NAT gia đình: kết nối nhàn rỗi bị NAT lặng lẽ vứt bản
# ghi ánh xạ, lần dùng lại sau đó gói tin bay vào hư không (Send-Q kẹt, không bao
# giờ có ACK). Liệt kê 19k object với RTT 249ms là thừa thời gian cho việc đó.
S3_CLIENT_CONFIG = Config(
    connect_timeout=10,
    read_timeout=60,
    # adaptive: gặp throttle thì tự giãn nhịp thay vì đập liên tục vào S3.
    retries={"max_attempts": 5, "mode": "adaptive"},
    # Trần kết nối đồng thời — liệt kê phân trang không cần nhiều, giữ thấp để
    # bớt số kết nối có thể chết già trong pool.
    max_pool_connections=10,
)

# Nén cả Bronze (nguồn small file) lẫn Silver (MERGE cũng đẻ file). Gold=table
# rebuild mỗi run nên không tích file — bỏ qua.
TABLES = [
    "bronze.transactions", "bronze.accounts", "bronze.merchants",
    "silver.transactions", "silver.accounts", "silver.merchants",
]
TARGET_FILE_BYTES = 128 * 1024 * 1024  # 128MB — chuẩn cho query analytics
RETAIN_SNAPSHOTS = 10                   # giữ 10 snapshot gần nhất cho time-travel

# Chỉ coi là mồ côi khi file cũ hơn ngần này giờ. 72h = mặc định an toàn của Iceberg.
# ⚠️ ĐỪNG HẠ XUỐNG THẤP khi stream đang chạy: file đang được ghi (chưa commit) nhìn
# y hệt file mồ côi -> hạ ngưỡng = tự xoá data đang bay vào. 72h bảo đảm mọi ghi
# dở đều đã kết thúc. Rác vẫn bị dọn, chỉ là trễ 3 ngày — đủ để chặn phình vô hạn.
ORPHAN_OLDER_THAN_HOURS = 72

# Iceberg ghi MỘT metadata.json mỗi commit, mỗi file chứa TOÀN BỘ lịch sử snapshot
# -> file sau to hơn file trước, và mặc định GIỮ TẤT CẢ mãi mãi. Đo thật 01-08:
# metadata.json chiếm 1.217MB = 82% dung lượng bronze.transactions, DATA thật chỉ
# 190MB = 13%. Mỗi lần mở bảng còn phải đọc file mới nhất (760KB, phình dần) ->
# trên S3 THẬT là egress tính tiền cho MỌI truy vấn.
# ⚠️ expire_snapshots KHÔNG dọn loại này. Áp ở đây để MỌI bảng đều được vá, kể cả
# bảng dbt tạo trước khi có config (ALTER idempotent, chạy lại vô hại).
METADATA_PROPERTIES = {
    "write.metadata.delete-after-commit.enabled": "true",
    "write.metadata.previous-versions-max": "10",
}


def s3_client_and_bucket():
    """Client boto3 + tên bucket. Endpoint do gtl_session quyết định (MinIO/S3)."""
    return s3_client(config=S3_CLIENT_CONFIG)


def table_location(spark, fqn: str) -> str:
    """Đường dẫn gốc của bảng, hỏi thẳng catalog thay vì đoán theo quy ước tên."""
    rows = spark.sql(f"DESCRIBE TABLE EXTENDED {fqn}").collect()
    for row in rows:
        if row["col_name"].strip().lower() == "location":
            return row["data_type"].strip()
    raise RuntimeError(f"không đọc được Location của {fqn}")


def s3_prefix(location: str, bucket: str) -> str:
    """Đổi `s3://<bucket>/warehouse/bronze/tbl` -> prefix `warehouse/bronze/tbl/`.

    Tách riêng khỏi phần gọi mạng để test được: đây là code cấp danh sách file cho
    một thủ tục XOÁ FILE, nên prefix sai một ký tự có thể có nghĩa là liệt kê nhầm
    thư mục. Rẻ hơn nhiều nếu sai ở đây bị bắt bằng unit test thay vì bằng data mất.
    """
    prefix = location.split(f"s3://{bucket}/", 1)[-1]
    return prefix.rstrip("/") + "/"


def build_file_list_view(spark, s3, bucket, location: str, view: str) -> int:
    """Liệt kê MỌI object dưới thư mục bảng bằng boto3 -> temp view cho Iceberg.

    VÌ SAO PHẢI TỰ LIỆT KÊ (sự cố 01-08):
      `remove_orphan_files` phải tìm file mà Iceberg KHÔNG biết (ghi lên S3 nhưng
      commit fail) -> không tra được metadata, buộc phải LIST storage. Phần listing
      trong Iceberg 1.9.2 dùng **Hadoop FileSystem API**, mà project cố ý không dùng
      `hadoop-aws` (chỉ S3FileIO, tránh xung đột version Hadoop/AWS SDK) ->
          UnsupportedFileSystemException: No FileSystem for scheme "s3"
      Nghĩa là cơ chế dọn mồ côi thêm hôm 30-07 CHƯA TỪNG chạy được.

    CÁCH SỬA — dùng tham số `file_list_view` của chính thủ tục đó:
      Ta chỉ cung cấp phần mang tính storage-cụ-thể (danh sách object). Phần NGUY
      HIỂM — quyết định file nào là mồ côi và xoá nó — VẪN do Iceberg làm, đối chiếu
      với metadata của nó. Không thêm dependency, không tự viết code có quyền xoá.
      Chính việc Iceberg chừa sẵn tham số này cho thấy nó được thiết kế cho object
      store nơi Hadoop FS không được cấu hình.

    View cần đúng 2 cột: file_path (string) · last_modified (timestamp).
    """
    prefix = s3_prefix(location, bucket)
    rows = []
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            # bỏ tzinfo: Spark timestamp không nhận offset-aware từ Python
            rows.append((f"s3://{bucket}/{obj['Key']}",
                         obj["LastModified"].replace(tzinfo=None)))
    spark.createDataFrame(
        rows, "file_path string, last_modified timestamp"
    ).createOrReplaceTempView(view)
    return len(rows)


def call_orphan(spark, table: str, cutoff: str, view: str, dry_run: bool) -> list:
    """Gọi `remove_orphan_files`. Tham số truyền TƯỜNG MINH, không bắt qua closure.

    Trước đây hàm này định nghĩa BÊN TRONG vòng lặp bảng và đóng gói `table`/`view`
    từ biến vòng lặp. Chạy vẫn đúng (gọi ngay trong cùng vòng), nhưng ruff B023 chỉ
    ra đây là mìn: ai đó gom các closure lại gọi sau thì TẤT CẢ dùng giá trị của
    bảng CUỐI CÙNG. Với code xoá file trong lakehouse thì "hiện tại vẫn đúng" không
    phải lý do đủ tốt để giữ.
    """
    return spark.sql(
        f"CALL {CATALOG}.system.remove_orphan_files("
        f"  table => '{table}',"
        f"  older_than => TIMESTAMP '{cutoff}',"
        f"  file_list_view => '{view}',"
        f"  dry_run => {str(dry_run).lower()})"
    ).collect()


def data_file_count(spark, fqn: str) -> int:
    """Số data file của bảng — đọc từ TÓM TẮT SNAPSHOT, không quét manifest.

    ⚠️ ĐÂY LÀ CHỖ JOB TỪNG TREO, và nó treo vì một dòng chỉ để TRANG TRÍ LOG.
      Bản cũ dùng `SELECT count(*) FROM <bảng>.files`. Câu đó trông vô hại nhưng
      nó MỞ TỪNG MANIFEST để đếm — với `bronze.accounts` (6.702 object) là hàng
      nghìn request S3, mỗi request 249ms khứ hồi tới us-east-1, qua NAT gia đình
      hay thả kết nối nhàn rỗi. Job treo ở Stage 0, tức là TRƯỚC KHI nén một file
      nào: chết vì đi đếm, không phải vì đi làm.

    Iceberg đã ghi sẵn con số này trong `summary` của mỗi snapshot. Đọc nó chỉ tốn
    MỘT lần đọc metadata.json — thay vì hàng nghìn.

    Không đọc được thì trả -1 và ĐI TIẾP: đây là số liệu báo cáo, không phải điều
    kiện để nén. Đừng bao giờ để phần trang trí chặn phần công việc.
    """
    try:
        rows = spark.sql(
            f"SELECT summary['total-data-files'] AS n FROM {fqn}.snapshots "
            f"ORDER BY committed_at DESC LIMIT 1"
        ).collect()
        return int(rows[0]["n"]) if rows and rows[0]["n"] is not None else -1
    except Exception:  # noqa: BLE001
        return -1


def maintain_table(spark, s3, bucket, table: str) -> str:
    """Bảo trì MỘT bảng. Trả về dòng tóm tắt để in.

    Tách riêng khỏi `main()` để một bảng hỏng không giết cả lượt chạy — xem lý do
    ở phần bắt lỗi trong main().
    """
    fqn = f"{CATALOG}.{table}"

    # 0) Vá property dọn metadata TRƯỚC (idempotent). Đặt trước compaction để
    #    chính các commit của bước nén cũng được dọn metadata ngay.
    for key, value in METADATA_PROPERTIES.items():
        spark.sql(f"ALTER TABLE {fqn} SET TBLPROPERTIES ('{key}'='{value}')")
    before = data_file_count(spark, fqn)

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

    # 2) Dọn snapshot cũ + file đã hết tham chiếu (giữ RETAIN_SNAPSHOTS gần nhất).
    spark.sql(
        f"CALL {CATALOG}.system.expire_snapshots("
        f"  table => '{table}',"
        f"  retain_last => {RETAIN_SNAPSHOTS})"
    )

    # 3) Dọn file MỒ CÔI (ghi xong nhưng commit fail -> không snapshot nào trỏ tới).
    #    Bước 2 KHÔNG thấy loại này. Đây là bước thiếu đã gây 397GB rác.
    #    Danh sách file do BOTO3 cung cấp (xem build_file_list_view: Iceberg 1.9.2
    #    liệt kê bằng Hadoop FS, mà project không dùng hadoop-aws).
    orphan_cutoff = (datetime.now() - timedelta(hours=ORPHAN_OLDER_THAN_HOURS)) \
        .strftime("%Y-%m-%d %H:%M:%S")
    view = f"file_list_{table.replace('.', '_')}"
    listed = build_file_list_view(spark, s3, bucket, table_location(spark, fqn), view)

    # DRY-RUN trước: đây là code XOÁ FILE trong lakehouse. Nếu nó định xoá một
    # lượng vô lý (vd gần bằng toàn bộ file đã liệt kê) thì gần như chắc chắn
    # logic đối chiếu sai -> BỎ QUA, để người xem lại, thà giữ rác còn hơn mất data.
    planned = call_orphan(spark, table, orphan_cutoff, view, dry_run=True)
    if planned and len(planned) > 0.5 * max(listed, 1):
        print(f"{table:24s} ⚠️  BỎ QUA dọn mồ côi: định xoá {len(planned):,}/{listed:,} "
              f"file (>50%) — nghi đối chiếu sai, cần xem lại thủ công")
        orphans = []
    else:
        orphans = call_orphan(spark, table, orphan_cutoff, view, dry_run=False)

    after = data_file_count(spark, fqn)
    return (f"{table:24s} files {before:>6,} -> {after:>4,}  "
        f"(rewrote {rewritten:,} -> {added:,}, "
        f"liệt kê {listed:,}, orphan xoá {len(orphans):,})")


def main() -> int:
    # Cho phép chạy TỪNG BẢNG:  python spark/maintenance.py bronze.merchants
    #
    # VÌ SAO CẦN (bài học 05-08): job này bị treo/giết 6 lần liên tiếp và CHƯA BAO
    # GIỜ chạy trọn 6 bảng. Mỗi lần treo là mất toàn bộ lượt chạy, kể cả những bảng
    # đã xong. Khi một thao tác không đáng tin, cách chữa không phải là làm nó đáng
    # tin hơn bằng mọi giá — mà là THU NHỎ ĐƠN VỊ CÔNG VIỆC để một lần hỏng chỉ tốn
    # một bảng. Bảng lớn nhất chạy riêng, bảng nhỏ gom lại.
    tables = sys.argv[1:] or TABLES
    unknown = [x for x in tables if x not in TABLES]
    if unknown:
        print(f"❌ bảng không có trong danh sách: {unknown}\n   hợp lệ: {TABLES}")
        return 2

    spark = get_spark(
        "gtl-maintenance", master="local[4]", driver_memory="6g",
        # Nén 4400+ file một lượt làm OOM khi broadcast — tắt broadcast join cho job này.
        extra_conf={"spark.sql.autoBroadcastJoinThreshold": "-1"},
    )
    s3, bucket = s3_client_and_bucket()

    ok, failed = [], []
    for table in tables:
        try:
            print(maintain_table(spark, s3, bucket, table), flush=True)
            ok.append(table)
        except Exception as exc:  # noqa: BLE001
            # CỐ Ý bắt rộng và ĐI TIẾP. Bảo trì là việc dọn dẹp: một bảng lỗi không
            # có lý do gì ngăn 5 bảng còn lại được dọn. Chết cả lượt vì một bảng
            # chính là kiểu hỏng đã khiến job này không bao giờ hoàn thành.
            print(f"{table:24s} ❌ LỖI: {type(exc).__name__}: {str(exc)[:160]}", flush=True)
            failed.append(table)

    spark.stop()
    print(f"\nDONE — xong {len(ok)}/{len(tables)} bảng"
          + (f" · LỖI: {failed}" if failed else ""))
    # Trả mã lỗi nếu có bảng hỏng: chạy trong Airflow thì task phải đỏ, không im lặng.
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
