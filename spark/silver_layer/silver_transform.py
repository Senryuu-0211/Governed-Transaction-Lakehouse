"""Silver layer — SCAFFOLD (Phase 2, chưa hiện thực).

Bronze ghi lại nguyên trạng; Silver DIỄN GIẢI. Đây mới chỉ là khung để cố định
cấu trúc thư mục và cho notebook một chỗ để gọi vào — logic thật viết ở Phase 2
sau khi đã brainstorm.

Silver sẽ làm (theo đúng thứ tự governance trong CLAUDE.md):
  1. Parse envelope Debezium từ cột `value` thô của Bronze → cột typed.
  2. Ép kiểu đúng (amount DECIMAL(15,2), timestamp, ...).
  3. Mask PII ở các cột 🔒 của accounts (customer_name/national_id/phone/email/dob).
  4. Khử trùng theo (primary key, kafka_offset) — Bronze có thể có bản trùng khi
     stream retry; dedup ở ĐÂY, không phải ở Bronze.
  5. `MERGE INTO` theo primary key → bảng CURRENT-STATE (một dòng mỗi entity, mới
     nhất). transactions key theo txn_id, accounts theo account_id.

Quy tắc nghiệp vụ phải giữ (từ CLAUDE.md):
  - REVERSED không đếm hai lần; net amount phản ánh đảo chiều.
  - FAILED không tính vào volume thật; chỉ COMPLETED = tiền thật.
  - PENDING treo (in-doubt) KHÔNG tính vào doanh số cho tới khi được chốt.
"""

from gtl_session import CATALOG, get_spark

BRONZE = {name: f"{CATALOG}.bronze.{name}" for name in ("transactions", "accounts", "merchants")}
# SILVER = {name: f"{CATALOG}.silver.{name}" for name in (...)}  # Phase 2


def read_bronze(spark, table: str):
    """Đọc một bảng Bronze — dùng được ngay để thăm dò trong notebook."""
    return spark.table(BRONZE[table])


def build_silver(spark):
    """Điểm vào của Phase 2. CHƯA hiện thực — xem docstring module."""
    raise NotImplementedError("Silver là công việc Phase 2 — sẽ brainstorm trước khi viết.")


if __name__ == "__main__":
    spark = get_spark("gtl-silver")
    build_silver(spark)
