"""Gold layer — SCAFFOLD (Phase 3+, chưa hiện thực).

Gold là tầng phục vụ: star schema cho phân tích, dựng TỪ current-state của Silver
(không đọc thẳng Bronze thô). Đây mới là khung cấu trúc thư mục.

Gold sẽ làm:
  - fact_transactions (chỉ COMPLETED = tiền thật; REVERSED trừ ra, không đếm hai lần)
  - dim_account (đã mask PII), dim_merchant, dim_date
  - Reconciliation: tổng amount COMPLETED ở Silver == tổng ở Gold mỗi lần chạy —
    gác cổng đúng-sai của cả pipeline (CLAUDE.md).

Phụ thuộc: cần Silver current-state ổn định trước → chỉ làm sau Phase 2.
"""

from gtl_session import CATALOG, get_spark

# SILVER = {...}   # nguồn đọc vào của Gold (Phase 2 định nghĩa)
# GOLD   = {...}   # Phase 3


def build_gold(spark):
    """Điểm vào của Phase 3. CHƯA hiện thực — xem docstring module."""
    raise NotImplementedError("Gold là công việc Phase 3 — cần Silver xong trước.")


if __name__ == "__main__":
    spark = get_spark("gtl-gold")
    build_gold(spark)
