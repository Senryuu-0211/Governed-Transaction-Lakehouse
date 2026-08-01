#!/usr/bin/env python3
"""Nghiệm thu Phase 4 — governance nâng cao. Exit != 0 nếu có check fail.

    PYTHONPATH=spark ~/working/gtl-spark-venv/bin/python scripts/verify_4.py

VÌ SAO LÀ PYTHON CHỨ KHÔNG PHẢI BASH như verify_1c/2/3:
  Bản bash đầu tiên gọi `dbt show` bảy lần -> BẢY lần khởi động JVM + Spark session
  (~40-60s mỗi lần) -> script chạy hơn 10 phút và bị timeout giết. Một script nghiệm
  thu mà chạy 10 phút thì không ai buồn chạy, tức là nó vô dụng.
  Ở đây: MỘT session duy nhất, mọi truy vấn dùng chung -> vài phút.

CÁCH CHẠY TEST GOVERNANCE MÀ KHÔNG CẦN GỌI `dbt test`:
  dbt đã biên dịch sẵn mỗi singular test thành SQL thuần trong
  target/compiled/.../tests/*.sql. Quy ước của dbt: test TRẢ VỀ DÒNG = FAIL.
  Nên chỉ cần chạy đúng SQL đó trong session sẵn có và đếm dòng — cùng ngữ nghĩa,
  không tốn thêm một lần khởi động Spark.
"""

import pathlib
import sys

from gtl_session import CATALOG, get_spark

PROJECT_ROOT = pathlib.Path(__file__).resolve().parent.parent
COMPILED_TESTS = PROJECT_ROOT / "dbt_project/target/compiled/gtl_lakehouse/tests"

# Test nào chấp nhận mức WARN: 3 anomaly test trả 1 dòng cho MỖI ngưỡng bị vượt
# (1 dòng = warn, 2+ = error — khớp config warn_if/error_if trong chính test).
# Reconciliation và PII thì KHÔNG có vùng xám: lệch một xu là lệch, lộ PII là lộ.
GOVERNANCE_TESTS = {
    "assert_reconciliation": False,              # tiền vào = tiền ra
    "assert_rowcount_not_dropped": True,         # anomaly: sụt sản lượng
    "assert_amount_distribution_stable": True,   # anomaly: dịch chuyển phân phối
    "assert_null_rate_low": True,                # anomaly: null vọt lên
    "assert_gold_no_raw_pii": False,             # PII không lọt lên Gold
}

_results: list[tuple[bool, str]] = []


def ok(msg: str) -> None:
    print(f"  ✅ {msg}")
    _results.append((True, msg))


def no(msg: str) -> None:
    print(f"  ❌ {msg}")
    _results.append((False, msg))


def main() -> int:
    print("== Phase 4 — verify ==")
    spark = get_spark("gtl-verify-4", master="local[2]", driver_memory="3g")

    # --- 1. Sổ đối soát tồn tại và có dữ liệu -------------------------------
    try:
        ledger = spark.table(f"{CATALOG}.gold.audit_reconciliation")
        n = ledger.count()
        ok(f"audit_reconciliation có {n} dòng (sổ append-only)") if n >= 1 \
            else no("sổ đối soát rỗng")
    except Exception as exc:
        no(f"không đọc được audit_reconciliation: {str(exc)[:80]}")
        n = 0

    # --- 2. Lần chạy MỚI NHẤT khớp tuyệt đối --------------------------------
    if n:
        row = spark.sql(f"""
            select count_diff, amount_diff, watermark_offset, count_gold, amount_gold
            from {CATALOG}.gold.audit_reconciliation
            where run_ts = (select max(run_ts) from {CATALOG}.gold.audit_reconciliation)
        """).collect()[0]
        if row["count_diff"] == 0 and row["amount_diff"] == 0:
            ok(f"reconciliation KHỚP: {row['count_gold']:,} giao dịch, "
               f"{row['amount_gold']:,.2f} — lệch 0")
        else:
            no(f"reconciliation LỆCH: count_diff={row['count_diff']}, "
               f"amount_diff={row['amount_diff']}")

        # Watermark = bằng chứng đối soát AS-OF một mốc, không phải 'bây giờ'.
        # Thiếu nó thì mỗi lần chạy so một lát cắt khác nhau -> không tái kiểm được.
        ok(f"sổ ghi watermark_offset={row['watermark_offset']:,} (đối soát as-of, tái kiểm được)") \
            if row["watermark_offset"] and row["watermark_offset"] > 0 \
            else no("thiếu watermark_offset")

    # --- 3. 5 test governance: chạy SQL đã compile -------------------------
    # Quy ước dbt: test trả dòng = có vấn đề. Với anomaly test hai mức, SỐ dòng
    # mới quyết mức độ (1 = warn, 2+ = error) — nên không thể coi "có dòng" = fail.
    for test, allows_warn in GOVERNANCE_TESTS.items():
        path = COMPILED_TESTS / f"{test}.sql"
        if not path.exists():
            no(f"{test}: chưa compile (chạy `bash scripts/dbt.sh compile` trước)")
            continue
        try:
            rows = spark.sql(path.read_text()).collect()
            n = len(rows)
            if n == 0:
                ok(f"{test}: PASS")
            elif allows_warn and n == 1:
                # WARN không chặn pipeline — nhưng verify PHẢI nói ra, không nuốt.
                ok(f"{test}: WARN — {rows[0]['breach']}")
            else:
                detail = rows[-1]["breach"] if "breach" in rows[-1].asDict() else f"{n} dòng"
                no(f"{test}: FAIL — {detail}")
        except Exception as exc:
            no(f"{test}: lỗi chạy — {str(exc)[:80]}")

    # --- 4. PII: 3 kỹ thuật áp đủ 100% dòng --------------------------------
    pii = spark.sql(f"""
        select
            count(*)                                                         as total,
            sum(case when length(customer_name_hash) = 64 then 1 else 0 end) as hashed,
            sum(case when phone_masked like '%*%' then 1 else 0 end)         as masked,
            sum(case when birth_year is not null then 1 else 0 end)          as generalized
        from {CATALOG}.gold.dim_account
    """).collect()[0]
    if pii["total"] and pii["hashed"] == pii["masked"] == pii["generalized"] == pii["total"]:
        ok(f"PII đủ 3 kỹ thuật trên 100% dòng ({pii['total']} tài khoản: "
           f"hash + mask + generalize)")
    else:
        no(f"PII thiếu: total={pii['total']} hashed={pii['hashed']} "
           f"masked={pii['masked']} generalized={pii['generalized']}")

    # --- 5. Time-travel: đọc được TRẠNG THÁI QUÁ KHỨ ------------------------
    snaps = spark.sql(
        f"select snapshot_id from {CATALOG}.bronze.transactions.snapshots "
        f"order by committed_at limit 1"
    ).collect()
    if snaps:
        sid = snaps[0]["snapshot_id"]
        past = spark.sql(
            f"select count(*) c from {CATALOG}.bronze.transactions "
            f"FOR VERSION AS OF {sid}"
        ).collect()[0]["c"]
        now = spark.table(f"{CATALOG}.bronze.transactions").count()
        # Quá khứ phải KHÁC hiện tại, nếu không thì có thể ta đang đọc nhầm bản hiện hành.
        ok(f"time-travel đọc được quá khứ: {past:,} dòng (hiện tại {now:,})") \
            if past < now else no(f"time-travel trả cùng số dòng ({past}) — nghi không hoạt động")
    else:
        no("không đọc được bảng .snapshots")

    # --- 6. Tài liệu + lineage ---------------------------------------------
    for doc in ("docs/pii-governance.md", "docs/time-travel-audit.md"):
        ok(doc) if (PROJECT_ROOT / doc).exists() else no(f"thiếu {doc}")
    catalog_json = PROJECT_ROOT / "dbt_project/target/catalog.json"
    ok("lineage (catalog.json) tồn tại") if catalog_json.exists() \
        else no("thiếu catalog.json — chạy `bash scripts/dbt.sh docs generate`")

    spark.stop()

    passed = sum(1 for good, _ in _results if good)
    failed = len(_results) - passed
    print(f"\n== PASS={passed} FAIL={failed} ==")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
