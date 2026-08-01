#!/usr/bin/env bash
# =============================================================================
# Verify Phase 4 — Governance nâng cao. Giữ tên `verify_4.sh` cho nhất quán với
# verify_1c / verify_2 / verify_2_5 / verify_3, nhưng phần kiểm nằm ở verify_4.py.
#
# VÌ SAO TÁCH SANG PYTHON: bản bash gọi `dbt show` bảy lần = bảy lần khởi động
# Spark (~40-60s mỗi lần) -> chạy >10 phút rồi bị timeout giết. verify_4.py dùng
# MỘT session cho tất cả truy vấn.
# =============================================================================
set -uo pipefail
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_ROOT" || exit 1
VENV_PYTHON="${VENV_PYTHON:-$HOME/working/gtl-spark-venv/bin/python}"

# Test governance được chạy từ SQL đã compile -> phải chắc target/ còn mới.
# `compile` không đụng data, chỉ render SQL (nhanh hơn `dbt test` rất nhiều).
bash scripts/dbt.sh compile >/dev/null 2>&1 \
  || echo "  ⚠️  dbt compile lỗi — verify sẽ dùng bản compile cũ nếu có"

PYTHONPATH=spark "$VENV_PYTHON" scripts/verify_4.py
