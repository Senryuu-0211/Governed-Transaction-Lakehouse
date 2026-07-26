#!/usr/bin/env bash
# =============================================================================
# Deploy DAG GTL vào dags folder chung của Airflow.
#
# Vì sao COPY chứ không symlink: container Airflow chỉ mount dags folder, KHÔNG
# thấy project dir -> symlink trỏ ra ngoài sẽ gãy. File DAG phải nằm THẬT trong
# dags folder. Source-of-truth vẫn là bản gốc trong git (airflow/dags/).
#
# Chỉ deploy 3 file ĐỊNH NGHĨA DAG. File Spark xử lý data KHÔNG deploy — chúng
# chạy trên host qua SSH, DAG chỉ trỏ tới bằng đường dẫn.
#
# Chạy lại script này mỗi khi sửa DAG (bước "deploy" chuẩn: code ở repo -> đẩy runtime).
# =============================================================================
set -euo pipefail
SRC="$(cd "$(dirname "${BASH_SOURCE[0]}")/../airflow/dags" && pwd)"
DEST="$HOME/working/notebooks/airflow/dags"

for f in gtl_common.py gtl_transform.py gtl_maintenance.py; do
  cp "$SRC/$f" "$DEST/$f"
  echo "deployed: $f -> $DEST/"
done
