"""gtl_maintenance — việc chạy MỖI NGÀY: đối soát tài chính + bảo trì kho Iceberg.

Chạy 00:00 UTC = 07:00 sáng VN — đầu buổi làm việc, để nếu có lệch thì biết ngay
đầu ngày.

Tách khỏi gtl_transform: hai việc này nhịp khác nhau và hỏng không được kéo nhau.
Bảo trì lỗi/chậm không được chặn đường dữ liệu hourly.

Vì sao reconciliation ở ĐÂY chứ không phải hourly (đổi 01-08): nó quét Bronze, mà
compute on-prem đọc S3 là data-transfer-out TÍNH TIỀN. Hourly ≈ 720 lần/tháng cho
một thứ vốn là chứng cứ kiểm toán — mỗi ngày một lần là đủ và rẻ hơn ~30 lần.
Đánh đổi: cổng chặn số liệu lệch chỉ siết mỗi ngày; bù lại bằng mail alert (Phase 5).
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow.utils.trigger_rule import TriggerRule
from gtl_common import VENV_PY, ssh_task

from airflow import DAG

default_args = {"retries": 1, "retry_delay": timedelta(minutes=5)}

with DAG(
    dag_id="gtl_maintenance",
    description="Iceberg compaction + expire snapshots (daily)",
    schedule="@daily",
    start_date=datetime(2026, 7, 25),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["gtl", "maintenance"],
) as dag:
    # Đối soát TRƯỚC khi nén: nén viết lại file và tạo snapshot mới, chạy sau sẽ
    # làm nhiễu bức tranh mà kiểm toán nhìn vào.
    # pool `gtl_dbt` (1 slot): xếp hàng với dbt_run/dbt_test của gtl_transform.
    # Hai DAG này cùng nổ lúc 00:00 UTC — không có pool thì hai dbt tranh CPU
    # (6+6+3 stream = 15/16 luồng) và cả hai cùng bò.
    reconcile = ssh_task(
        "reconcile",
        "bash scripts/dbt.sh build --select audit_reconciliation assert_reconciliation",
        pool="gtl_dbt",
    )

    maintenance = ssh_task(
        "maintenance", f"PYTHONPATH=spark {VENV_PY} spark/maintenance.py"
    )

    # ALL_DONE: nén vẫn CHẠY dù đối soát fail. Đây là chủ ý — nén + dọn file mồ côi
    # là việc giữ kho khỏi phình và khỏi đốt tiền; không được để một cảnh báo số
    # liệu chặn nó (khác hẳn push_marts bên gtl_transform, nơi CHẶN là đúng vì số
    # bẩn không được ra tới người dùng). Đối soát lệch vẫn hiện đỏ trong Airflow.
    maintenance.trigger_rule = TriggerRule.ALL_DONE
    reconcile >> maintenance
