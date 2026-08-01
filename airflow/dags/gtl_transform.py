"""gtl_transform — nhánh batch hourly: Bronze mới -> Silver/Gold/marts -> Superset.

dbt_test là CỔNG chất lượng: fail thì push_marts skip (trigger_rule all_success),
số liệu bẩn không bao giờ ra Superset.
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from gtl_common import CDC_HEALTH_CMD, VENV_PY, ssh_task

default_args = {"retries": 2, "retry_delay": timedelta(minutes=3)}

with DAG(
    dag_id="gtl_transform",
    description="Bronze -> Silver/Gold/marts -> Superset (hourly, gated by dbt tests)",
    schedule="@hourly",
    start_date=datetime(2026, 7, 25),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["gtl", "transform"],
) as dag:
    # Health check không retry: stream chết thì báo đỏ NGAY, đừng trì hoãn.
    cdc_health = ssh_task("cdc_health", CDC_HEALTH_CMD, cmd_timeout=120, retries=0)
    # LOẠI reconciliation khỏi nhịp hourly (01-08): nó quét Bronze theo cửa sổ, mà
    # compute chạy on-prem nên mỗi lần đọc S3 là data-transfer-out TÍNH TIỀN.
    # Hourly ≈ 720 lần/tháng là lãng phí — đối soát là chứng cứ kiểm toán, mỗi ngày
    # một lần là đủ. Nó chạy trong gtl_maintenance (00:00 UTC = 07:00 sáng VN).
    # pool `gtl_dbt` (1 slot) xếp hàng MỌI task chạy dbt trên toàn bộ Airflow.
    # Vì sao cần dù đã cô lập Derby: mỗi dbt lấy local[6]; hai cái cùng chạy + stream
    # local[3] = 15/16 luồng -> vỡ core budget, cả hai cùng bò. Derby fix lo tính
    # ĐÚNG ĐẮN (chạy được), pool lo TRẬT TỰ TÀI NGUYÊN (chạy lần lượt).
    # Thấy rõ nhất lúc 00:00 UTC: gtl_transform (@hourly) và gtl_maintenance (@daily)
    # cùng nổ một lúc.
    dbt_run = ssh_task("dbt_run", "bash scripts/dbt.sh run --exclude audit_reconciliation",
                       pool="gtl_dbt")
    dbt_test = ssh_task("dbt_test", "bash scripts/dbt.sh test --exclude assert_reconciliation",
                        pool="gtl_dbt")
    push_marts = ssh_task(
        "push_marts", f"PYTHONPATH=spark {VENV_PY} spark/gold_layer/push_marts.py"
    )

    cdc_health >> dbt_run >> dbt_test >> push_marts
