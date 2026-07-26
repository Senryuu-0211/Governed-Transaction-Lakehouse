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
    dbt_run = ssh_task("dbt_run", "bash scripts/dbt.sh run")
    dbt_test = ssh_task("dbt_test", "bash scripts/dbt.sh test")
    push_marts = ssh_task(
        "push_marts", f"PYTHONPATH=spark {VENV_PY} spark/gold_layer/push_marts.py"
    )

    cdc_health >> dbt_run >> dbt_test >> push_marts
