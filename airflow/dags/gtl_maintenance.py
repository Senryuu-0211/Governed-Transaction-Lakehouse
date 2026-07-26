"""gtl_maintenance — bảo trì kho Iceberg (daily): nén small-file + expire snapshots.

Tách khỏi gtl_transform: bảo trì lỗi/chậm không kéo đường dữ liệu hourly.
spark/maintenance.py đã gộp rewrite_data_files + expire_snapshots trong 1 job.
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from gtl_common import VENV_PY, ssh_task

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
    maintenance = ssh_task(
        "maintenance", f"PYTHONPATH=spark {VENV_PY} spark/maintenance.py"
    )
