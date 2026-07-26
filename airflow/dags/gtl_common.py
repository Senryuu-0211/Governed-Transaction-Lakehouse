"""Dùng chung cho các DAG GTL: factory SSHOperator + lệnh health-check.

Airflow đưa DAGS_FOLDER vào sys.path nên các DAG import trực tiếp
`from gtl_common import ...`. File này KHÔNG khai báo DAG nào.
"""
from __future__ import annotations

from airflow.providers.ssh.operators.ssh import SSHOperator

SSH_CONN_ID = "gtl_host_ssh"
PROJECT = "~/working/projects/Governed-Transaction-Lakehouse"
VENV_PY = "~/working/gtl-spark-venv/bin/python"
CDC_MAX_AGE_S = 900  # stream im lâu hơn 15' coi như chết

# Liveness KHÔNG cần Spark: batch commit mới nhất của stream 'transactions'
# (luồng luôn bận nhờ faker) phải tươi hơn CDC_MAX_AGE_S giây.
CDC_HEALTH_CMD = (
    'f=$(ls -t _checkpoints/transactions/commits/ 2>/dev/null | head -1); '
    '[ -n "$f" ] || { echo "NO COMMITS - stream chưa chạy?"; exit 1; }; '
    'age=$(( $(date +%s) - $(stat -c %Y "_checkpoints/transactions/commits/$f") )); '
    f'echo "freshest commit age=${{age}}s (limit {CDC_MAX_AGE_S}s)"; '
    f'[ "$age" -lt {CDC_MAX_AGE_S} ]'
)


def host_cmd(inner: str) -> str:
    """Bọc lệnh: cd vào project để PYTHONPATH=spark + scripts/dbt.sh phân giải
    đúng. dbt.sh và gtl_session.py tự nạp .env nên không cần source ở đây."""
    return f"cd {PROJECT} && {inner}"


def ssh_task(task_id: str, inner: str, cmd_timeout: int = 3600, **kwargs) -> SSHOperator:
    return SSHOperator(
        task_id=task_id,
        ssh_conn_id=SSH_CONN_ID,
        command=host_cmd(inner),
        cmd_timeout=cmd_timeout,
        **kwargs,
    )
