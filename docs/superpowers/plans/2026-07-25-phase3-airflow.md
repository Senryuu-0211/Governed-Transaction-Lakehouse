# Phase 3 — Airflow Orchestration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Điều phối nhánh batch (dbt transform → serve → maintenance) bằng 2 DAG Airflow, với cầu SSH worker→host và cổng chất lượng `dbt_test` chặn số liệu bẩn ra Superset.

**Architecture:** Container Airflow (stack `airflow-docker` sẵn có) chỉ *ra lệnh* qua `SSHOperator`; mọi tính toán chạy trong venv host `~/working/gtl-spark-venv`. DAG file version-control trong project, Airflow thấy qua symlink vào `~/working/notebooks/airflow/dags`.

**Tech Stack:** Airflow 2.x (CeleryExecutor), `apache-airflow-providers-ssh` 3.11.2, ssh ed25519 key, dbt-spark, PySpark 3.5.0, Iceberg trên MinIO.

## Global Constraints

- **KHÔNG `git commit` khi Mr. Senryuu chưa yêu cầu.** Mỗi task ghi rõ điểm commit nhưng thực thi phải **gộp lại và hỏi** — quy ước cứng của dự án.
- **KHÔNG sửa image/compose của stack `airflow-docker`** — provider SSH + ssh client đã có sẵn; connection lưu private key trong extra, không mount file.
- **KHÔNG sửa container đang chạy hay xoá data** mà chưa xin phép (quy ước home server). Tạo key/connection/symlink là SAFE; `docker exec ... airflow` là SAFE (chỉ đọc/ghi metadata Airflow).
- Đường dẫn project trên host: `~/working/projects/Governed-Transaction-Lakehouse`.
- Worker container: `airflow-docker-airflow-worker-1`. Dags folder host: `~/working/notebooks/airflow/dags` (mount `rwxrwxrwx`).
- Core budget giữ nguyên: dbt `local[6]/5g`, push `local[2]`, maintenance `local[4]/6g`. `max_active_runs=1` mỗi DAG.
- Ngưỡng liveness stream: **900s (15 phút)**.

---

## File Structure

```
airflow/
  dags/gtl_common.py        # hằng số + factory ssh_task() + CDC_HEALTH_CMD
  dags/gtl_transform.py     # DAG @hourly: cdc_health → dbt_run → dbt_test → push_marts
  dags/gtl_maintenance.py   # DAG @daily: maintenance
  README.md                 # setup key/connection/symlink + cách chứng minh cổng chặn
scripts/setup_airflow_conn.sh   # tạo key ed25519 + authorized_keys + airflow connection (idempotent)
scripts/verify_3.sh             # nghiệm thu Phase 3
```
Symlink (Task 5): `~/working/notebooks/airflow/dags/gtl_{common,transform,maintenance}.py → <project>/airflow/dags/*`.

---

### Task 1: Cầu SSH (key riêng + Airflow connection)

**Files:**
- Create: `scripts/setup_airflow_conn.sh`

**Interfaces:**
- Produces: Airflow connection `gtl_host_ssh` (ssh, host=gateway host, login=senryuu, private key trong extra) mà mọi DAG dùng qua `ssh_conn_id="gtl_host_ssh"`.

- [ ] **Step 1: Viết `scripts/setup_airflow_conn.sh`**

```bash
#!/usr/bin/env bash
# =============================================================================
# Dựng cầu SSH worker(Airflow) -> host: key riêng + Airflow connection.
# Idempotent — chạy lại nhiều lần an toàn. CHẠY TRÊN HOST (user senryuu).
# =============================================================================
set -euo pipefail
KEY="$HOME/.ssh/gtl_airflow_ed25519"
WORKER="airflow-docker-airflow-worker-1"
CONN="gtl_host_ssh"

# 1. Key riêng cho Airflow (tách key cá nhân -> thu hồi độc lập)
[ -f "$KEY" ] || ssh-keygen -t ed25519 -N '' -C 'gtl-airflow' -f "$KEY"

# 2. Cho phép key này SSH vào chính host (idempotent)
PUB="$(cat "$KEY.pub")"
touch "$HOME/.ssh/authorized_keys"; chmod 600 "$HOME/.ssh/authorized_keys"
grep -qF "$PUB" "$HOME/.ssh/authorized_keys" || echo "$PUB" >> "$HOME/.ssh/authorized_keys"

# 3. Gateway host nhìn từ worker (động — không hard-code subnet docker)
GW="$(docker inspect "$WORKER" --format '{{range .NetworkSettings.Networks}}{{.Gateway}}{{end}}')"
echo "host gateway (từ worker) = $GW"

# 4. Airflow connection: private key + tắt strict host-key (LAN Tailscale tin cậy)
EXTRA="$(python3 - "$KEY" <<'PY'
import json, sys
key = open(sys.argv[1]).read()
print(json.dumps({"private_key": key, "no_host_key_check": True, "conn_timeout": 30}))
PY
)"
docker exec "$WORKER" airflow connections delete "$CONN" >/dev/null 2>&1 || true
docker exec "$WORKER" airflow connections add "$CONN" \
  --conn-type ssh --conn-host "$GW" --conn-login senryuu --conn-extra "$EXTRA"

# 5. Kiểm SSH thông: worker -> host chạy whoami
echo "-- test SSH worker -> host --"
docker exec "$WORKER" python -c "
from airflow.providers.ssh.hooks.ssh import SSHHook
c = SSHHook(ssh_conn_id='$CONN').get_conn()
_, out, err = c.exec_command('whoami')
who = out.read().decode().strip()
print('remote whoami =', who or err.read().decode())
assert who == 'senryuu', 'SSH bridge FAIL'
print('OK: SSH bridge worker -> host hoạt động')
"
```

- [ ] **Step 2: Chạy script**

Lv: MODERATE (ghi `~/.ssh/authorized_keys`, tạo Airflow connection — không đụng container đang chạy, không sudo).
Run: `bash scripts/setup_airflow_conn.sh`
Expected (cuối output): `remote whoami = senryuu` và `OK: SSH bridge worker -> host hoạt động`.

- [ ] **Step 3: Xác nhận connection tồn tại độc lập**

Run: `docker exec airflow-docker-airflow-worker-1 airflow connections get gtl_host_ssh`
Expected: in ra JSON có `"conn_type": "ssh"`, `"login": "senryuu"`.

- [ ] **Step 4: Commit** *(CHỜ Mr. Senryuu duyệt — không tự chạy)*

```bash
git add scripts/setup_airflow_conn.sh
git commit -m "feat(phase3): SSH bridge worker->host cho Airflow (key riêng + connection)"
```

---

### Task 2: DAG helper `gtl_common.py`

**Files:**
- Create: `airflow/dags/gtl_common.py`

**Interfaces:**
- Consumes: connection `gtl_host_ssh` (Task 1).
- Produces:
  - `ssh_task(task_id: str, inner: str, cmd_timeout: int = 3600, **kwargs) -> SSHOperator`
  - `CDC_HEALTH_CMD: str` — lệnh bash kiểm liveness stream, exit!=0 nếu stale/không có commit.
  - hằng: `SSH_CONN_ID`, `PROJECT`, `VENV_PY`, `CDC_MAX_AGE_S`.

- [ ] **Step 1: Viết `airflow/dags/gtl_common.py`**

```python
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
```

- [ ] **Step 2: Kiểm import sạch bằng venv của worker**

Run:
```bash
docker exec airflow-docker-airflow-worker-1 python -c "import sys; sys.path.insert(0,'/opt/airflow/dags'); import types" 2>&1 | head
```
(Ở bước này file chưa symlink; chỉ kiểm cú pháp Python:)
Run: `python3 -c "import ast; ast.parse(open('airflow/dags/gtl_common.py').read()); print('parse OK')"`
Expected: `parse OK`.

- [ ] **Step 3: Commit** *(CHỜ Mr. Senryuu duyệt)*

```bash
git add airflow/dags/gtl_common.py
git commit -m "feat(phase3): helper gtl_common (ssh_task factory + cdc health cmd)"
```

---

### Task 3: DAG `gtl_transform` (@hourly)

**Files:**
- Create: `airflow/dags/gtl_transform.py`

**Interfaces:**
- Consumes: `ssh_task`, `CDC_HEALTH_CMD`, `VENV_PY` (Task 2).
- Produces: DAG id `gtl_transform` với chuỗi `cdc_health → dbt_run → dbt_test → push_marts`.

- [ ] **Step 1: Viết `airflow/dags/gtl_transform.py`**

```python
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
```

- [ ] **Step 2: Kiểm cú pháp**

Run: `python3 -c "import ast; ast.parse(open('airflow/dags/gtl_transform.py').read()); print('parse OK')"`
Expected: `parse OK`.

- [ ] **Step 3: Commit** *(CHỜ Mr. Senryuu duyệt)*

```bash
git add airflow/dags/gtl_transform.py
git commit -m "feat(phase3): DAG gtl_transform hourly (cdc_health->dbt->gate->push)"
```

---

### Task 4: DAG `gtl_maintenance` (@daily)

**Files:**
- Create: `airflow/dags/gtl_maintenance.py`

**Interfaces:**
- Consumes: `ssh_task`, `VENV_PY` (Task 2).
- Produces: DAG id `gtl_maintenance` với 1 task `maintenance`.

- [ ] **Step 1: Viết `airflow/dags/gtl_maintenance.py`**

```python
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
```

- [ ] **Step 2: Kiểm cú pháp**

Run: `python3 -c "import ast; ast.parse(open('airflow/dags/gtl_maintenance.py').read()); print('parse OK')"`
Expected: `parse OK`.

- [ ] **Step 3: Commit** *(CHỜ Mr. Senryuu duyệt)*

```bash
git add airflow/dags/gtl_maintenance.py
git commit -m "feat(phase3): DAG gtl_maintenance daily (compaction + expire)"
```

---

### Task 5: Nối DAG vào Airflow (symlink) + xác nhận nạp

**Files:**
- Create (symlink, không track trong git): 3 symlink trong `~/working/notebooks/airflow/dags/`

**Interfaces:**
- Consumes: 3 file DAG (Task 2-4) + connection (Task 1).
- Produces: `gtl_transform` + `gtl_maintenance` xuất hiện trong `airflow dags list`, parse không lỗi.

- [ ] **Step 1: Tạo symlink**

Lv: SAFE (tạo symlink trong dags folder mount `rwxrwxrwx`).
Run:
```bash
DAGS=~/working/notebooks/airflow/dags
SRC=~/working/projects/Governed-Transaction-Lakehouse/airflow/dags
for f in gtl_common.py gtl_transform.py gtl_maintenance.py; do
  ln -sf "$SRC/$f" "$DAGS/$f"
done
ls -l "$DAGS"/gtl_*.py
```
Expected: 3 symlink trỏ về `.../projects/Governed-Transaction-Lakehouse/airflow/dags/`.

- [ ] **Step 2: Đợi scheduler quét & kiểm không có import error**

Run:
```bash
sleep 30
docker exec airflow-docker-airflow-worker-1 airflow dags list-import-errors
```
Expected: **không** dòng nào chứa `gtl_transform` / `gtl_maintenance` / `gtl_common` (không lỗi parse). Nếu có → sửa file nguồn, symlink tự cập nhật.

- [ ] **Step 3: Kiểm 2 DAG đã nạp**

Run: `docker exec airflow-docker-airflow-worker-1 airflow dags list | grep gtl_`
Expected: hai dòng `gtl_transform` và `gtl_maintenance`.

- [ ] **Step 4: (không commit — symlink là artefact runtime, đã gitignore `_checkpoints/`; symlink nằm ngoài repo)**

---

### Task 6: Nghiệm thu `verify_3.sh` + README + chạy thật đầu-cuối

**Files:**
- Create: `scripts/verify_3.sh`
- Create: `airflow/README.md`

**Interfaces:**
- Consumes: tất cả task trên.
- Produces: script nghiệm thu exit!=0 khi fail; tài liệu vận hành.

- [ ] **Step 1: Viết `scripts/verify_3.sh`**

```bash
#!/usr/bin/env bash
# =============================================================================
# Verify Phase 3 — Airflow orchestrate. Exit != 0 nếu có check fail.
# =============================================================================
set -uo pipefail
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
WORKER="airflow-docker-airflow-worker-1"
AF() { docker exec "$WORKER" airflow "$@"; }

pass=0; fail=0
ok() { echo "  ✅ $1"; pass=$((pass+1)); }
no() { echo "  ❌ $1"; fail=$((fail+1)); }

echo "== Phase 3 — verify =="

# 1. Connection SSH tồn tại
AF connections get gtl_host_ssh >/dev/null 2>&1 && ok "connection gtl_host_ssh tồn tại" || no "thiếu connection gtl_host_ssh"

# 2. Không lỗi parse DAG
errs=$(AF dags list-import-errors 2>/dev/null | grep -E 'gtl_(transform|maintenance|common)' || true)
[ -z "$errs" ] && ok "DAG parse sạch (không import error)" || no "DAG có import error: $errs"

# 3. Hai DAG đã nạp
for d in gtl_transform gtl_maintenance; do
  AF dags list 2>/dev/null | grep -q "$d" && ok "DAG $d đã nạp" || no "DAG $d chưa nạp"
done

# 4. Cổng chặn được BẢO ĐẢM bởi cấu trúc: push_marts.trigger_rule = all_success
tr=$(AF tasks list gtl_transform 2>/dev/null | grep -c push_marts || echo 0)
[ "$tr" -ge 1 ] && ok "task push_marts tồn tại trong gtl_transform" || no "thiếu task push_marts"

# 5. SSH thông + cdc_health chạy thật trên host (đi qua đúng cầu SSH)
if AF tasks test gtl_transform cdc_health 2>&1 | grep -q "freshest commit age"; then
  ok "cdc_health chạy qua SSH tới host (stream tươi)"
else
  no "cdc_health lỗi — kiểm cầu SSH hoặc stream đã chết"
fi

echo
echo "== PASS=${pass} FAIL=${fail} =="
[ "$fail" -eq 0 ]
```

- [ ] **Step 2: Chạy verify_3.sh**

Lv: SAFE (`airflow tasks test` chạy 1 task lẻ, không schedule; cdc_health chỉ đọc file).
Run: `bash scripts/verify_3.sh`
Expected: `== PASS=5 FAIL=0 ==` (hoặc hơn nếu thêm check), exit 0.

- [ ] **Step 3: Chạy thật đầu-cuối 1 lần (đồng bộ, không cần đợi lịch)**

Lv: MODERATE (chạy dbt run/test + push_marts thật trên host — ghi Silver/Gold/marts + Postgres. Idempotent, an toàn chạy lại).
Run: `docker exec airflow-docker-airflow-worker-1 airflow dags test gtl_transform 2>&1 | tail -20`
Expected: các task `cdc_health, dbt_run, dbt_test, push_marts` đều `success`; dòng cuối `DAG run ... state=success`.
Kiểm serving cập nhật (DBeaver/psql `marts` db): `select max(updated_at) from marts.mart_daily_volume;` tiến so với trước.

- [ ] **Step 4: Viết `airflow/README.md`**

```markdown
# Phase 3 — Airflow orchestration (GTL)

Airflow (stack `airflow-docker`, container) điều phối nhánh **batch**; mọi Spark/dbt
chạy ở **venv host** qua `SSHOperator`. Stream Bronze KHÔNG do Airflow quản
(tiến trình nền độc lập).

## DAG
- `gtl_transform` (@hourly): `cdc_health → dbt_run → dbt_test → push_marts`.
  `dbt_test` fail ⇒ `push_marts` **skip** (số bẩn không ra Superset).
- `gtl_maintenance` (@daily): nén small-file Iceberg + expire snapshots.

## Setup (một lần)
```bash
bash scripts/setup_airflow_conn.sh          # key riêng + connection gtl_host_ssh
DAGS=~/working/notebooks/airflow/dags
SRC=~/working/projects/Governed-Transaction-Lakehouse/airflow/dags
for f in gtl_common.py gtl_transform.py gtl_maintenance.py; do ln -sf "$SRC/$f" "$DAGS/$f"; done
bash scripts/verify_3.sh
```

## Chứng minh cổng chặn (thủ công, phá tạm rồi khôi phục)
Thêm 1 test luôn fail rồi chạy `airflow dags test gtl_transform`, xác nhận
`push_marts` chuyển trạng thái **skipped**, sau đó gỡ test:
```bash
echo "select 1 as x" > dbt_project/tests/assert_force_fail.sql   # trả về dòng => fail
docker exec airflow-docker-airflow-worker-1 airflow dags test gtl_transform 2>&1 | grep -E "dbt_test|push_marts"
rm dbt_project/tests/assert_force_fail.sql                        # khôi phục
```
Kỳ vọng: `dbt_test` = failed, `push_marts` = skipped.

## Bẫy
- Gateway host (conn host) lấy động từ `docker inspect worker`. Nếu dựng lại network
  Airflow đổi subnet → chạy lại `setup_airflow_conn.sh`.
- Job Spark tự nạp `.env` (dbt.sh + gtl_session.py) — wrapper chỉ `cd` vào project.
```

- [ ] **Step 5: Commit** *(CHỜ Mr. Senryuu duyệt)*

```bash
git add scripts/verify_3.sh airflow/README.md
git commit -m "feat(phase3): verify_3 nghiệm thu + README vận hành Airflow"
```

---

## Self-Review

**Spec coverage:**
- §2 nguyên lý điều phối/tính toán → Task 1-2 (SSH bridge + host_cmd). ✅
- §3 cầu SSH (key/connection/wrapper) → Task 1 + `host_cmd` (Task 2). ✅ (bỏ `source .env` vì entrypoint tự nạp — cải tiến so spec, ghi rõ ở README §Bẫy)
- §4 gtl_transform + cổng → Task 3 + verify §4/§5. ✅
- §5 gtl_maintenance → Task 4. ✅
- §6 idempotent/backfill → `max_active_runs=1` + bản chất MERGE/overwrite (không cần code thêm). ✅
- §7 lỗi/core budget → retries trong DAG; `max_active_runs=1` chặn oversubscription. ✅
- §8 bố cục + symlink → Task 5. ✅
- §9 nghiệm thu 5 bước → Task 6 (check 1-5 + chạy thật + gate proof trong README). ✅
- §10 bẫy (cwd/.env, host key, symlink perm, checkpoint path) → xử lý trong Task 1/2/5 + README. ✅ (checkpoint path đã sửa thành `_checkpoints/transactions/commits`)
- §11 nợ kỹ thuật (lag thật Phase 5) → ghi trong spec, không phải task Phase 3. ✅

**Placeholder scan:** không TBD/TODO; mọi step có code/lệnh + expected cụ thể. ✅
**Type consistency:** `ssh_task`, `CDC_HEALTH_CMD`, `VENV_PY`, `SSH_CONN_ID`, `gtl_host_ssh` dùng nhất quán qua Task 2→3→4→6. ✅
```
