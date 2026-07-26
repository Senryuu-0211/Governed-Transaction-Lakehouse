# Phase 3 — Airflow Orchestration (Design Spec)

> Ngày: 2026-07-25 · Trạng thái: **đã duyệt thiết kế, chờ review spec** · Phần của: Governed Transaction Lakehouse

## 1. Mục tiêu & phạm vi

Điều phối **nhánh batch** của pipeline (dbt transform → serve → maintenance) bằng Airflow:
idempotent, quan sát được, có cổng chất lượng chặn số liệu bẩn. Bám north-star của project:
*số liệu chỉ có giá trị khi non-DE tin và dùng được* → cổng test bảo vệ đúng điều đó.

**Trong phạm vi:** 2 DAG (transform hourly, maintenance daily), cầu SSH worker→host, health-check
stream, cổng `dbt_test`, verify script.

**Ngoài phạm vi (Phase khác):** giám sát/khởi động lại stream Bronze (vẫn là tiến trình nền độc lập,
không do Airflow quản); alerting/email (Phase 5); CI chạy dbt test pre-merge (Phase 6); replay lịch
sử sâu bằng Iceberg time-travel.

## 2. Nguyên lý nền: Airflow điều phối, host tính toán

Container Airflow **không có** venv/Java/Spark của host (đã kiểm chứng: worker thấy Python 3.12, không
Java, không thấy `~/working/gtl-spark-venv`). Vì vậy mọi task chỉ là **SSHOperator** SSH ngược về host
chạy lệnh trong môi trường thật. Airflow = nhạc trưởng (ra lệnh), host = nhạc công (chơi).

Quyết định đã chốt: **tái dùng stack `airflow-docker` sẵn có** (đang chạy 24/7) thay vì dựng Airflow
host-native mới — tránh trùng lắp orchestrator, giữ kỷ luật container/reproducible.

## 3. Cầu SSH (worker → host)

- **Key riêng:** `~/.ssh/gtl_airflow_ed25519` (ed25519, tách biệt key cá nhân → thu hồi được độc lập).
  Pubkey append vào `~/.ssh/authorized_keys` của `senryuu`.
- **Airflow Connection `gtl_host_ssh`:** `host=100.71.245.124` (Tailscale IP — ổn định qua việc dựng
  lại docker network), `login=senryuu`, private key nhét trong connection extra (không mount vào
  worker → **không sửa compose Airflow**). Tạo bằng `airflow connections add` qua `docker exec`.
- **Lệnh chuẩn hoá** mỗi task chạy trên host (wrapper lo cwd + nạp `.env` để có AWS creds cho S3):
  ```bash
  cd ~/working/projects/Governed-Transaction-Lakehouse && set -a && . ./.env && set +a && <CMD>
  ```
- Provider `apache-airflow-providers-ssh` 3.11.2 + `/usr/bin/ssh` đã có sẵn trong worker → **không
  đổi image**.

## 4. DAG `gtl_transform` (`@hourly`)

| Task | CMD trên host | Vai trò |
|---|---|---|
| `cdc_health` | kiểm mtime file offset mới nhất trong `spark/_checkpoints/bronze_*/offsets/` < ngưỡng (mặc định 15 phút) | Stream còn tươi? Chỉ đọc file, **không khởi động Spark**. Stream chết → fail sớm, không xử data cũ âm thầm |
| `dbt_run` | `bash scripts/dbt.sh run` | Silver → Gold → marts (incremental MERGE theo PK) |
| `dbt_test` | `bash scripts/dbt.sh test` | **Cổng chất lượng** |
| `push_marts` | `PYTHONPATH=spark ~/working/gtl-spark-venv/bin/python spark/gold_layer/push_marts.py` | Đẩy marts → Postgres cho Superset |

**Phụ thuộc:** `cdc_health → dbt_run → dbt_test → push_marts` (tuyến tính).
`dbt_test` fail → `push_marts` **skip** (trigger_rule mặc định `all_success`). Số bẩn không ra Superset.

**Cấu hình:** `retries=2`, `retry_delay=3min` (Spark cold-start đôi khi flaky), `catchup=False`,
`max_active_runs=1` (không cho 2 lần chạy chồng — tránh đụng độ ghi Iceberg/JDBC).

## 5. DAG `gtl_maintenance` (`@daily`)

| Task | CMD trên host | Vai trò |
|---|---|---|
| `maintenance` | `PYTHONPATH=spark ~/working/gtl-spark-venv/bin/python spark/maintenance.py` | `rewrite_data_files` (nén small-file) + `expire_snapshots` |

Tách hẳn khỏi transform: bảo trì kho lỗi/chậm không kéo đường dữ liệu hourly. `retries=1`,
`catchup=False`, `max_active_runs=1`. `maintenance.py` đã gộp compaction + expire trong một job
(local[4]/6g theo core budget) → một task là đủ.

## 6. Idempotent & backfill

- **Idempotent:** dbt incremental MERGE theo PK (chạy lại cùng cửa sổ không đếm trùng); push_marts
  overwrite+truncate; maintenance vốn idempotent. Chạy lại một task an toàn.
- **Backfill = bắt kịp:** Silver là *current-state CDC* → mỗi lần chạy xử toàn bộ Bronze mới tới hiện
  tại. Không có reprocess theo ngày kiểu bảng partition thời gian, và đó là *đúng* cho current-state.
  Replay lịch sử sâu (nếu cần) khai thác Iceberg time-travel — ngoài phạm vi Phase 3.

## 7. Xử lý lỗi & quan sát

- Lỗi hiện đỏ trong Airflow UI + log task (stdout SSH kèm về). `on_failure_callback` ghi log gọn.
- **Chưa** alerting/email (Phase 5). Không bật SMTP.
- Core budget giữ nguyên: transform gọi dbt local[6]/5g + push local[2]; maintenance local[4]/6g.
  Vì `max_active_runs=1` và 2 DAG nhịp khác nhau, không lo oversubscription 16 luồng khi trùng giờ
  (kịch bản xấu nhất: maintenance daily 2h sáng trùng transform hourly → local[4]+local[6]=10 < 16).

## 8. Bố cục file (version-control trong project, Airflow thấy qua symlink)

```
<project>/airflow/
  dags/gtl_transform.py
  dags/gtl_maintenance.py
  dags/gtl_common.py           # helper build lệnh SSH chuẩn hoá + hằng số
  README.md                    # cách setup key + connection + symlink
scripts/setup_airflow_conn.sh  # tạo key ed25519 + append authorized_keys + airflow connections add (idempotent)
scripts/verify_3.sh            # nghiệm thu
```

Symlink `~/working/notebooks/airflow/dags/gtl_transform.py → <project>/airflow/dags/gtl_transform.py`
(và maintenance, common). → git track trong repo project, Airflow scheduler vẫn nạp (nó theo symlink).

## 9. Nghiệm thu — `verify_3.sh`

1. `airflow dags list` (qua `docker exec`) thấy `gtl_transform` + `gtl_maintenance`, **parse không lỗi**.
2. Connection `gtl_host_ssh` tồn tại; chạy được 1 lệnh test SSH trên host (vd `whoami` → `senryuu`).
3. `cdc_health` chạy độc lập pass khi stream sống (offset file tươi).
4. Trigger `gtl_transform` một lần → chạy tới `push_marts`; kiểm marts trong Postgres (`marts` db)
   có cập nhật (row count / max updated_at tiến).
5. **Chứng minh cổng chặn:** giả lập `dbt_test` fail (hoặc 1 test tạm luôn fail) → xác nhận
   `push_marts` bị **skip**, không chạy.

## 10. Bẫy đã lường trước

- **cwd + `.env`:** job Spark cần AWS creds cho S3; wrapper phải `cd` project + `source .env` trước
  khi gọi venv python, nếu không `push_marts`/`maintenance` fail xác thực MinIO.
- **SSH host key:** lần SSH đầu worker sẽ hỏi `known_hosts`; setup script phải `ssh-keyscan` hoặc
  connection tắt strict host key checking (chấp nhận được trong LAN Tailscale tin cậy).
- **Symlink permission:** dags folder mount `rwxrwxrwx` (đã kiểm) → symlink tạo được.
- **Checkpoint path cho `cdc_health`:** phải xác nhận tên thư mục checkpoint thật của bronze stream
  (đọc từ `bronze_stream.py`) trước khi hard-code ngưỡng.

## 11. Nợ kỹ thuật ghi nhận

- `cdc_health` bản v1 chỉ đọc mtime offset file (đủ phát hiện stream chết). Đo *lag thực* (offset
  Bronze vs offset Kafka mới nhất) để Phase 5 làm cùng dashboard freshness.
- Nếu sau này muốn backfill theo ngày thật → cần tham số hoá execution_date vào dbt vars; hiện chưa cần.
