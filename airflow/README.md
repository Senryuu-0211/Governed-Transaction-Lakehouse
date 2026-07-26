# Phase 3 — Airflow orchestration (GTL)

Airflow (stack `airflow-docker`, container) điều phối nhánh **batch**; mọi Spark/dbt
chạy ở **venv host** qua `SSHOperator`. Stream Bronze KHÔNG do Airflow quản
(tiến trình nền độc lập).

## Nguyên lý
- **Airflow = nhạc trưởng, host = nhạc công.** Container Airflow không có venv/Java/Spark
  → chỉ SSH về host ra lệnh; host chạy job trong môi trường thật.
- **Chỉ file định nghĩa DAG** được deploy vào Airflow. **File Spark xử lý data**
  (`spark/**/*.py`) KHÔNG copy đi đâu — chạy tại chỗ trên host, DAG trỏ tới bằng đường dẫn.

## DAG
- `gtl_transform` (@hourly): `cdc_health → dbt_run → dbt_test → push_marts`.
  `dbt_test` fail ⇒ `push_marts` **skip** (số bẩn không ra Superset).
- `gtl_maintenance` (@daily): nén small-file Iceberg + expire snapshots.

## Setup (một lần)
```bash
bash scripts/setup_airflow_conn.sh   # key riêng + connection gtl_host_ssh
bash scripts/deploy_dags.sh          # COPY 3 file DAG vào dags folder chung của Airflow
docker exec airflow-docker-airflow-worker-1 airflow dags reserialize   # ép parse ngay
bash scripts/verify_3.sh
```
Mỗi khi **sửa DAG**: chạy lại `scripts/deploy_dags.sh` (bước deploy: code ở repo → đẩy runtime).

## Bật lịch (DAG mới mặc định paused)
```bash
docker exec airflow-docker-airflow-worker-1 airflow dags unpause gtl_transform
docker exec airflow-docker-airflow-worker-1 airflow dags unpause gtl_maintenance
```

## Chạy thật một lần (đồng bộ, không cần đợi lịch)
```bash
docker exec airflow-docker-airflow-worker-1 airflow dags test gtl_transform
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
- **Deploy = copy, không symlink.** Container chỉ mount dags folder, symlink trỏ ra
  project dir sẽ gãy (`No such file or directory`). Sửa DAG phải chạy lại `deploy_dags.sh`.
- **Gateway host** (conn host) lấy động từ `docker inspect worker` = `172.18.0.1`. Nếu dựng
  lại network Airflow đổi subnet → chạy lại `setup_airflow_conn.sh`.
- Job Spark tự nạp `.env` (dbt.sh + gtl_session.py) — wrapper SSH chỉ `cd` vào project.
