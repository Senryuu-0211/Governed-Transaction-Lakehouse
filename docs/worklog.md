# Worklog — Governed Transaction Lakehouse

Nhật ký để resume nhanh sau khi context bị nén. Mới nhất ở trên.

---

## 13-07-2026

### Trạng thái tổng
- **Phase 1:** Step 1a ✅ · Step 1b ✅ · **Step 1c ⬜ (tiếp theo)**
- Repo: `github.com/Senryuu-0211/Governed-Transaction-Lakehouse`, branch `main`, auth **SSH** (key ed25519 đã ở `~/.ssh` trên server; account-level trên GitHub → mọi repo push được).
- **Không commit (private):** `.claude/CLAUDE.md`, `PLAN_V1.md`, `.env`. README là mặt tiền public.
- **Cấm nhắc** tên tổ chức / quốc gia mục tiêu + đơn vị tiền vùng đó ở mọi file public. Currency = `USD` (trung tính).

### Đã làm — Step 1a (Postgres CDC source)
- `docker-compose.yml`: `postgres:16` (host **5433**, `wal_level=logical`, mem 1G, healthcheck) + `faker` service.
- `postgres/init/`: `00_pg_hba.sh` (mở replication cho role debezium) · `01_schema.sql` (accounts PII, transactions DECIMAL + status lifecycle PENDING→COMPLETED/FAILED→REVERSED + trigger `updated_at` + `REPLICA IDENTITY FULL`, merchants) · `02_cdc_role.sh` (role `debezium` REPLICATION+SELECT + publication `dbz_publication`).
- `faker/`: seed 100 accounts / 50 merchants, ~20 txn/s, INSERT + UPDATE lifecycle + REVERSAL, **balance mutation** khi COMPLETED (đảo lại khi REVERSED), `RANDOM_SEED`.
- Secrets ra `.env` (gitignore) + `.env.example`. `scripts/verify.sh` (11 check).

### Đã làm — Step 1b (Debezium + Kafka)
- compose += `kafka` (apache/kafka:3.8.1, KRaft, broker **9093** / controller **9095**, mem 2G) · `connect` (quay.io/debezium/connect:2.7, REST **8083**) · `kafka-ui` (provectuslabs v0.7.2, **8092**).
- `debezium/connector-config.json`: connector `gtl-postgres-connector` — `pgoutput`, `slot.name=debezium_slot`, `publication.name=dbz_publication` (`autocreate=disabled`), `table.include.list=public.{accounts,merchants,transactions}`, topic prefix `gtl` → topics `gtl.public.*`, **`decimal.handling.mode=string`** (tiền chính xác), snapshot `initial`, JSON converter schemas off.
- `scripts/register-connector.sh` (idempotent, inject password từ `.env` bằng python). `scripts/verify_1b.sh` (8 check).
- **Verified:** connector RUNNING; 3 topic đầy; envelope `r/c/u/d` + tombstone (đã bắt tận mắt op=u before/after status, op=d before + after=null).

### Quyết định quan trọng
- **Tái dùng Spark sandbox có sẵn** (`~/working/spark-docker`, Spark 3.5.0) cho 1c/transform — KHÔNG dựng Spark thứ 2 (tiết kiệm RAM). Ghi trong CLAUDE.md.
- **Phase 5 observability tái dùng** Prometheus/Grafana đang chạy (`~/working/projects/Resource-Monitoring-Dashboard`) thay vì dựng mới — xem `issues/001`.
- `issues/002`: TRANSFER chưa có tài khoản đích (counterparty) — hoãn.

### Cách resume nhanh
```bash
cd ~/working/projects/Governed-Transaction-Lakehouse
docker compose up -d --build
bash scripts/register-connector.sh      # đăng ký lại connector (idempotent)
bash scripts/verify.sh && bash scripts/verify_1b.sh
```
Đổi schema Postgres → `docker compose down -v` rồi up (init chỉ chạy trên volume trống; data faker throwaway).

### NEXT — Step 1c (đích Phase 1)
Spark Structured Streaming đọc `gtl.public.*` → **Bronze Iceberg trên MinIO qua S3 API** (`s3a://`, KHÔNG filesystem — yêu cầu cứng để Phần 2 chỉ đổi endpoint lên S3 thật).
- Ports dự kiến: MinIO API **9001** / Console **9002**, Iceberg REST **8181**, Spark UI **8086**.
- **Bronze** = raw append, giữ CDC metadata (op/ts/source), partition theo ingestion date, KHÔNG dedup (dedup ở Silver — Phase 2).
- ⚠️ **Điểm hỏng số 1:** version JAR `iceberg-spark-runtime` + `hadoop-aws` + `aws-java-sdk-bundle` phải khớp Spark 3.5.0. Nếu ClassNotFound → show lỗi thật, không đoán version.
- Tận dụng `CONFIG/spark_utils.py` (đã có helper session/IO/JDBC) nếu hợp.

### Bài học kỹ thuật (đừng vấp lại)
- `set -o pipefail` + `docker exec` thoát mã≠0 → poison exit pipeline làm `grep` false-negative. Hứng output ra biến rồi mới grep.
- Kafka 3.8 đổi package: `kafka.tools.GetOffsetShell` → `org.apache.kafka.tools.GetOffsetShell`; dùng `kafka-get-offsets.sh`.
- Live consumer bỏ lỡ event nếu produce TRƯỚC khi subscribe → chạy consumer nền trước, rồi mới thao tác (đã dùng để bắt op=d).
- pg_hba: database `replication` là mục đặc biệt, `host all all all` không bao gồm → phải thêm dòng riêng cho Debezium.
