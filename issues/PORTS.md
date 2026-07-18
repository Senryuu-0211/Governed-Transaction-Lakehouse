# PORTS — Governed Transaction Lakehouse (Project 2)

Kiểm tra `ss -ltn | grep :<port>` trước khi thêm service mới.

| Service | Host port | Trạng thái | Ghi chú |
|---|---|---|---|
| Postgres source | **5433** | ✅ dùng (Step 1a) | 5432 đã có Postgres Airflow |
| Kafka broker | 9093 | ✅ dùng | Step 1b |
| Kafka controller | 9095 | ✅ dùng | Step 1b |
| Kafka UI | 8092 | ✅ dùng | Step 1b |
| Debezium Connect | 8083 | ✅ dùng | Step 1b |
| MinIO API | 9001 | trống | Step 1c |
| MinIO Console | 9002 | trống | Step 1c |
| Iceberg REST | 8181 | trống | Step 1c |
| Spark Master UI (P2) | 8086 | trống | Step 1c |
| Airflow web | 8085 | trống | Phase 3 |
| Prometheus | 9090 | ⚠️ **XUNG ĐỘT** | Đang dùng bởi monitoring stack (bind Tailscale IP 100.71.245.124). Phase 5 phải đổi port hoặc gộp. |
| Grafana | 3000 | ⚠️ **XUNG ĐỘT** | monitoring stack đang chiếm (Tailscale IP). Phase 5: gộp dùng chung hoặc đổi. |

## Đã chiếm sẵn trên server (né ra)
- 5432 Postgres Airflow · 8081 Airflow web · 8080 Spark master (sandbox) · 8082 Spark worker
- 8888 Jupyter · 8000/9443 Portainer · 3000/9090 monitoring (Tailscale-bound) · 3306? chưa
