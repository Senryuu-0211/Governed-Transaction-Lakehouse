# Issue 005 — iceberg-rest OOM (heap 128MB) giết Bronze stream

**Ngày:** 2026-07-26 · **Mức:** nghiêm trọng (stream chết, không tự dậy được) · **Trạng thái:** ĐÃ FIX

## Triệu chứng
- Bronze stream chết với `CommitStateUnknownException: Service failed: 500` →
  `java.lang.OutOfMemoryError: Java heap space`.
- Xảy ra 2 kịch bản: (1) stream + `dbt run` chạy đồng thời (end-to-end test Phase 3);
  (2) stream chạy MỘT MÌNH nhưng nuốt backlog ~27h dồn trong Kafka.

## Nguyên nhân gốc
`iceberg-rest` (catalog) bị `deploy.resources.limits.memory: 512M` và **không set `-Xmx`**.
JVM lấy heap mặc định ~25% mem_limit → chỉ **~128MB**. Quá nhỏ cho một JVM service khi bị
dồn commit đồng thời hoặc commit backlog lớn (mỗi request serialize JSON / merge metadata
đều ngốn heap). Catalog OOM giữa commit → stream không biết commit thành/bại
(`CommitStateUnknownException`) → coi là chí mạng → tự tắt.

## Fix
`docker-compose.yml`, service `iceberg-rest`:
- `command`: thêm `-Xmx1g` (heap tường minh 1GB thay vì mặc định 128MB).
- `deploy.resources.limits.memory`: `512M` → `1536M` (chứa heap 1g + metaspace/off-heap).
- `docker compose up -d iceberg-rest` (catalog data ở Postgres `iceberg_catalog` → recreate mất 0 byte).

Kết quả: stream nuốt backlog 27h ổn định, batch tiến liên tục, không OOM.

## Bài học (đưa vào CORE BUDGET)
Core budget cũ chỉ tính **core/heap của Spark**, BỎ SÓT **heap của service catalog dùng chung**.
Mà `iceberg-rest` là điểm hội tụ của MỌI luồng đọc/ghi (stream + dbt + maintenance) → chính nó
là nút cổ chai ẩn. Khi chạy nhiều luồng song song (kịch bản production hourly), phải cấp heap
đủ cho catalog, không để mặc định.

## Việc còn lại
- [ ] Cập nhật `.claude/CLAUDE.md` mục CORE BUDGET: thêm dòng iceberg-rest heap 1g/1536M.
- [ ] Xem lại các service JVM khác có bị mem_limit thấp + không -Xmx không (apicurio, kafka-connect).
