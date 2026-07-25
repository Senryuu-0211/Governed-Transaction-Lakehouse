# Issue #003 — Chưa có signal channel: phương án phục hồi của CDC risk #1 mới nằm trên giấy

**Trạng thái:** OPEN
**Phát hiện:** Phase 1 Step 1c (2026-07-20)
**Ảnh hưởng:** Phase 5 (ops readiness) — CHƯA chặn hiện tại
**Mức:** Medium (nợ vận hành, không phải bug)

## Vấn đề

`CLAUDE.md` mô tả lớp bảo vệ số 2 của CDC risk #1 như sau:

> *Vượt → Postgres invalidate slot (**hy sinh CDC, re-snapshot được**) thay vì đầy đĩa (sập DB).
> Trade-off có chủ đích: thà mất CDC còn hơn sập core DB.*

Trade-off này chỉ đứng vững nếu **re-snapshot thật sự làm được**. Hiện tại thì chưa:
connector **không cấu hình kênh signal nào**, nên không có cách nào yêu cầu Debezium
snapshot lại một bảng cụ thể.

Nếu slot bị invalidate thật, lựa chọn duy nhất bây giờ là **full re-snapshot toàn bộ 3 bảng** —
trên demo thì vô hại, nhưng đúng kịch bản đó ở quy mô thật (bảng hàng trăm GB) là điều
mà cả thiết kế này sinh ra để tránh.

Nói cách khác: lớp bảo vệ đang có **cơ chế phát hiện** (`max_slot_wal_keep_size`) nhưng
**chưa có cơ chế phục hồi**.

## Vì sao là incremental snapshot

Snapshot mặc định (`snapshot.mode=initial`) đọc toàn bộ mọi bảng, chặn streaming, và
không tách lẻ được. Incremental snapshot (Debezium 1.6+, thuật toán DBLog) thì:

- chia **chunk** theo khoá chính, **chạy song song** với streaming (không có khoảng mù);
- **kích hoạt theo yêu cầu** cho **đúng bảng cần**, không restart connector;
- **resume được** khi đứt giữa chừng;
- dùng **watermark** để bản snapshot cũ không bao giờ ghi đè bản stream mới hơn.

## Hướng xử lý

Bật **Kafka signal channel** (KHÔNG dùng signal table):

```
signal.enabled.channels=source,kafka
signal.kafka.topic=gtl-signals
signal.kafka.bootstrap.servers=kafka:9092
```

**Lý do bắt buộc chọn Kafka channel:** signal table đòi cấp quyền `INSERT` cho role
`debezium` trên DB nguồn. Nhưng `scripts/verify.sh` có hẳn một check *"debezium bị chặn
ghi (least-privilege đúng)"* — cấp quyền ghi là **tự phá vỡ** thuộc tính bảo mật mà
project đang cam kết và kiểm tra tự động. Kafka channel không đụng gì tới DB nguồn.

Kích hoạt bằng cách gửi message vào topic signal (key = `topic.prefix`, tức `gtl`):

```json
{"type":"execute-snapshot","data":{"data-collections":["public.merchants"]}}
```

## Giới hạn cần ghi nhớ

Incremental snapshot khôi phục **trạng thái hiện tại**, KHÔNG khôi phục lịch sử thay đổi.
Dòng quay lại dưới dạng `op=r` mang giá trị tại thời điểm snapshot; các lần chuyển trạng
thái đã mất khỏi Kafka thì mất vĩnh viễn. Đây là lý do retention + giám sát lag vẫn là
tuyến phòng thủ thứ nhất, còn re-snapshot chỉ là lưới cuối.

## Bối cảnh phát hiện

Ngày 2026-07-20, topic `gtl.public.merchants` bị retention 7 ngày xoá sạch 50 message
(snapshot ban đầu sinh ngày 13-07, không có consumer bền nào cho tới khi Bronze ra đời ở
1c). `gtl.public.transactions` cũng mất 1,326,675 event đầu. Đã xử lý bằng cách nâng
retention lên 30 ngày + đặt trần dung lượng; **không** khôi phục merchants vì đây là data
test và sẽ reset sạch trước Phase 2 (xem `scripts/reset_all.sh`).

Chính sự cố đó làm lộ ra rằng project chưa hề có đường phục hồi — đó là nội dung issue này.
