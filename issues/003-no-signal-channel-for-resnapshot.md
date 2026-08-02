# Issue #003 — Chưa có signal channel: phương án phục hồi của CDC risk #1 mới nằm trên giấy

**Trạng thái:** 🟡 ĐANG LÀM DỞ — hạ tầng đã dựng và hoạt động, chunk chưa phát ra dòng nào
**Phát hiện:** Phase 1 Step 1c (2026-07-20)
**Cập nhật:** 2026-08-02 — xem mục "Tiến độ 02-08" ở cuối
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


---

## Tiến độ 02-08 — và một chỗ issue này SUY LUẬN SAI

### ❌ Chỗ sai của chính issue này

Phần "Hướng xử lý" phía trên khẳng định chỉ cần Kafka signal channel, và lập luận:

> *"Kafka channel không đụng gì tới DB nguồn."*

**Sai.** Chạy thật mới lộ ra. Debezium từ chối:

```
Incremental snapshot is not properly configured, either signalling
data collection is not provided or connector-specific snapshotting not set
```

Nguyên nhân nằm ở bản chất thuật toán **DBLog**: nó chia bảng thành chunk và chèn
**watermark low/high** để bảo đảm dòng snapshot cũ không ghi đè dòng stream mới hơn.
Watermark đó phải đi qua **chính luồng replication**, tức là phải **GHI vào một bảng nằm
trong publication**. Kafka channel mang được **mệnh lệnh**, không mang được **watermark**.

Kiểm chứng trên connector Postgres đang chạy: có `incremental.snapshot.watermarking.strategy`,
**không có** `read.only`. (MySQL/MariaDB có chế độ read-only dùng GTID thay watermark;
**Postgres không có**.)

→ **Kết luận đúng: cần CẢ HAI** — Kafka channel để gửi lệnh, và `signal.data.collection`
để ghi watermark.

### ✅ Mâu thuẫn least-privilege nhỏ hơn issue lo

Issue này lo rằng cấp quyền ghi sẽ phá vỡ check của `verify.sh`. Đọc lại đúng phép thử #7:

```bash
INSERT INTO merchants(...)  →  phải "permission denied"
```

Nó kiểm debezium **không ghi được BẢNG NGHIỆP VỤ**. Cấp quyền trên một bảng
`debezium_signal` chuyên dụng thì phép thử vẫn xanh — và đúng cả tinh thần: bảng signal là
**hạ tầng**, không phải dữ liệu nghiệp vụ. Quyền thực tế sau khi cấp:

```
accounts        : SELECT
merchants       : SELECT
transactions    : SELECT
debezium_signal : SELECT, INSERT, UPDATE, DELETE
```

Ranh giới giờ **hẹp hơn và kiểm được**, chứ không phải nới ra.

### Đã làm được

- Kafka signal channel: **hoạt động** — signal tới đúng topic đúng key, `SignalProcessor`
  nhận và parse được
- Bảng `debezium_signal` + quyền hẹp + đưa vào publication (cả ở `postgres/init/02_cdc_role.sh`
  cho lần dựng lại, lẫn áp thẳng lên DB đang chạy để khỏi `down -v` mất data)
- `scripts/cdc_resnapshot.sh` — phục hồi bằng **một lệnh**, có kiểm connector RUNNING trước
  khi gửi (signal vào topic không ai đọc thì im lặng tuyệt đối, đúng kiểu hỏng tệ nhất lúc
  đang xử lý sự cố)
- Debezium **chấp nhận và đăng ký** snapshot:
  `Incremental snapshot for table 'public.merchants' will end at position [50]`
- Watermark **open và close đều được ghi** và **đều đi qua WAL** vào
  `gtl.public.debezium_signal`

### ❌ Chưa làm được — điều kiện để đóng issue

Chunk **không phát ra dòng nào**: offset `gtl.public.merchants` đứng nguyên ở 50, dù log
báo `loading of initial chunk completed`. State của connector cho thấy snapshot vẫn đang
treo: `incremental_snapshot_primary_key = aced000570` (Java-serialized `null`) — chưa đi
qua được chunk đầu tiên. Restart task để resume cũng không đổi.

**Giả thuyết chưa thử:**
1. `incremental.snapshot.chunk.size` vs bảng chỉ 50 dòng — thử hạ chunk size
2. Tương tác với **Avro converter**: dòng `op=r` có thể chết im ở khâu serialize
3. Thử trên bảng **lớn hơn** (`accounts`, 100 dòng) để loại trừ ca biên bảng nhỏ
4. Bật DEBUG cho `io.debezium.pipeline.source.snapshot.incremental` **trước khi**
   connector khởi động (bật lúc đang chạy không thấy log ra)

**Đừng đóng issue này cho tới khi thấy `op=r` thật sự về tới Bronze** và đối soát vẫn
lệch 0 — vì đó mới là điều issue này hứa.
