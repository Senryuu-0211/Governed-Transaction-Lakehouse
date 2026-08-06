# Issue #003 — Chưa có signal channel: phương án phục hồi của CDC risk #1 mới nằm trên giấy

**Trạng thái:** ✅ **ĐÃ ĐÓNG 06-08-2026** — gốc rễ: `signal.data.collection` ghi ba phần
thay vì hai. Xem mục cuối file.
**Phát hiện:** Phase 1 Step 1c (2026-07-20)
**Cập nhật:** 2026-08-02, 2026-08-05, **2026-08-06 (đóng)**
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


---

## Điều tra 05-08 — khoanh vùng được lỗi, VẪN CHƯA SỬA

Buổi này không sửa được, nhưng thu hẹp đáng kể. Ghi lại để lần sau không mò lại từ đầu.

### Ba điều MỚI biết

**1. `stop-snapshot` KHÔNG dọn được state kẹt.**
Gửi signal `{"type":"stop-snapshot",...}`, Debezium log `Requested stop of snapshot` — nhưng
`incremental_snapshot_collections` trong offset **vẫn còn `public.merchants`**. Hệ quả nghiêm
trọng: **mọi yêu cầu snapshot sau đó bị chặn im lặng** (thử `public.accounts` → signal được nhận
nhưng không có cả dòng `will end at position`). Đây chính là lý do lần trước bật DEBUG mà không
thấy log gì — snapshot mới chưa bao giờ được khởi động.

**Cách dọn thật (Kafka Connect 3.6+):**
```bash
C=gtl-postgres-connector
curl -X PUT  localhost:8083/connectors/$C/stop
curl -s localhost:8083/connectors/$C/offsets | python3 -c "
import json,sys; d=json.load(sys.stdin); o=d['offsets'][0]
o['offset']={k:v for k,v in o['offset'].items() if not k.startswith('incremental_snapshot')}
print(json.dumps({'offsets':[o]}))" \
 | curl -X PATCH -H 'Content-Type: application/json' --data @- localhost:8083/connectors/$C/offsets
curl -X PUT  localhost:8083/connectors/$C/resume
```
Giữ nguyên `lsn*`/`txId`/`ts_usec` → **không mất vị trí WAL**, chỉ gỡ state snapshot.

**2. Cửa sổ watermark MỞ VÀ ĐÓNG BÌNH THƯỜNG — lỗi không nằm ở đó.**
Sau 4 lần thử, bảng `debezium_signal` có **4 cặp** `snapshot-window-open` / `snapshot-window-close`,
và cả 4 cặp đều đi qua WAL vào topic `gtl.public.debezium_signal`. Nghĩa là phần khó nhất của
DBLog (ghi watermark + đọc lại qua replication) **hoạt động đúng**.

**3. Lỗi nằm ở BƯỚC ĐỌC CHUNK — nó trả về RỖNG.**
Bằng chứng: `incremental_snapshot_primary_key` = `aced000570` = Java-serialized `null`, **không
bao giờ nhích**, qua cả 4 lần. Cửa sổ mở → đọc chunk → đóng cửa sổ → **phát 0 dòng** → khoá không
tiến → lặp lại. Bảng `merchants` có 50 dòng và `debezium` có `SELECT`, nên "không có quyền" và
"bảng rỗng" đều bị loại.

### Đã loại trừ

- ❌ Payload signal sai — message trong topic đúng nguyên văn (`cat -A` xác nhận key `gtl` + JSON)
- ❌ Thiếu `signal.data.collection` — đã cấu hình, watermark ghi được
- ❌ Thiếu quyền — `debezium` có `SELECT` trên `merchants`, `INSERT/UPDATE/DELETE` trên signal table
- ❌ Bảng quá nhỏ (ca biên 50 dòng) — thử `accounts` (100 dòng) cũng không chạy
- ❌ State kẹt là nguyên nhân gốc — dọn sạch rồi vẫn treo y hệt ở chunk đầu

### Giả thuyết còn lại, theo thứ tự nên thử

1. **DEBUG phải bật TỪ LÚC WORKER KHỞI ĐỘNG.** Bật qua `/admin/loggers` lúc đang chạy thì
   `AbstractIncrementalSnapshotChangeEventSource` vẫn không in dòng DEBUG nào (chỉ 1 dòng DEBUG
   trong cả log). Cần đặt log level trong `docker-compose.yml` (biến `CONNECT_LOG4J_LOGGERS`)
   rồi restart — chỉ khi thấy được câu SQL của chunk mới biết nó truy vấn cái gì.
2. **Bắt câu SQL ở phía Postgres**: bật `log_statement='all'` tạm thời rồi lọc query có
   `ORDER BY merchant_id LIMIT` — xem nó có chạy không và trả về gì.
3. **Tương tác với Avro/Apicurio**: dòng `op='r'` có thể chết im ở khâu serialize. Thử tạm đổi
   `value.converter` sang JSON cho một lần chạy để loại trừ.
4. **Phiên bản Debezium**: kiểm changelog xem có bug đã biết với incremental snapshot +
   `pgoutput` + `REPLICA IDENTITY FULL`.

### Điều kiện đóng issue (không đổi)

Thấy `op='r'` **thật sự về tới Bronze**, và `audit_reconciliation` vẫn lệch **0**.


---

## ✅ ĐÓNG 06-08 — gốc rễ: một chuỗi cấu hình thừa một phần

### Sửa gì

`debezium/connector-config.json`, đúng một dòng:

```diff
-    "signal.data.collection": "banking.public.debezium_signal",
+    "signal.data.collection": "public.debezium_signal",
```

### Vì sao ba phần thì hỏng

Chuỗi này được `CommonConnectorConfig` (dòng 1217) đưa qua `TableId.parse()`, rồi so với
bảng trong luồng bằng `equals()`:

```java
// CommonConnectorConfig.java:1641
public boolean isSignalDataCollection(DataCollectionId dataCollectionId) {
    return signalingDataCollectionId != null && signalingDataCollectionId.equals(dataCollectionId);
}
```

`TableId.equals()` → `compareTo()` → so **chuỗi `id`**, mà `id` được dựng bằng cách nối các
phần và **bỏ qua phần null** (`TableId.java:290`). Trong khi đó pgoutput dựng TableId cho
**mọi** bảng Postgres với `catalog = null`:

```java
// PgOutputMessageDecoder.java:305
final TableId tableId = new TableId(null, schemaName, tableName);
```

| | catalog | schema | table | chuỗi `id` |
|---|---|---|---|---|
| Bảng trong stream | `null` | `public` | `debezium_signal` | `public.debezium_signal` |
| Config ba phần (cũ) | `banking` | `public` | `debezium_signal` | `banking.public.debezium_signal` ❌ |
| Config hai phần (mới) | `public` | `null` | `debezium_signal` | `public.debezium_signal` ✅ |

Hai phần khớp nhờ đúng cái quy tắc bỏ-null đó: `catalog='public' + schema=null` cho ra cùng
một chuỗi với `catalog=null + schema='public'`. Trông như ăn may, nhưng đó là hành vi cố ý
của `TableId` và là lý do tài liệu Debezium ghi định dạng Postgres là `<schema>.<table>`.

### Vì sao mất 4 ngày mới tìm ra

**Không có một dòng log lỗi nào.** Chuỗi sai vẫn trỏ đúng bảng khi Debezium *ghi*, chỉ hỏng
khi Debezium *nhận diện*. Nên mọi bước đều "thành công":

1. Signal nhận được → log `Requested 'INCREMENTAL' snapshot of '[public.merchants]'` ✅
2. Watermark open ghi vào bảng ✅ (câu INSERT dựng thẳng từ chuỗi config → vẫn đúng bảng)
3. Chunk query **chạy và đọc trọn 50 dòng** vào buffer ✅
   `SELECT * FROM "public"."merchants" ORDER BY "merchant_id" LIMIT 1024`
4. Watermark close ghi vào bảng ✅
5. Dòng close quay lại qua WAL, qua EventDispatcher, **ra tới Kafka** ✅
6. …nhưng `isSignalDataCollection()` trả `false` → `closeWindow()` **không chạy** →
   `sendWindowEvents()` không chạy → **50 dòng trong buffer bị vứt im lặng**
7. `primary_key` đứng ở `null` → không có chunk kế → snapshot treo và **chặn im lặng** mọi
   yêu cầu sau

Hỏng ở bước 6, mà bước 6 không in gì cả.

### 🔴 Hai kết luận SAI của chính issue này

**1. "Lỗi nằm ở BƯỚC ĐỌC CHUNK — nó trả về RỖNG" (mục 05-08, điểm 3) — SAI.**
Chunk query chạy hoàn hảo và đọc đủ 50 dòng. Bắt được nguyên văn câu SQL bằng cách bật
`log_statement='all'` phía Postgres. Suy luận cũ đi từ triệu chứng (`primary_key` không
nhích) tới nguyên nhân (`chunk rỗng`) mà **không có bằng chứng trực tiếp nào** cho bước
giữa — và bước giữa chính là chỗ sai.

**2. "Cửa sổ watermark MỞ VÀ ĐÓNG BÌNH THƯỜNG — lỗi không nằm ở đó" (mục 05-08, điểm 2) — SAI MỘT NỬA.**
*Ghi* watermark hoạt động. *Đọc lại* watermark thì không. Việc thấy đủ cặp open/close trong
bảng và thấy chúng có mặt trong topic đã bị hiểu nhầm thành "phần này ổn", nên vùng nghi ngờ
bị thu hẹp **sai hướng** và loại bỏ đúng chỗ đang hỏng.

Cả hai sai lầm cùng một dạng: **suy ra một bước từ kết quả của bước khác.** Chỉ có `log_statement='all'`
mới cắt được vòng luẩn quẩn đó, vì nó cho thấy DB *thật sự* nhận câu gì — độc lập hoàn toàn
với việc Debezium tự thuật lại nó đang làm gì.

### Bằng chứng đóng issue

Đúng điều kiện đã đặt ra: `op='r'` về tới Bronze, đối soát lệch 0.

```
topic gtl.public.merchants   50 -> 150 (xả 2 yêu cầu tồn đọng khi task khởi động lại)
                            150 -> 200 (signal mới sau khi dọn state)
message cuối mang cờ snapshot = "incremental"
offset sau khi xong          : SẠCH — snapshot tự kết thúc và tự dọn state

bronze.merchants   200 dòng /  50 khoá   <- 4 bản mỗi merchant, ĐÚNG như thiết kế
silver.merchants    50 dòng /  50 khoá   <- khử trùng theo _kafka_offset

dbt build                    : 91 PASS / 0 ERROR / 0 WARN (84s)
audit_reconciliation 13:11:49: 2.692.703 giao dịch · $11.207.350.475,15 · lệch $0,00
```

### Điều này thay đổi gì về mặt vận hành

Lớp bảo vệ số 2 của CDC risk #1 **giờ thật sự dùng được**, không còn nằm trên giấy:
slot bị invalidate → `bash scripts/cdc_resnapshot.sh public.merchants` → dữ liệu quay lại,
không restart connector, không mất vị trí WAL, không dừng streaming.

Giới hạn ở mục "Giới hạn cần ghi nhớ" phía trên **vẫn nguyên giá trị**: đây khôi phục
*trạng thái hiện tại*, không khôi phục *lịch sử thay đổi*. Và với bảng fact cỡ thật thì
snapshot toàn bộ vẫn là sai — đúng phương án là backfill theo cửa sổ bằng
`additional-condition` của signal.

### Bài học

1. **Định dạng tên bảng trong config Debezium khác nhau theo từng connector.** Postgres là
   `<schema>.<table>`; MySQL là `<database>.<table>`; SQL Server là ba phần. Chép nhầm quy
   ước từ connector khác không gây lỗi cấu hình — nó gây **hỏng im lặng**.
2. **Một chuỗi cấu hình có thể vừa đúng vừa sai cùng lúc.** Chuỗi này đúng khi dùng để
   *dựng câu SQL*, sai khi dùng để *so khớp*. Không có kiểm tra khởi động nào bắt được, vì
   xét riêng từng cách dùng thì đều hợp lệ.
3. **Khi mọi bước đều báo thành công mà kết quả không có, hãy đo từ phía KHÁC.**
   `log_statement='all'` ở Postgres phá được thế bí sau nhiều ngày, đơn giản vì nó không
   phụ thuộc vào lời tự thuật của Debezium.
