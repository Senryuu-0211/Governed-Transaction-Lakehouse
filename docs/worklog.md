# Worklog — Governed Transaction Lakehouse

Nhật ký để resume nhanh sau khi context bị nén. Mới nhất ở trên.

---

## 01-08-2026 — Tối ưu chi phí S3 + sửa 3 lỗi lộ ra khi chạy thật

Ngày này KHÔNG thêm tính năng mới — toàn bộ là **hệ quả của việc chuyển sang S3 thật**:
chi phí biến mọi lãng phí thành tiền, và tiền làm lộ những lỗi mà MinIO che kín.

### 💸 Ước tính chi phí phát hiện một quả bom: ~$3.000/tháng
Mr. Senryuu hỏi "chạy liên tục 1 tháng tốn bao nhiêu". Tính ra:
storage ~$1.3 · PUT ~$4 · **data-transfer-out ~$3.000**.
**Gốc:** compute chạy **on-prem**, storage ở **cloud** → mỗi byte Spark đọc từ S3 là
egress TÍNH TIỀN. Hai thủ phạm:
1. `audit_reconciliation` **quét TOÀN BỘ Bronze mỗi giờ** (thiết kế của tôi hôm 31-07)
   → Bronze cuối tháng ~100GB × 720 lần ≈ **36TB egress**.
2. `fact_transactions` là `materialized='table'` → **rebuild trọn từ Silver mỗi giờ** ≈ 7TB.

**Bài học nền (data gravity):** *compute on-prem + storage cloud = trả tiền cho MỌI lần đọc.*
Nếu Spark chạy trên EC2 cùng region thì egress = $0. Đây là lý do THẬT khiến người ta đưa
compute về cùng vùng với storage, không phải vì "cloud cho tiện".

### Sửa 1 — reconciliation theo CỬA SỔ, chạy 1 lần/sáng
- Lọc `ingest_ts >= current_date - N` → **prune partition** (Bronze partition `days(ingest_ts)`),
  đọc ~5GB thay vì toàn kho. Thêm cột `window_days` vào sổ để tự mô tả.
- Chuyển khỏi `gtl_transform` @hourly → task `reconcile` trong `gtl_maintenance` @daily
  (00:00 UTC = **07:00 sáng VN**, đúng ý Mr. Senryuu: biết lệch ngay đầu ngày).
- **Ngữ nghĩa đổi có chủ đích:** từ *"toàn lịch sử khớp"* → *"mọi thứ nạp gần đây khớp"*.
  Đúng chuẩn kiểm toán hơn — mỗi ngày chứng minh tại thời điểm đó rồi ghi vĩnh viễn vào sổ;
  không ai đi chứng minh lại năm 2019 mỗi sáng.
- `maintenance` dùng `TriggerRule.ALL_DONE`: nén vẫn chạy dù đối soát fail (nén là việc chống
  phình kho — khác `push_marts` nơi CHẶN mới đúng).

### Sửa 2 — `fact_transactions`: `table` → `incremental MERGE`
Mr. Senryuu chốt đúng: **dim full snapshot thì bình thường, fact full snapshot là chết.**
Kimball có 3 loại fact; chỉ *periodic snapshot* mới là snapshot theo kỳ. Của mình là
**transaction fact** → phải incremental. MERGE (không append) vì `status` đổi theo vòng đời.
- ⚠️ **Bẫy soft-delete:** bản `table` lọc `where not is_deleted` nên dòng bị purge tự biến mất
  khi rebuild. Với incremental thì KHÔNG — lô mới không chứa nó, MERGE không đụng, dòng ở lại
  vĩnh viễn → sai số. Fix: **fact GIỮ dòng soft-delete kèm cờ `is_deleted`**, 3 mart + 1 test
  lọc ra. Đúng tinh thần lakehouse có kiểm toán: *không xoá dấu vết, chỉ đánh dấu*.
- Thêm `_kafka_offset` vào fact: vừa là watermark incremental, vừa là dấu vết lineage.

**Ước tính sau sửa: ~$6-10/tháng** (từ ~$3.000).

### 🐛 Lỗi 1 — Derby metastore lock: hai DAG sẽ đụng nhau mỗi sáng
```
ERROR XSDB6: Another instance of Derby may have already booted the database
```
dbt-spark session mode boot một Hive metastore **Derby nhúng**, Derby chỉ cho **MỘT** tiến
trình mở database. Mà tôi vừa tạo ra tình huống: `gtl_transform` (@hourly) và `gtl_maintenance`
(@daily, có task reconcile) **cùng nổ lúc 00:00 UTC** → một cái chết mỗi sáng.
**Fix:** `scripts/dbt.sh` tạo `mktemp -d` riêng mỗi lần chạy, chứa **conf đã render + Derby
metastore riêng** (`-Dderby.system.home`), tự dọn khi thoát. An toàn vì Derby gần như không
dùng — catalog thật là Iceberg REST. Tiện thể fix một race khác: hai lần chạy cùng render đè
lên `spark-defaults.conf` chung.
**Chứng minh:** chạy 2 dbt song song (của tôi + của DAG) → `Done. PASS=31 ERROR=0`, không XSDB6.

### 🐛 Lỗi 2 — chạy được ≠ nên chạy: vỡ core budget
Sau khi fix Derby, hai dbt chạy được cùng lúc — nhưng mỗi cái lấy `local[6]`, cộng stream
`local[3]` = **15/16 luồng** → JVM giành nhau, cả hai cùng bò (đo: 66%/11%/8.9% CPU).
**Fix:** **Airflow pool `gtl_dbt` 1 slot**, gán cho `dbt_run`/`dbt_test`/`reconcile`.
`cdc_health` (đọc file), `push_marts` (local[2]), `maintenance` (local[4]) để `default_pool`.
→ Derby fix lo **tính đúng đắn**; pool lo **trật tự tài nguyên**. Cần cả hai.

### 🔴 Lỗi 3 — metadata.json chiếm 82% dung lượng (thủ phạm ẩn lớn nhất)
Đo phân loại file trên `bronze.transactions`:
```
metadata.json   1.217 MB  81.8%  (1.601 file)
DATA (parquet)    190 MB  12.8%
manifest-list      61 MB   4.1%
manifest           21 MB   1.4%
```
**Cơ chế:** Iceberg ghi **một `metadata.json` mỗi commit**, mỗi file chứa **toàn bộ lịch sử
snapshot** → file sau to hơn file trước. Trigger 30s = 2.880 commit/ngày. Mặc định **giữ tất
cả vĩnh viễn**. Tệ hơn: mỗi lần **mở bảng** đều đọc file mới nhất (760KB, phình dần) → trên S3
là **egress cho MỌI truy vấn**.
⚠️ **`expire_snapshots` KHÔNG dọn loại này** — tôi đã tưởng nó dọn rồi.
**Fix:** hai table property, áp ở **3 chỗ** để không sót — DDL Bronze (bảng mới),
`dbt_project.yml` (Silver/Gold/marts), `maintenance.py` (vá mọi bảng đang tồn tại, mỗi ngày):
```
write.metadata.delete-after-commit.enabled = true
write.metadata.previous-versions-max       = 10
```
**Chưa hội tụ:** sau 1 lần maintenance mới giảm 1.217→1.103 MB. Nghi file cũ nằm ngoài
metadata-log nên không được theo dõi → **cần xác minh lại**.

### 🔴 Lỗi 4 (CHƯA SỬA) — `remove_orphan_files` không chạy được trên S3
```
UnsupportedFileSystemException: No FileSystem for scheme "s3"
```
Thủ tục này phải tìm **file Iceberg KHÔNG biết** → không tra được metadata, buộc phải **LIST
storage**, và phần listing đó dùng **Hadoop FileSystem API** — trong khi project cố ý **không
dùng `hadoop-aws`** (chỉ `S3FileIO`, để tránh xung đột version Hadoop/AWS SDK).
→ **Cơ chế dọn file mồ côi thêm hôm 30-07 CHƯA TỪNG hoạt động.** Lớp phòng thủ chính cho sự cố
397GB đang rỗng. Lỗi của tôi: thêm rồi báo "đã fix" mà không chạy thật để kiểm.

**Đã điều tra xong, có đường sạch (chưa triển khai):**
| Kiểm tra | Kết quả |
|---|---|
| `S3FileIO` liệt kê được? | ✅ có `listPrefix`/`deletePrefix` |
| Iceberg 1.9.2 tự dùng cho orphan? | ❌ 0 class trong `spark/actions` tham chiếu `SupportsPrefixOperations` |
| Cửa thoát chính thức? | ✅ tham số **`file_list_view`** |
| Schema cần | `file_path` (string) · `last_modified` (timestamp) |

**Phương án chốt (c):** boto3 liệt kê S3 → DataFrame → temp view → truyền `file_list_view` cho
`remove_orphan_files`. Ta chỉ cung cấp **phần mang tính storage-cụ-thể** (danh sách object),
còn **phần nguy hiểm — quyết định cái gì là mồ côi và xoá nó — vẫn do Iceberg làm**. Không thêm
`hadoop-aws`, không tự viết code có quyền xoá.

### Kiến trúc: S3 vs HDFS — chốt lại vì Mr. Senryuu lo stack sai
| | HDFS | S3 |
|---|---|---|
| Thư mục | thật | ảo (chỉ là tiền tố key) |
| Đổi tên | **nguyên tử, rẻ** | **không có** (copy+delete) |
| LIST | rẻ | đắt, tính tiền |
Bảng **Hive** đời cũ dựa vào đúng hai thứ S3 không có (LIST thư mục + rename nguyên tử) →
trên S3 vừa chậm vừa sai. **Iceberg sinh ra chính vì điều đó** (Netflix, bảng Hive trên S3 hỏng):
không LIST (file ghi tường minh trong manifest), không rename (commit = **hoán đổi con trỏ
nguyên tử trong CATALOG** — chính là `iceberg-rest` + Postgres của mình).
→ **S3 là mục tiêu thiết kế số một của Iceberg, không phải trường hợp phụ.** Stack không sai.
Bằng chứng: đổi MinIO→S3 chỉ sửa endpoint, `dbt build` 82 PASS, không sửa dòng model nào.
Lỗi 4 chỉ là **rough edge của một thủ tục bảo trì**, và chính việc Iceberg chừa sẵn
`file_list_view` chứng tỏ nó được thiết kế cho môi trường không có Hadoop FS.

### Thêm: 2 mức nghiêm trọng cho anomaly test (học từ pipeline production của Mr. Senryuu)
Tài liệu `campaign_master` bên anh dùng **Error vs Warning** (`volume_no_decrease` = Error,
`volume_growth_within_10pct` = Warning). Mình đang hard-fail tất → sản lượng tụt tự nhiên ngày
lễ cũng chặn báo cáo → vài lần là người ta **phớt lờ đèn đỏ**, cổng mất uy tín.
**Fix:** test trả **1 dòng cho MỖI ngưỡng bị vượt**, dbt map số dòng → mức
(`warn_if='>0'`, `error_if='>1'`), kèm cột `breach` ghi lý do bằng tiếng người:
| Test | WARN | ERROR |
|---|---|---|
| Sụt sản lượng | >20% | >40% |
| Lệch phân phối tiền | >30% | >60% |
| NULL merchant_id | >1% | >5% |
`assert_reconciliation` + `assert_gold_no_raw_pii` **giữ error tuyệt đối** — lệch một xu là
lệch, lộ PII là lộ, không có vùng xám.
Điểm xác nhận thiết kế: bên anh *"chưa có baseline → bỏ qua, không fail"* **giống hệt** guard
7 ngày của mình — hai bên độc lập nghĩ ra cùng giải pháp.

### Bài học chung của ngày
**Plan không thay được việc chạy và đo.** Cả 4 lỗi hôm nay đều KHÔNG có trong plan Phase 4;
chúng chỉ lộ khi (a) chạy thật, (b) tính ra tiền, (c) đo từng loại file. Và hai trong số đó là
lỗi do **chính tôi tạo ra** khi sửa thứ khác.

---

## 31-07-2026 — Phase 4: Governance nâng cao (reconciliation · anomaly · PII · audit)

### Mục tiêu đạt
Bốn trụ governance, **toàn bộ bằng dbt/Iceberg-native — không thêm một tool nào**:
1. **Sổ đối soát** `gold.audit_reconciliation` (append-only) — chứng minh tiền vào = tiền ra.
2. **3 anomaly test** dbt-native — bắt loại lỗi mà 71 test cấu trúc mù hoàn toàn.
3. **PII** — tài liệu hoá 3 tầng access + test chặn hồi quy lộ PII ở Gold.
4. **Audit/time-travel** — khai thác Iceberg snapshot + tài liệu hoá giới hạn của nó.

### Quyết định (ghi rõ đánh đổi)
- **BỎ Great Expectations** (plan gốc có). Đọc kỹ JD ngân hàng: nó đòi năng lực *"anomaly
  detection"*, **không nêu tool nào**. dbt singular test làm được → thêm GE chỉ để gọi tên một
  tool mà JD không đòi = lạm dụng stack. (Tôi từng lập luận ngược, Mr. Senryuu bắt đúng: *"tôi có
  thấy bảo dùng GE đâu nhỉ?"* — tôi đã suy diễn sai từ JD.)
- **Reconcile tính lại Bronze ĐỘC LẬP từ CDC thô**, không tái dùng Silver: nếu lấy số từ Silver
  rồi so Gold thì hai vế cùng đi qua một đường transform → lỗi chung triệt tiêu nhau, hoá ra đối
  soát với chính mình.
- **Audit hướng A**: khai thác Iceberg snapshot (có sẵn) + CHỈ thêm sổ đối soát. Không xây bảng
  `pipeline_runs` tuỳ biến — Airflow + Iceberg đã ghi run/commit rồi, xây nữa là hai nguồn sự thật.

### 🔑 BÀI HỌC LỚN NHẤT: reconciliation trên pipeline streaming phải AS-OF một mốc
Bản đầu tiên so `count(Bronze)` với `count(Gold)` → **lệch 31.253 giao dịch / 129 triệu tiền**.
Không phải mất tiền: Bronze được stream nạp LIÊN TỤC, còn Gold chỉ đóng băng tại lần dbt chạy gần
nhất → so **mục tiêu di động với ảnh chụp**, lệch to dần theo thời gian.
**Fix:** lấy `max(_kafka_offset)` của Silver làm **watermark**, chỉ tính phần Bronze nằm trong mốc
(lọc offset TRƯỚC rồi mới dedup — phải lấy trạng thái cuối *mà Silver đã thấy*). Ghi luôn
`watermark_offset` vào sổ để kiểm toán tái kiểm được.
→ Kết quả: **130.480 = 130.480, lệch 0đ trên $542.883.061,63**.

### 🐛 BẮT ĐƯỢC LỖI THẬT (đúng mục đích tồn tại của Phase 4)
`birth_year` NULL cho **toàn bộ 100 tài khoản** → `age`, `age_band` chết theo, mọi phân tích theo
độ tuổi im lặng vô dụng. Truy ra: **hồi quy từ Phase 2.5**. Macro `generalize_birth_year` viết
thời JSON converter (Debezium mã hoá DATE = số ngày từ epoch), nhưng Avro trả **chuỗi ISO**
`"1957-06-16"` → `cast(... as int)` = NULL → `year(NULL)` = NULL.
**Fix:** `year(to_date(expr))` + **thêm `not_null` test trên birth_year cả Silver lẫn Gold**.
Fix macro chỉ chữa triệu chứng; cái chữa GỐC là test — lỗi sống được nhiều ngày chỉ vì không ai kiểm.
**Bài học:** đổi format serialize phải soi lại MỌI chỗ parse kiểu dữ liệu.

### Chứng minh anomaly test hoạt động (không chỉ "PASS")
Data hiện chỉ có **1 ngày lịch sử** → 2 test dựa-trên-lịch-sử pass **vì guard**, không phải vì đã
kiểm chứng logic. Nên mô phỏng bằng data giả để chứng minh cả hai hành vi:
- 7 ngày nền × 1000/ngày, hôm nay 400 (sụt 60%) → **BẮT ĐƯỢC** ✅
- 2 ngày nền, cùng mức sụt → **im lặng** (guard chặn báo động giả) ✅
Guard tồn tại vì một test kêu oan ngay ngày đầu sẽ dạy người ta phớt lờ cổng — phá đúng mục đích cổng.

### Cấu hình mấu chốt (nhớ để không vấp lại)
- `audit_reconciliation`: `full_refresh=false` → model **phớt lờ `--full-refresh`**, bảo vệ sổ
  kiểm toán bằng CODE chứ không bằng lời dặn. Thêm cột phải có `on_schema_change='append_new_columns'`.
- Đổi schema bảng incremental đã tồn tại → phải DROP tay (đúng tinh thần: reset sổ kiểm toán là
  hành động CÓ CHỦ ĐÍCH).
- Time-travel chỉ lùi được ~10 snapshot (`expire_snapshots retain_last=10`) → bằng chứng dài hạn
  PHẢI nằm ở sổ append-only, không dựa time-travel.

---

## 26-07-2026 — Phase 3: Airflow orchestration (`verify_3.sh` 6/6 + end-to-end xanh)

### Mục tiêu đạt
2 DAG điều phối nhánh batch, chạy từ stack `airflow-docker` sẵn có (KHÔNG dựng Airflow thứ 2):
- `gtl_transform @hourly`: `cdc_health → dbt_run → dbt_test → push_marts`. **`dbt_test` là CỔNG** —
  fail thì `push_marts` skip (trigger_rule all_success) → số bẩn không ra Superset.
- `gtl_maintenance @daily`: `maintenance.py` (compaction + expire snapshots).
End-to-end thật: dbt **11 model PASS**, **71 test PASS**, marts Postgres refresh. Cả 2 DAG **unpause**.

### Quyết định (ghi rõ đánh đổi)
- **Airflow điều phối, host tính toán — qua `SSHOperator`.** Container Airflow KHÔNG có venv/Java/Spark
  (đã chứng minh: worker thấy Python 3.12, không java, không thấy gtl-spark-venv). Nên mọi task chỉ
  SSH về host chạy trong venv thật. Không nhét pyspark vào image Airflow (phá core budget, trộn
  điều phối với tính toán). Không dựng Airflow host-native (trùng lắp orchestrator, phá reproducible).
- **Deploy-by-COPY, không symlink.** Container chỉ mount dags folder → symlink trỏ ra project dir
  GÃY (`No such file or directory` trong container). `scripts/deploy_dags.sh` copy 3 file DAG vào
  dags folder. File Spark xử lý data KHÔNG copy — chạy tại host qua SSH, DAG chỉ trỏ đường dẫn.
- **Cầu SSH:** key ed25519 riêng (`gtl_airflow_ed25519`, thu hồi độc lập) + connection `gtl_host_ssh`
  → gateway `172.18.0.1` (lấy động từ `docker inspect worker`), `no_host_key_check` (LAN Tailscale).

### Sự cố lớn (bài học đắt — `issues/005`)
Chạy end-to-end lúc stream live → **iceberg-rest OOM** (`Service failed: 500: OutOfMemoryError`).
Gốc: catalog `mem_limit 512M` + KHÔNG `-Xmx` → heap mặc định ~128MB, quá nhỏ khi commit đồng thời /
backlog lớn. Catalog chết giữa commit → stream `CommitStateUnknownException` → tự tắt. Qua 27h
(ranh giới phiên) stream chết, backlog 27h + hàng nghìn small-file dồn lại → restart stream cũng OOM,
và dbt đọc Bronze bị MinIO `Connection reset`.
**Fix:** `-Xmx1g` + mem_limit 1536M cho iceberg-rest (data ở Postgres+MinIO → recreate mất 0 byte);
`maintenance.py` nén backlog (accounts 9.102→15 file). **Bài học:** core budget chỉ lo Spark, BỎ SÓT
heap của service catalog dùng chung — nút cổ chai ẩn của mọi luồng đọc/ghi. Và: pipeline nằm im
KHÔNG orchestration chính là thứ sinh ra backlog — đúng lý do Phase 3 tồn tại.

### Cấu hình mấu chốt (nhớ để không vấp lại)
- Setup 1 lần: `setup_airflow_conn.sh` → `deploy_dags.sh` → `airflow dags reserialize` → `verify_3.sh`.
- Sửa DAG: chạy lại `deploy_dags.sh` (bước deploy: code ở repo → đẩy runtime).
- `cdc_health` đọc mtime `_checkpoints/transactions/commits/` (< 900s) — không cần khởi động Spark.
- Bẫy verify: `set -o pipefail` + `grep -q` → grep khớp đóng pipe → SIGPIPE → fail giả. Hứng biến rồi grep.

---

## 24-07-2026 (chiều) — Phase 2.5: Schema Registry + Avro (`verify_2_5.sh` 17/17)

### Mục tiêu đạt
Debezium giờ phát **Avro** qua **Apicurio Schema Registry** (không còn JSON trần). Hợp đồng
schema được ENFORCE ở cổng Kafka: đổi schema không tương thích **bị chặn 409**, thay vì để
Silver lặng lẽ ra NULL. Toàn bộ Silver/Gold/marts **không sửa một dòng** — chứng minh bằng
`dbt build --full-refresh` 82/82 PASS trên Bronze-từ-Avro.

### Quyết định (có tranh luận, ghi rõ đánh đổi)
- **Format = Avro** (không JSON Schema). Lý do "chuẩn banking/interviewer" — nhưng ĐÁNH ĐỔI có
  chủ đích: Bronze giờ lưu JSON **đã decode từ Avro**, không còn byte-exact của Debezium. Đổi
  "audit byte-exact" lấy "typing chặt + chuẩn ngành". (Lean ban đầu của tôi chưa chặt, đã nói rõ.)
- **Registry = Apicurio** (không Confluent SR). Vì image Debezium **bundle sẵn** Apicurio converter
  (`apicurio-registry-utils-converter-2.4.1`), Confluent thì phải build image thêm jar. Apicurio
  Apache-2.0, bật ccompat API (v7) nên tooling đọc như Confluent. Storage **kafkasql** (schema lưu
  trong topic Kafka, sống sót restart, không cần DB riêng).

### Cấu hình mấu chốt (nhớ để không vấp lại)
- **`ENABLE_APICURIO_CONVERTERS=true`** trên connect: jar Apicurio CÓ trong image nhưng nằm ở
  `/kafka/external_libs/apicurio` — cờ này nạp vào classpath. Thiếu nó: connector 400
  "Class ...AvroConverter could not be found".
- Connector: `key/value.converter=io.apicurio...AvroConverter`, `apicurio.registry.url=.../apis/registry/v2`,
  `auto-register=true`, **`headers.enabled=false` + `as-confluent=true`** (wire-format Confluent:
  1 magic byte + 4-byte id → Spark đọc được sau khi bỏ 5 byte).
- **Bronze decode:** `spark-avro_2.12:3.5.0` + `from_avro`. Lấy schema qua Apicurio **API v2
  `?dereference=true`** (KHÔNG ccompat/latest) — vì Debezium tách type `...Source` ra artifact
  riêng (schema reference); ccompat trả schema chưa resolve → from_avro chết
  "...Source is not a defined name". Dereference inline Source → schema tự chứa. Decode:
  `to_json(from_avro(substring(value,6,...), schema))` → `value` là JSON y như trước → Silver nguyên vẹn.

### Enforcement (điểm cốt lõi)
- `scripts/test_schema_compat.py`: set **BACKWARD** lên 3 subject value production (hardening thật)
  + demo trên subject throwaway: incompatible (đổi kiểu field) → **409 BỊ CHẶN**, compatible
  (thêm optional có default) → 200. Bẫy: POST schema qua ccompat phải **giữ `references`** (không
  thì 422 "invalid Avro schema"), hoặc dùng schema tự chứa.

### Reset an toàn hoá (bài học SPOF)
- `reset_all.sh` cũ dùng `down -v` → xoá pgdata = mất luôn `iceberg_catalog`/`marts`/`superset`
  (3 DB tạo tay). Sửa: **`postgres/init/03_extra_databases.sh`** tự tái lập 3 DB + 2 user
  (marts_ro, superset_meta) trên volume trống → `down -v` tái lập được từ số 0. Postgres service
  +2 env password. reset_all thêm bước chờ Apicurio trước khi đăng ký connector.
- Lưu ý: reset xoá metadata Superset → **dataset marts phải đăng ký lại** (đã script qua API).

### Bẫy gặp & xử
- Subject `transactions-value` bị **ẩn khỏi listing** sau khi tôi xoá 1 version thử nghiệm (nhưng
  version 1 vẫn active, connector vẫn chạy). `verify_2_5.sh` đổi sang kiểm `/versions` (functional)
  thay `/subjects` (listing).
- Quản lý background stream chập chờn qua các phiên (nohup log biến mất, false pgrep) → chuyển sang
  chạy stream qua cơ chế background của harness cho ổn định.

### Xong Phase 2.5 (gồm README public đã cập nhật status + highlight). 
NEXT = Phase 3 (Airflow orchestrate) — giờ pipeline đã ổn định trên Avro nên orchestrate không
phải làm lại. Xem `CURRENT_STATUS.md` (gitignored) để nắm nhanh trạng thái vận hành.

---

## 24-07-2026

### Phase 2 (core) — dbt-on-Spark: Silver + Gold XONG, 71/71 tests PASS
- **dbt stack:** dbt-core 1.12 + dbt-spark 1.11, **method: session** trên host venv (không
  Thrift server). Spark conf nạp qua `SPARK_CONF_DIR` (`dbt_project/spark-conf/`), secrets đi
  bằng env `AWS_*` (S3FileIO default chain) → profiles.yml + spark-defaults.conf KHÔNG chứa
  secret, commit được. Chạy qua `scripts/dbt.sh <lệnh>`.
- **Đúng trình tự plan: TESTS TRƯỚC** — sources.yml (freshness 15m/2h) + contracts đầy đủ
  cho Silver/Gold viết trước khi viết SQL. Tổng: 12 source + 32 silver + 27 gold = **71 PASS**.
- **Silver** (`models/silver/`): current-state MERGE theo PK, incremental theo watermark
  `_kafka_offset` (topic 1 partition → offset order = commit order; nhiều partition thì phải
  watermark per-partition). Macro dùng chung `cdc_latest_events`/`cdc_field` (macros/cdc.sql) —
  logic collapse viết 1 lần cho cả 3 model. Soft-delete (op=d → is_deleted, giá trị lấy từ
  before-image). **PII mask ngay tại Silver** (macros/mask_pii.sql): hash SHA-256
  (name/national_id), partial mask (phone/email), generalize (dob→birth_year).
  Kết quả: 2.92M txn current-state, 16.5k soft-deleted, 2.7k PENDING (gồm nhóm treo).
- **Parse theo BẰNG CHỨNG envelope thật** (đừng đoán): TIMESTAMPTZ → chuỗi ISO (cast timestamp
  được), DATE → SỐ NGÀY epoch (date_add), DECIMAL → string (cast đúng 1 lần ở Silver).
- **Gold** (`models/gold/`): star schema Kimball — `fact_transactions` (grain: 1 dòng/txn,
  loại dòng purged; **net_amount = amount nếu COMPLETED else 0**; is_real_money) + dim_account
  (age_band, natural key có ghi trade-off) + dim_merchant + dim_date (spine sinh từ min ngày)
  + dim_channel (is_digital). Materialized=table rebuild → idempotent bẩm sinh cho Phase 3.
- **Luật REVERSAL/FAILED/PENDING có CỔNG TỰ ĐỘNG:** singular test
  `tests/assert_reversed_never_counted.sql` (vi phạm → fail run). MỌI cột Gold có mô tả
  nghiệp vụ (gate của OPTIMIZATION 1.3). `dbt docs generate` ra catalog/manifest/lineage.
- Ghi chú data: faker không có luật chống thấu chi → balance âm cực lớn; ghi caveat trong
  dim_account, ứng viên check Great Expectations Phase 4.

### Catalog SQLite → POSTGRES backend (sự cố lần 2 → làm luôn OPTIMIZATION 2.5)
- dbt build đầu tiên chết: **`SQLITE_BUSY_SNAPSHOT`** — lỗi ĐẶC THÙ WAL mode khi dbt (writer
  mới) chen vào 3 stream; busy_timeout KHÔNG cứu được loại này (transaction phải restart,
  không phải chờ). Chính journal_mode=WAL thêm hôm 22-07 mở ra failure mode này.
- Fix đúng đơn đã kê: **JdbcCatalog backend → Postgres** (db mới `iceberg_catalog` trong
  gtl-postgres, KHÔNG đụng db banking). Image fixture không có driver Postgres → mount
  `jars/postgresql-42.7.4.jar` + override command sang `java -cp ... RESTCatalogServer`.
  Migrate = copy **4 dòng** (iceberg_tables ×3 + namespace_properties ×1, catalog_name
  `rest_backend`) từ sqlite sang. Verify: REST trả đủ bronze/3 bảng; stream + dbt ghi
  **song song** không lỗi. SQLite cũ giữ nguyên tại `iceberg-catalog/` làm backup.
- Bài học: URI đổi backend chỉ 1 dòng, nhưng phải nhớ (1) driver jar không có sẵn trong
  image, (2) copy đúng `catalog_name`, (3) dừng writer trước khi migrate.

### Phase 2 — serving + checkpoint XONG (`verify_2.sh` 16/16)
- **Marts** (`models/marts/`): 3 bảng aggregate (daily_volume, channel_daily, category_daily),
  mọi số tiền qua `net_amount`, có `_refreshed_at` (freshness stamp cho business). Exposure
  `transaction_overview_dashboard` khai trong dbt → lineage chạy tới tận dashboard.
- **Đường A (mart→Postgres):** `spark/gold_layer/push_marts.py` đẩy mart Iceberg → db `marts`
  trong gtl-postgres (overwrite+truncate giữ grant). User `marts_ro` chỉ-SELECT (verify chặn ghi).
- **Superset** (service compose, port 8088): image `apache/superset:4.1.1` + psycopg2 (Dockerfile
  riêng vì bản mới lược driver). Metadata DB = db `superset`; datasource "GTL Marts" trỏ marts_ro;
  bootstrap.sh tự migrate+admin+init idempotent. 3 dataset đăng ký qua API. **Chứng minh
  end-to-end:** Superset chạy SQL thật trên marts ra đúng số. Business login là dựng chart được.
- **Checkpoint:** `scripts/verify_2.sh` 16/16 — star schema đủ, dbt 71 tests, REVERSAL-assert,
  lineage, marts tươi, least-privilege, Superset query được.

### SỰ CỐ: small-files problem (bài học lakehouse quan trọng NHẤT phiên này)
- **Triệu chứng:** full `dbt build` (Silver incremental MERGE) lỗi `Connection reset` từ MinIO,
  15 lỗi khi có stream / 3 lỗi khi dừng stream. Subset build thì xanh. Không phải OOM.
- **Root cause (đo bằng `.files`):** Bronze stream commit mỗi 5s → **bronze.transactions 4,449
  file** × ~2000 dòng, **accounts 4,454 file** (small-files kinh điển của streaming). Incremental
  Silver quét hàng nghìn file nhỏ → bùng nổ S3 GET → MinIO single-node reset connection.
- **Fix chuẩn ngành:** `spark/maintenance.py` — Iceberg `rewrite_data_files` (nén → 128MB) +
  `expire_snapshots` (giữ 10). Kết quả **accounts 4,454→3, transactions →6 file**. Sau nén,
  full build + stream chạy **song song** đều 82/82 xanh.
  - Sub-bẫy: nén 4400 file một lượt OOM + broadcast → thêm `autoBroadcastJoinThreshold=-1`,
    `partial-progress.enabled=true`, `max-file-group-size-bytes=256MB`, driver 6g.
- **Bài học:** streaming LUÔN đẻ small file → compaction định kỳ là BẮT BUỘC, không phải tuỳ chọn.
  Production auto-compact; ở đây `maintenance.py` chạy tay giờ, vào DAG Airflow Phase 3. Đây chính
  là task "Iceberg snapshot maintenance" mà plan xếp Phase 4 — nhưng cần SỚM vì downstream đọc.
- **MinIO limit 512M→1536M** (từng nghẹt 86% khi 3 workload đồng thời) — nhưng nén mới là fix gốc.

### Hạ tầng Postgres giờ gánh 4 DB (1 instance, tách bạch)
`banking` (nguồn CDC) · `iceberg_catalog` (JDBC catalog backend) · `marts` (serving) ·
`superset` (metadata BI). Users least-privilege: `bank`, `debezium` (RO replication),
`marts_ro` (RO), `superset_meta`. Đúng tinh thần tách quyền, tái dùng instance thay vì dựng mới.

---

## 20-07-2026

### Phase 1c ✅ XONG — Bronze Iceberg trên MinIO (`verify_1c.sh` 13/13)
- **Kết quả:** Bronze `transactions` 16.9M dòng · `accounts` 7.84M · lag **82 message** (~vài giây)
  → **near-real-time đạt**. `op`: c=8.37M, u=8.53M, **d=3** (mục tiêu Phase 1: bắt được DELETE).
  Envelope nguyên vẹn, tiền giữ dạng **string**.
- **Dựng mới:** `minio` (S3 API 9001 / console 9002, bucket `warehouse`) · `iceberg-rest`
  (8181, catalog sqlite bền qua bind mount) · **Spark venv trên HOST** (`~/working/gtl-spark-venv`,
  pyspark 3.5.0, Java 17 JRE) — KHÔNG dùng container.
- **Code:** `spark/{gtl_session,bronze_stream,smoke_test,verify_bronze}.py` ·
  `scripts/{verify_1c,reset_all}.sh` · spec `docs/phase-1c-bronze-design.md`.

### Quyết định thiết kế (có tranh luận, không mặc định)
- **3 bảng mirror 1:1 với nguồn** (1 fact + 2 dim), KHÔNG gộp 1 bảng. Lý do quyết định:
  fact/dim khác hồ sơ vật lý (partition theo ngày chỉ có nghĩa với fact), và **Silver `MERGE INTO`
  theo PK khác nhau** (`txn_id` vs `account_id`) → gộp thì merge-theo-key vô nghĩa.
- **Bronze RAW, không flatten** — giữ nguyên envelope trong cột `value`; flatten/ép kiểu/mask PII/
  dedup là việc của Silver. Bronze ghi, Silver diễn giải.
- **Spark chạy host, không container:** Kafka đã advertise sẵn `EXTERNAL://localhost:9093` cho
  client ở host → khỏi bắc cầu network giữa 2 stack.
- **Né `hadoop-aws`**: dùng `iceberg-aws-bundle` + `S3FileIO` → tránh hẳn xung đột version Hadoop/AWS SDK.
- **3 query trong 1 SparkSession**, checkpoint riêng từng bảng → cô lập, restart/backfill độc lập.
- **Nguồn sự thật vẫn là Postgres**; lakehouse là bản sao phái sinh. Giữ CDC-từ-bảng (không đổi
  sang app bắn event thẳng → dính dual-write). **Outbox pattern** ghi vào roadmap mở rộng.

### Sự cố & bài học (quan trọng — đừng vấp lại)
1. **Kafka retention 7 ngày đã xoá data trước khi có ai hứng.** `merchants` mất sạch 50 message,
   `transactions` mất 1,326,675 event đầu. Nguyên nhân: 1b dựng xong 13-07, faker bơm liên tục,
   nhưng Bronze (consumer bền) mãi 1c mới có → 7 ngày sau retention cắt.
   → **Đã nâng lên 30 ngày + `retention.bytes` chặn trên** (40/25/1 GB). Đo thật: 20.6GB/7 ngày
   → ~88GB cho 30 ngày, đĩa còn 382GB.
   → Nguyên tắc: retention định cỡ theo **thời gian consumer chết lâu nhất chấp nhận được**,
   KHÔNG phải "giữ càng lâu càng tốt" (vô hạn chỉ đẩy vấn đề sang đĩa). Bảo vệ thật là
   **giám sát lag so với mép retention** → Phase 5.
   → KHÔNG khôi phục merchants: data test, sẽ reset sạch trước Phase 2.
2. **`apache/iceberg-rest-fixture:1.6.1` KHÔNG tồn tại.** Version dùng được = giao của
   *tag image đã phát hành* ∩ *JAR trên Maven* → chốt **1.9.2**. Luôn kiểm tra CẢ HAI tập
   trước khi pin, đừng giả định version có ở mọi nơi.
3. **`SQLITE_CANTOPEN`** — named volume Docker tạo ra thuộc `root:root` 755, image chạy uid 1000
   → không ghi được. Đổi sang **bind mount thư mục host** (senryuu cũng uid 1000). Cùng họ lỗi
   work-dir của `spark-worker`.
4. **Spark cache metadata bảng Iceberg trong session** → `count()` lần hai trả về snapshot cũ,
   stream đang chạy mà trông như đứng hình. Phải `REFRESH TABLE` trước khi đếm lại.
5. **Check phải phân biệt "pipeline hỏng" với "thượng nguồn không có gì".** `verify_1c.sh` bản đầu
   fail vì `bronze.merchants` rỗng — nhưng Kafka không còn message nào để đọc. Đã sửa: so với
   offset Kafka, và đo **lag** thay vì đòi "số dòng phải tăng" (đòi tăng cũng sai khi đã bắt kịp).
6. **BẪY RESET (nguy hiểm nhất, chưa gặp nhưng đã chặn):** khoá chính là
   `GENERATED ALWAYS AS IDENTITY` + faker có `RANDOM_SEED` cố định → sau `down -v` sequence quay
   về 1, lứa data mới **tái dùng đúng khoá chính cũ**. Nếu Bronze còn data cũ → hai thế hệ trùng PK
   → Silver `MERGE ON txn_id` **gộp nhầm hai giao dịch không liên quan, không báo lỗi**.
   → `docker compose down -v` KHÔNG đủ: nó không đụng `iceberg-catalog/` (bind mount) và
   `_checkpoints/` (host). Bỏ sót checkpoint còn tệ hơn — stream tưởng đã đọc rồi và **bỏ qua data mới**.
   → Viết `scripts/reset_all.sh` reset nguyên tử **4 kho** + dừng job trước khi xoá.

### Reset thật + faker sinh DELETE (cuối phiên)
- Đã chạy `reset_all.sh` → 4 kho sạch, dựng lại, `merchants` có lại đủ 50 message
  (xác nhận: reset giải quyết gọn, KHÔNG cần incremental snapshot).
- **Phát hiện sau reset: `op=d` biến mất.** Vì faker **chưa bao giờ xoá gì** — 3 dòng `op=d`
  ở lần chạy trước là do tay tôi xoá thử lúc verify 1b, không phải workload sinh ra.
  → Thêm **`purge_failed()`** vào faker: định kỳ xoá giao dịch `FAILED` quá hạn lưu trữ
  (`PURGE_AFTER_MINUTES=2`, demo nén thang thời gian; chính sách thật tính bằng tháng).
  → **CHỈ xoá `FAILED`** — nó không chuyển tiền nên không phá đối soát Bronze=Gold.
  `COMPLETED`/`REVERSED` là tiền thật, KHÔNG BAO GIỜ hard-delete (ngân hàng đảo chiều bằng
  REVERSAL, không xoá bản ghi).
  → Lý do chính đáng chứ không phải vá test: hệ thống *không bao giờ xoá gì* mới là thứ phi thực tế;
  và purge thường là **job chạy NGOÀI ứng dụng** — đúng loại thay đổi mà CDC log-based bắt được
  còn polling theo timestamp **bỏ sót hoàn toàn** (dòng biến mất, không còn timestamp để so).
- Kết quả: `verify_1c.sh` **13/13** trên dataset sạch, `d=12` khớp đúng `12` tombstone.
- Sửa thêm: `verify_1c.sh` đọc offset stream TRƯỚC rồi mới hỏi Kafka (trước đó ra lag âm).

### Mô phỏng SỰ CỐ trong faker — "nghĩ đến trường hợp tệ nhất"
Phản hồi của Mr. Senryuu: faker luôn giải quyết mọi PENDING = đang mô phỏng một thế giới không
bao giờ có sự cố, và **đó là sai** — DE phải nghĩ tới ca tệ nhất.
- **Lý do kỹ thuật (không chỉ realism):** nếu dữ liệu nguồn không bao giờ chứa ca xấu thì các
  nhánh xử lý ca xấu ở Silver/Gold/alerting **không bao giờ được chạy** → sai mà không ai phát
  hiện. Cụ thể chưa được kiểm chứng: (1) PENDING treo có tính vào doanh số Gold không (KHÔNG —
  tiền chưa đi); (2) đối soát Bronze=Gold xử lý PENDING ra sao; (3) treo bao lâu thì báo động.
- **Ba nhóm giao dịch giờ luôn tồn tại trong bảng nguồn:**
  | Nhóm | Chọn bằng | Mô phỏng |
  |---|---|---|
  | bình thường ~99% | `txn_id % 200 <> 0` | chốt trong tích tắc |
  | treo rồi được cứu | `% 200 = 0` và `% 1000 <> 0` | downstream timeout → sweeper chốt sau 3 phút |
  | treo VĨNH VIỄN | `% 1000 = 0` | in-doubt, cần người can thiệp → Phase 5 phải báo động |
- **Chọn theo `txn_id % N` chứ KHÔNG dùng `random()`**: một dòng đã treo thì treo thật, không
  phải mỗi vòng lặp lại tung xúc xắc rồi vô tình thoát.
- `sweep_stuck_pending()` dùng `SWEEP_COMPLETE_RATE=0.5` (thấp hơn 0.85 của vòng đời thường):
  giao dịch đã timeout thì khả năng hỏng cao hơn. Hệ thống thật xác định kết quả bằng cách
  **hỏi lại mạng thanh toán**, không phải tung đồng xu — con số này chỉ để dữ liệu đúng xu hướng.
- `advance_pending` phải LOẠI TRỪ nhóm treo **ngay trong câu SELECT** — nếu không, các dòng treo
  (cũ nhất) chiếm hết `LIMIT` và chặn đứng việc xử lý giao dịch bình thường.
- Trạng thái ổn định quan sát được: nhóm 2 hội tụ ~18 dòng (0.1/s × ngưỡng 180s = hàng đợi có
  giới hạn), nhóm 3 tăng mãi ~72 dòng/giờ vì demo không có người chốt — **đúng như thực tế** một
  hệ thống không ai xử lý in-doubt thì backlog phải phình.
- Kết quả sau thay đổi: `verify_1c.sh` **13/13**, `op=d`=225 khớp đúng 225 tombstone.

### Tái cấu trúc `spark/` + sự cố catalog SQLite (cuối phiên 20-07 / đầu 22-07)
- **Chia `spark/` theo layer** (yêu cầu của Mr. Senryuu), đồng bộ hậu tố `*_layer`:
  `bronze_layer/` (bronze_stream.py, verify_bronze.py, bronze_explore.ipynb) ·
  `silver_layer/` (silver_transform.py scaffold + notebook) ·
  `gold_layer/` (gold_model.py scaffold + notebook). `gtl_session.py` + `smoke_test.py` giữ ở gốc
  `spark/` (dùng chung, import qua `PYTHONPATH=spark` nên file dời xuống con vẫn resolve).
  Mỗi layer có **1 .py + 1 .ipynb** (notebook chạy bằng venv host, đã execute thử bronze OK).
  Cập nhật đường dẫn trong `verify_1c.sh`, `reset_all.sh`, README, worklog.
- **Stream chết khi phiên trước đứt** (checkpoint đóng băng ở 480k, Kafka bơm tiếp lên 3.99M).
  Restart resume đúng offset — **không mất data** nhờ checkpoint.
- **SỰ CỐ MỚI khi restart — `SQLITE_BUSY: database is locked` → HTTP 500 → stream abort.**
  Nguyên nhân gốc: **3 query streaming commit SONG SONG vào cùng catalog SQLite**. SQLite chỉ cho
  MỘT writer/lần và mặc định thất bại NGAY khi gặp khoá. Lúc backfill 3 topic đồng thời, commit
  đụng nhau. (KHÔNG phải OOM — RAM catalog chỉ 42%.)
  → Vá: `CATALOG_URI` thêm `?busy_timeout=30000&journal_mode=WAL` — SQLite CHỜ khoá tới 30s
  (commit chỉ mất ms) thay vì gục; WAL cho reader song song writer.
  → **Bài học kiến trúc (designed-for-scale, demo-small):** catalog SQLite KHÔNG chịu được nhiều
  writer — production dùng **Postgres/Glue/Nessie** làm JDBC catalog backend. busy_timeout chỉ là
  cách hợp lệ ở quy mô demo. Ghi rõ ranh giới này trong compose + đây.

### Nợ đã ghi nhận
- `issues/003`: **chưa có signal channel** → phương án "re-snapshot được" của CDC risk #1 mới nằm
  trên giấy. Khi làm, phải dùng **Kafka signal channel** chứ KHÔNG dùng signal table (signal table
  đòi cấp quyền `INSERT` cho role `debezium` → phá vỡ least-privilege mà `verify.sh` đang kiểm tra).
- `issues/004`: thiếu **`idempotency_key`** trên `transactions` — hàng rào chống trừ tiền hai lần
  khi client retry. Cũng là vấn đề của tầng dữ liệu: thiếu nó thì đối soát Phase 4 KHÔNG phân biệt
  được "khách trả hai lần" với "hệ thống trừ nhầm hai lần". Hoãn có chủ đích vì đổi schema cần
  `down -v` → **gộp vào lần reset trước Phase 2**, cùng với `issues/002` (counterparty TRANSFER).

### NEXT — Phase 2 (Medallion transform)
Silver: parse envelope → ép kiểu → mask PII → **`MERGE INTO` theo PK ra current-state**;
REVERSED không đếm hai lần, FAILED không tính vào volume. Gold star schema + dbt structural tests.
**Nên chạy `scripts/reset_all.sh` trước Phase 2** để Silver có dataset sạch, nhất quán.

### Cách chạy lại
```bash
cd ~/working/projects/Governed-Transaction-Lakehouse
docker compose up -d
bash scripts/register-connector.sh
PYTHONPATH=spark ~/working/gtl-spark-venv/bin/python spark/bronze_layer/bronze_stream.py   # chạy nền
bash scripts/verify.sh && bash scripts/verify_1b.sh && bash scripts/verify_1c.sh
```

---

## 18-07-2026

### Chốt phiên
- **1b verified ĐẦY ĐỦ end-to-end** (đã sửa overclaim của entry 13-07): bắt tận mắt envelope
  `op=c` (before=null→PENDING), `op=u` (PENDING→COMPLETED, before-image), `op=d` (before + after=null
  + tombstone). `amount` là string (decimal.handling.mode). `verify_1b.sh` 10/10.
- **Slot protection set NGAY**: `max_slot_wal_keep_size=10GB` vào Postgres command (recreate, không mất data).
  Slot khỏe (retained_wal ~1MB). → bảo vệ core DB từ dev.

### Review PLAN update (V2 delta) — đã đồng bộ vào CẢ 3 file (CLAUDE.md, PLAN_V2, README)
- **CDC operational risks**: (1) replication slot có thể đầy đĩa→sập core DB (4 lớp bảo vệ, slot-lag
  = metric hạng nhất); (2) logical decoding chỉ phát txn đã commit (Bronze tự sạch).
- **Ext1** lineage viz (DataHub/OpenMetadata primary, có ES→on-demand; alt OpenLineage+Marquez nhẹ) —
  chốt ở Ext1 sau khi đo RAM. **Ext2** Gemini report (4 guard: LLM không chạm số + chart code vẽ +
  validate output + data-residency). Cả 2 CHỈ làm sau core Phase 1-4.
- **Philosophy #5/#6**: data platform phục vụ business/phòng ban, không chỉ team data.

### Privacy (QUAN TRỌNG)
- **BỎ HẲN** framing tên tổ chức/quốc gia mục tiêu + đơn vị tiền vùng đó khỏi MỌI file (kể cả private).
  Framing giờ trung tính: **"on-premise + cloud"** (code S3 API → chạy local $0, đổi endpoint lên cloud AWS-ready).
- History GitHub đã scrub sạch (orphan-squash + force-push). Currency = **USD**.
- `.gitignore` chặn `PLAN_*.md`, `BANKING_PROJECT_*`, `CLAUDE_BANKING*`, `.claude/`, `CLAUDE.md`, `.env`.

### RAM investigation (kết luận: KHÔNG có vấn đề)
- Thật: **used ~8.5G / 46G, available 37G**. Không gần 48GB (cảnh báo "vượt 48GB" trước là NÓI QUÁ —
  do cộng LIMIT chứ không phải usage).
- Airflow "ngốn RAM" (docker stats 3.36G/4.8G) = **kernel slab_reclaimable (dentry/inode cache)**,
  reclaimable, KHÔNG leak. RSS thật: worker 1.57G, triggerer 0.6G. Nguồn: uptime 10 ngày + file ops (không phải log — log chỉ 77M).
- Peak dự kiến full Phase 1-4 ~20-25G. Chỉ DataHub(ES) + Spark 2 job đồng thời mới đẩy sát trần → đã phòng (reuse Spark + on-demand DataHub).

### Repo state
main sạch (2 commit: `e0ca946` core 1a+1b, `3861bc5` README). PLAN_V2/CLAUDE.md/.env private (không lên GitHub).

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
