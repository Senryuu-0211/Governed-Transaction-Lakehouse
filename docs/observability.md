# Observability (Phase 5)

> Câu hỏi mà phase này trả lời: **"pipeline có đang chạy đúng không, và nếu không
> thì tôi có biết trước khi quá muộn không?"**
>
> Ba sự cố lớn nhất của project đều KHÔNG được phát hiện bằng lỗi — chúng hỏng
> trong im lặng và chỉ lộ ra khi có người tình cờ nhìn: stream chết 10 tiếng
> (26-07), Kafka xoá mất 1.326.675 event (20-07), storage phình 397GB làm đầy ổ
> (28-07). Phase 5 tồn tại để ba thứ đó không bao giờ im lặng nữa.

---

## 1. Đường đi của một con số

```
  spark/gtl_session, Postgres, Kafka, S3
              │
              ▼  (mỗi 2 phút, crontab của user — không cần sudo)
  scripts/metrics_exporter.py
              │
              ▼  ghi ATOMIC (tmp + rename)
  ~/working/metrics/gtl.prom
              │
              ▼  textfile collector (mount ro)
  node_exporter :9100          ← ĐÃ chạy 24/7 sẵn cho dashboard host
              │
              ▼  scrape 15s
  Prometheus :9090             ← retention 15 ngày
              │
              ▼  đánh giá 1 phút
  Grafana :3000  ─── dashboard "GTL · Pipeline Health"
                 └── 9 luật alert ──► email
```

### Vì sao textfile collector, không phải một exporter HTTP riêng

Cách "chuẩn sách vở" là dựng một tiến trình `prometheus_client` mở cổng `/metrics`
chạy 24/7. Ở đây **mọi thứ cần đo đều tính được bằng một lệnh rời** — một câu SQL,
một lần `stat` file, một lần gọi boto3 — không có state nào phải giữ giữa hai lần
đo. Nuôi thêm một daemon chỉ để chạy năm câu lệnh mỗi hai phút là thêm **một thứ
nữa có thể chết lặng lẽ**, mà chết lặng lẽ chính là vấn đề ta đang đi giải quyết.

`node_exporter` thì đã chạy sẵn, đã được Prometheus scrape sẵn, và đã có sẵn cơ chế
đọc file `.prom`. Ta chỉ cần bật một cờ.

**Đánh đổi phải biết:** số liệu cũ tối đa bằng chu kỳ cron (2 phút). Các rủi ro ta
canh đều diễn tiến hàng chục phút tới hàng giờ nên 2 phút quá đủ. Nếu sau này cần
đo thứ biến thiên theo giây thì mới đáng đổi cách.

### Vì sao ghi atomic

`node_exporter` có thể đọc thư mục đúng lúc ta đang ghi. Ghi thẳng vào `gtl.prom`
sẽ có lúc nó đọc được nửa file → collector báo lỗi parse và **vứt bỏ toàn bộ file**.
`os.replace()` là thao tác nguyên tử: người đọc thấy hoặc file cũ trọn vẹn, hoặc
file mới trọn vẹn, không bao giờ thấy dở.

---

## 2. Metric — mỗi cái ứng một sự cố có thật

Không có metric nào ở đây chỉ để "cho đẹp dashboard". Metric mà không ai hành động
theo thì chỉ làm loãng thứ thật sự quan trọng.

| Metric | Trả lời câu hỏi | Ứng với |
|---|---|---|
| `gtl_replication_slot_lag_bytes` | WAL có đang tích tụ ở DB nguồn không? | **Rủi ro #1** — nguy hiểm nhất: giết CORE DB, không phải giết pipeline |
| `gtl_replication_slot_active` | Có slot mồ côi nào vẫn giữ WAL không? | Con đường âm thầm dẫn tới rủi ro #1 |
| `gtl_kafka_retention_margin_ratio` | Bronze còn cách mép retention bao xa? | **Sự cố 20-07** — mất 1,3 triệu event, không một lỗi nào |
| `gtl_bronze_freshness_seconds` | Stream còn ghi được không? | **Sự cố 26-07** — chết im 10 tiếng, `pgrep` báo "đang chạy" |
| `gtl_kafka_lag_messages` | Có bám kịp nguồn không? | Lag kéo dài sẽ ăn dần vào retention margin |
| `gtl_reconciliation_amount_diff` | **Số có đúng không?** | Governance — Phase 4 |
| `gtl_s3_bytes` / `gtl_s3_objects` | Kho có phình bất thường không? | **Sự cố 28-07** — 397GB làm đầy ổ 512GB |
| `gtl_pipeline_enabled` | Đang tắt có chủ đích hay đang chết? | Cổng cho mọi alert (xem §4) |
| `gtl_exporter_up`, `node_textfile_mtime_seconds` | Bản thân hệ giám sát còn sống không? | Xem §5 |

### Hai bẫy đã dính khi đặt ngưỡng — đều cùng một gốc

Cả hai đều là **áp một ngưỡng tuyệt đối lên thứ có ý nghĩa tương đối**, và cả hai
đều chỉ lộ ra khi chạy thật, không lộ ra khi đọc code:

**(a) `merchants` làm cháy alert retention.** Topic này cả đời chỉ có 50 message và
Bronze đã nuốt hết sạch — tức là **an toàn tuyệt đối**. Nhưng ngưỡng "cảnh báo khi
margin < 50.000 message" thì nó vi phạm vĩnh viễn. Sửa bằng cách đo **tỷ lệ** (đã
đọc tới đâu trong cửa sổ retention, 0..1) thay vì số message: đúng cho topic 50
message y như cho topic 3 triệu message.

**(b) `merchants` làm cháy luôn alert freshness.** Bảng tĩnh, không còn CDC event
→ không còn micro-batch nào chạy → checkpoint **không bao giờ mới lại** → tuổi tăng
vô hạn. Lấy `max(freshness)` là lấy đúng cái bảng tĩnh đó và báo động mãi mãi. Câu
hỏi thật là *"tiến trình stream còn ghi được không"*, và bằng chứng là **bất kỳ**
bảng nào vừa commit → `min(freshness)` mới trả lời đúng câu đó.

> Bài học chung: ngưỡng chỉ đúng khi nó đo cùng thứ mà câu hỏi đang hỏi. Viết
> ngưỡng theo trực giác rồi chạy thật, nó sẽ tự chỉ ra chỗ sai — nhưng chỉ khi ta
> chịu nhìn trạng thái alert sau khi provision, thay vì thấy "đã tạo 9 luật" là đi.

---

## 3. Alert — 9 luật, chia làm ba nhóm

| Nhóm | Luật | Ngưỡng | Có cổng? |
|---|---|---|---|
| **Hạ tầng nguồn** | slot giữ WAL quá nhiều | >5GB, 10 phút | không |
| | slot mồ côi | active=0, 30 phút | không |
| **Dòng chảy data** | Bronze ngừng ghi | freshness >900s, 5 phút | **có** |
| | sắp mất event | ratio <0.05, 5 phút | không |
| | tụt lại sau Kafka | lag >100k, **30 phút** | **có** |
| **Đúng đắn & chi phí** | đối soát lệch | \|diff\| >$0.01 | không |
| | kho S3 phình | >30GB, 15 phút | không |
| | đĩa host đầy | >80%, 10 phút | không |
| | exporter chết | mtime >900s | không |

Vài lựa chọn đáng nói:

- **Ngưỡng slot lag 5GB đặt DƯỚI `max_slot_wal_keep_size=10GB`** — còn nửa quãng
  đường để người xử lý trước khi Postgres buộc phải hy sinh slot.
- **`for: 30m` ở luật lag** mới là phần quan trọng, không phải con số 100k. Nó phân
  biệt *"đang đuổi kịp"* (bình thường sau khi bật lại) với *"đuổi mãi không kịp"*.
- **Ngưỡng đối soát là $0.01 chứ không phải 0** — so sánh decimal sau tổng hợp có
  thể lệch ở chữ số cuối; kêu vì làm tròn thì lại thành nhiễu.
- **Alert đối soát là loại một ngân hàng quan tâm nhất.** Tám luật kia nói *"hạ tầng
  có chạy không"*; luật này nói *"số có đúng không"*. Hạ tầng xanh mà số sai thì còn
  tệ hơn hạ tầng đỏ — vì người ta vẫn tin vào báo cáo.

---

## 4. Cổng `gtl_pipeline_enabled` — maintenance window làm cách rẻ nhất

Pipeline này **cố ý bị tắt** những lúc không dùng, để không đốt tiền S3
(`scripts/pipeline.sh stop`). Nếu alert không biết điều đó thì mỗi đêm tắt máy là
sáng hôm sau một hộp thư đầy cảnh báo "stream chết", "lag tăng" — **toàn tin đúng
nhưng vô dụng**. Vài đêm như vậy là người ta bắt đầu lọc thư, và lúc có sự cố thật
cũng lọc nốt. Cảnh báo nhiễu còn tệ hơn không có cảnh báo.

Cơ chế:

- `pipeline.sh stop` ghi `0` vào `~/working/metrics/.pipeline_state` **trước khi**
  tắt (đặt sau thì có một khoảng stream đã chết mà cờ còn =1 → bắn oan).
- `pipeline.sh start` dựng cờ lên `1` **sau cùng** (stream cần vài chục giây mới có
  commit đầu; dựng sớm là bắn oan lúc khởi động bình thường).
- Chỉ ghi **file trạng thái**, không ghi thẳng `.prom` — file đó do
  `metrics_exporter.py` độc quyền ghi. Hai tiến trình cùng ghi vào thư mục textfile
  là công thức cho file đọc dở.
- Alert nào cần cổng thì thêm `and on() (max(gtl_pipeline_enabled) == 1)`.
- **Không đọc được cờ → mặc định 1 (đang bật).** Cố ý chọn phía an toàn: thà báo
  nhầm còn hơn im lặng vì thiếu một file.

Các alert về **đĩa, S3 và slot** thì **KHÔNG có cổng**: đĩa vẫn đầy, S3 vẫn tính
tiền, và slot vẫn tích WAL kể cả khi ta đã tắt Spark.

---

## 5. Ai canh người canh gác

Tám luật đầu đều dựa trên file `.prom` do exporter ghi. Exporter chết (cron hỏng,
venv gãy, treo) thì mọi metric **đứng yên ở giá trị cuối** — Prometheus vẫn có số,
dashboard vẫn xanh, alert vẫn im, và ta **mù hoàn toàn mà tưởng mọi thứ ổn**. Đây
là kiểu hỏng nguy hiểm nhất của mọi hệ giám sát.

`node_textfile_mtime_seconds` do **chính `node_exporter` đo** (không đi qua exporter
của ta) nên đó là điểm quan sát độc lập duy nhất. Luật `gtl-exporter-stale` bắn khi
file không được ghi mới quá 15 phút, và `noDataState: Alerting` — mất hẳn metric
cũng là bắn, không phải im.

---

## 6. Bật gửi email

Alert **vẫn chạy và vẫn hiện đỏ trong Grafana** kể cả khi chưa cấu hình SMTP — chỉ
là không gửi được thư. Giám sát không được phép sập vì thiếu một mật khẩu email.

Để bật thật, sửa `.env` của project `Resource-Monitoring-Dashboard`:

```bash
GF_SMTP_ENABLED=true
GF_SMTP_PASSWORD=<App Password 16 ký tự>
```

`GF_SMTP_PASSWORD` là **App Password của Google**, không phải mật khẩu Gmail
(Google đã chặn đăng nhập bằng mật khẩu chính từ ứng dụng bên thứ ba từ 2022):
myaccount.google.com → Security → 2-Step Verification → App passwords.

Rồi `docker compose restart grafana`.

> **Ở production thật, email KHÔNG đủ cho alert `critical`** — email lúc 3h sáng
> không ai đọc. Alert đánh thức người phải đi PagerDuty/Opsgenie. Email ở đây
> chứng minh **đường đi** của cảnh báo là thông suốt; đổi kênh chỉ là đổi một khối
> cấu hình trong `gtl-contactpoints.yaml`.

---

## 7. File ở đâu

| File | Vai trò |
|---|---|
| `scripts/metrics_exporter.py` | Thu thập + ghi `.prom` (project GTL) |
| `scripts/pipeline.sh` | Ghi cờ `.pipeline_state` khi bật/tắt |
| `scripts/verify_5.sh` | Kiểm CHUỖI từ nguồn tới cảnh báo (16 kiểm tra) |
| `<monitoring>/docker-compose.yaml` | node-exporter textfile + SMTP grafana |
| `<monitoring>/grafana/provisioning/alerting/gtl-rules.yaml` | 9 luật alert |
| `<monitoring>/grafana/provisioning/alerting/gtl-contactpoints.yaml` | Kênh email + định tuyến |
| `<monitoring>/grafana/dashboards-gtl/gtl-pipeline.json` | Dashboard |

Cấu hình Grafana **đặt ở project monitoring** vì Grafana chỉ đọc provisioning từ
thư mục của chính nó. ⚠️ Sửa file provisioning phải `docker compose restart grafana`
mới nạp — và phải `restart`, không phải `up -d` (compose config không đổi thì `up -d`
là no-op, container không khởi động lại và file mới không được đọc).
