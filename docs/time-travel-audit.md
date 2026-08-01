# Time-travel & Lineage Audit

Trả lời hai câu mà kiểm toán tài chính luôn hỏi: **"số liệu lúc đó là gì?"** và
**"con số này từ đâu ra?"**

---

## 1. "Số liệu lúc đó là gì?" — Iceberg time-travel

Iceberg ghi lại **mọi lần commit** thành một snapshot bất biến. Không cần dựng gì thêm: khả năng
xem lại quá khứ có sẵn từ lúc chọn Iceberg làm table format.

**Xem lịch sử commit của một bảng:**
```sql
SELECT snapshot_id, committed_at, operation, summary
FROM gtl.bronze.transactions.snapshots
ORDER BY committed_at DESC;
```
```
7847173647281219198 | 2026-07-31 07:59 | append
7177715079321676579 | 2026-07-31 07:58 | append
```

**Đọc dữ liệu đúng như tại thời điểm T** (báo cáo tháng trước ra số X, giờ chạy lại ra Y — bảng
đã đổi hay logic đã đổi?):
```sql
-- theo thời điểm
SELECT * FROM gtl.gold.fact_transactions
FOR TIMESTAMP AS OF TIMESTAMP '2026-07-30 12:00:00';

-- theo đúng một snapshot cụ thể
SELECT * FROM gtl.gold.fact_transactions FOR VERSION AS OF 7847173647281219198;
```

**So sánh hai thời điểm** — thấy chính xác cái gì đã đổi:
```sql
SELECT count(*) FROM gtl.gold.fact_transactions FOR TIMESTAMP AS OF TIMESTAMP '2026-07-30 12:00:00'
UNION ALL
SELECT count(*) FROM gtl.gold.fact_transactions;
```

### ⚠️ Giới hạn phải biết: snapshot HẾT HẠN

`spark/maintenance.py` chạy `expire_snapshots(retain_last=10)` mỗi ngày — **bắt buộc**, vì không
dọn thì metadata phình vô hạn và storage tính tiền mãi. Hệ quả: **time-travel chỉ lùi được vài
snapshot gần nhất**, không phải vĩnh viễn.

→ Vì vậy bằng chứng đối soát dài hạn **không** dựa vào time-travel, mà nằm ở bảng
`gold.audit_reconciliation` (append-only, `full_refresh=false`). Time-travel dùng để **điều tra sự
cố gần đây**; sổ đối soát dùng để **chứng minh trước kiểm toán** nhiều tháng sau.

| Câu hỏi | Công cụ | Giữ được bao lâu |
|---|---|---|
| "Bảng này lúc 12h hôm qua thế nào?" | Iceberg time-travel | ~10 snapshot gần nhất |
| "Chứng minh ngày 20/7 tiền khớp" | `audit_reconciliation` | vĩnh viễn |

---

## 2. "Con số này từ đâu ra?" — lineage

```bash
bash scripts/dbt.sh docs generate   # sinh target/manifest.json + catalog.json
```

dbt dựng đồ thị phụ thuộc từ chính các lời gọi `ref()` / `source()` trong model — nên lineage
**luôn khớp code đang chạy**, không thể lệch như sơ đồ vẽ tay.

Đường đi đầy đủ của một con số trên dashboard:
```
Postgres.transactions → (Debezium/Avro) → Kafka → bronze.transactions
   → silver.transactions (khử trùng CDC, ép kiểu, mask PII)
   → gold.fact_transactions (net_amount: chỉ COMPLETED là tiền thật)
   → marts.mart_daily_volume → Postgres marts → Superset
```

Mỗi cạnh đều kiểm chứng được: `manifest.json` chứa danh sách phụ thuộc từng model, cột và mô tả
nghiệp vụ (Ext: đổ sang DataHub để business tự tra, không cần đọc code).

---

## 3. Vì sao audit trail này đáng tin

- **Bronze giữ envelope thô** — mọi con số truy được về đúng byte hệ nguồn phát ra.
- **Iceberg snapshot bất biến** — không sửa tại chỗ; mỗi thay đổi là một phiên bản mới.
- **`audit_reconciliation` append-only** — bằng chứng đối soát không xoá/sửa được, kể cả khi
  chạy `dbt build --full-refresh`.
- **Kafka offset làm mốc** — mỗi dòng đối soát ghi `watermark_offset`, nên kiểm tra lại độc lập được.
