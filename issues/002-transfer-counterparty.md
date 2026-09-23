# Issue #002 — TRANSFER chưa có tài khoản đích (counterparty)

**Trạng thái:** ✅ **ĐÃ ĐÓNG 22-09-2026** — xem mục cuối file
**Phát hiện:** Phase 1 Step 1a (2026-07-08)
**Ảnh hưởng:** Realism nghiệp vụ + logic Gold (Phase 2)
**Mức:** Low-Medium (cải tiến schema, làm khi cần chuyển khoản đúng nghĩa)

## Vấn đề
Schema `transactions` chỉ có `account_id` (tài khoản khởi tạo) + `merchant_id`. Với
`txn_type='TRANSFER'` (chuyển khoản liên tài khoản) KHÔNG có cột **tài khoản đích**.

Hiện faker mô hình TRANSFER như **dòng tiền RA** khỏi `account_id` (giống DEBIT) — nhất quán
với hàng dữ liệu, nhưng chưa phản ánh việc tiền **vào** một tài khoản khác.

## Hướng xử lý (khi cần)
- Thêm cột `counterparty_account_id BIGINT NULL REFERENCES accounts(account_id)` (NULL cho
  giao dịch merchant, NOT NULL cho TRANSFER).
- Faker: TRANSFER completed → trừ `account_id`, cộng `counterparty_account_id` (2 UPDATE accounts
  → 2 CDC event, đúng như chuyển khoản thật).
- Gold (Phase 2): đảm bảo TRANSFER không phồng tổng volume (tiền chỉ dịch chuyển, không sinh thêm).

## Ghi chú
- Chưa làm ở 1a để giữ schema tối giản. Ưu tiên thấp — nâng khi làm phần reconciliation/Gold.


---

## ✅ ĐÓNG 22-09 — và nó lộ ra một luật tiền thứ hai

Gộp vào cùng lần reset với issue #004 (cả hai sửa schema nguồn nên cùng cần `down -v`;
làm riêng là xoá data hai lần không vì lý do gì).

### Schema

```sql
counterparty_account_id BIGINT NULL REFERENCES accounts(account_id),

CONSTRAINT ck_transfer_has_counterparty CHECK (
    (txn_type =  'TRANSFER' AND counterparty_account_id IS NOT NULL)
 OR (txn_type <> 'TRANSFER' AND counterparty_account_id IS NULL)
),
CONSTRAINT ck_counterparty_not_self CHECK (
    counterparty_account_id IS NULL OR counterparty_account_id <> account_id
)
```

Ràng buộc **HAI CHIỀU** chứ không chỉ "TRANSFER thì bắt buộc có": nếu để một giao dịch
POS vẫn kèm counterparty thì downstream không biết nên cộng tiền cho ai. Và cấm tự
chuyển cho chính mình — cùng một tài khoản vừa bị trừ vừa được cộng thì đối soát lệch.

### Faker

TRANSFER hoàn tất sinh **HAI** UPDATE trên `accounts` (trừ nguồn, cộng đích) → hai CDC
event, đúng như một lệnh chuyển khoản thật. Trước đây chỉ trừ mà không cộng vào đâu cả —
tiền **bốc hơi**, trong một hệ thống lấy "tiền vào = tiền ra" làm mệnh đề trung tâm.

### 🔴 Thứ issue này KHÔNG lường trước: chuyển khoản nội bộ không phải doanh số

Sửa xong phần cộng-trừ mới thấy vấn đề lớn hơn, và nó không nằm ở tầng nguồn mà ở tầng
**ngữ nghĩa**:

> Một TRANSFER hoàn tất trừ tài khoản A, cộng tài khoản B. Xét trên toàn ngân hàng,
> **tổng tiền KHÔNG ĐỔI** — không đồng nào vào hay ra khỏi hệ thống. Cộng nó vào
> "doanh số" là đếm một khoản tiền chỉ đổi chỗ như thể vừa kiếm được.

Với tỷ lệ TRANSFER đo thật **14,8%**, con số doanh số bị thổi lên tương ứng.

Đây là loại sai mà **mọi test từng-dòng đều xanh**: mỗi dòng đều hợp lệ, chỉ có phép
**CỘNG** là sai nghĩa. Cùng họ với lý do phải có bảng đối soát.

**LUẬT TIỀN THẬT #2** (`gold/fact_transactions.sql`):

```sql
t.txn_type = 'TRANSFER'                                as is_internal_transfer,
case when t.status = 'COMPLETED' and t.txn_type <> 'TRANSFER' then t.amount
     else cast(0 as decimal(15,2)) end                 as external_amount,
```

`net_amount` giữ nguyên nghĩa (đo **lưu lượng**), `external_amount` là **doanh số**.
Mọi mart giờ có cả hai, và mô tả cột nói rõ cái nào dùng cho câu hỏi nào.

### Test gác

`tests/assert_transfer_rules.sql` — 5 ca vi phạm, chạy ở **ĐẦU KIA** của đường ống:

1. TRANSFER thiếu tài khoản đích
2. Không phải TRANSFER nhưng có tài khoản đích
3. Chuyển khoản cho chính tài khoản đó
4. **Chuyển khoản nội bộ bị tính vào doanh số**
5. Giao dịch bên ngoài nhưng doanh số lệch `net_amount`

Nguồn đã có CHECK constraint, nhưng constraint chỉ gác ở Postgres. Test này gác sau khi
dữ liệu đi qua CDC → Kafka → Avro → Spark → Iceberg → MERGE. Đó mới là thứ cần chứng
minh: ràng buộc **còn nguyên sau sáu lần biến đổi**.

### Bằng chứng (22-09)

```
nguồn        1.226.897 / 8.273.288 giao dịch là TRANSFER (14,8%)
             100% có counterparty_account_id
assert_transfer_rules                       PASS
dbt build --full-refresh                    113 PASS / 0 ERROR
đối soát     8.273.288 = 8.273.288 · $13.327.419.703,21 · lệch $0,00
```
