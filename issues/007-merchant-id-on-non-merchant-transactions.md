# Issue #007 — `merchant_id NOT NULL` ép giao dịch không-có-merchant phải mượn tên một merchant

**Trạng thái:** ✅ **ĐÃ ĐÓNG 23-09-2026** — reset xong, dbt 135 PASS / 0 ERROR, đối soát lệch $0
**Phát hiện:** 23-09-2026, khi dựng `mart_txn_daily` cho tầng semantic
**Ảnh hưởng:** schema nguồn — cần `down -v` (init script chỉ chạy trên volume trống)
**Mức:** High — làm sai **đáp án chấm điểm** của agent, không chỉ sai một con số

## Vấn đề

`transactions.merchant_id` khai là `BIGINT NOT NULL REFERENCES merchants(merchant_id)`.
Nhưng có **hai loại giao dịch không thuộc về merchant nào**, và cả hai đều bị ép phải
điền một merchant:

### (a) Chuyển khoản nội bộ — 1.230.360 dòng (14,8%)

`generate_transactions.py` bốc merchant **trước** khi rẽ nhánh loại giao dịch:

```python
mer_i = draw.merchants(1, ...)[0]      # bốc merchant TRƯỚC
category = categories[mer_i]
...
if rng.random() < world.TRANSFER_SHARE:   # rồi mới quyết định có phải TRANSFER không
    txn_type = "TRANSFER"
```

Kết quả: một lần chuyển tiền cho bạn được ghi là *"Grocery, Metro North"*.

```
txn_type | n       | has_merchant
TRANSFER | 1230360 |      1230360   ← 100%
```

### (b) Lương — 99.041 dòng, tất cả gán cho MỘT merchant

```python
def _salary_rows(day, draw, personas, rng):
    yield (draw.accs[i], draw.mers[0], ...)   # LUÔN LUÔN merchant index 0
```

`draw.mers[0]` là merchant **lớn nhất** (trọng số Pareto xếp giảm dần). Nên toàn bộ lương
của 25.000 tài khoản đổ vào *Rodriguez, Figueroa and Sanchez* — một cửa hàng **Grocery** ở
vùng **Central**.

| | |
|---|---|
| Tổng giao dịch của merchant #1 | 1.160.201 |
| Trong đó là lương | **99.041 (8,5%)** |

## Vì sao nghiêm trọng hơn "một con số hơi lệch"

Ba hậu quả, xếp theo mức tệ dần:

1. **Ngành Grocery và vùng Central bị thổi phồng** 99k giao dịch chưa từng là mua sắm.
2. **Mọi ngành hàng bị thổi ~15%** bởi chuyển khoản — trước khi mart lọc ra.
3. **Sự cố #1 (merchant lớn nhất tắt) bị nhiễu** — và đây mới là chỗ chết người. Sự cố này
   là **đáp án dùng để chấm điểm agent** (CURRENT_STATUS mục 16). Merchant đang "tắt" mà
   ngày 15/07 vẫn có 16.257 giao dịch, vì `_salary_rows` không hỏi merchant có đang hoạt
   động không:

```
07-08  11,484
       (07-09 … 07-14 không có dòng nào — đúng thiết kế)
07-15  16,257   ← ngày lương, toàn bộ là CREDIT, không một DEBIT/TRANSFER nào
       (07-16 lại mất)
07-17  13,762
```

Xây agent trên đáp án sai nghĩa là làm xong phải kiểm lại toàn bộ.

## Vì sao KHÔNG chặn được ở tầng mart

Chuyển khoản thì chặn được, vì fact đã có cờ `is_internal_transfer` — một **hợp đồng**,
không phải suy đoán. Đã làm: `mart_txn_daily` gán nhãn riêng `INTERNAL_TRANSFER`, còn
`mart_category_daily` và `mart_merchant_daily` lọc hẳn ra.

Lương thì **không**. Nhận diện nó phải dựa vào `CREDIT + BRANCH + ngày 1/15 + khoảng tiền`
— bốn điều kiện suy đoán chồng lên nhau, không có cái nào là hợp đồng. Một mart dựng trên
suy đoán sẽ âm thầm sai khi thế giới mô phỏng đổi tham số.

## Cách sửa

Gốc rễ chung: những giao dịch này **không có merchant**, nên cột phải cho phép NULL —
đúng khuôn mẫu đã dùng cho `counterparty_account_id` ở issue #002, chỉ ngược chiều.

```sql
merchant_id BIGINT NULL REFERENCES merchants(merchant_id),

CONSTRAINT ck_merchant_only_for_purchases CHECK (
    (txn_type = 'TRANSFER' AND merchant_id IS NULL)
 OR (txn_type <> 'TRANSFER' AND ...)
)
```

Và `_salary_rows` trả `None` thay cho `draw.mers[0]`.

⚠️ Ràng buộc CHECK cho lương cần một cách **khai báo** để nhận ra dòng lương — nếu không
thì lại rơi vào chính cái bẫy suy đoán ở trên. Hai lối:
- thêm giá trị `SALARY` vào `txn_type` ENUM (rõ ràng nhất, và đúng với thực tế: lương
  không phải là một khoản CREDIT bất kỳ), hoặc
- một cột `is_payroll BOOLEAN NOT NULL DEFAULT false`

Lối thứ nhất tốt hơn: nó làm cho "lương" thành một **khái niệm có tên** trong mô hình, nên
mọi tầng phía sau đều dùng được mà không phải đoán.

## Kèm theo: lỗ ngày chuyển giao

Cùng lần sửa nên xử luôn: `backfill()` phủ ngày −90..−1 rồi dừng, còn luồng live bắt đầu
lúc chạy thật (16:00 ngày 22-09). Ngày giao nhau vì vậy có **lỗ 16 tiếng thật**, và
`assert_rowcount_not_dropped` báo đỏ đúng — chặn `push_marts` cho tới 00:00 hôm sau.

Cổng báo đúng, không nên sửa cổng. Nên sửa là để backfill chạy **tới thời điểm hiện tại**
thay vì dừng ở hôm qua.

## Ghi chú

Cùng họ với **LUẬT TIỀN #2**: từng dòng đều hợp lệ, chỉ phép **GOM NHÓM** là sai nghĩa —
loại sai mà mọi test từng-dòng đều xanh. Đó cũng là lý do `tests/assert_mart_totals_match_fact.sql`
ra đời trong cùng ngày: nó gác TỔNG, không gác dòng.


---

## ✅ Cách sửa đã áp dụng (23-09-2026) — chờ `reset_all.sh` để có hiệu lực

### Nguồn — `postgres/init/01_schema.sql`

```sql
CREATE TYPE txn_type AS ENUM ('CREDIT', 'DEBIT', 'TRANSFER', 'SALARY');

merchant_id BIGINT NULL REFERENCES merchants(merchant_id),

CONSTRAINT ck_merchant_only_for_purchases CHECK (
    (txn_type IN ('TRANSFER', 'SALARY') AND merchant_id IS NULL)
 OR (txn_type IN ('CREDIT', 'DEBIT')    AND merchant_id IS NOT NULL)
)
```

Chọn `SALARY` thành **loại giao dịch riêng** chứ không phải một cờ phụ, vì như thế lương
trở thành một **khái niệm có tên** trong mô hình. Mọi tầng phía sau nhận ra nó bằng khai
báo, không phải bằng `CREDIT + BRANCH + ngày 1/15 + khoảng tiền` — bốn suy đoán chồng lên
nhau, sẽ âm thầm sai ngày ai đó đổi tham số mô phỏng.

Ràng buộc viết **hai chiều** (mua bán *phải có*, chuyển khoản/lương *không được có*), cùng
khuôn mẫu với `ck_transfer_has_counterparty` ở issue #002. Viết một chiều là để ngỏ đúng
cái lỗ đã sinh ra issue này.

**Đã thử trong DB tạm trước khi reset** — 3 hình dạng sai đều bị chặn, 3 hình dạng đúng
đều qua:

```
SALARY   | merchant NULL | —          ✅
TRANSFER | merchant NULL | đích = 2   ✅
DEBIT    | merchant 1    | —          ✅
mua bán thiếu merchant · chuyển khoản đeo merchant · lương đeo merchant  → đều CHẶN
```

### Faker

| Chỗ | Sửa |
|---|---|
| `_salary_rows` | `draw.mers[0]` → `None`, loại `CREDIT` → `SALARY` |
| `_normal_row` | chuyển khoản không ghi merchant (vẫn rút `mer_i` vì nó quyết định khoảng tiền) |
| luồng live | cùng một luật — nếu lệch thì "hôm nay" mang hình dạng khác mọi ngày lịch sử |
| `balance_delta` | `SALARY` là tiền VÀO, cùng chiều `CREDIT` |
| `_ts` | thêm `max_hour` — chặn sinh giao dịch ở giờ TƯƠNG LAI |
| `backfill` | `include_today=True` + tỷ lệ theo **nhịp giờ**, không theo đồng hồ |

Chỗ cuối là phần phụ nhưng đáng nói: 06:00 **không phải** 25% sản lượng một ngày, vì ban
đêm gần như không có giao dịch. Chia theo đồng hồ là tạo ra một ngày méo kiểu khác.

### dbt

| Chỗ | Sửa |
|---|---|
| `silver/schema.yml` | `accepted_values` + `SALARY` |
| `fact_transactions` | thêm cờ `is_payroll` |
| `mart_txn_daily` | nhãn riêng `PAYROLL` cho vùng và ngành |
| `mart_category_daily` / `mart_merchant_daily` | chỉ giao dịch mua bán |
| `assert_transfer_rules` | +3 ca: chuyển khoản đeo merchant · lương đeo merchant · mua bán thiếu merchant |
| `assert_mart_totals_match_fact` | tách `payroll_txn` khỏi `merchant_txn` |
| `tests/test_world.py` | +1 test: backfill phủ tới hôm nay, chuỗi ngày liền mạch |

**Vì sao `PAYROLL` phải là nhãn riêng chứ không để rơi vào `UNKNOWN`:** `UNKNOWN` cần giữ
đúng MỘT nghĩa — *"merchant đã bị xoá ở nguồn"*. Gộp ba thứ khác hẳn nhau vào một nhãn là
làm mất khả năng trả lời, mà không hề báo lỗi.

### ✅ Đã reset và kiểm chứng (23-09, sau khi chạy `reset_all.sh`)

Nguồn sạch:

```
txn_type |    n      | có merchant
DEBIT    | 3,483,235 |   3,483,235
CREDIT   | 3,482,331 |   3,482,331
TRANSFER | 1,230,058 |           0   ✅
SALARY   |    97,542 |           0   ✅
```

**Sự cố #1 sạch** — merchant lớn nhất biến mất TRỌN 8 ngày 10/07→17/07, kể cả ngày lương
15/07 (trước khi sửa ngày đó có 16.257 giao dịch):

```
09/07  11,535
       (10/07 … 17/07 không có dòng nào)
18/07   7,533
```

### 🔧 Một lỗi phụ tự gây ra rồi sửa: giao dịch từ TƯƠNG LAI

Sau reset, ngày hôm nay có giao dịch tới `10:59:59` trong khi đồng hồ mới `10:21`.

Gốc: `max_hour = now.hour` cho phép cả **giờ đang diễn ra**, nên phút/giây rút ngẫu nhiên
trong giờ đó vượt lên tới 59 phút phía trước. Trong khi `today_share` lại chỉ cộng
`HOUR_WEIGHTS[:now.hour]` — tức các giờ **đã xong**. Hai vế lệch nhau: sản lượng của 10 giờ
đầu bị trải lên 11 giờ.

Sửa: `max_hour = now.hour - 1`. Backfill chỉ lấp giờ đã xong, giờ đang chạy để luồng live
lấp — và hai vế khớp lại. Giờ 0 thì `share = 0` nên không sinh gì (đã có chặn `n <= 0`).

Dữ liệu hiện tại vẫn còn phần vượt đó, nhưng nó **tự hết trong vòng một giờ** khi đồng hồ
đuổi kịp, nên không reset lại.

### 🔴 MỘT THAY ĐỔI, BA CỔNG GÃY — phần đáng học nhất của issue này

Nới `merchant_id` cho phép NULL làm **ba test đang gác bật đỏ**, và không test nào trong
số đó xuất hiện khi rà `grep txn_type`. Chúng bám vào cột, không bám vào khái niệm:

| Test | Báo gì | Vì sao nó đúng khi viết, sai từ hôm nay |
|---|---|---|
| `not_null_transactions_merchant_id` (Silver) | FAIL **1.327.788** | = đúng TRANSFER + SALARY. "Phải có merchant" không phải tính chất của CỘT, mà của LOẠI giao dịch — `not_null` không diễn đạt được điều đó |
| `not_null_...merchant_id` (Gold) | cùng lý do | cùng cách sửa |
| `assert_null_rate_low` | FAIL 2 (16% NULL > ngưỡng 5%) | đo tỷ lệ NULL toàn bảng với giả định "NULL = join hỏng" |

Hai cái đầu: **bỏ `not_null`**, để luật thật nằm ở `assert_transfer_rules` — nơi diễn đạt
được cả hai chiều (mua bán phải có, chuyển khoản/lương không được có). `relationships` giữ
nguyên vì dbt vốn bỏ qua NULL.

Cái thứ ba đáng nói hơn. **Cách sai là nới ngưỡng 5% → 20%** cho hết đỏ: làm thế thì test
vẫn xanh trong khi một lỗi join thật ăn mất 15% merchant của giao dịch mua bán. Ngưỡng
giữ nguyên; thứ phải sửa là **MẪU ĐO** — chỉ đo trên những giao dịch đáng lẽ phải có
merchant. Test sắc hơn trước chứ không lỏng đi.

⚠️ **Bài học: khi nới một ràng buộc ở nguồn, thứ gãy không phải code mà là các CỔNG.**
Và phản xạ đầu tiên — nới ngưỡng cho hết đỏ — là phản xạ biến cổng thành đồ trang trí,
đúng thứ đã xảy ra với `severity='warn'` (22-09).

### Kết quả sau reset

```
dbt build   : PASS=135  WARN=0  ERROR=0  SKIP=0
đối soát    : 8.294.805 = 8.294.805 · $13.402.070.329,97 · lệch $0,00
cube        : mart_txn_daily 154.708 dòng
nhãn mới    : INTERNAL_TRANSFER 1.230.292 · PAYROLL 97.542 · UNKNOWN 0
ngành hàng  : Grocery 26,9% (mục tiêu 28%) … Travel 5,0% (mục tiêu 5%)
sự cố #1    : merchant tắt biến mất TRỌN 10/07 → 17/07
```
