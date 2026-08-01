# PII Governance

Ai được thấy dữ liệu cá nhân nào, ở tầng nào, và điều đó được **cưỡng chế bằng code** ra sao.

---

## Nguyên tắc: mask theo MỤC ĐÍCH dùng, không phải một cỡ cho tất cả

Che hết mọi thứ là an toàn nhưng vô dụng — analyst không đếm nổi khách hàng, nhân viên hỗ trợ
không xác minh nổi ai gọi đến. Vì vậy mỗi trường được chọn kỹ thuật theo **việc người ta thật sự
cần làm với nó**:

| Kỹ thuật | Macro | Trường | Vì sao chọn kỹ thuật này | Đảo ngược được? |
|---|---|---|---|---|
| **Băm SHA-256** | `mask_hash` | `customer_name`, `national_id` | Chỉ cần JOIN / COUNT DISTINCT — không ai cần *đọc* giá trị. Băm giữ được tính "cùng người" mà bỏ hoàn toàn danh tính | ❌ một chiều |
| **Che một phần** | `mask_partial`, `mask_email` | `phone`, `email` | Nhân viên hỗ trợ cần **nhận ra** ("số đuôi ...20 phải không ạ?"). Đủ để xác minh, vô dụng với kẻ trộm | ❌ mất phần giữa |
| **Hạ độ phân giải** | `generalize_birth_year` | `date_of_birth` → `birth_year` | Analytics cần **phân bố tuổi**, không cần biết sinh nhật từng người | ❌ mất độ chính xác |

Cả ba đều **một chiều**: không có khoá nào khôi phục lại được, kể cả người vận hành hệ thống.
Đây là chủ ý — nếu tồn tại đường khôi phục thì đó là một bề mặt tấn công phải canh giữ.

Code: [`dbt_project/macros/mask_pii.sql`](../dbt_project/macros/mask_pii.sql)

---

## Ba tầng, quyền giảm dần

| Tầng | Chứa gì | Ai chạm được | Cưỡng chế bằng |
|---|---|---|---|
| **Bronze** | PII **thô** — envelope Debezium nguyên vẹn | RESTRICTED: chỉ pipeline ingestion | Không expose ra serving; Superset không có đường tới đây |
| **Silver** | Đã mask (băm / che / hạ phân giải) | Analyst | Macro mask áp trong chính model — không có đường vòng |
| **Gold / marts** | Không PII cấp dòng; chỉ hash + mask + `birth_year` | Business, BI (Superset) | Test `assert_gold_no_raw_pii` chặn hồi quy |
| **Postgres `marts`** | Bản sao phục vụ | `marts_ro` — **chỉ SELECT** | Quyền cấp ở DB: Superset không thể ghi dù muốn |

**Vì sao Bronze vẫn giữ PII thô?** Vì Bronze là *bằng chứng gốc*: khi một con số bị tranh cãi,
phải truy được về đúng byte mà hệ nguồn đã phát ra. Mask ở Bronze là phá huỷ chứng cứ kiểm toán.
Đánh đổi được xử lý bằng **giới hạn truy cập**, không phải bằng cách bóp méo dữ liệu.

---

## Vì sao có test `assert_gold_no_raw_pii`

Chính sách chỉ có giá trị khi **hồi quy bị chặn tự động**. Một người sửa Silver→Gold sáu tháng sau
có thể vô tình kéo cột thô vào Gold; tài liệu không ngăn được điều đó, test thì có.

Test kiểm mọi dòng `dim_account` thật sự đã mask đúng chuẩn:
- `customer_name_hash` phải là SHA-256 hex — **đúng 64 ký tự** (tên thật lọt vào là sai độ dài ngay)
- `phone_masked` phải chứa ký tự che `*`
- `email_masked` phải chứa `***`

Fail ⇒ `push_marts` bị skip ⇒ PII **không bao giờ** tới Superset. Cùng cơ chế cổng như
reconciliation: chất lượng và quyền riêng tư đều là **cổng chặn**, không phải cảnh báo.

---

## Dữ liệu ở đây là tổng hợp

Toàn bộ do Faker sinh — **không có dữ liệu cá nhân thật nào** trong project. Kỹ thuật mask là thật
và kiểm chứng được; con người thì không có thật.
