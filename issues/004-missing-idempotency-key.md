# Issue #004 — Bảng `transactions` thiếu `idempotency_key`

**Trạng thái:** OPEN
**Phát hiện:** Phase 1 Step 1c (2026-07-20), khi rà soát ca hỏng của hệ thống thanh toán
**Ảnh hưởng:** schema nguồn — cần `down -v` (init script chỉ chạy trên volume trống)
**Mức:** Medium (thiếu sót về tính thực tế của mô hình, không chặn phase nào)

## Vấn đề

Bảng `transactions` hiện không có khoá idempotency. Hệ thống thanh toán thật **luôn** có,
và luôn kèm ràng buộc `UNIQUE` — đó là hàng rào chống **trừ tiền hai lần**.

Kịch bản mà nó chặn: khách bấm "Thanh toán", mạng chậm, khách bấm lại (hoặc client tự retry
sau timeout). Không có khoá idempotency thì server coi đây là **hai lệnh khác nhau** và trừ
tiền hai lần. Có khoá thì lần thứ hai nhận ra khoá đã xử lý và **trả về kết quả cũ** thay vì
tạo giao dịch mới.

Đây không phải chuyện hiếm gặp: retry là hành vi MẶC ĐỊNH của mọi client và mọi API gateway.
Một hệ thống tiền mà không idempotent thì hỏng ở lần mất gói tin đầu tiên.

## Vì sao nó liên quan tới pipeline dữ liệu (không chỉ tầng ứng dụng)

Khoá idempotency là dữ liệu **đi qua CDC vào lakehouse**, nên nó dùng được ở downstream:

- **Silver/Gold:** phát hiện các lần thử trùng nhau của cùng một ý định thanh toán —
  khác hoàn toàn với hai giao dịch thật sự riêng biệt có cùng số tiền và cùng tài khoản.
- **Reconciliation (Phase 4):** không có nó thì không phân biệt được "khách trả hai lần"
  với "hệ thống trừ nhầm hai lần" — mà đó chính là câu hỏi mà đối soát sinh ra để trả lời.
- **Chất lượng dữ liệu:** `UNIQUE` trên nguồn cho phép dbt kiểm tra ràng buộc đó vẫn đúng
  sau khi qua toàn bộ pipeline.

## Hướng xử lý

Thêm cột vào `postgres/init/01_schema.sql`:

```sql
idempotency_key UUID NOT NULL DEFAULT gen_random_uuid(),
...
CONSTRAINT uq_transactions_idempotency UNIQUE (idempotency_key)
```

Faker sinh khoá từ phía "client" và **cố ý retry một tỷ lệ nhỏ với cùng khoá**, để dữ liệu
chứa sẵn ca trùng lặp — cùng lý do với việc mô phỏng giao dịch treo: nếu dữ liệu nguồn không
bao giờ có ca xấu thì logic xử lý ca xấu ở downstream không bao giờ được chạy.

## Vì sao chưa làm ngay

Đổi schema đòi `docker compose down -v` (init script chỉ chạy trên volume trống). Vừa reset
xong ở 1c, và Phase 2 sẽ cần một lần reset nữa để Silver có dữ liệu nhất quán —
**gộp thay đổi schema này vào lần reset đó** thay vì xoá dữ liệu thêm một lần không cần thiết.

## Liên quan

- `issues/002` — `transactions` cũng chưa có tài khoản đích cho TRANSFER. Cùng nhóm
  "cải tiến schema nguồn", nên làm chung một lần.
- Ca hỏng ở tầng ứng dụng đã được mô phỏng trong faker (giao dịch treo + job quét, 2026-07-20);
  idempotency là mảnh còn thiếu cuối cùng của mô hình đó.
