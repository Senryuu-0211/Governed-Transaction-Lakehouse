-- GOVERNANCE PII: Gold KHÔNG được lộ giá trị cá nhân thô.
--
-- Chính sách viết trong tài liệu thì sáu tháng nữa không ai nhớ. Test này biến nó
-- thành thứ máy cưỡng chế: ai sửa Silver->Gold mà vô tình kéo cột thô vào sẽ bị
-- chặn tại cổng, PII không bao giờ tới Superset.
--
-- Kiểm bằng HÌNH DẠNG giá trị chứ không phải tên cột — vì đổi tên cột vẫn có thể
-- nhét dữ liệu thô vào đúng chỗ cũ:
--   * hash SHA-256 luôn là 64 ký tự hex -> tên thật lọt vào sẽ sai độ dài ngay
--   * giá trị đã che một phần BẮT BUỘC còn dấu '*' -> số điện thoại đầy đủ sẽ trượt
--
-- Trả về dòng = VI PHẠM -> fail -> push_marts skip.

select
    account_id,
    length(customer_name_hash) as name_hash_len,
    phone_masked,
    email_masked
from {{ ref('dim_account') }}
where length(customer_name_hash) != 64                                  -- không phải SHA-256
   or phone_masked not like '%*%'                                       -- chưa che
   or (email_masked is not null and email_masked not like '%***%')      -- chưa che
