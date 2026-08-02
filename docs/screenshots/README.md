# Ảnh chụp cho README

README nhúng 3 ảnh từ thư mục này. Chưa có file thì GitHub hiện ô ảnh vỡ — nên
hoặc chụp đủ, hoặc xoá 3 dòng tương ứng trong README.

**Chụp lúc pipeline ĐANG CHẠY** (`bash scripts/pipeline.sh start`, đợi ~2 phút cho
số liệu tươi). Ảnh chụp lúc mọi thứ đang tắt thì phản tác dụng: nó chứng minh ngược.

| File cần có | Chụp ở đâu | Khung hình nên lấy |
|---|---|---|
| `grafana-pipeline-health.png` | Grafana → folder **GTL Pipeline** → dashboard **GTL · Pipeline Health**, đặt khoảng thời gian **6 giờ** | Lấy trọn hàng 6 stat tile trên cùng + hai biểu đồ ngay dưới. Đó là phần kể được câu chuyện: WAL slot, độ tươi Bronze, mép retention, S3 |
| `superset-marts.png` | Superset `:8088` → một dashboard trên các bảng mart | Ưu tiên biểu đồ có **số tiền** và **xu hướng theo ngày** — nó cho thấy người dùng nghiệp vụ tự trả lời được câu hỏi, đúng mục tiêu cuối của project |
| `airflow-dag.png` | Airflow `:8085` → DAG **`gtl_transform`** → tab **Graph** | Phải nhìn rõ `dbt_test` nằm TRƯỚC `push_marts` — đó chính là bằng chứng "test là cổng", không phải báo cáo |

## Vài lưu ý để ảnh không phản tác dụng

- **Che thông tin nhạy cảm**: tên bucket S3, địa chỉ IP Tailscale, email. Người ta
  sẽ phóng to đọc. (Chính file này lúc đầu cũng viết thẳng IP ra làm ví dụ — đúng
  loại rò rỉ mà nó đang dặn phải tránh.)
- **Đừng chụp dashboard toàn số 0** hoặc toàn "No data". Thà không có ảnh còn hơn
  có ảnh chứng minh hệ thống không chạy.
- **Nên có một alert đang đỏ** trong ảnh Grafana nếu tiện — nó chứng minh cảnh báo
  hoạt động thật, chứ không phải bảng màu xanh trang trí. (Cách tạo: tắt
  `bronze_stream` khoảng 20 phút, luật `Bronze stream ngừng ghi` sẽ bắn.)
- Ảnh PNG, bề ngang ~1400px là đủ nét trên GitHub mà không nặng repo.
- Chế độ **sáng** dễ đọc hơn khi nhúng vào README trên nền trắng của GitHub.
