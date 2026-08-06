# Issue #006 — Không nén được 2 bảng Bronze: small-files gặp data gravity

**Trạng thái:** ✅ **ĐÃ ĐÓNG 06-08** — giải bằng cách bỏ nguyên nhân gốc, xem cuối file
**Phát hiện:** 05/06-08-2026, khi cố chạy trọn `maintenance.py` để đóng sổ project
**Ảnh hưởng:** Chất lượng đọc + chi phí S3. **KHÔNG** ảnh hưởng tính đúng đắn của dữ liệu
**Mức:** Medium

---

## Triệu chứng

`spark/maintenance.py` chạy được trên bảng nhỏ nhưng **treo vô hạn** trên hai bảng Bronze lớn.
JVM ở **0% CPU** — không phải chậm, mà là **đang chờ**.

```
bronze.merchants   ✅ exit 0
silver.merchants   ✅ exit 0
bronze.accounts    ❌ treo > 20 phút, không tiến một stage nào
bronze.transactions ❌ (chưa thử tới, cùng cỡ)
```

Số object đo được:

| Bảng | Object |
|---|---|
| `bronze/transactions` | **6.817** |
| `bronze/accounts` | **6.702** |
| mọi bảng khác | **< 150** |

Ranh giới rất rõ: bảng ~1 file thì xong sạch, bảng ~6.800 file thì treo.

---

## Nguyên nhân — chuỗi đầy đủ

```
trigger = 5 giây  (LỖI THIẾT KẾ CỦA CHÚNG TA)
   │
   ▼  ~17.000 file/ngày/bảng
6.800 file nhỏ tồn đọng
   │
   ▼  nén 6.800 file = HÀNG NGHÌN request metadata
mỗi request tốn 244ms khứ hồi (nhà ở VN → us-east-1)
   │
   ▼  hàng nghìn × 244ms, qua NAT gia đình
một kết nối kẹt → job đứng im
```

Socket kẹt luôn có hình dạng giống hệt nhau qua mọi lần thử: **đúng ~1001 byte không gửi đi
được** tới một IP AWS, `unacked:1`, JVM 0% CPU.

---

## 🔴 Thừa nhận: đây là lỗi thiết kế của project, không phải lỗi của S3

**Nguồn gốc là `trigger = "5 seconds"` trong `bronze_stream.py`.** Mỗi micro-batch đẻ một file
parquet tí hon; 5 giây một lần nghĩa là ~17.000 file/ngày/bảng. Đã sửa thành 30s (giảm ~6 lần),
**nhưng nợ cũ vẫn nằm nguyên trên S3** và giờ chính nó chặn công cụ sinh ra để dọn nó.

Đây là vòng luẩn quẩn đáng ghi nhớ: **compaction vừa là thứ cần chạy, vừa là thứ bị chính vấn
đề nó định sửa chặn lại.** Để nợ càng lâu thì càng khó trả.

Nói cho công bằng với S3: cùng sai lầm đó trên MinIO đã từng biểu hiện thành **đầy ổ 397GB làm
chết cả server** (sự cố 28-07). S3 không tạo ra vấn đề — nó chỉ đổi hậu quả từ *thảm hoạ* sang
*chậm và tốn tiền*. Vấn đề luôn là **nhịp commit không kiểm soát**.

---

## Đã loại trừ trong quá trình chẩn đoán

| Giả thuyết | Kết quả |
|---|---|
| Mạng hỏng | ❌ 0% mất gói · RTT 244ms ổn định · boto3 lấy 1000 key trong **2,2s** |
| Thiếu socket timeout | ❌ đặt `socket-timeout-ms=60000` — vô tác dụng, vì nó chỉ áp cho **đọc**, mà luồng kẹt ở **ghi** |
| Kết nối pool chết già vì NAT | ❌ đặt `connection-max-idle-time-ms` + `time-to-live` + `idle-reaper` + `tcp-keep-alive` — vẫn kẹt |
| Câu đếm manifest quá đắt | ✅ **ĐÚNG MỘT PHẦN** — đã sửa, xem dưới |
| `tcp_retries2=15` quá kiên nhẫn | ❌ hạ xuống **8** (~30s thay vì ~15 phút) — **vẫn kẹt sau 20 phút** |

Điểm cuối là quan trọng nhất: `retries2=8` lẽ ra phải giết một socket chết trong 30 giây mà
không giết được. Nghĩa là nó **không phải socket chết** — phía bên kia ngừng đọc (zero-window),
và không có timeout nào ở tầng ứng dụng phá được thế đó.

---

## Đã sửa được (có bằng chứng)

**1. `data_file_count()` — bỏ quét manifest.**
Bản cũ dùng `SELECT count(*) FROM <bảng>.files` **chỉ để in ra log cho đẹp**. Câu đó mở **từng
manifest** để đếm → hàng nghìn request. Job treo ở **Stage 0**, tức chết vì đi *đếm*, chưa hề
bắt đầu *nén*. Đổi sang đọc `summary['total-data-files']` của snapshot = **một** lần đọc
metadata.json. Sau khi sửa, Stage 0 **COMPLETE**.

> Bài học: đừng để phần trang trí chặn phần công việc. Một dòng "cho đẹp log" đã chặn cả job.

**2. Cô lập lỗi theo từng bảng.**
Trước đây một bảng hỏng giết cả lượt chạy, kể cả những bảng đã xong — job này bị treo/giết **7
lần liên tiếp** và chưa bao giờ hoàn thành. Giờ mỗi bảng bọc `try/except`, lỗi thì in ra và đi
tiếp; nhận tên bảng qua `argv` để chạy riêng bảng lớn. Kết quả: `merchants` **exit 0, xong 2/2** —
lần đầu tiên job này chạy trọn kể từ khi viết.

> Khi một thao tác không đáng tin, cách chữa không phải là ép nó đáng tin bằng mọi giá — mà là
> **thu nhỏ đơn vị công việc** để một lần hỏng chỉ tốn một phần.

**3. Timeout cho client boto3.**
Tôi từng sót đúng chỗ này: sửa timeout cho S3FileIO rồi tưởng xong, trong khi
`build_file_list_view()` vẫn gọi S3 bằng boto3 **mặc định**. Có **HAI** đường ra S3, phải cấu
hình cả hai.

---

## Ảnh hưởng thực tế

**Không ảnh hưởng tính đúng đắn.** Dữ liệu vẫn đúng, đối soát vẫn lệch 0, mọi model dbt vẫn
chạy. Iceberg không quan tâm file to hay nhỏ.

**Có ảnh hưởng:**
- **Đọc chậm dần** — mỗi truy vấn Bronze phải mở nhiều file hơn cần thiết
- **Tốn tiền** — mỗi file là một PUT đã trả, và mỗi lần đọc là thêm GET. Kích thước trung bình
  hiện tại ~249KB/object, quá nhỏ so với mức hợp lý (~128MB)
- **`remove_orphan_files` không chạy được trên 2 bảng đó** → rác mồ côi (nếu có) chưa được dọn

Với quy mô dữ liệu test hiện tại thì cả ba đều **chưa gây đau**. Ở quy mô thật thì đây là nợ
phải trả.

---

## Cách sửa thật (PHẦN 2)

**Đưa maintenance về chạy CẠNH DATA.** Cùng đoạn code đó chạy trên một EC2 nhỏ hoặc Glue job
trong `us-east-1` sẽ có RTT ~1ms thay vì 244ms — hàng nghìn request metadata trở nên tầm thường.

Đây không phải giải pháp chắp vá: **đó là lý do AWS bán Glue**, và nó vốn đã nằm trong kế hoạch
PHẦN 2 của project.

Phương án khác đã cân nhắc và **bỏ**: `scripts/reset_all.sh` xoá sạch làm lại. Tốn nửa ngày để
giải quyết một thứ không gây hại, và sẽ gặp lại đúng bức tường đó khi dữ liệu lớn lên.

---

## Bài học rút ra

1. **Nhịp commit của streaming là quyết định kiến trúc, không phải tham số vặt.** 5s vs 30s là
   khác biệt 6 lần về số file, và số file là thứ quyết định maintenance có khả thi hay không.
2. **Compute và storage nên ở cùng một chỗ.** Tách đôi thì mọi thao tác nặng metadata phải trả
   giá bằng độ trễ mạng — nhân với số lượng request, không phải cộng.
3. **Timeout ở tầng ứng dụng không phá được thế kẹt ở tầng TCP.** `SO_TIMEOUT` chỉ áp cho đọc;
   một luồng kẹt ở ghi thì phải sửa ở kernel hoặc ở phía bên kia.
4. **Đo trước khi kết luận.** Nếu dừng ở "S3 chậm quá" thì đã bỏ lỡ nguyên nhân thật: trigger 5s
   của chính mình.


---

## ✅ Đã đóng 06-08 — bằng cách bỏ NGUYÊN NHÂN, không phải chữa TRIỆU CHỨNG

Mr. Senryuu quyết định **quay lại MinIO cho PHẦN 1**, để S3 thật cho PHẦN 2. Không
phải vì S3 tệ, mà vì cấu hình cũ là **compute-ở-nhà + storage-trên-mây** — một thể
lai không nằm trong kế hoạch nào, lấy nhược điểm của cả hai.

### Kết quả đo được

```
bronze.transactions   files  45 ->  4    (nén 45 -> 4)
bronze.accounts       files  45 ->  2
bronze.merchants      files   1 ->  1
silver.transactions   files   8 ->  1
silver.accounts       files   1 ->  1
silver.merchants      files   1 ->  1
DONE — xong 6/6 bảng          exit=0
```

Chạy **trong lúc stream vẫn đang ghi** — không một xung đột commit nào. Vài phút,
thay vì treo vô hạn.

### Vì sao nó xong, trong khi trước đó bất khả thi

| | S3 (cũ) | MinIO (mới) |
|---|---|---|
| Object | **14.404** | **259** |
| Kích thước TB/object | 249 KB | **2.085 KB** |
| Dữ liệu | 683K txn | **2,35M txn** |
| Độ trễ mỗi request | 244ms | ~0 (localhost) |

**Nhiều dữ liệu gấp 3,4 lần nhưng ít hơn 55 lần số file.**

Đó là bằng chứng cho điều issue này đã kết luận: núi 14.404 file **không phải do dữ
liệu nhiều**, mà do `trigger=5s` chạy ba tuần liền. Dựng lại một lượt sạch với
trigger 30s cho ra đúng thứ mà compaction lẽ ra phải tạo — và khi số file hợp lý thì
compaction chạy được kể cả trên link chậm.

### Bài học quan trọng nhất của issue này

Vấn đề **không được sửa** — nó **bị loại bỏ**. Tôi đã dành hàng giờ đuổi theo triệu
chứng (socket timeout, connection lifetime, `tcp_retries2`, quét manifest) và không
cái nào giải quyết được, vì tất cả đều là hệ quả. Chỉ khi bỏ nguyên nhân gốc — số
file — thì triệu chứng biến mất mà không cần chạm tới.

> Khi mọi cách chữa triệu chứng đều thất bại, thường là vì đang chữa nhầm chỗ.

### Vẫn giữ lại từ lần điều tra này

Ba sửa đổi trong `maintenance.py` là thật và có giá trị độc lập, không phụ thuộc
storage backend nào:
1. `data_file_count()` đọc snapshot summary thay vì quét manifest
2. Cô lập lỗi theo từng bảng + nhận `argv` + exit code
3. Timeout cho client boto3

Và kiến thức về **hai đường ra S3** (S3FileIO + boto3) — cấu hình một cái không đủ.

### Còn nợ

Dữ liệu cũ trên bucket S3 (**14.404 object, ~4,6GB**) vẫn còn, chưa nén được, giữ
nguyên cho PHẦN 2. Tốn ~$0,11/tháng. Khi làm PHẦN 2 với compute in-region thì nén
nó là chuyện vài phút.
