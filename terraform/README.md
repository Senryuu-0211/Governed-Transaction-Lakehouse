# Terraform — hạ tầng AWS của lakehouse

## Nó quản cái gì (và cố ý KHÔNG quản cái gì)

| | Ai quản |
|---|---|
| Bucket tồn tại, chặn public access, mã hoá, lifecycle | **Terraform** (thư mục này) |
| IAM user + policy least-privilege | **Terraform** |
| Glue database, Athena workgroup (PHẦN 2) | **Terraform**, sẽ thêm |
| File parquet/metadata **bên trong** bucket | **Iceberg** — `spark/maintenance.py` |

Terraform quản **cái hộp**, không quản **cái bên trong hộp**. Nếu nó đi dọn file dữ
liệu thì mới đáng sợ — vì nó không biết file nào còn được snapshot tham chiếu.

## Vì sao đáng có, khi chỉ có ~6 tài nguyên

Hai lý do, cả hai đều cụ thể:

1. **Policy least-privilege là một biện pháp bảo mật, mà trước đây nó chỉ nằm trong
   database của AWS** — không ai review được, không có lịch sử, sửa nhầm không có
   gì so sánh. Giờ nó là text đi qua CI như mọi code khác.
2. **PHẦN 2 sẽ là `apply` → chụp màn hình → `destroy`.** Đó là cách dùng AWS thật
   mà không nổ ví. Click tay để xoá thì rất dễ **sót một tài nguyên**, mà sót ở AWS
   nghĩa là tiền chảy âm thầm cho tới lúc nhận hoá đơn.

> Terraform ở đây không phải để **tạo** hạ tầng — mà để **biết chắc đã xoá hết**.

⚠️ **Nhưng `destroy` KHÔNG phải giao dịch nguyên tử.** Đo được khi test: một lần
destroy lỗi ở bucket đã kịp xoá xong 5 tài nguyên còn lại. Nghĩa là destroy hỏng
giữa chừng để lại trạng thái **lai** — luôn phải chạy lại và kiểm bằng mắt, đừng
tin một lần chạy.

## Chạy trên LocalStack ($0)

```bash
docker run -d --name gtl-localstack -p 4566:4566 \
  -e SERVICES=s3,iam,sts localstack/localstack:4.0

cp localstack.auto.tfvars.example localstack.auto.tfvars
terraform init
terraform apply           # mặc định use_localstack = true
terraform destroy
```

`use_localstack` mặc định **true** có chủ đích: một lệnh `apply` lỡ tay sẽ **không**
chạm vào AWS thật.

## Lên AWS thật

Bucket `gtl-lakehouse-*` **đã tồn tại** (tạo bằng tay trước khi có thư mục này), nên
đừng `apply` thẳng — nó sẽ báo `BucketAlreadyExists`. Phải **import** trước:

```bash
terraform import -var="use_localstack=false" -var="bucket_name=<tên-thật>" \
  aws_s3_bucket.lakehouse <tên-thật>
terraform import -var="use_localstack=false" -var="bucket_name=<tên-thật>" \
  aws_iam_user.pipeline gtl-lakehouse

terraform plan -var="use_localstack=false" -var="bucket_name=<tên-thật>"
```

`plan` sau khi import chính là thứ đáng xem nhất: nó cho biết **cấu hình đang chạy
thật lệch bao nhiêu so với cấu hình đã khai** — thứ mà trước đây không ai trả lời được.

## Hai giới hạn đã đo được của LocalStack

Đây là lý do phải **chạy thật** chứ không chỉ `terraform validate`. Cả hai chỉ lộ ra
khi `apply`:

**1. Provider AWS `5.100` không apply được `aws_s3_bucket_lifecycle_configuration`
trên LocalStack.** Bản 5.100 thêm một bước chờ-ổn-định sau khi tạo, poll cho tới khi
đọc lại thấy khớp; LocalStack không thoả được, `apply` treo đúng 3 phút rồi timeout —
**dù tài nguyên đã được tạo đúng** (kiểm bằng boto3 thấy đủ 2 rule). Vì vậy provider
được **ghim `~> 5.70.0`**. Đây là ghim vì lý do cụ thể, không phải vì sợ nâng cấp;
nâng lên được khi LocalStack theo kịp.

**2. LocalStack đi sau API của AWS.** Bản 3.8 không trả `TransitionDefaultMinimumObjectSize`
(AWS thêm cuối 2024). Bài học chung: **LocalStack không phải AWS**. Nó tốt để bắt lỗi
cú pháp, quan hệ phụ thuộc và logic policy, nhưng lần `apply` đầu tiên lên AWS thật
vẫn có thể lộ ra thứ mới. Đừng coi "xanh trên LocalStack" là "chắc chắn chạy trên AWS".

## Một phát hiện đáng nhớ: `Suspended` ≠ chưa từng bật versioning

Cách viết tưởng là đúng và tường minh:

```hcl
resource "aws_s3_bucket_versioning" "lakehouse" {
  versioning_configuration { status = "Suspended" }
}
```

Thực tế nó đẩy bucket vào trạng thái **Suspended**, khác hẳn "chưa bao giờ bật". Ở
trạng thái đó, **mỗi lần xoá object để lại một delete marker** (`VersionId: null`).
Hậu quả đo được:

- `list_objects_v2` trả về **rỗng**, nhưng `DeleteBucket` vẫn báo **`BucketNotEmpty`**
  → `terraform destroy` chết, đúng cái việc Terraform có mặt ở đây để làm
- Lakehouse này xoá **hàng nghìn file** mỗi lần `expire_snapshots` → hàng nghìn marker

Nên thư mục này **không khai báo versioning resource nào cả**. Đánh đổi: Terraform sẽ
không phát hiện nếu sau này có người bật versioning bằng tay — chấp nhận được, vì hậu
quả của khai `Suspended` tệ hơn hẳn.

## Vì sao Terraform KHÔNG tạo access key

`aws_iam_access_key` sẽ ghi secret vào **state ở dạng chữ thô**, mà state là một file
trên đĩa — chỉ là **dời chỗ rò rỉ** chứ không giải quyết. Đúng loại lỗi vừa gặp 01-08
khi secret bị Spark in ra `ps aux` vì truyền qua `--conf`.

Tạo key bằng tay rồi điền vào `.env`. Ở production thì đúng nhất là **không có access
key nào cả**: workload chạy trên EC2/EKS lấy credential tạm qua IAM role — và code
hiện tại đã sẵn sàng, vì nó dùng `DefaultCredentialsProvider`.
