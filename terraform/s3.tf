# =============================================================================
# Bucket lakehouse — nơi Iceberg đặt toàn bộ data + metadata.
# =============================================================================

resource "aws_s3_bucket" "lakehouse" {
  bucket = var.bucket_name

  # KHÔNG đặt `force_destroy = true`. Mặc định của Terraform là TỪ CHỐI xoá một
  # bucket còn dữ liệu, và đó là tính năng chứ không phải hạn chế: hạ tầng bị xoá
  # nhầm thì dựng lại trong 30 giây, dữ liệu bị xoá nhầm thì mất vĩnh viễn.
  # Muốn xoá thật thì phải chủ động dọn bucket trước — một bước cố ý làm chậm tay.
}

# Bucket chứa dữ liệu giao dịch ngân hàng (dù là giả). Chặn public ở CẢ BỐN mức:
# ba mức đầu chặn việc GẮN ACL/policy công khai, mức thứ tư vô hiệu hoá luôn các
# ACL công khai đã lỡ tồn tại. Bật đủ bốn thì không có đường nào một dòng cấu hình
# sai làm lộ cả kho.
resource "aws_s3_bucket_public_access_block" "lakehouse" {
  bucket                  = aws_s3_bucket.lakehouse.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Mã hoá lúc nghỉ. SSE-S3 (AES256) chứ không KMS: KMS tính phí THEO REQUEST, mà
# Iceberg sinh ra rất nhiều request nhỏ -> hoá đơn KMS có thể vượt cả tiền lưu
# trữ. Ngân hàng thật sẽ dùng KMS với CMK cho yêu cầu kiểm soát khoá; ở đây ghi
# rõ đánh đổi thay vì âm thầm chọn cái rẻ.
resource "aws_s3_bucket_server_side_encryption_configuration" "lakehouse" {
  bucket = aws_s3_bucket.lakehouse.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
    bucket_key_enabled = true
  }
}

# VERSIONING CỐ Ý TẮT — đây là quyết định đáng giải thích.
# Phản xạ thông thường là "bật versioning cho an toàn". Với Iceberg thì SAI:
#   · File dữ liệu Iceberg là BẤT BIẾN — không bao giờ bị ghi đè, chỉ được tạo mới
#     rồi thôi tham chiếu. Versioning bảo vệ khỏi ghi đè, mà ở đây không có ghi đè.
#   · Khả năng quay ngược thời gian ĐÃ CÓ, ở tầng đúng hơn: snapshot của Iceberg
#     (xem docs/time-travel-audit.md). Versioning của S3 không biết gì về giao dịch.
#   · Nó chỉ nhân đôi chi phí: mọi file bị `expire_snapshots`/`remove_orphan_files`
#     xoá sẽ biến thành noncurrent version và TIẾP TỤC tính tiền — đúng cái cơ chế
#     đã làm phình 397GB, nhưng lần này vô hình khi list bucket.
#
# ⚠️ VÀ VÌ SAO Ở ĐÂY KHÔNG CÓ `aws_s3_bucket_versioning` NÀO CẢ — phát hiện khi test:
#   Cách viết "tường minh" tưởng là đúng:
#       resource "aws_s3_bucket_versioning" { status = "Suspended" }
#   Nhưng **Suspended KHÁC với chưa bao giờ bật**. Bucket ở trạng thái Suspended thì
#   mỗi lần xoá object để lại một DELETE MARKER (`VersionId: null`). Hậu quả đo được:
#     · `list_objects_v2` trả về RỖNG, nhưng `DeleteBucket` vẫn báo `BucketNotEmpty`
#       -> `terraform destroy` chết, đúng cái việc mà Terraform có mặt ở đây để làm
#     · Lakehouse này xoá HÀNG NGHÌN file mỗi lần dọn dẹp -> hàng nghìn marker
#   Bucket chưa từng bật versioning thì xoá là xoá thật, không để lại gì.
#   Nên: KHÔNG khai báo resource này. Đánh đổi phải biết — Terraform sẽ không phát
#   hiện nếu sau này có người bật versioning bằng tay. Chấp nhận được, vì hậu quả
#   của khai báo Suspended (destroy hỏng, marker chồng chất) tệ hơn hẳn.

resource "aws_s3_bucket_lifecycle_configuration" "lakehouse" {
  bucket = aws_s3_bucket.lakehouse.id

  # Phần upload dở của job bị kill giữa chừng. Đây là RÁC VÔ HÌNH: không hiện khi
  # list object nhưng VẪN TÍNH TIỀN. Stream ở đây từng bị kill rất nhiều lần nên
  # gần như chắc chắn có loại rác này.
  rule {
    id     = "abort-incomplete-multipart-1d"
    status = "Enabled"
    # `prefix = ""` (áp cho cả bucket) chứ không phải `filter {}` rỗng: hai cái
    # tương đương trên AWS thật, nhưng LocalStack trả về `Filter: {Prefix: ""}`
    # trong khi provider chờ đúng hình dạng đã khai -> vòng chờ ổn định không bao
    # giờ thoả, apply treo 3 phút rồi timeout dù tài nguyên ĐÃ tạo đúng.
    filter {
      prefix = ""
    }
    abort_incomplete_multipart_upload {
      days_after_initiation = 1
    }
  }

  # Prefix tạm của smoke test — prefix DUY NHẤT an toàn để xoá theo tuổi.
  rule {
    id     = "expire-smoke-test-1d"
    status = "Enabled"
    filter {
      prefix = var.smoke_test_prefix
    }
    expiration {
      days = 1
    }
  }

  # ⚠️ CỐ Ý KHÔNG CÓ RULE HẾT HẠN THEO TUỔI CHO `warehouse/`.
  # Nhìn thì có vẻ tiết kiệm, thực tế là XOÁ DATA: một file parquet ghi từ tháng
  # trước vẫn đang được snapshot hiện tại tham chiếu. S3 lifecycle không đọc được
  # metadata Iceberg nên không thể biết file nào còn dùng — nó chỉ biết tuổi.
  # Việc "file này còn ai tham chiếu không" CHỈ Iceberg trả lời được, và đó chính
  # là `expire_snapshots` + `remove_orphan_files` trong spark/maintenance.py.
  # Đây là ranh giới trách nhiệm quan trọng nhất của cả file này.
}
