# =============================================================================
# IAM cho pipeline — least-privilege, phạm vi ĐÚNG MỘT bucket.
#
# Đây là lý do quan trọng nhất khiến file Terraform này đáng tồn tại: policy dưới
# đây là một BIỆN PHÁP BẢO MẬT, nhưng trước khi có file này nó chỉ nằm trong
# database của AWS — không ai review được, không có lịch sử thay đổi, sửa nhầm
# không có gì để so sánh. Giờ nó là text đi qua CI như mọi code khác.
#
# Phạm vi hẹp này đã được kiểm chứng ngoài ý muốn: 01-08 tôi thử đọc chi phí AWS
# từ credential của pipeline và bị từ chối `ce:GetCostAndUsage`. Đúng như thiết kế.
# =============================================================================

resource "aws_iam_user" "pipeline" {
  name = var.pipeline_user_name
  path = "/service/"
}

data "aws_iam_policy_document" "lakehouse_access" {
  # Quyền cấp BUCKET. Tách khỏi quyền cấp OBJECT vì chúng áp lên ARN khác nhau —
  # gộp làm một là hoặc thừa quyền, hoặc thiếu quyền một cách khó hiểu.
  statement {
    sid = "BucketLevel"
    actions = [
      "s3:ListBucket",        # Iceberg + boto3 liệt kê file (dọn mồ côi)
      "s3:GetBucketLocation", # SDK hỏi region trước khi ký request
      "s3:ListBucketMultipartUploads",
    ]
    resources = [aws_s3_bucket.lakehouse.arn]
  }

  statement {
    sid = "ObjectLevel"
    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject", # expire_snapshots / remove_orphan_files cần
      "s3:AbortMultipartUpload",
      "s3:ListMultipartUploadParts",
    ]
    resources = ["${aws_s3_bucket.lakehouse.arn}/*"]
  }

  # CỐ Ý KHÔNG CẤP:
  #   · s3:DeleteBucket        — không job nào có lý do xoá cả kho
  #   · s3:PutBucketPolicy     — không cho tự nới quyền chính mình
  #   · s3:PutBucketVersioning / PutLifecycleConfiguration — chính sách vòng đời là
  #     việc của Terraform, không phải của job đang chạy
  #   · ce:* / budgets:*       — đọc hoá đơn không phải việc của pipeline
  #   · s3:* trên bucket khác  — bán kính thiệt hại dừng ở đúng một bucket
}

resource "aws_iam_user_policy" "lakehouse_access" {
  name   = "gtl-lakehouse-access"
  user   = aws_iam_user.pipeline.name
  policy = data.aws_iam_policy_document.lakehouse_access.json
}

# KHÔNG tạo `aws_iam_access_key` bằng Terraform, CÓ CHỦ ĐÍCH.
# Terraform sẽ ghi secret key vào state ở dạng CHỮ THÔ, và state cục bộ là một file
# trên đĩa. Như vậy chỉ là dời chỗ rò rỉ chứ không giải quyết — đúng loại lỗi vừa
# gặp 01-08, khi secret bị Spark in ra `ps aux` vì truyền qua `--conf`.
# Access key tạo bằng tay trong console (hoặc `aws iam create-access-key`) rồi điền
# thẳng vào `.env`. Ở production thì đúng nhất là KHÔNG có access key nào cả:
# workload chạy trên EC2/EKS lấy credential tạm qua IAM role, và code hiện tại đã
# sẵn sàng cho điều đó vì nó dùng DefaultCredentialsProvider.
