variable "aws_region" {
  description = "Region đặt bucket. Đổi region = đổi độ trễ tới compute on-prem (đo thật từ VN tới us-east-1: RTT 249ms)."
  type        = string
  default     = "us-east-1"
}

variable "bucket_name" {
  description = "Tên bucket lakehouse. KHÔNG đặt default: tên bucket là duy nhất toàn cầu, một default vô tình đúng tên người khác là lỗi khó hiểu."
  type        = string

  validation {
    # Bucket có dấu chấm không dùng được virtual-host style với TLS (chứng thư
    # wildcard *.s3.amazonaws.com không phủ nhiều cấp) -> chặn ngay từ đây thay vì
    # để lỗi TLS khó hiểu lộ ra lúc Spark ghi.
    condition     = can(regex("^[a-z0-9][a-z0-9-]{1,61}[a-z0-9]$", var.bucket_name))
    error_message = "Tên bucket chỉ gồm chữ thường, số và dấu gạch ngang (không dấu chấm)."
  }
}

variable "environment" {
  description = "Nhãn môi trường, vào default_tags. Dùng để trả lời 'tài nguyên nào của demo, xoá được?'"
  type        = string
  default     = "dev"
}

variable "use_localstack" {
  description = "true = trỏ mọi endpoint sang LocalStack ($0). Mặc định true để một lệnh `apply` lỡ tay KHÔNG chạm vào AWS thật."
  type        = bool
  default     = true
}

variable "localstack_endpoint" {
  description = "Địa chỉ LocalStack."
  type        = string
  default     = "http://localhost:4566"
}

variable "pipeline_user_name" {
  description = "IAM user mà Spark/boto3 dùng. Least-privilege: chỉ đúng một bucket."
  type        = string
  default     = "gtl-lakehouse"
}

variable "smoke_test_prefix" {
  description = "Prefix cho dữ liệu smoke test — prefix DUY NHẤT được phép xoá theo tuổi (xem lifecycle trong s3.tf)."
  type        = string
  default     = "_smoke/"
}
