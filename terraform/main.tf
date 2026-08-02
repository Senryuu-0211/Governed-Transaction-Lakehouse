# =============================================================================
# Hạ tầng AWS của Governed Transaction Lakehouse.
#
# ⭐ TERRAFORM QUẢN CÁI HỘP, KHÔNG QUẢN CÁI BÊN TRONG HỘP.
#   Ở đây khai báo: bucket có tồn tại không, chặn public access, lifecycle rule,
#   IAM user và policy. KHÔNG khai báo file parquet/metadata bên trong bucket —
#   đó là việc của Iceberg (`spark/maintenance.py`: compaction, expire_snapshots,
#   remove_orphan_files). Nếu Terraform mà dọn file dữ liệu thì mới đáng sợ.
#
# ⭐ VÌ SAO CÓ FILE NÀY KHI CHỈ CÓ ~4 TÀI NGUYÊN
#   1. Policy least-privilege của IAM user hiện CHỈ tồn tại trong database của AWS:
#      không ai review được, không có lịch sử thay đổi, sửa nhầm không có gì so
#      sánh. Nó là một BIỆN PHÁP BẢO MẬT mà lại không nằm trong repo.
#   2. PHẦN 2 (Glue + Athena) sẽ là `apply` -> chụp màn hình -> `destroy`. Đó là
#      cách dùng AWS thật mà không nổ ví. Click tay để xoá thì rất dễ SÓT một tài
#      nguyên, mà sót ở AWS nghĩa là tiền chảy âm thầm tới lúc nhận hoá đơn.
#      `terraform destroy` xoá ĐÚNG những gì `apply` đã tạo, và nói trước nó sắp
#      xoá gì.
#   Nói gọn: Terraform ở đây không phải để TẠO hạ tầng — mà để BIẾT CHẮC đã xoá hết.
#
# Xem `terraform/README.md` để biết cách chạy trên LocalStack ($0) và cách
# `import` bucket đang tồn tại (nó được tạo bằng tay trước khi có file này).
# =============================================================================

terraform {
  required_version = ">= 1.5"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.70.0"
    }
  }

  # BACKEND CỤC BỘ, CÓ CHỦ ĐÍCH.
  # Backend S3 là chuẩn cho nhiều người, nhưng ở đây nó tạo vòng lặp con-gà-quả-
  # trứng: bucket giữ state lại chính là thứ file này tạo ra. Production giải bằng
  # một bucket state RIÊNG dựng trước (thường bằng tay hoặc một stack bootstrap).
  # Với một người vận hành thì state cục bộ là đúng — và `terraform.tfstate` đã
  # nằm trong .gitignore vì nó CHỨA giá trị nhạy cảm ở dạng chữ thô.
  backend "local" {}
}

provider "aws" {
  region = var.aws_region

  # --- Chuyển hướng sang LocalStack khi bật cờ ------------------------------
  # Cùng một đoạn code chạy được ở hai nơi: LocalStack ($0, để phát triển và cho
  # CI) và AWS thật. Không phân nhánh module, không copy file — đúng tinh thần
  # "đổi endpoint là lên cloud" mà tầng application đã chứng minh khi bỏ MinIO.
  dynamic "endpoints" {
    for_each = var.use_localstack ? [1] : []
    content {
      s3  = var.localstack_endpoint
      iam = var.localstack_endpoint
      sts = var.localstack_endpoint
    }
  }

  # LocalStack không kiểm chữ ký thật -> credential giả là đủ, và phải giả để
  # không có đường nào lỡ tay đụng vào AWS thật khi đang test.
  access_key                  = var.use_localstack ? "test" : null
  secret_key                  = var.use_localstack ? "test" : null
  skip_credentials_validation = var.use_localstack
  skip_requesting_account_id  = var.use_localstack
  skip_metadata_api_check     = var.use_localstack

  # S3 thật dùng virtual-host style; LocalStack cần path-style. Cùng một khác
  # biệt đã gặp ở tầng application khi chuyển từ MinIO sang S3.
  s3_use_path_style = var.use_localstack

  default_tags {
    tags = {
      Project   = "governed-transaction-lakehouse"
      ManagedBy = "terraform"
      # Tag này để trả lời được câu "tài nguyên nào là của demo, xoá được?" —
      # chính là câu mà lúc dọn dẹp PHẦN 2 sẽ cần.
      Environment = var.environment
    }
  }
}
