output "bucket_name" {
  description = "Tên bucket lakehouse — điền vào S3_BUCKET trong .env."
  value       = aws_s3_bucket.lakehouse.id
}

output "bucket_arn" {
  value = aws_s3_bucket.lakehouse.arn
}

output "warehouse_uri" {
  description = "Giá trị cho spark.sql.catalog.gtl.warehouse."
  value       = "s3://${aws_s3_bucket.lakehouse.id}/warehouse/"
}

output "pipeline_user_arn" {
  description = "IAM user mà Spark/boto3 dùng. Access key KHÔNG do Terraform tạo — xem chú thích cuối iam.tf."
  value       = aws_iam_user.pipeline.arn
}
