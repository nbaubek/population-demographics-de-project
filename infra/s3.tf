resource "aws_s3_bucket" "iceberg_data" {
  bucket = "${var.bucket_name}-iceberg"

  tags = {
    Project     = "population-demographics"
    Environment = "dev"
    ManagedBy   = "terraform"
  }
}

resource "aws_s3_bucket_versioning" "iceberg_data" {
  bucket = aws_s3_bucket.iceberg_data.id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "iceberg_data" {
  bucket = aws_s3_bucket.iceberg_data.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "iceberg_data" {
  bucket = aws_s3_bucket.iceberg_data.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

output "iceberg_bucket_name" {
  description = "Name of the Iceberg S3 bucket"
  value       = aws_s3_bucket.iceberg_data.id
}

output "iceberg_bucket_arn" {
  description = "ARN of the Iceberg S3 bucket"
  value       = aws_s3_bucket.iceberg_data.arn
}