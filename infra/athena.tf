resource "aws_s3_bucket" "athena_results" {
  bucket = "${var.bucket_name}-athena-results"

  tags = {
    Project     = "population-demographics"
    Environment = "dev"
    ManagedBy   = "terraform"
  }
}

resource "aws_s3_bucket_public_access_block" "athena_results" {
  bucket = aws_s3_bucket.athena_results.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_athena_workgroup" "main" {
  name        = "population-demographics"
  description = "Workgroup for ACS socioeconomic modeling"

  configuration {
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = false
    bytes_scanned_cutoff_per_query      = 1073741824  # 1GB limit per query — prevents runaway scans

    result_configuration {
      output_location = "s3://${aws_s3_bucket.athena_results.bucket}/results/"

      encryption_configuration {
        encryption_option = "SSE_S3"
      }
    }

    engine_version {
      selected_engine_version = "Athena engine version 3"
    }
  }

  tags = {
    Project     = "population-demographics"
    Environment = "dev"
    ManagedBy   = "terraform"
  }
}

output "athena_workgroup_name" {
  value = aws_athena_workgroup.main.name
}

output "athena_results_bucket" {
  value = aws_s3_bucket.athena_results.bucket
}