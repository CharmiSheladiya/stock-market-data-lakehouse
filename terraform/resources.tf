# ------------------------------------------------------------------ #
#  S3 Bucket — Lakehouse storage (Bronze / Silver / Gold layers)
# ------------------------------------------------------------------ #
resource "aws_s3_bucket" "lakehouse" {
  bucket = "${var.project_name}-${var.environment}"
}

resource "aws_s3_bucket_versioning" "lakehouse" {
  bucket = aws_s3_bucket.lakehouse.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "lakehouse" {
  bucket = aws_s3_bucket.lakehouse.id

  rule {
    id     = "archive-raw-data"
    status = "Enabled"

    filter {
      prefix = "raw/"
    }

    transition {
      days          = 90
      storage_class = "STANDARD_IA"
    }

    transition {
      days          = 365
      storage_class = "GLACIER"
    }
  }

  rule {
    id     = "cleanup-temp"
    status = "Enabled"

    filter {
      prefix = "tmp/"
    }

    expiration {
      days = 7
    }
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "lakehouse" {
  bucket = aws_s3_bucket.lakehouse.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "lakehouse" {
  bucket = aws_s3_bucket.lakehouse.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# ------------------------------------------------------------------ #
#  IAM Role for pipeline (EC2 / ECS / Lambda)
# ------------------------------------------------------------------ #
resource "aws_iam_role" "pipeline_role" {
  name = "${var.project_name}-pipeline-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = ["ecs-tasks.amazonaws.com", "lambda.amazonaws.com"]
        }
      }
    ]
  })
}

resource "aws_iam_role_policy" "pipeline_s3_access" {
  name = "${var.project_name}-s3-access"
  role = aws_iam_role.pipeline_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:ListBucket",
          "s3:DeleteObject"
        ]
        Resource = [
          aws_s3_bucket.lakehouse.arn,
          "${aws_s3_bucket.lakehouse.arn}/*"
        ]
      }
    ]
  })
}

# ------------------------------------------------------------------ #
#  Glue Catalog — for Athena/Spark SQL queries on Delta tables
# ------------------------------------------------------------------ #
resource "aws_glue_catalog_database" "lakehouse" {
  name        = replace(var.project_name, "-", "_")
  description = "Stock market lakehouse catalog"
}

# ------------------------------------------------------------------ #
#  CloudWatch Log Group
# ------------------------------------------------------------------ #
resource "aws_cloudwatch_log_group" "pipeline_logs" {
  name              = "/pipeline/${var.project_name}"
  retention_in_days = 30
}
