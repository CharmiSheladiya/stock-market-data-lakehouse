output "s3_bucket_name" {
  description = "Lakehouse S3 bucket name"
  value       = aws_s3_bucket.lakehouse.bucket
}

output "s3_bucket_arn" {
  description = "Lakehouse S3 bucket ARN"
  value       = aws_s3_bucket.lakehouse.arn
}

output "pipeline_role_arn" {
  description = "IAM role ARN for pipeline execution"
  value       = aws_iam_role.pipeline_role.arn
}

output "glue_database_name" {
  description = "Glue catalog database name"
  value       = aws_glue_catalog_database.lakehouse.name
}
