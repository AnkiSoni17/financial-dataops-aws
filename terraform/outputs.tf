output "data_lake_bucket_name" {
  description = "Name of the financial data lake S3 bucket"
  value       = aws_s3_bucket.data_lake.bucket
}

output "glue_database_name" {
  description = "Name of the Glue Data Catalog database"
  value       = aws_glue_catalog_database.financial.name
}

output "glue_role_arn" {
  description = "IAM role ARN used by AWS Glue"
  value       = aws_iam_role.glue_role.arn
}