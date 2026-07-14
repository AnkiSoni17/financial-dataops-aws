resource "aws_s3_object" "raw_to_silver_script" {
  bucket = aws_s3_bucket.data_lake.id
  key    = "scripts/raw_to_silver.py"
  source = "${path.module}/../glue/raw_to_silver.py"
  etag   = filemd5("${path.module}/../glue/raw_to_silver.py")
}

resource "aws_s3_object" "transformation_library" {
  bucket = aws_s3_bucket.data_lake.id
  key    = "scripts/financial_transformations.zip"
  source = "${path.module}/../build/financial_transformations.zip"
  etag = filemd5(
    "${path.module}/../build/financial_transformations.zip"
  )
}

resource "aws_glue_job" "raw_to_silver" {
  name     = "${var.project_name}-${var.environment}-raw-to-silver"
  role_arn = aws_iam_role.glue_role.arn

  glue_version      = "5.0"
  worker_type       = "G.1X"
  number_of_workers = 5
  timeout           = 60
  max_retries       = 0

  command {
    name            = "glueetl"
    python_version  = "3"
    script_location = "s3://${aws_s3_bucket.data_lake.id}/${aws_s3_object.raw_to_silver_script.key}"
  }

  default_arguments = {
    "--RAW_TRANSACTIONS_PATH" = "s3://${aws_s3_bucket.data_lake.id}/bronze/transactions/"
    "--RAW_CARDS_PATH"        = "s3://${aws_s3_bucket.data_lake.id}/bronze/cards/"
    "--RAW_USERS_PATH"        = "s3://${aws_s3_bucket.data_lake.id}/bronze/users/"
    "--RAW_MCC_PATH"          = "s3://${aws_s3_bucket.data_lake.id}/bronze/mcc/"
    "--RAW_FRAUD_LABELS_PATH" = "s3://${aws_s3_bucket.data_lake.id}/bronze/fraud-labels/"
    "--SILVER_BASE_PATH"      = "s3://${aws_s3_bucket.data_lake.id}/silver"
    "--QUARANTINE_BASE_PATH"  = "s3://${aws_s3_bucket.data_lake.id}/quarantine"

    "--extra-py-files" = "s3://${aws_s3_bucket.data_lake.id}/scripts/financial_transformations.zip"

    "--enable-metrics"                   = "true"
    "--enable-observability-metrics"     = "true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-spark-ui"                  = "true"
    "--spark-event-logs-path"            = "s3://${aws_s3_bucket.data_lake.id}/temp/spark-logs/"
    "--job-language"                     = "python"
  }

  execution_property {
    max_concurrent_runs = 1
  }

  depends_on = [
    aws_s3_object.raw_to_silver_script,
    aws_s3_object.transformation_library,
    aws_iam_role_policy_attachment.glue_service_role,
    aws_iam_role_policy_attachment.glue_permissions
  ]
}