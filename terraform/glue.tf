resource "aws_s3_object" "raw_to_silver_script" {
  bucket = aws_s3_bucket.data_lake.id
  key    = "scripts/raw_to_silver.py"
  source = "${path.module}/../glue/raw_to_silver.py"
  etag   = filemd5("${path.module}/../glue/raw_to_silver.py")
}

resource "aws_s3_object" "fraud_json_to_silver_script" {
  bucket = aws_s3_bucket.data_lake.id
  key    = "scripts/fraud_json_to_silver.py"
  source = "${path.module}/../glue/fraud_json_to_silver.py"
  etag   = filemd5("${path.module}/../glue/fraud_json_to_silver.py")
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
    script_location = "s3://${aws_s3_bucket.data_lake.bucket}/${aws_s3_object.raw_to_silver_script.key}"
  }

  default_arguments = {
    "--RAW_TRANSACTIONS_PATH" = "s3://${aws_s3_bucket.data_lake.bucket}/bronze/transactions/"
    "--RAW_CARDS_PATH"        = "s3://${aws_s3_bucket.data_lake.bucket}/bronze/cards/"
    "--RAW_USERS_PATH"        = "s3://${aws_s3_bucket.data_lake.bucket}/bronze/users/"
    "--RAW_MCC_PATH"          = "s3://${aws_s3_bucket.data_lake.bucket}/bronze/mcc_codes/"

    "--SILVER_BASE_PATH"     = "s3://${aws_s3_bucket.data_lake.bucket}/silver/"
    "--QUARANTINE_BASE_PATH" = "s3://${aws_s3_bucket.data_lake.bucket}/quarantine/"

    "--enable-metrics"                   = "true"
    "--enable-observability-metrics"     = "true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-spark-ui"                  = "true"
    "--spark-event-logs-path"            = "s3://${aws_s3_bucket.data_lake.bucket}/temp/spark-logs/"
    "--job-language"                     = "python"
  }

  execution_property {
    max_concurrent_runs = 1
  }

  depends_on = [
    aws_s3_object.raw_to_silver_script,
    aws_iam_role_policy_attachment.glue_service_role,
    aws_iam_role_policy_attachment.glue_permissions
  ]
}

resource "aws_glue_job" "fraud_json_to_silver" {
  name     = "${var.project_name}-${var.environment}-fraud-json-to-silver"
  role_arn = aws_iam_role.glue_role.arn

  glue_version      = "5.0"
  worker_type       = "G.1X"
  number_of_workers = 2
  timeout           = 30
  max_retries       = 0

  command {
    name            = "glueetl"
    python_version  = "3"
    script_location = "s3://${aws_s3_bucket.data_lake.bucket}/${aws_s3_object.fraud_json_to_silver_script.key}"
  }

  default_arguments = {
    "--RAW_FRAUD_LABELS_PATH" = "s3://${aws_s3_bucket.data_lake.bucket}/bronze/train_fraud_labels/"
    "--SILVER_FRAUD_PATH"     = "s3://${aws_s3_bucket.data_lake.bucket}/silver/fraud-labels/"
    "--TempDir"               = "s3://${aws_s3_bucket.data_lake.bucket}/temp/"

    "--enable-metrics"                   = "true"
    "--enable-observability-metrics"     = "true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--job-language"                     = "python"
  }

  execution_property {
    max_concurrent_runs = 1
  }

  depends_on = [
    aws_s3_object.fraud_json_to_silver_script,
    aws_iam_role_policy_attachment.glue_service_role,
    aws_iam_role_policy_attachment.glue_permissions
  ]
}