resource "aws_glue_crawler" "bronze" {
  name          = "${var.project_name}-${var.environment}-bronze-crawler"
  role          = aws_iam_role.glue_role.arn
  database_name = aws_glue_catalog_database.financial.name

  s3_target {
    path = "s3://${aws_s3_bucket.data_lake.bucket}/bronze/cards/"
  }

  s3_target {
    path = "s3://${aws_s3_bucket.data_lake.bucket}/bronze/users/"
  }

  s3_target {
    path = "s3://${aws_s3_bucket.data_lake.bucket}/bronze/transactions/"
  }

  table_prefix = "bronze_"

  schema_change_policy {
    update_behavior = "UPDATE_IN_DATABASE"
    delete_behavior = "LOG"
  }

  tags = {
    Project     = var.project_name
    Environment = var.environment
    Layer       = "bronze"
  }
}

resource "aws_glue_crawler" "silver" {
  name          = "${var.project_name}-${var.environment}-silver-crawler"
  role          = aws_iam_role.glue_role.arn
  database_name = aws_glue_catalog_database.financial.name

  s3_target {
    path = "s3://${aws_s3_bucket.data_lake.bucket}/silver/cards/"
  }

  s3_target {
    path = "s3://${aws_s3_bucket.data_lake.bucket}/silver/users/"
  }

  s3_target {
    path = "s3://${aws_s3_bucket.data_lake.bucket}/silver/transactions/"
  }

  s3_target {
    path = "s3://${aws_s3_bucket.data_lake.bucket}/silver/mcc/"
  }
  s3_target {
    path = "s3://${aws_s3_bucket.data_lake.bucket}/silver/fraud-labels/"
  }

  table_prefix = "silver_"

  schema_change_policy {
    update_behavior = "UPDATE_IN_DATABASE"
    delete_behavior = "LOG"
  }

  tags = {
    Project     = var.project_name
    Environment = var.environment
    Layer       = "silver"
  }
}