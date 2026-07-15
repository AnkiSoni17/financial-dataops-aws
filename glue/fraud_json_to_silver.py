import sys
import uuid
from datetime import datetime, timezone

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F


# ---------------------------------------------------------
# Glue job arguments
# ---------------------------------------------------------

args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "RAW_FRAUD_LABELS_PATH",
        "SILVER_FRAUD_PATH",
    ],
)


# ---------------------------------------------------------
# Spark and Glue setup
# ---------------------------------------------------------

sc = SparkContext.getOrCreate()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
logger = glue_context.get_logger()

job = Job(glue_context)
job.init(args["JOB_NAME"], args)

pipeline_run_id = str(uuid.uuid4())
ingestion_timestamp = datetime.now(timezone.utc)


# ---------------------------------------------------------
# Read fraud-label JSON
# ---------------------------------------------------------

logger.info(
    f"Reading fraud labels from "
    f"{args['RAW_FRAUD_LABELS_PATH']}"
)

raw_fraud_text = (
    spark.read
    .option("wholetext", True)
    .text(args["RAW_FRAUD_LABELS_PATH"])
)


# ---------------------------------------------------------
# Parse JSON and create rows
# ---------------------------------------------------------

fraud_labels = (
    raw_fraud_text
    .select(
        F.from_json(
            F.col("value"),
            "struct<target:map<string,string>>",
        ).alias("json_data")
    )
    .filter(F.col("json_data").isNotNull())
    .select(
        F.explode(
            F.col("json_data.target")
        ).alias(
            "transaction_id",
            "fraud_label",
        )
    )
    .select(
        F.col("transaction_id")
        .cast("long")
        .alias("transaction_id"),

        F.when(
            F.upper(
                F.trim(F.col("fraud_label"))
            ) == "YES",
            F.lit(True),
        )
        .when(
            F.upper(
                F.trim(F.col("fraud_label"))
            ) == "NO",
            F.lit(False),
        )
        .otherwise(
            F.lit(None).cast("boolean")
        )
        .alias("is_fraud"),
    )
    .filter(F.col("transaction_id").isNotNull())
    .dropDuplicates(["transaction_id"])
    .withColumn(
        "pipeline_run_id",
        F.lit(pipeline_run_id),
    )
    .withColumn(
        "ingestion_timestamp",
        F.lit(ingestion_timestamp),
    )
)


# ---------------------------------------------------------
# Validate output
# ---------------------------------------------------------

if fraud_labels.limit(1).count() == 0:
    raise ValueError(
        "No fraud-label records were produced. "
        "Check the JSON structure and input path."
    )


# ---------------------------------------------------------
# Write Silver Parquet
# ---------------------------------------------------------

logger.info(
    f"Writing fraud labels to "
    f"{args['SILVER_FRAUD_PATH']}"
)

(
    fraud_labels.write
    .mode("overwrite")
    .option("compression", "snappy")
    .parquet(args["SILVER_FRAUD_PATH"])
)

logger.info(
    f"Fraud-label transformation completed successfully: "
    f"{pipeline_run_id}"
)

job.commit()