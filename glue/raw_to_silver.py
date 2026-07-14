import sys
import uuid
from datetime import datetime, timezone

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F

from src.card_transformations import clean_cards
from src.transaction_transformations import (
    add_transaction_rejection_reason,
    clean_transactions,
)
from src.user_transformations import clean_users


args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "RAW_TRANSACTIONS_PATH",
        "RAW_CARDS_PATH",
        "RAW_USERS_PATH",
        "RAW_MCC_PATH",
        "RAW_FRAUD_LABELS_PATH",
        "SILVER_BASE_PATH",
        "QUARANTINE_BASE_PATH",
    ],
)

sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
logger = glue_context.get_logger()

job = Job(glue_context)
job.init(args["JOB_NAME"], args)

pipeline_run_id = str(uuid.uuid4())
ingestion_timestamp = datetime.now(timezone.utc)

logger.info(
    f"Starting raw-to-silver run: {pipeline_run_id}"
)

# ---------------------------------------------------------
# Transactions
# ---------------------------------------------------------

raw_transactions = (
    spark.read
    .option("header", True)
    .option("inferSchema", False)
    .csv(args["RAW_TRANSACTIONS_PATH"])
)

transactions = clean_transactions(raw_transactions)
validated_transactions = add_transaction_rejection_reason(
    transactions
)

valid_transactions = (
    validated_transactions
    .filter(F.col("rejection_reason").isNull())
    .drop("rejection_reason")
    .dropDuplicates(["transaction_id"])
    .withColumn("pipeline_run_id", F.lit(pipeline_run_id))
    .withColumn(
        "ingestion_timestamp",
        F.lit(ingestion_timestamp),
    )
)

invalid_transactions = (
    validated_transactions
    .filter(F.col("rejection_reason").isNotNull())
    .withColumn("pipeline_run_id", F.lit(pipeline_run_id))
    .withColumn(
        "ingestion_timestamp",
        F.lit(ingestion_timestamp),
    )
)

(
    valid_transactions.write
    .mode("overwrite")
    .partitionBy("transaction_year", "transaction_month")
    .option("compression", "snappy")
    .parquet(
        f"{args['SILVER_BASE_PATH']}/transactions/"
    )
)

if invalid_transactions.limit(1).count() > 0:
    (
        invalid_transactions.write
        .mode("overwrite")
        .option("compression", "snappy")
        .parquet(
            f"{args['QUARANTINE_BASE_PATH']}/transactions/"
        )
    )

# ---------------------------------------------------------
# Cards
# ---------------------------------------------------------

raw_cards = (
    spark.read
    .option("header", True)
    .option("inferSchema", False)
    .csv(args["RAW_CARDS_PATH"])
)

cards = (
    clean_cards(raw_cards)
    .dropDuplicates(["card_id"])
    .withColumn("pipeline_run_id", F.lit(pipeline_run_id))
    .withColumn(
        "ingestion_timestamp",
        F.lit(ingestion_timestamp),
    )
)

(
    cards.write
    .mode("overwrite")
    .option("compression", "snappy")
    .parquet(
        f"{args['SILVER_BASE_PATH']}/cards/"
    )
)

# ---------------------------------------------------------
# Users
# ---------------------------------------------------------

raw_users = (
    spark.read
    .option("header", True)
    .option("inferSchema", False)
    .csv(args["RAW_USERS_PATH"])
)

users = (
    clean_users(raw_users)
    .dropDuplicates(["client_id"])
    .withColumn("pipeline_run_id", F.lit(pipeline_run_id))
    .withColumn(
        "ingestion_timestamp",
        F.lit(ingestion_timestamp),
    )
)

(
    users.write
    .mode("overwrite")
    .option("compression", "snappy")
    .parquet(
        f"{args['SILVER_BASE_PATH']}/users/"
    )
)

# ---------------------------------------------------------
# MCC codes
# ---------------------------------------------------------

raw_mcc = spark.read.option("multiLine", True).json(
    args["RAW_MCC_PATH"]
)

# mcc_codes.json is commonly represented as a JSON object:
# {"5812": "Eating Places, Restaurants", ...}
#
# Convert all object properties into key/value rows.

mcc_map = raw_mcc.select(
    F.explode(
        F.map_from_arrays(
            F.array(
                *[
                    F.lit(field.name)
                    for field in raw_mcc.schema.fields
                ]
            ),
            F.array(
                *[
                    F.col(f"`{field.name}`")
                    for field in raw_mcc.schema.fields
                ]
            ),
        )
    ).alias("mcc", "mcc_description")
)

mcc = mcc_map.select(
    F.col("mcc").cast("integer").alias("mcc"),
    F.col("mcc_description").cast("string"),
)

(
    mcc.write
    .mode("overwrite")
    .option("compression", "snappy")
    .parquet(
        f"{args['SILVER_BASE_PATH']}/mcc/"
    )
)

# ---------------------------------------------------------
# Fraud labels
# ---------------------------------------------------------

raw_fraud_labels = (
    spark.read
    .option("multiLine", True)
    .json(args["RAW_FRAUD_LABELS_PATH"])
)

fraud_fields = raw_fraud_labels.schema["target"].dataType.fields

fraud_labels = (
    raw_fraud_labels
    .select(
        F.explode(
            F.map_from_arrays(
                F.array(
                    *[
                        F.lit(field.name)
                        for field in fraud_fields
                    ]
                ),
                F.array(
                    *[
                        F.col(f"target.`{field.name}`")
                        for field in fraud_fields
                    ]
                ),
            )
        ).alias("transaction_id", "fraud_label")
    )
    .select(
        F.col("transaction_id")
        .cast("long")
        .alias("transaction_id"),

        F.when(
            F.upper(F.col("fraud_label")) == "YES",
            F.lit(True),
        )
        .when(
            F.upper(F.col("fraud_label")) == "NO",
            F.lit(False),
        )
        .otherwise(F.lit(None))
        .alias("is_fraud"),
    )
)

(
    fraud_labels.write
    .mode("overwrite")
    .option("compression", "snappy")
    .parquet(
        f"{args['SILVER_BASE_PATH']}/fraud-labels/"
    )
)

logger.info(
    f"Raw-to-silver run completed: {pipeline_run_id}"
)

job.commit()