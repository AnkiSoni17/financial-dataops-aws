import sys
import uuid
from datetime import datetime, timezone

from awsglue.context import GlueContext  # type: ignore[import]
from awsglue.job import Job  # type: ignore[import]
from awsglue.utils import getResolvedOptions  # type: ignore[import]
from pyspark.context import SparkContext
from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType


# =========================================================
# Shared helper functions
# =========================================================

def clean_currency(column_name: str) -> Column:
    """
    Remove dollar signs, commas, and surrounding spaces.

    Examples:
        "$12,500.50" -> "12500.50"
        " 1,000 "    -> "1000"
    """
    return F.regexp_replace(
        F.regexp_replace(
            F.trim(F.col(column_name)),
            r"\$",
            "",
        ),
        ",",
        "",
    )


def normalise_yes_no(column_name: str) -> Column:
    """
    Convert Yes/No strings to Boolean values.

    Yes -> True
    No  -> False
    Other/null values -> null
    """
    return (
        F.when(
            F.upper(F.trim(F.col(column_name))) == "YES",
            F.lit(True),
        )
        .when(
            F.upper(F.trim(F.col(column_name))) == "NO",
            F.lit(False),
        )
        .otherwise(F.lit(None).cast("boolean"))
    )


# =========================================================
# User transformations
# =========================================================

def clean_users(df: DataFrame) -> DataFrame:
    return (
        df.select(
            F.col("id").cast("long").alias("client_id"),
            F.col("current_age").cast("integer").alias("current_age"),
            F.col("retirement_age")
            .cast("integer")
            .alias("retirement_age"),
            F.col("birth_year").cast("integer").alias("birth_year"),
            F.col("birth_month").cast("integer").alias("birth_month"),
            F.trim(F.col("gender")).alias("gender"),

            # Protect personally identifiable information.
            F.sha2(
                F.col("address").cast("string"),
                256,
            ).alias("address_hash"),

            F.col("latitude").cast("double").alias("latitude"),
            F.col("longitude").cast("double").alias("longitude"),

            clean_currency("per_capita_income")
            .cast(DecimalType(18, 2))
            .alias("per_capita_income"),

            clean_currency("yearly_income")
            .cast(DecimalType(18, 2))
            .alias("yearly_income"),

            clean_currency("total_debt")
            .cast(DecimalType(18, 2))
            .alias("total_debt"),

            F.col("credit_score")
            .cast("integer")
            .alias("credit_score"),

            F.col("num_credit_cards")
            .cast("integer")
            .alias("num_credit_cards"),
        )
        .withColumn(
            "debt_to_income_ratio",
            F.when(
                F.col("yearly_income") > 0,
                F.round(
                    F.col("total_debt") / F.col("yearly_income"),
                    4,
                ),
            ).otherwise(F.lit(None).cast(DecimalType(18, 4))),
        )
        .withColumn(
            "age_group",
            F.when(F.col("current_age").isNull(), "UNKNOWN")
            .when(F.col("current_age") < 25, "UNDER_25")
            .when(F.col("current_age") < 40, "25_TO_39")
            .when(F.col("current_age") < 60, "40_TO_59")
            .otherwise("60_PLUS"),
        )
        .withColumn(
            "credit_score_band",
            F.when(F.col("credit_score").isNull(), "UNKNOWN")
            .when(F.col("credit_score") < 580, "POOR")
            .when(F.col("credit_score") < 670, "FAIR")
            .when(F.col("credit_score") < 740, "GOOD")
            .when(F.col("credit_score") < 800, "VERY_GOOD")
            .otherwise("EXCELLENT"),
        )
    )


# =========================================================
# Card transformations
# =========================================================

def clean_cards(df: DataFrame) -> DataFrame:
    return df.select(
        F.col("id").cast("long").alias("card_id"),
        F.col("client_id").cast("long").alias("client_id"),
        F.trim(F.col("card_brand")).alias("card_brand"),
        F.trim(F.col("card_type")).alias("card_type"),

        # Do not publish the original card number.
        F.sha2(
            F.col("card_number").cast("string"),
            256,
        ).alias("card_number_hash"),

        # Keep only the final four digits.
        F.substring(
            F.col("card_number").cast("string"),
            -4,
            4,
        ).alias("card_last_four"),

        F.to_date(
            F.concat(
                F.lit("01/"),
                F.col("expires"),
            ),
            "dd/MM/yyyy",
        ).alias("expiry_date"),

        normalise_yes_no("has_chip").alias("has_chip"),

        F.col("num_cards_issued")
        .cast("integer")
        .alias("num_cards_issued"),

        clean_currency("credit_limit")
        .cast(DecimalType(18, 2))
        .alias("credit_limit"),

        F.to_date(
            F.concat(
                F.lit("01/"),
                F.col("acct_open_date"),
            ),
            "dd/MM/yyyy",
        ).alias("account_open_date"),

        F.col("year_pin_last_changed")
        .cast("integer")
        .alias("year_pin_last_changed"),

        normalise_yes_no(
            "card_on_dark_web"
        ).alias("card_on_dark_web"),
    )


# =========================================================
# Transaction transformations
# =========================================================

VALID_TRANSACTION_METHODS = [
    "Swipe Transaction",
    "Chip Transaction",
    "Online Transaction",
]


def clean_transactions(df: DataFrame) -> DataFrame:
    return (
        df.select(
            F.col("id").cast("long").alias("transaction_id"),
            F.to_timestamp(F.col("date")).alias(
                "transaction_timestamp"
            ),
            F.col("client_id").cast("long").alias("client_id"),
            F.col("card_id").cast("long").alias("card_id"),

            clean_currency("amount")
            .cast(DecimalType(18, 2))
            .alias("amount"),

            F.trim(F.col("use_chip")).alias(
                "transaction_method"
            ),

            F.col("merchant_id")
            .cast("long")
            .alias("merchant_id"),

            F.trim(F.col("merchant_city")).alias(
                "merchant_city"
            ),

            F.trim(F.col("merchant_state")).alias(
                "merchant_state"
            ),

            F.col("zip").cast("string").alias("merchant_zip"),
            F.col("mcc").cast("integer").alias("mcc"),

            F.trim(F.col("errors")).alias(
                "transaction_errors"
            ),
        )
        .withColumn(
            "transaction_date",
            F.to_date("transaction_timestamp"),
        )
        .withColumn(
            "transaction_year",
            F.year("transaction_timestamp"),
        )
        .withColumn(
            "transaction_month",
            F.month("transaction_timestamp"),
        )
        .withColumn(
            "has_transaction_error",
            F.col("transaction_errors").isNotNull()
            & (F.trim(F.col("transaction_errors")) != ""),
        )
        .withColumn(
            "is_card_present",
            F.col("transaction_method").isin(
                "Swipe Transaction",
                "Chip Transaction",
            ),
        )
    )


def add_transaction_rejection_reason(
    df: DataFrame,
) -> DataFrame:
    return df.withColumn(
        "rejection_reason",
        F.when(
            F.col("transaction_id").isNull(),
            "INVALID_TRANSACTION_ID",
        )
        .when(
            F.col("transaction_timestamp").isNull(),
            "INVALID_TRANSACTION_DATE",
        )
        .when(
            F.col("client_id").isNull(),
            "MISSING_CLIENT_ID",
        )
        .when(
            F.col("card_id").isNull(),
            "MISSING_CARD_ID",
        )
        .when(
            F.col("amount").isNull(),
            "INVALID_AMOUNT",
        )
        .when(
            F.col("mcc").isNull(),
            "MISSING_MCC",
        )
        .when(
            F.col("transaction_method").isNull(),
            "MISSING_TRANSACTION_METHOD",
        )
        .when(
            ~F.col("transaction_method").isin(
                VALID_TRANSACTION_METHODS
            ),
            "INVALID_TRANSACTION_METHOD",
        )
        .otherwise(F.lit(None).cast("string")),
    )


# =========================================================
# Utility functions
# =========================================================

def add_pipeline_metadata(
    df: DataFrame,
    pipeline_run_id: str,
    ingestion_timestamp: datetime,
) -> DataFrame:
    return (
        df.withColumn(
            "pipeline_run_id",
            F.lit(pipeline_run_id),
        )
        .withColumn(
            "ingestion_timestamp",
            F.lit(ingestion_timestamp),
        )
    )


def path_join(base_path: str, child_path: str) -> str:
    """
    Join S3 paths without creating duplicate slashes.
    """
    return (
        f"{base_path.rstrip('/')}/"
        f"{child_path.strip('/')}/"
    )


# =========================================================
# Glue job setup
# =========================================================

args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "RAW_TRANSACTIONS_PATH",
        "RAW_CARDS_PATH",
        "RAW_USERS_PATH",
        "RAW_MCC_PATH",
        "SILVER_BASE_PATH",
        "QUARANTINE_BASE_PATH",
    ],
)

sc = SparkContext.getOrCreate()
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


# =========================================================
# Transactions
# =========================================================

logger.info(
    f"Reading transactions from "
    f"{args['RAW_TRANSACTIONS_PATH']}"
)

raw_transactions = (
    spark.read
    .option("header", True)
    .option("inferSchema", False)
    .csv(args["RAW_TRANSACTIONS_PATH"])
)

validated_transactions = (
    raw_transactions
    .transform(clean_transactions)
    .transform(add_transaction_rejection_reason)
)

valid_transactions = (
    validated_transactions
    .filter(F.col("rejection_reason").isNull())
    .drop("rejection_reason")
    .dropDuplicates(["transaction_id"])
)

valid_transactions = add_pipeline_metadata(
    valid_transactions,
    pipeline_run_id,
    ingestion_timestamp,
)

invalid_transactions = (
    validated_transactions
    .filter(F.col("rejection_reason").isNotNull())
)

invalid_transactions = add_pipeline_metadata(
    invalid_transactions,
    pipeline_run_id,
    ingestion_timestamp,
)

silver_transactions_path = path_join(
    args["SILVER_BASE_PATH"],
    "transactions",
)

quarantine_transactions_path = path_join(
    args["QUARANTINE_BASE_PATH"],
    "transactions",
)

logger.info(
    f"Writing valid transactions to "
    f"{silver_transactions_path}"
)

(
    valid_transactions.write
    .mode("overwrite")
    .partitionBy(
        "transaction_year",
        "transaction_month",
    )
    .option("compression", "snappy")
    .parquet(silver_transactions_path)
)

if invalid_transactions.limit(1).count() > 0:
    logger.info(
        f"Writing invalid transactions to "
        f"{quarantine_transactions_path}"
    )

    (
        invalid_transactions.write
        .mode("overwrite")
        .option("compression", "snappy")
        .parquet(quarantine_transactions_path)
    )


# =========================================================
# Cards
# =========================================================

logger.info(
    f"Reading cards from {args['RAW_CARDS_PATH']}"
)

raw_cards = (
    spark.read
    .option("header", True)
    .option("inferSchema", False)
    .csv(args["RAW_CARDS_PATH"])
)

cards = (
    clean_cards(raw_cards)
    .dropDuplicates(["card_id"]) #error
)

cards = add_pipeline_metadata(
    cards,
    pipeline_run_id,
    ingestion_timestamp,
)

silver_cards_path = path_join(
    args["SILVER_BASE_PATH"],
    "cards",
)

logger.info(
    f"Writing cards to {silver_cards_path}"
)

(
    cards.write
    .mode("overwrite")
    .option("compression", "snappy")
    .parquet(silver_cards_path)
)


# =========================================================
# Users
# =========================================================

logger.info(
    f"Reading users from {args['RAW_USERS_PATH']}"
)

raw_users = (
    spark.read
    .option("header", True)
    .option("inferSchema", False)
    .csv(args["RAW_USERS_PATH"])
)

users = (
    clean_users(raw_users)
    .dropDuplicates(["client_id"])
)

users = add_pipeline_metadata(
    users,
    pipeline_run_id,
    ingestion_timestamp,
)

silver_users_path = path_join(
    args["SILVER_BASE_PATH"],
    "users",
)

logger.info(
    f"Writing users to {silver_users_path}"
)

(
    users.write
    .mode("overwrite")
    .option("compression", "snappy")
    .parquet(silver_users_path)
)


# =========================================================
# MCC codes
# =========================================================

logger.info(
    f"Reading MCC codes from {args['RAW_MCC_PATH']}"
)

raw_mcc = (
    spark.read
    .option("multiLine", True)
    .json(args["RAW_MCC_PATH"])
)

mcc_field_names = [
    field.name
    for field in raw_mcc.schema.fields
]

if not mcc_field_names:
    raise ValueError(
        "MCC JSON contains no discoverable fields."
    )

mcc = (
    raw_mcc
    .select(
        F.explode(
            F.map_from_arrays(
                F.array(
                    *[
                        F.lit(field_name)
                        for field_name in mcc_field_names
                    ]
                ),
                F.array(
                    *[
                        F.col(f"`{field_name}`")
                        for field_name in mcc_field_names
                    ]
                ),
            )
        ).alias(
            "mcc",
            "mcc_description",
        )
    )
    .select(
        F.col("mcc")
        .cast("integer")
        .alias("mcc"),

        F.col("mcc_description")
        .cast("string")
        .alias("mcc_description"),
    )
    .dropDuplicates(["mcc"])
)

mcc = add_pipeline_metadata(
    mcc,
    pipeline_run_id,
    ingestion_timestamp,
)

silver_mcc_path = path_join(
    args["SILVER_BASE_PATH"],
    "mcc",
)

logger.info(
    f"Writing MCC codes to {silver_mcc_path}"
)

(
    mcc.write
    .mode("overwrite")
    .option("compression", "snappy")
    .parquet(silver_mcc_path)
)


# =========================================================
# Complete job
# =========================================================

logger.info(
    f"Raw-to-silver run completed successfully: "
    f"{pipeline_run_id}"
)

job.commit()