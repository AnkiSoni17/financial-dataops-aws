from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

from src.common import clean_currency


VALID_TRANSACTION_METHODS = [
    "Swipe Transaction",
    "Chip Transaction",
    "Online Transaction",
]


def clean_transactions(df: DataFrame) -> DataFrame:
    return (
        df
        .select(
            F.col("id").cast("long").alias("transaction_id"),
            F.to_timestamp("date").alias("transaction_timestamp"),
            F.col("client_id").cast("long").alias("client_id"),
            F.col("card_id").cast("long").alias("card_id"),
            clean_currency("amount")
            .cast(DecimalType(18, 2))
            .alias("amount"),
            F.trim(F.col("use_chip")).alias("transaction_method"),
            F.col("merchant_id").cast("long").alias("merchant_id"),
            F.trim(F.col("merchant_city")).alias("merchant_city"),
            F.trim(F.col("merchant_state")).alias("merchant_state"),
            F.col("zip").cast("string").alias("merchant_zip"),
            F.col("mcc").cast("integer").alias("mcc"),
            F.trim(F.col("errors")).alias("transaction_errors"),
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


def add_transaction_rejection_reason(df: DataFrame) -> DataFrame:
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
            ~F.col("transaction_method").isin(
                VALID_TRANSACTION_METHODS
            ),
            "INVALID_TRANSACTION_METHOD",
        ),
    )