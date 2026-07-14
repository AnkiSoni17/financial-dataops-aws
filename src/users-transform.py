from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

from src.common import clean_currency


def clean_users(df: DataFrame) -> DataFrame:
    return (
        df
        .select(
            F.col("id").cast("long").alias("client_id"),
            F.col("current_age").cast("integer").alias("current_age"),
            F.col("retirement_age").cast("integer").alias(
                "retirement_age"
            ),
            F.col("birth_year").cast("integer").alias("birth_year"),
            F.col("birth_month").cast("integer").alias("birth_month"),
            F.trim(F.col("gender")).alias("gender"),

            # Avoid publishing the full raw address.
            F.sha2(
                F.col("address"),
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
                    F.col("total_debt")
                    / F.col("yearly_income"),
                    4,
                ),
            ),
        )
        .withColumn(
            "age_group",
            F.when(F.col("current_age") < 25, "UNDER_25")
            .when(F.col("current_age") < 40, "25_TO_39")
            .when(F.col("current_age") < 60, "40_TO_59")
            .otherwise("60_PLUS"),
        )
        .withColumn(
            "credit_score_band",
            F.when(F.col("credit_score") < 580, "POOR")
            .when(F.col("credit_score") < 670, "FAIR")
            .when(F.col("credit_score") < 740, "GOOD")
            .when(F.col("credit_score") < 800, "VERY_GOOD")
            .otherwise("EXCELLENT"),
        )
    )