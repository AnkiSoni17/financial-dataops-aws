from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

from src.common import clean_currency, normalise_yes_no


def clean_cards(df: DataFrame) -> DataFrame:
    return (
        df
        .select(
            F.col("id").cast("long").alias("card_id"),
            F.col("client_id").cast("long").alias("client_id"),
            F.trim(F.col("card_brand")).alias("card_brand"),
            F.trim(F.col("card_type")).alias("card_type"),

            # Never publish raw card number or CVV.
            F.sha2(
                F.col("card_number").cast("string"),
                256,
            ).alias("card_number_hash"),

            F.right(
                F.col("card_number").cast("string"),
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
                "acct_open_date",
                "MM/yyyy",
            ).alias("account_open_date"),

            F.col("year_pin_last_changed")
            .cast("integer")
            .alias("year_pin_last_changed"),

            normalise_yes_no(
                "card_on_dark_web"
            ).alias("card_on_dark_web"),
        )
    )