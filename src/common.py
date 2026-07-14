from pyspark.sql import Column
from pyspark.sql import functions as F


def clean_currency(column_name: str) -> Column:
    """Remove currency symbols and separators and return a numeric string."""

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
    return (
        F.when(
            F.upper(F.trim(F.col(column_name))) == "YES",
            F.lit(True),
        )
        .when(
            F.upper(F.trim(F.col(column_name))) == "NO",
            F.lit(False),
        )
        .otherwise(F.lit(None))
    )