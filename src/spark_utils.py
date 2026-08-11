import re
from pyspark.sql import functions as F


def sanitize_column_name(column_name: str) -> str:
    column_name = column_name.strip().lower()

    column_name = re.sub(
        r"[^a-zA-Z0-9_]+",
        "_",
        column_name
    )
    return column_name.strip("_")


def sanitize_columns(df):
    return df.select(
        *[
            F.col(f"`{column}`").alias(
                sanitize_column_name(column)
            )
            for column in df.columns
        ]
    )


def read_bls_file(
    spark,
    file_path: str,
    schema
):

    return (
        spark.read
        .option("header", "true")
        .option("sep", "\t")
        .schema(schema)
        .csv(file_path)
    )


def read_population_file(
    spark,
    file_path: str,
    schema
):
    df = (
        spark.read
        .option("multiline", "true")
        .schema(schema)
        .json(file_path)
    )

    population_df = (
        df
        .selectExpr("explode(data) AS record")
        .select("record.*")
    )

    return sanitize_columns(population_df)