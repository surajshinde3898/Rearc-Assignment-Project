from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, LongType ,TimestampType


MANIFEST_SCHEMA = StructType([
    StructField("source", StringType(), False),
    StructField("file_name", StringType(), False),
    StructField("file_path", StringType(), False),
    StructField("file_size", LongType(), True),
    StructField("source_modified_time", TimestampType(), True),
    StructField("ingestion_time", TimestampType(), False),
    StructField("status", StringType(), False)
])


def get_manifest_table_name(config: dict) -> str:
    catalog = config["catalog"]["name"]
    bronze_schema = config["schemas"]["bronze"]
    table = config["tables"]["ingestion_manifest"]

    return f"{catalog}.{bronze_schema}.{table}"


def get_latest_manifest_state(
    spark,
    manifest_table: str,
    source: str
):
    
    # Return latest manifest state for every source file, regardless of              # SUCCESS /  REMOVED / FAILED.
    

    manifest_df = (
        spark.table(manifest_table)
        .filter(F.col("source") == source)
    )

    window_spec = (
        Window
        .partitionBy("source", "file_name")
        .orderBy(
            F.col("ingestion_time").desc()
        )
    )

    return (
        manifest_df
        .withColumn(
            "rn",
            F.row_number().over(window_spec)
        )
        .filter(F.col("rn") == 1)
        .drop("rn")
    )


def get_latest_successful_manifest(
    spark,
    manifest_table: str,
    source: str
):

    # Return latest successful source version per file.


    manifest_df = (
        spark.table(manifest_table)
        .filter(
            (F.col("source") == source) &
            (F.col("status") == "SUCCESS")
        )
    )

    window_spec = (
        Window
        .partitionBy("source", "file_name")
        .orderBy(
            F.col("source_modified_time").desc_nulls_last(),
            F.col("ingestion_time").desc()
        )
    )

    return (
        manifest_df
        .withColumn(
            "rn",
            F.row_number().over(window_spec)
        )
        .filter(F.col("rn") == 1)
        .drop("rn")
    )


def append_manifest_records(
    spark,
    manifest_table: str,
    records: list
):
    if not records:
        return

    manifest_df = spark.createDataFrame(
        records,
        schema=MANIFEST_SCHEMA
    )

    (
        manifest_df.write
        .mode("append")
        .saveAsTable(manifest_table)
    )