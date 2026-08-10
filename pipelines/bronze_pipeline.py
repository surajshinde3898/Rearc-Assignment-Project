from pyspark import pipelines as dp

from src.spark_utils import (
    read_bls_file,
    read_population_file
)

from src.schemas import (
    BLS_ALL_DATA_SCHEMA,
    BLS_SERIES_SCHEMA,
    BLS_CLASS_SCHEMA,
    BLS_DURATION_SCHEMA,
    BLS_FOOTNOTE_SCHEMA,
    BLS_MEASURE_SCHEMA,
    BLS_PERIOD_SCHEMA,
    BLS_SEASONAL_SCHEMA,
    BLS_SECTOR_SCHEMA,
    POPULATION_SCHEMA
)


BLS_RAW_PATH = spark.conf.get("bls_raw_path")
POPULATION_RAW_PATH = spark.conf.get("population_raw_path")


@dp.table(
    name="bronze_bls_series",
    comment="Raw BLS productivity series metadata."
)
def bronze_bls_series():

    return read_bls_file(
        spark,
        f"{BLS_RAW_PATH}/pr.series",
        BLS_SERIES_SCHEMA
    )


@dp.table(
    name="bronze_bls_class",
    comment="Raw BLS class reference data."
)
def bronze_bls_class():

    return read_bls_file(
        spark,
        f"{BLS_RAW_PATH}/pr.class",
        BLS_CLASS_SCHEMA
    )


@dp.table(
    name="bronze_bls_duration",
    comment="Raw BLS duration reference data."
)
def bronze_bls_duration():

    return read_bls_file(
        spark,
        f"{BLS_RAW_PATH}/pr.duration",
        BLS_DURATION_SCHEMA
    )


@dp.table(
    name="bronze_bls_footnote",
    comment="Raw BLS footnote reference data."
)
def bronze_bls_footnote():

    return read_bls_file(
        spark,
        f"{BLS_RAW_PATH}/pr.footnote",
        BLS_FOOTNOTE_SCHEMA
    )


@dp.table(
    name="bronze_bls_measure",
    comment="Raw BLS measure reference data."
)
def bronze_bls_measure():

    return read_bls_file(
        spark,
        f"{BLS_RAW_PATH}/pr.measure",
        BLS_MEASURE_SCHEMA
    )


@dp.table(
    name="bronze_bls_period",
    comment="Raw BLS period reference data."
)
def bronze_bls_period():

    return read_bls_file(
        spark,
        f"{BLS_RAW_PATH}/pr.period",
        BLS_PERIOD_SCHEMA
    )


@dp.table(
    name="bronze_bls_seasonal",
    comment="Raw BLS seasonal reference data."
)
def bronze_bls_seasonal():

    return read_bls_file(
        spark,
        f"{BLS_RAW_PATH}/pr.seasonal",
        BLS_SEASONAL_SCHEMA
    )


@dp.table(
    name="bronze_bls_sector",
    comment="Raw BLS sector reference data."
)
def bronze_bls_sector():

    return read_bls_file(
        spark,
        f"{BLS_RAW_PATH}/pr.sector",
        BLS_SECTOR_SCHEMA
    )


@dp.table(
    name="bronze_bls_current",
    comment="Raw current BLS productivity observations."
)
def bronze_bls_current():

    return read_bls_file(
        spark,
        f"{BLS_RAW_PATH}/pr.data.0.Current",
        BLS_ALL_DATA_SCHEMA
    )


@dp.table(
    name="bronze_bls_all_data",
    comment="Raw historical BLS productivity observations."
)
def bronze_bls_all_data():

    return read_bls_file(
        spark,
        f"{BLS_RAW_PATH}/pr.data.1.AllData",
        BLS_ALL_DATA_SCHEMA
    )


@dp.table(
    name="bronze_population",
    comment="Raw population records extracted from the Population API response."
)
def bronze_population():

    return read_population_file(
        spark,
        f"{POPULATION_RAW_PATH}/population.json",
        POPULATION_SCHEMA
    )