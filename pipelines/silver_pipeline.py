from pyspark import pipelines as dp
from pyspark.sql import functions as F


# ------------------------------------------------------------------
# Pipeline configuration
# ------------------------------------------------------------------

CATALOG = spark.conf.get("catalog_name")
BRONZE_SCHEMA = spark.conf.get("bronze_schema")


# ------------------------------------------------------------------
# Bronze source tables
# ------------------------------------------------------------------

BRONZE_BLS_ALL_DATA = (
    f"{CATALOG}.{BRONZE_SCHEMA}.bronze_bls_all_data"
)

BRONZE_BLS_SERIES = (
    f"{CATALOG}.{BRONZE_SCHEMA}.bronze_bls_series"
)

BRONZE_BLS_MEASURE = (
    f"{CATALOG}.{BRONZE_SCHEMA}.bronze_bls_measure"
)

BRONZE_BLS_SECTOR = (
    f"{CATALOG}.{BRONZE_SCHEMA}.bronze_bls_sector"
)

BRONZE_BLS_CLASS = (
    f"{CATALOG}.{BRONZE_SCHEMA}.bronze_bls_class"
)

BRONZE_BLS_DURATION = (
    f"{CATALOG}.{BRONZE_SCHEMA}.bronze_bls_duration"
)

BRONZE_BLS_SEASONAL = (
    f"{CATALOG}.{BRONZE_SCHEMA}.bronze_bls_seasonal"
)

BRONZE_BLS_PERIOD = (
    f"{CATALOG}.{BRONZE_SCHEMA}.bronze_bls_period"
)

BRONZE_POPULATION = (
    f"{CATALOG}.{BRONZE_SCHEMA}.bronze_population"
)


# ------------------------------------------------------------------
# Silver BLS observations fact
# ------------------------------------------------------------------

@dp.table(
    name="silver_bls_observations_fact",
    comment="Cleaned and typed historical BLS productivity observations."
)
@dp.expect_or_drop(
    "valid_bls_observation",
    """
    series_id IS NOT NULL
    AND year IS NOT NULL
    AND period IN ('Q01', 'Q02', 'Q03', 'Q04', 'Q05')
    AND value IS NOT NULL
    """
)
def silver_bls_observations_fact():

    return (
        spark.read
        .table(BRONZE_BLS_ALL_DATA)
        .select(
            F.trim(F.col("series_id"))
            .alias("series_id"),

            F.trim(F.col("year"))
            .cast("int")
            .alias("year"),

            F.trim(F.col("period"))
            .alias("period"),

            F.trim(F.col("value"))
            .cast("double")
            .alias("value"),

            F.trim(F.col("footnote_codes"))
            .alias("footnote_codes")
        )
    )


# ------------------------------------------------------------------
# Silver BLS measure dimension
# ------------------------------------------------------------------

@dp.table(
    name="silver_bls_measure_dim",
    comment="Cleaned BLS measure reference dimension."
)
def silver_bls_measure_dim():

    return (
        spark.read
        .table(BRONZE_BLS_MEASURE)
        .select(
            F.trim(F.col("measure_code"))
            .alias("measure_code"),

            F.trim(F.col("measure_text"))
            .alias("measure_text"),

            F.trim(F.col("display_level"))
            .cast("int")
            .alias("display_level"),

            F.trim(F.col("selectable"))
            .alias("selectable"),

            F.trim(F.col("sort_sequence"))
            .cast("int")
            .alias("sort_sequence")
        )
    )


# ------------------------------------------------------------------
# Silver BLS sector dimension
# ------------------------------------------------------------------

@dp.table(
    name="silver_bls_sector_dim",
    comment="Cleaned BLS sector reference dimension."
)
def silver_bls_sector_dim():

    return (
        spark.read
        .table(BRONZE_BLS_SECTOR)
        .select(
            F.trim(F.col("sector_code"))
            .alias("sector_code"),

            F.trim(F.col("sector_name"))
            .alias("sector_name"),

            F.trim(F.col("display_level"))
            .cast("int")
            .alias("display_level"),

            F.trim(F.col("selectable"))
            .alias("selectable"),

            F.trim(F.col("sort_sequence"))
            .cast("int")
            .alias("sort_sequence")
        )
    )


# ------------------------------------------------------------------
# Silver BLS class dimension
# ------------------------------------------------------------------

@dp.table(
    name="silver_bls_class_dim",
    comment="Cleaned BLS worker class reference dimension."
)
def silver_bls_class_dim():

    return (
        spark.read
        .table(BRONZE_BLS_CLASS)
        .select(
            F.trim(F.col("class_code"))
            .alias("class_code"),

            F.trim(F.col("class_text"))
            .alias("class_text"),

            F.trim(F.col("display_level"))
            .cast("int")
            .alias("display_level"),

            F.trim(F.col("selectable"))
            .alias("selectable"),

            F.trim(F.col("sort_sequence"))
            .cast("int")
            .alias("sort_sequence")
        )
    )


# ------------------------------------------------------------------
# Silver BLS duration dimension
# ------------------------------------------------------------------

@dp.table(
    name="silver_bls_duration_dim",
    comment="Cleaned BLS duration/reference calculation dimension."
)
def silver_bls_duration_dim():

    return (
        spark.read
        .table(BRONZE_BLS_DURATION)
        .select(
            F.trim(F.col("duration_code"))
            .alias("duration_code"),

            F.trim(F.col("duration_text"))
            .alias("duration_text"),

            F.trim(F.col("display_level"))
            .cast("int")
            .alias("display_level"),

            F.trim(F.col("selectable"))
            .alias("selectable"),

            F.trim(F.col("sort_sequence"))
            .cast("int")
            .alias("sort_sequence")
        )
    )


# ------------------------------------------------------------------
# Silver BLS seasonal dimension
# ------------------------------------------------------------------

@dp.table(
    name="silver_bls_seasonal_dim",
    comment="Cleaned BLS seasonal adjustment reference dimension."
)
def silver_bls_seasonal_dim():

    return (
        spark.read
        .table(BRONZE_BLS_SEASONAL)
        .select(
            F.trim(F.col("seasonal_code"))
            .alias("seasonal_code"),

            F.trim(F.col("seasonal_text"))
            .alias("seasonal_text")
        )
    )


# ------------------------------------------------------------------
# Silver BLS period dimension
# ------------------------------------------------------------------

@dp.table(
    name="silver_bls_period_dim",
    comment="Cleaned BLS reporting period reference dimension."
)
def silver_bls_period_dim():

    return (
        spark.read
        .table(BRONZE_BLS_PERIOD)
        .select(
            F.trim(F.col("period"))
            .alias("period"),

            F.trim(F.col("period_abbr"))
            .alias("period_abbr"),

            F.trim(F.col("period_name"))
            .alias("period_name")
        )
    )


# ------------------------------------------------------------------
# Silver BLS series dimension
# ------------------------------------------------------------------

@dp.table(
    name="silver_bls_series_dim",
    comment="Cleaned BLS productivity series master dimension."
)
@dp.expect_or_drop(
    "valid_series",
    "series_id IS NOT NULL"
)
def silver_bls_series_dim():

    return (
        spark.read
        .table(BRONZE_BLS_SERIES)
        .select(
            F.trim(F.col("series_id"))
            .alias("series_id"),

            F.trim(F.col("sector_code"))
            .alias("sector_code"),

            F.trim(F.col("class_code"))
            .alias("class_code"),

            F.trim(F.col("measure_code"))
            .alias("measure_code"),

            F.trim(F.col("duration_code"))
            .alias("duration_code"),

            F.trim(F.col("seasonal"))
            .alias("seasonal_code"),

            F.when(
                F.trim(F.col("base_year")) == "-",
                None
            )
            .otherwise(
                F.trim(F.col("base_year"))
            )
            .cast("int")
            .alias("base_year"),

            F.trim(F.col("footnote_codes"))
            .alias("footnote_codes"),

            F.trim(F.col("begin_year"))
            .cast("int")
            .alias("begin_year"),

            F.trim(F.col("begin_period"))
            .alias("begin_period"),

            F.trim(F.col("end_year"))
            .cast("int")
            .alias("end_year"),

            F.trim(F.col("end_period"))
            .alias("end_period")
        )
    )


# ------------------------------------------------------------------
# Silver Population yearly fact
# ------------------------------------------------------------------

@dp.table(
    name="silver_population_yearly_fact",
    comment="Cleaned yearly United States population observations."
)
@dp.expect_or_drop(
    "valid_population",
    """
    nation = 'United States'
    AND year IS NOT NULL
    AND population > 0
    """
)
def silver_population_yearly_fact():

    return (
        spark.read
        .table(BRONZE_POPULATION)
        .filter(
            F.col("nation") == "United States"
        )
        .select(
            F.trim(F.col("nation"))
            .alias("nation"),

            F.trim(F.col("nation_id"))
            .alias("nation_id"),

            F.col("year")
            .cast("int")
            .alias("year"),

            F.col("population")
            .cast("long")
            .alias("population")
        )
    )