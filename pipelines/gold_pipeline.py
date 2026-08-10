from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window


# ------------------------------------------------------------------
# Pipeline configuration
# ------------------------------------------------------------------

CATALOG = spark.conf.get("catalog_name")
SILVER_SCHEMA = spark.conf.get("silver_schema")


# ------------------------------------------------------------------
# Silver source tables
# ------------------------------------------------------------------

SILVER_POPULATION = (
    f"{CATALOG}.{SILVER_SCHEMA}.silver_population_yearly_fact"
)

SILVER_OBSERVATIONS = (
    f"{CATALOG}.{SILVER_SCHEMA}.silver_bls_observations_fact"
)

SILVER_SERIES = (
    f"{CATALOG}.{SILVER_SCHEMA}.silver_bls_series_dim"
)

SILVER_SECTOR = (
    f"{CATALOG}.{SILVER_SCHEMA}.silver_bls_sector_dim"
)

SILVER_CLASS = (
    f"{CATALOG}.{SILVER_SCHEMA}.silver_bls_class_dim"
)

SILVER_MEASURE = (
    f"{CATALOG}.{SILVER_SCHEMA}.silver_bls_measure_dim"
)

SILVER_DURATION = (
    f"{CATALOG}.{SILVER_SCHEMA}.silver_bls_duration_dim"
)

SILVER_SEASONAL = (
    f"{CATALOG}.{SILVER_SCHEMA}.silver_bls_seasonal_dim"
)


# ==================================================================
# Question 1
# Mean and population standard deviation of US population
# between 2013 and 2018 inclusive
# ==================================================================

@dp.table(
    name="gold_q1_population_stats_2013_2018",
    comment=(
        "Mean and population standard deviation of United States "
        "population from 2013 through 2018 inclusive."
    )
)
def gold_q1_population_stats_2013_2018():

    return (
        spark.read
        .table(SILVER_POPULATION)
        .filter(
            F.col("year").between(2013, 2018)
        )
        .agg(
            F.avg("population")
            .alias("mean_population"),

            F.stddev_pop("population")
            .alias("population_stddev")
        )
    )


# ==================================================================
# Question 2
# For every series_id:
# - sum quarterly values Q01-Q04 by year
# - find year(s) having the highest sum
# - preserve genuine ties
# - identify latest year among tied best years
# - add human-readable series metadata
# ==================================================================

@dp.table(
    name="gold_q2_bls_best_year_by_series",
    comment=(
        "For each BLS series, returns the year or years with the "
        "highest sum of quarterly values Q01-Q04 and enriches the "
        "result with human-readable series metadata."
    )
)
def gold_q2_bls_best_year_by_series():

    # --------------------------------------------------------------
    # 1. Read observations and keep true quarterly periods only
    # Q05 is Annual Average and must not be included
    # --------------------------------------------------------------

    observations_df = (
        spark.read
        .table(SILVER_OBSERVATIONS)
        .filter(
            F.col("period").isin(
                "Q01",
                "Q02",
                "Q03",
                "Q04"
            )
        )
    )

    # --------------------------------------------------------------
    # 2. Sum quarterly values by series and year
    # --------------------------------------------------------------

    yearly_sum_df = (
        observations_df
        .groupBy(
            "series_id",
            "year"
        )
        .agg(
            F.sum("value")
            .alias("yearly_quarterly_sum")
        )
    )

    # --------------------------------------------------------------
    # 3. Rank yearly sums within each series
    # dense_rank preserves genuine ties
    # --------------------------------------------------------------

    best_value_window = (
        Window
        .partitionBy("series_id")
        .orderBy(
            F.col("yearly_quarterly_sum").desc()
        )
    )

    ranked_df = (
        yearly_sum_df
        .withColumn(
            "value_rank",
            F.dense_rank().over(best_value_window)
        )
    )

    # --------------------------------------------------------------
    # 4. Keep only year(s) having the maximum sum
    # --------------------------------------------------------------

    best_year_df = (
        ranked_df
        .filter(
            F.col("value_rank") == 1
        )
    )

    # --------------------------------------------------------------
    # 5. Among tied best years, identify the latest year
    # --------------------------------------------------------------

    latest_tied_year_window = (
        Window
        .partitionBy("series_id")
        .orderBy(
            F.col("year").desc()
        )
    )

    best_year_df = (
        best_year_df
        .withColumn(
            "tie_year_rank",
            F.row_number().over(
                latest_tied_year_window
            )
        )
        .withColumn(
            "is_latest_best_year",
            F.col("tie_year_rank") == 1
        )
    )

    # --------------------------------------------------------------
    # 6. Read series and reference dimensions
    # --------------------------------------------------------------

    series_df = (
        spark.read
        .table(SILVER_SERIES)
    )

    sector_df = (
        spark.read
        .table(SILVER_SECTOR)
    )

    class_df = (
        spark.read
        .table(SILVER_CLASS)
    )

    measure_df = (
        spark.read
        .table(SILVER_MEASURE)
    )

    duration_df = (
        spark.read
        .table(SILVER_DURATION)
    )

    seasonal_df = (
        spark.read
        .table(SILVER_SEASONAL)
    )

    # --------------------------------------------------------------
    # 7. Enrich with readable metadata
    # --------------------------------------------------------------

    enriched_df = (
        best_year_df.alias("best")

        .join(
            series_df.alias("series"),
            F.col("best.series_id")
            == F.col("series.series_id"),
            "left"
        )

        .join(
            sector_df.alias("sector"),
            F.col("series.sector_code")
            == F.col("sector.sector_code"),
            "left"
        )

        .join(
            class_df.alias("class"),
            F.col("series.class_code")
            == F.col("class.class_code"),
            "left"
        )

        .join(
            measure_df.alias("measure"),
            F.col("series.measure_code")
            == F.col("measure.measure_code"),
            "left"
        )

        .join(
            duration_df.alias("duration"),
            F.col("series.duration_code")
            == F.col("duration.duration_code"),
            "left"
        )

        .join(
            seasonal_df.alias("seasonal"),
            F.col("series.seasonal_code")
            == F.col("seasonal.seasonal_code"),
            "left"
        )
    )

    # --------------------------------------------------------------
    # 8. Final Gold output
    # --------------------------------------------------------------

    return (
        enriched_df
        .select(
            F.col("best.series_id")
            .alias("series_id"),

            F.col("best.year")
            .alias("best_year"),

            F.round(F.col("best.yearly_quarterly_sum"),3).alias("yearly_quarterly_sum"),

            F.col("best.is_latest_best_year"),

            F.col("sector.sector_name"),

            F.col("class.class_text"),

            F.col("measure.measure_text"),

            F.col("duration.duration_text"),

            F.col("seasonal.seasonal_text"),

            F.concat_ws(
                " | ",
                F.col("sector.sector_name"),
                F.col("class.class_text"),
                F.col("measure.measure_text"),
                F.col("duration.duration_text"),
                F.col("seasonal.seasonal_text")
            ).alias("series_description")
        )
    )


# ==================================================================
# Question 3
# For PRS30006032 and Q01:
# return yearly BLS value and population where available
# ==================================================================

@dp.table(
    name="gold_q3_bls_q01_with_population",
    comment=(
        "Yearly Q01 values for BLS series PRS30006032 joined with "
        "United States population where population data is available."
    )
)
def gold_q3_bls_q01_with_population():

    bls_df = (
        spark.read
        .table(SILVER_OBSERVATIONS)
        .filter(
            (F.col("series_id") == "PRS30006032")
            & (F.col("period") == "Q01")
        )
        .select(
            F.col("year"),
            F.col("series_id"),
            F.col("period"),
            F.col("value")
            .alias("bls_value")
        )
    )

    population_df = (
        spark.read
        .table(SILVER_POPULATION)
        .select(
            F.col("year"),
            F.col("population")
        )
    )

    return (
        bls_df.alias("bls")
        .join(
            population_df.alias("pop"),
            F.col("bls.year")
            == F.col("pop.year"),
            "left"
        )
        .select(
            F.col("bls.year")
            .alias("year"),

            F.col("bls.series_id")
            .alias("series_id"),

            F.col("bls.period")
            .alias("period"),

            F.col("bls.bls_value")
            .alias("bls_value"),

            F.col("pop.population")
            .alias("population")
        )
    )