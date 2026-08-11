from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType,ArrayType


BLS_ALL_DATA_SCHEMA = StructType([
    StructField("series_id", StringType(), True),
    StructField("year", StringType(), True),
    StructField("period", StringType(), True),
    StructField("value", StringType(), True),
    StructField("footnote_codes", StringType(), True)
])


BLS_SERIES_SCHEMA = StructType([
    StructField("series_id", StringType(), True),
    StructField("sector_code", StringType(), True),
    StructField("class_code", StringType(), True),
    StructField("measure_code", StringType(), True),
    StructField("duration_code", StringType(), True),
    StructField("seasonal", StringType(), True),
    StructField("base_year", StringType(), True),
    StructField("footnote_codes", StringType(), True),
    StructField("begin_year", StringType(), True),
    StructField("begin_period", StringType(), True),
    StructField("end_year", StringType(), True),
    StructField("end_period", StringType(), True)
])


BLS_MEASURE_SCHEMA = StructType([
    StructField("measure_code", StringType(), True),
    StructField("measure_text", StringType(), True),
    StructField("display_level", StringType(), True),
    StructField("selectable", StringType(), True),
    StructField("sort_sequence", StringType(), True)
])


BLS_SECTOR_SCHEMA = StructType([
    StructField("sector_code", StringType(), True),
    StructField("sector_name", StringType(), True),
    StructField("display_level", StringType(), True),
    StructField("selectable", StringType(), True),
    StructField("sort_sequence", StringType(), True)
])


BLS_CLASS_SCHEMA = StructType([
    StructField("class_code", StringType(), True),
    StructField("class_text", StringType(), True),
    StructField("display_level", StringType(), True),
    StructField("selectable", StringType(), True),
    StructField("sort_sequence", StringType(), True)
])


BLS_DURATION_SCHEMA = StructType([
    StructField("duration_code", StringType(), True),
    StructField("duration_text", StringType(), True),
    StructField("display_level", StringType(), True),
    StructField("selectable", StringType(), True),
    StructField("sort_sequence", StringType(), True)
])


BLS_SEASONAL_SCHEMA = StructType([
    StructField("seasonal_code", StringType(), True),
    StructField("seasonal_text", StringType(), True)
])


BLS_PERIOD_SCHEMA = StructType([
    StructField("period", StringType(), True),
    StructField("period_abbr", StringType(), True),
    StructField("period_name", StringType(), True)
])


BLS_FOOTNOTE_SCHEMA = StructType([
    StructField("footnote_code", StringType(), True),
    StructField("footnote_text", StringType(), True)
])


POPULATION_RECORD_SCHEMA = StructType([
    StructField("Nation ID", StringType(), True),
    StructField("Nation", StringType(), True),
    StructField("Year", LongType(), True),
    StructField("Population", DoubleType(), True)
])


POPULATION_SCHEMA = StructType([
    StructField("data",
    ArrayType(POPULATION_RECORD_SCHEMA),
    True
    )
])