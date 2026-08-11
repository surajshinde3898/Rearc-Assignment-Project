from datetime import datetime
import pytest
from pyspark.sql import SparkSession
from src.manifest import (
    MANIFEST_SCHEMA,
    get_manifest_table_name,
    get_latest_manifest_state,
    get_latest_successful_manifest,
    append_manifest_records
)


@pytest.fixture(scope="session")
def spark():
    return SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()


# Manifest table name


def test_get_manifest_table_name():
    config = {
        "catalog": {
            "name": "test_catalog"
        },
        "schemas": {
            "bronze": "bronze"
        },
        "tables": {
            "ingestion_manifest": "ingestion_manifest"
        }
    }

    table_name = get_manifest_table_name(
        config
    )

    assert table_name == (
        "test_catalog.bronze.ingestion_manifest"
    )

# Explicit manifest schema


def test_manifest_schema():
    fields = {
        field.name: field.dataType.simpleString()
        for field in MANIFEST_SCHEMA.fields
    }

    assert fields["source"] == "string"
    assert fields["file_name"] == "string"
    assert fields["file_path"] == "string"
    assert fields["file_size"] == "bigint"
    assert fields["source_modified_time"] == "timestamp"
    assert fields["ingestion_time"] == "timestamp"
    assert fields["status"] == "string"


# Latest overall state


def test_get_latest_manifest_state(
    spark,
    monkeypatch
):
    manifest_data = [
        (
            "BLS",
            "pr.series",
            "/Volumes/test/pr.series",
            100,
            datetime(
                2026,
                8,
                1,
                8,
                30
            ),
            datetime(
                2026,
                8,
                1,
                10,
                0
            ),
            "SUCCESS"
        ),
        (
            "BLS",
            "pr.series",
            "/Volumes/test/pr.series",
            100,
            datetime(
                2026,
                8,
                1,
                8,
                30
            ),
            datetime(
                2026,
                8,
                2,
                10,
                0
            ),
            "REMOVED"
        )
    ]

    manifest_df = spark.createDataFrame(
        manifest_data,
        schema=MANIFEST_SCHEMA
    )

    monkeypatch.setattr(
        spark,
        "table",
        lambda table_name: manifest_df
    )

    result_df = get_latest_manifest_state(
        spark,
        "dummy.manifest",
        "BLS"
    )

    result = result_df.collect()

    assert len(result) == 1

    assert result[0]["file_name"] == "pr.series"

    assert result[0]["status"] == "REMOVED"


# Latest SUCCESS based primarily on source_modified_time


def test_get_latest_successful_manifest(
    spark,
    monkeypatch
):
    manifest_data = [
        (
            "BLS",
            "pr.series",
            "/Volumes/test/pr.series",
            100,
            datetime(
                2026,
                8,
                1,
                8,
                30
            ),
            datetime(
                2026,
                8,
                5,
                10,
                0
            ),
            "SUCCESS"
        ),
        (
            "BLS",
            "pr.series",
            "/Volumes/test/pr.series",
            200,
            datetime(
                2026,
                8,
                6,
                8,
                30
            ),
            datetime(
                2026,
                8,
                6,
                10,
                0
            ),
            "SUCCESS"
        )
    ]

    manifest_df = spark.createDataFrame(
        manifest_data,
        schema=MANIFEST_SCHEMA
    )

    monkeypatch.setattr(
        spark,
        "table",
        lambda table_name: manifest_df
    )

    result_df = get_latest_successful_manifest(
        spark,
        "dummy.manifest",
        "BLS"
    )

    result = result_df.collect()

    assert len(result) == 1

    assert result[0]["file_name"] == "pr.series"

    assert result[0]["file_size"] == 200

    assert result[0]["source_modified_time"] == datetime(
        2026,
        8,
        6,
        8,
        30
    )


# Latest SUCCESS should ignore FAILED and REMOVED


def test_latest_successful_manifest_ignores_other_statuses(
    spark,
    monkeypatch
):
    manifest_data = [
        (
            "BLS",
            "pr.series",
            "/Volumes/test/pr.series",
            100,
            datetime(
                2026,
                8,
                1,
                8,
                30
            ),
            datetime(
                2026,
                8,
                1,
                10,
                0
            ),
            "SUCCESS"
        ),
        (
            "BLS",
            "pr.series",
            "/Volumes/test/pr.series",
            200,
            datetime(
                2026,
                8,
                6,
                8,
                30
            ),
            datetime(
                2026,
                8,
                6,
                10,
                0
            ),
            "FAILED"
        ),
        (
            "BLS",
            "pr.series",
            "/Volumes/test/pr.series",
            100,
            datetime(
                2026,
                8,
                1,
                8,
                30
            ),
            datetime(
                2026,
                8,
                7,
                10,
                0
            ),
            "REMOVED"
        )
    ]

    manifest_df = spark.createDataFrame(
        manifest_data,
        schema=MANIFEST_SCHEMA
    )

    monkeypatch.setattr(
        spark,
        "table",
        lambda table_name: manifest_df
    )

    result_df = get_latest_successful_manifest(
        spark,
        "dummy.manifest",
        "BLS"
    )

    result = result_df.collect()

    assert len(result) == 1

    assert result[0]["status"] == "SUCCESS"

    assert result[0]["file_size"] == 100


# Source filtering


def test_get_latest_state_filters_source(
    spark,
    monkeypatch
):
    manifest_data = [
        (
            "BLS",
            "pr.series",
            "/Volumes/test/pr.series",
            100,
            datetime(
                2026,
                8,
                6,
                8,
                30
            ),
            datetime(
                2026,
                8,
                6,
                10,
                0
            ),
            "SUCCESS"
        ),
        (
            "POPULATION",
            "population.json",
            "/Volumes/test/population.json",
            500,
            None,
            datetime(
                2026,
                8,
                6,
                11,
                0
            ),
            "SUCCESS"
        )
    ]

    manifest_df = spark.createDataFrame(
        manifest_data,
        schema=MANIFEST_SCHEMA
    )

    monkeypatch.setattr(
        spark,
        "table",
        lambda table_name: manifest_df
    )

    result_df = get_latest_manifest_state(
        spark,
        "dummy.manifest",
        "BLS"
    )

    result = result_df.collect()

    assert len(result) == 1
    assert result[0]["source"] == "BLS"
    assert result[0]["file_name"] == "pr.series"


# Empty records must do nothing


def test_append_manifest_records_empty(
    spark
):
    # Main purpose:
    # make sure function simply returns
    # and does not raise an exception.

    append_manifest_records(
        spark,
        "dummy.manifest",
        []
    )


# Schema can handle nullable source_modified_time


def test_manifest_schema_allows_null_source_modified_time(
    spark
):
    records = [
        {
            "source": "POPULATION",
            "file_name": "population.json",
            "file_path": (
                "/Volumes/test/population.json"
            ),
            "file_size": 500,
            "source_modified_time": None,
            "ingestion_time": datetime(
                2026,
                8,
                9,
                10,
                0
            ),
            "status": "SUCCESS"
        }
    ]

    df = spark.createDataFrame(
        records,
        schema=MANIFEST_SCHEMA
    )

    row = df.collect()[0]

    assert row["source_modified_time"] is None