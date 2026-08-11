from datetime import datetime
import pytest
from pyspark.sql import SparkSession



from src.bls_ingestion import (
    BLSParser,
    parse_bls_inventory,
    build_comparison_df,
    detect_removed_files,
    build_removed_manifest_records
)


@pytest.fixture(scope="session")
def spark():
    return SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()


SAMPLE_HTML = """
<html>
<head>
    <title>BLS Test Directory</title>
</head>
<body>
<pre>

8/6/2026  8:30 AM          102
<A HREF="/pub/time.series/pr/pr.class">pr.class</A>

8/6/2026  8:30 AM      3237169
<A HREF="/pub/time.series/pr/pr.data.1.AllData">pr.data.1.AllData</A>

8/6/2026  8:30 AM        15657
<A HREF="/pub/time.series/pr/pr.series">pr.series</A>

<A HREF="/pub/time.series/">[To Parent Directory]</A>

<A HREF="/readme.txt">readme.txt</A>

</pre>
</body>
</html>
"""


def test_bls_parser_discovers_pr_files():
    parser = BLSParser()

    parser.feed(SAMPLE_HTML)

    assert "pr.class" in parser.files
    assert "pr.data.1.AllData" in parser.files
    assert "pr.series" in parser.files

    assert len(parser.files) == 3


def test_bls_parser_ignores_non_pr_files():
    html = """
    <A HREF="/readme.txt">readme.txt</A>
    <A HREF="/pub/time.series/pr/pr.series">pr.series</A>
    """

    parser = BLSParser()
    parser.feed(html)

    assert parser.files == ["pr.series"]



# Inventory parsing


def test_parse_bls_inventory():
    inventory = parse_bls_inventory(
        SAMPLE_HTML,
        "https://download.bls.gov/pub/time.series/pr/"
    )

    assert len(inventory) == 3

    first = inventory[0]

    assert first["file_name"] == "pr.class"

    assert first["file_size"] == 102

    assert first["source_url"] == (
        "https://download.bls.gov/"
        "pub/time.series/pr/pr.class"
    )

    assert first["source_modified_time"] == datetime(
        2026,
        8,
        6,
        8,
        30
    )



# NEW comparison


def test_bls_file_new(spark):
    inventory = [
        {
            "file_name": "pr.series",
            "source_url": (
                "https://download.bls.gov/"
                "pub/time.series/pr/pr.series"
            ),
            "file_size": 100,
            "source_modified_time": datetime(
                2026,
                8,
                6,
                8,
                30
            )
        }
    ]

    manifest_schema = """
        source string,
        file_name string,
        file_path string,
        file_size long,
        source_modified_time timestamp,
        ingestion_time timestamp,
        status string
    """

    latest_manifest_df = spark.createDataFrame(
        [],
        schema=manifest_schema
    )

    result_df = build_comparison_df(
        spark,
        inventory,
        latest_manifest_df
    )

    result = result_df.collect()

    assert len(result) == 1

    assert result[0]["file_name"] == "pr.series"
    assert result[0]["action"] == "NEW"



# UNCHANGED comparison


def test_bls_file_unchanged(spark):
    modified_time = datetime(
        2026,
        8,
        6,
        8,
        30
    )

    inventory = [
        {
            "file_name": "pr.series",
            "source_url": (
                "https://download.bls.gov/"
                "pub/time.series/pr/pr.series"
            ),
            "file_size": 100,
            "source_modified_time": modified_time
        }
    ]

    manifest_data = [
        (
            "BLS",
            "pr.series",
            "/Volumes/test/pr.series",
            100,
            modified_time,
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

    manifest_schema = """
        source string,
        file_name string,
        file_path string,
        file_size long,
        source_modified_time timestamp,
        ingestion_time timestamp,
        status string
    """

    latest_manifest_df = spark.createDataFrame(
        manifest_data,
        schema=manifest_schema
    )

    result_df = build_comparison_df(
        spark,
        inventory,
        latest_manifest_df
    )

    result = result_df.collect()

    assert len(result) == 1
    assert result[0]["action"] == "UNCHANGED"



# CHANGED because size changed


def test_bls_file_changed_by_size(spark):
    modified_time = datetime(
        2026,
        8,
        6,
        8,
        30
    )

    inventory = [
        {
            "file_name": "pr.series",
            "source_url": (
                "https://download.bls.gov/"
                "pub/time.series/pr/pr.series"
            ),
            "file_size": 200,
            "source_modified_time": modified_time
        }
    ]

    manifest_data = [
        (
            "BLS",
            "pr.series",
            "/Volumes/test/pr.series",
            100,
            modified_time,
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

    manifest_schema = """
        source string,
        file_name string,
        file_path string,
        file_size long,
        source_modified_time timestamp,
        ingestion_time timestamp,
        status string
    """

    latest_manifest_df = spark.createDataFrame(
        manifest_data,
        schema=manifest_schema
    )

    result_df = build_comparison_df(
        spark,
        inventory,
        latest_manifest_df
    )

    result = result_df.collect()

    assert result[0]["action"] == "CHANGED"



# CHANGED because modified timestamp changed


def test_bls_file_changed_by_modified_time(spark):
    old_modified_time = datetime(
        2026,
        8,
        1,
        8,
        30
    )

    new_modified_time = datetime(
        2026,
        8,
        6,
        8,
        30
    )

    inventory = [
        {
            "file_name": "pr.series",
            "source_url": (
                "https://download.bls.gov/"
                "pub/time.series/pr/pr.series"
            ),
            "file_size": 100,
            "source_modified_time": new_modified_time
        }
    ]

    manifest_data = [
        (
            "BLS",
            "pr.series",
            "/Volumes/test/pr.series",
            100,
            old_modified_time,
            datetime(
                2026,
                8,
                1,
                10,
                0
            ),
            "SUCCESS"
        )
    ]

    manifest_schema = """
        source string,
        file_name string,
        file_path string,
        file_size long,
        source_modified_time timestamp,
        ingestion_time timestamp,
        status string
    """

    latest_manifest_df = spark.createDataFrame(
        manifest_data,
        schema=manifest_schema
    )

    result_df = build_comparison_df(
        spark,
        inventory,
        latest_manifest_df
    )

    result = result_df.collect()

    assert result[0]["action"] == "CHANGED"



# REMOVED detection


def test_bls_removed_file_detected(spark):
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
        )
    ]

    manifest_schema = """
        source string,
        file_name string,
        file_path string,
        file_size long,
        source_modified_time timestamp,
        ingestion_time timestamp,
        status string
    """

    latest_manifest_state_df = spark.createDataFrame(
        manifest_data,
        schema=manifest_schema
    )

    inventory_schema = """
        file_name string,
        source_url string,
        file_size long,
        source_modified_time timestamp
    """

    # Current source inventory is empty,
    # meaning pr.series disappeared from BLS.
    inventory_df = spark.createDataFrame(
        [],
        schema=inventory_schema
    )

    removed_df = detect_removed_files(
        latest_manifest_state_df,
        inventory_df
    )

    removed = removed_df.collect()

    assert len(removed) == 1
    assert removed[0]["file_name"] == "pr.series"



# Already REMOVED should not be marked REMOVED again


def test_bls_already_removed_not_detected_again(spark):
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
            "REMOVED"
        )
    ]

    manifest_schema = """
        source string,
        file_name string,
        file_path string,
        file_size long,
        source_modified_time timestamp,
        ingestion_time timestamp,
        status string
    """

    latest_manifest_state_df = spark.createDataFrame(
        manifest_data,
        schema=manifest_schema
    )

    inventory_schema = """
        file_name string,
        source_url string,
        file_size long,
        source_modified_time timestamp
    """

    inventory_df = spark.createDataFrame(
        [],
        schema=inventory_schema
    )

    removed_df = detect_removed_files(
        latest_manifest_state_df,
        inventory_df
    )

    assert removed_df.count() == 0


# Build REMOVED manifest record


def test_build_removed_manifest_records(spark):
    removed_data = [
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
        )
    ]

    schema = """
        source string,
        file_name string,
        file_path string,
        file_size long,
        source_modified_time timestamp,
        ingestion_time timestamp,
        status string
    """

    removed_df = spark.createDataFrame(
        removed_data,
        schema=schema
    )

    records = build_removed_manifest_records(
        removed_df
    )

    assert len(records) == 1

    assert records[0]["source"] == "BLS"
    assert records[0]["file_name"] == "pr.series"
    assert records[0]["status"] == "REMOVED"