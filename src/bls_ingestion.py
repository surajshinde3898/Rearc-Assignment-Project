import logging
import re
from pathlib import Path
from datetime import datetime, timezone
from urllib.parse import urljoin
from html.parser import HTMLParser
import requests
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)

class BLSParser(HTMLParser):

    # Extract BLS file names dynamically from the directory listing.

    def __init__(self):
        super().__init__()
        self.files = []

    def handle_starttag(self, tag, attrs):

        if tag != "a":
            return

        for key, value in attrs:

            if key != "href":
                continue

            file_name = Path(value).name

            if file_name.startswith("pr."):
                self.files.append(file_name)


def fetch_bls_directory(
    url: str,
    headers: dict
) -> str:

    logger.info(
        "Fetching BLS directory: %s",
        url
    )

    response = requests.get(
        url,
        headers=headers,
        timeout=30
    )

    response.raise_for_status()

    logger.info(
        "BLS directory fetched successfully."
    )

    return response.text


def parse_bls_inventory(
    html: str,
    base_url: str
) -> list:

    # Parse BLS directory metadata including filename, URL, size and modified time.

    pattern = re.compile(
        r'(?P<modified>\d{1,2}/\d{1,2}/\d{4}\s+'
        r'\d{1,2}:\d{2}\s+[AP]M)'
        r'\s+'
        r'(?P<size>\d+)'
        r'\s+'
        r'<A HREF="(?P<href>[^"]+)">'
        r'(?P<file_name>pr\.[^<]+)</A>',
        re.IGNORECASE
    )

    inventory = []

    for match in pattern.finditer(html):

        inventory.append({
            "file_name": match.group("file_name"),
            "source_url": urljoin(
                base_url,
                match.group("href")
            ),
            "file_size": int(
                match.group("size")
            ),
            "source_modified_time":
                datetime.strptime(
                    match.group("modified"),
                    "%m/%d/%Y %I:%M %p"
                )
        })

    logger.info(
        "Discovered %s BLS source file(s).",
        len(inventory)
    )

    return inventory


def build_comparison_df(
    spark,
    inventory: list,
    latest_manifest_df
):

    # Compare current source inventory with latest successful manifest state.


    inventory_df = spark.createDataFrame(
        inventory
    )

    comparison_df = (
        inventory_df.alias("src")
        .join(
            latest_manifest_df.alias("m"),
            F.col("src.file_name")
            == F.col("m.file_name"),
            "left"
        )
        .select(
            F.col("src.file_name"),
            F.col("src.source_url"),

            F.col("src.file_size")
            .alias("source_file_size"),

            F.col("src.source_modified_time"),

            F.col("m.file_name")
            .alias("manifest_file_name"),

            F.col("m.file_size")
            .alias("manifest_file_size"),

            F.col("m.source_modified_time")
            .alias("manifest_modified_time")
        )
        .withColumn(
            "action",
            F.when(
                F.col("manifest_file_name").isNull(),
                "NEW"
            )
            .when(
                (
                    F.col("source_file_size")
                    != F.col("manifest_file_size")
                )
                |
                (
                    F.col("source_modified_time")
                    != F.col("manifest_modified_time")
                ),
                "CHANGED"
            )
            .otherwise("UNCHANGED")
        )
    )

    return comparison_df


def download_bls_files(
    comparison_df,
    target_path: str,
    headers: dict
) -> list:
    
    # Download only NEW or CHANGED BLS files.
    
    results = []

    files_to_download = (
        comparison_df
        .filter(
            F.col("action")
            .isin("NEW", "CHANGED")
        )
        .collect()
    )

    logger.info(
        "%s BLS file(s) require download.",
        len(files_to_download)
    )

    for row in files_to_download:

        file_name = row["file_name"]
        source_url = row["source_url"]
        expected_file_size = row[
            "source_file_size"
        ]
        source_modified_time = row[
            "source_modified_time"
        ]

        target_file_path = (
            f"{target_path}/{file_name}"
        )

        logger.info(
            "Downloading BLS file: %s from %s",
            file_name,
            source_url
        )

        try:
            response = requests.get(
                source_url,
                headers=headers,
                timeout=60
            )

            response.raise_for_status()

            actual_file_size = len(
                response.content
            )

            if (
                expected_file_size
                is not None
                and actual_file_size
                != expected_file_size
            ):
                logger.warning(
                    (
                        "Downloaded size mismatch for %s. "
                        "Expected=%s Actual=%s"
                    ),
                    file_name,
                    expected_file_size,
                    actual_file_size
                )

            with open(
                target_file_path,
                "wb"
            ) as file:
                file.write(
                    response.content
                )

            results.append({
                "source": "BLS",
                "file_name": file_name,
                "file_path": target_file_path,
                "file_size": actual_file_size,
                "source_modified_time": source_modified_time,
                "ingestion_time":datetime.now(timezone.utc).replace(tzinfo=None),
                "status": "SUCCESS"
            })

            logger.info("Successfully downloaded BLS file: %s",file_name)

        except Exception:

            logger.exception("Failed to download BLS file: %s",file_name)

            results.append({
                "source": "BLS",
                "file_name": file_name,
                "file_path": target_file_path,

                "file_size":
                    expected_file_size,

                "source_modified_time":
                    source_modified_time,

                "ingestion_time":
                    datetime.now(
                        timezone.utc
                    ).replace(
                        tzinfo=None
                    ),

                "status": "FAILED"
            })

    return results


def detect_removed_files(
    latest_manifest_state_df,
    inventory_df
):
    active_manifest_df = (
        latest_manifest_state_df
        .filter(
            F.col("status") == "SUCCESS"
        )
    )

    removed_files_df = (
        active_manifest_df.alias("m")
        .join(
            inventory_df.alias("src"),
            F.col("m.file_name")
            == F.col("src.file_name"),
            "left_anti"
        )
    )

    return removed_files_df


def build_removed_manifest_records(
    removed_files_df
) -> list:
    
    """
    Convert removed BLS files into manifest records.
    A removed file is retained in the raw Volume
    for auditability, but a new manifest record is
    written with status='REMOVED'.
    """

    removed_results = []

    removed_rows = (
        removed_files_df.collect()
    )

    for row in removed_rows:

        logger.warning("BLS file no longer exists at source: %s",row["file_name"])

        removed_results.append({
            "source": "BLS",
            "file_name":
                row["file_name"],

            "file_path":
                row["file_path"],

            "file_size":
                row["file_size"],

            "source_modified_time":
                row[
                    "source_modified_time"
                ],

            "ingestion_time":
                datetime.now(
                    timezone.utc
                ).replace(
                    tzinfo=None
                ),

            "status": "REMOVED"
        })

    logger.info("%s BLS file(s) marked as REMOVED.",len(removed_results))

    return removed_results