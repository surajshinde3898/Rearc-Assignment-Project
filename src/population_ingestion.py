import logging
from datetime import datetime, timezone

import requests

logger = logging.getLogger(__name__)


def fetch_population(
    url: str
):
    """
    Call Population API and validate response.
    """

    logger.info(
        "Fetching Population API: %s",
        url
    )

    response = requests.get(
        url,
        timeout=30
    )

    response.raise_for_status()

    payload = response.json()

    required_keys = {
        "columns",
        "data"
    }

    missing_keys = (
        required_keys - payload.keys()
    )

    if missing_keys:

        logger.error(
            "Population API response missing required keys: %s",
            missing_keys
        )

        raise ValueError(
            "Population API response missing "
            f"required keys: {missing_keys}"
        )

    logger.info(
        "Population API fetched successfully with %s record(s).",
        len(payload["data"])
    )

    return response


def determine_population_action(
    file_path: str,
    current_content: bytes
) -> str:
    """
    Determine whether Population payload
    is NEW, CHANGED or UNCHANGED.
    """

    try:
        with open(
            file_path,
            "rb"
        ) as file:

            existing_content = (
                file.read()
            )

    except FileNotFoundError:

        logger.info(
            "Population file does not exist yet. Action=NEW"
        )

        return "NEW"

    if existing_content != current_content:

        logger.info(
            "Population payload has changed. Action=CHANGED"
        )

        return "CHANGED"

    logger.info(
        "Population payload unchanged. Action=UNCHANGED"
    )

    return "UNCHANGED"


def write_population_file(
    response,
    target_path: str
) -> tuple:
    """
    Persist Population API JSON only when
    payload is NEW or CHANGED.
    """

    file_name = "population.json"

    file_path = (
        f"{target_path}/{file_name}"
    )

    content = response.content

    action = determine_population_action(
        file_path,
        content
    )

    if action == "UNCHANGED":

        logger.info(
            "Skipping Population file write because payload is unchanged."
        )

        return action, []

    results = []

    logger.info(
        "Writing Population file. Action=%s Path=%s",
        action,
        file_path
    )

    try:
        with open(
            file_path,
            "wb"
        ) as file:

            file.write(content)

        results.append({
            "source": "POPULATION",
            "file_name": file_name,
            "file_path": file_path,
            "file_size": len(content),
            "source_modified_time": None,

            "ingestion_time":
                datetime.now(
                    timezone.utc
                ).replace(
                    tzinfo=None
                ),

            "status": "SUCCESS"
        })

        logger.info(
            "Population file written successfully. "
            "Action=%s Size=%s bytes",
            action,
            len(content)
        )

    except Exception:

        logger.exception(
            "Failed to write Population file: %s",
            file_path
        )

        results.append({
            "source": "POPULATION",
            "file_name": file_name,
            "file_path": file_path,
            "file_size": len(content),
            "source_modified_time": None,

            "ingestion_time":
                datetime.now(
                    timezone.utc
                ).replace(
                    tzinfo=None
                ),

            "status": "FAILED"
        })

    return action, results