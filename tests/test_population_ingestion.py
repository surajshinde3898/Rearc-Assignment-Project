from unittest.mock import Mock, patch

import pytest

from src.population_ingestion import (
    determine_population_action,
    fetch_population
)


def test_population_new(tmp_path):
    file_path = tmp_path / "population.json"

    action = determine_population_action(
        str(file_path),
        b'{"data":[1]}'
    )

    assert action == "NEW"


def test_population_unchanged(tmp_path):
    file_path = tmp_path / "population.json"

    content = b'{"data":[1]}'

    file_path.write_bytes(content)

    action = determine_population_action(
        str(file_path),
        content
    )

    assert action == "UNCHANGED"


def test_population_changed(tmp_path):
    file_path = tmp_path / "population.json"

    file_path.write_bytes(
        b'{"data":[1]}'
    )

    action = determine_population_action(
        str(file_path),
        b'{"data":[2]}'
    )

    assert action == "CHANGED"


@patch("src.population_ingestion.requests.get")
def test_fetch_population_valid_response(mock_get):
    mock_response = Mock()

    mock_response.json.return_value = {
        "columns": [
            "Nation ID",
            "Nation",
            "Year",
            "Population"
        ],
        "data": [
            {
                "Nation": "United States",
                "Year": 2024,
                "Population": 340110990
            }
        ]
    }

    mock_get.return_value = mock_response

    response = fetch_population(
        "https://example.com"
    )

    assert response == mock_response
    mock_response.raise_for_status.assert_called_once()


@patch("src.population_ingestion.requests.get")
def test_fetch_population_missing_data(mock_get):
    mock_response = Mock()

    mock_response.json.return_value = {
        "columns": [
            "Nation",
            "Year"
        ]
    }

    mock_get.return_value = mock_response

    with pytest.raises(ValueError):
        fetch_population(
            "https://example.com"
        )