import pytest
from src.config_loader import load_config

def test_load_config_success(tmp_path):
    config_file = tmp_path / "config.yaml"

    config_file.write_text(
        """
environment: dev

catalog:
  name: test_catalog

sources:
  bls:
    url: https://example.com
"""
    )
    config = load_config(str(config_file))
    assert config["environment"] == "dev"
    assert config["catalog"]["name"] == "test_catalog"
    assert config["sources"]["bls"]["url"] == "https://example.com"


def test_load_config_empty_file(tmp_path):
    config_file = tmp_path / "config.yaml"
    config_file.write_text("")

    with pytest.raises(ValueError):
        load_config(str(config_file))