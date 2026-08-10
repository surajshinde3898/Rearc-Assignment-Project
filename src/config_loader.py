import yaml


def load_config(config_path: str) -> dict:
    """
    Load YAML configuration from the given path.

    Args:
        config_path: Path to config.yaml.

    Returns:
        Parsed configuration dictionary.

    Raises:
        ValueError: If the configuration file is empty.
    """

    with open(config_path, "r") as file:
        config = yaml.safe_load(file)

    if config is None:
        raise ValueError(
            f"Configuration file is empty: {config_path}"
        )

    return config