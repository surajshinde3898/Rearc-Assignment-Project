import yaml


def load_config(config_path: str) -> dict:

    with open(config_path, "r") as file:
        config = yaml.safe_load(file)

    if config is None:
        raise ValueError(
            f"Configuration file is empty: {config_path}"
        )

    return config