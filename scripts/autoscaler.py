import os
import pathlib
import sys

import click

from util import run

SCRIPT_DIR = pathlib.Path(os.path.dirname(__file__))
AUTOSCALER_DIR = SCRIPT_DIR / "config" / "autoscaler"
AUTOSCALER_CONFIG_TEMPLATE_PATH = AUTOSCALER_DIR / "raysort-cluster-template.yaml"
VARIABLES_TO_REPLACE = [
    "${CLUSTER_NAME}",
    "${S3_BUCKET}",
    "${CONFIG}",
    "${WANDB_API_KEY}",
]


def get_or_create_autoscaler_config(cluster_name: str) -> pathlib.Path:
    config_path = AUTOSCALER_DIR / f"_{cluster_name}-autoscaler.yaml"
    if os.path.exists(config_path):
        click.echo(f"Found existing configuration for {cluster_name}")
        return config_path

    assert not os.path.exists(config_path), f"{config_path} must not exist"
    assert os.path.exists(
        AUTOSCALER_CONFIG_TEMPLATE_PATH
    ), f"{AUTOSCALER_CONFIG_TEMPLATE_PATH} must exist"
    run(
        f"envsubst '{' '.join(VARIABLES_TO_REPLACE)}' < '{AUTOSCALER_CONFIG_TEMPLATE_PATH}' > '{config_path}'"
    )
    click.echo(f"Created autoscaler config file for {cluster_name}")
    return config_path


def main():
    cluster_name = os.getenv("CLUSTER_NAME")
    assert cluster_name is not None, "CLUSTER_NAME must be set for autoscaler"
    assert len(sys.argv) > 1, "Must specify a command for ray autoscaler e.g. up"
    config_path = get_or_create_autoscaler_config(cluster_name)

    run(f"ray {sys.argv[1]} '{config_path}' {' '.join(sys.argv[2:])}")


if __name__ == "__main__":
    main()
