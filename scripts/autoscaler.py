import os
import pathlib
import string
import sys

import click
from util import run

SCRIPT_DIR = pathlib.Path(os.path.dirname(__file__))
AUTOSCALER_DIR = SCRIPT_DIR / "config" / "autoscaler"
AUTOSCALER_CONFIG_TEMPLATE_PATH = AUTOSCALER_DIR / "raysort-cluster-template.yaml"
VARIABLES_TO_REPLACE = [
    "CLUSTER_NAME",
    "S3_BUCKET",
    "CONFIG",
    "WANDB_API_KEY",
]


def get_or_create_autoscaler_config(raysort_config: str) -> pathlib.Path:
    autoscaler_config_path = AUTOSCALER_DIR / f"_{raysort_config}-autoscaler.yaml"
    if os.path.exists(autoscaler_config_path):
        click.echo(f"Found existing configuration for {raysort_config} config")
        return autoscaler_config_path

    assert not os.path.exists(autoscaler_config_path), f"{autoscaler_config_path} must not exist"
    assert os.path.exists(
        AUTOSCALER_CONFIG_TEMPLATE_PATH
    ), f"{AUTOSCALER_CONFIG_TEMPLATE_PATH} must exist"

    with open(AUTOSCALER_CONFIG_TEMPLATE_PATH, "r") as template_file:
        template = string.Template(template_file.read())

    autoscaler_config = template.substitute(
        **{var: os.getenv(var) for var in VARIABLES_TO_REPLACE}
    )

    with open(autoscaler_config_path, "w") as config_file:
        config_file.write(autoscaler_config)

    click.echo(f"Created autoscaler config file for {raysort_config} config")
    return autoscaler_config_path


def main():
    raysort_config = os.getenv("CONFIG")
    assert raysort_config is not None, "CONFIG must be set for autoscaler"
    assert len(sys.argv) > 1, "Must specify a command for ray autoscaler e.g. up"
    config_path = get_or_create_autoscaler_config(raysort_config)

    run(f"ray {sys.argv[1]} '{config_path}' {' '.join(sys.argv[2:])}")


if __name__ == "__main__":
    main()
