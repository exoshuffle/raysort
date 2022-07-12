import os
import pathlib
import string
import sys

import click
import util

SCRIPT_DIR = pathlib.Path(os.path.dirname(__file__))
AUTOSCALER_DIR = SCRIPT_DIR / "config" / "autoscaler"
AUTOSCALER_CONFIG_TEMPLATE_PATH = AUTOSCALER_DIR / "raysort-cluster-template.yaml"
VARIABLES_TO_REPLACE = [
    "CLUSTER_NAME",
    "S3_BUCKET",
    "CONFIG",
    "WANDB_API_KEY",
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

    with open(AUTOSCALER_CONFIG_TEMPLATE_PATH, "r") as template_file:
        template = string.Template(template_file.read())

    config = template.substitute(
        **{var: os.getenv(var) for var in VARIABLES_TO_REPLACE}
    )

    with open(config_path, "w") as config_file:
        config_file.write(config)

    click.echo(f"Created autoscaler config file for {cluster_name}")
    return config_path


def run_once(config_path: str):
    util.run(f"ray up -y '{config_path}'")
    check_ready(config_path)
    util.run(f"ray submit '{config_path}' raysort/main.py")
    util.run(f"ray down -y '{config_path}'")


def check_ready(config_path, num_tries=7, wait_time=60):
    while num_tries > 0:
        status = util.run_output(
            f"ray exec '{config_path}' 'conda activate raysort && ray status'"
        )
        if "no pending nodes" in status:
            click.echo(f"Ray cluster is ready: {status}")
            return
        click.echo(
            f"Ray cluster is not ready yet, sleeping for {wait_time} secs, current status=\n{status}"
        )
        util.sleep(wait_time, "worker nodes starting up...")
        num_tries -= 1
    util.run(f"ray down -y '{config_path}'")
    raise RuntimeError("Ray cluster is not ready")


def main():
    cluster_name = os.getenv("CLUSTER_NAME")
    assert cluster_name is not None, "CLUSTER_NAME must be set for autoscaler"
    assert len(sys.argv) > 1, "Must specify a command for ray autoscaler e.g. up"
    config_path = get_or_create_autoscaler_config(cluster_name)

    if sys.argv[1] == "run_once":
        run_once(config_path)
    else:
        util.run(f"ray {sys.argv[1]} '{config_path}' {' '.join(sys.argv[2:])}")


if __name__ == "__main__":
    main()
