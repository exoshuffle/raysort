import string
import subprocess

from absl import app
from absl import flags
from absl import logging
import ray

from raysort import ray_utils


FLAGS = flags.FLAGS
flags.DEFINE_string(
    "cluster_config_template",
    "config/raysort-cluster.yaml.template",
    "Path to the cluster config file relative to repository root",
)
flags.DEFINE_integer(
    "num_mappers",
    4,
    "number of mapper nodes",
    short_name="m",
)
flags.DEFINE_integer(
    "num_reducers",
    4,
    "number of reducer nodes",
    short_name="r",
)


def run(cmd, **kwargs):
    logging.info("$ " + cmd)
    return subprocess.run(cmd, shell=True, **kwargs)


def write_cluster_config():
    template_path = FLAGS.cluster_config_template
    assert template_path.endswith(".yaml.template"), template_path
    with open(template_path) as fin:
        template = fin.read()
    template = string.Template(template)
    conf = template.substitute(
        {
            "NUM_MAPPERS": FLAGS.num_mappers,
            "NUM_REDUCERS": FLAGS.num_reducers,
            "TOTAL_NUM_NODES": FLAGS.num_mappers + FLAGS.num_reducers,
        }
    )
    output_path, _ = template_path.rsplit(".", 1)
    with open(output_path, "w") as fout:
        fout.write(conf)
    return output_path


def launch_ray_cluster(cluster_config_file):
    # run("ray stop")
    run(f"ray up -y {cluster_config_file}")
    # head_ip = (
    #     run(f"ray get-head-ip {cluster_config_file}", capture_output=True)
    #     .stdout.decode("ascii")
    #     .strip()
    # )
    # run(f"ulimit -n 65536 && ray start --address='{head_ip}:6379'")


def main(argv):
    del argv  # Unused.
    cluster_config_file = write_cluster_config()
    launch_ray_cluster(cluster_config_file)
    # ray_utils.check_ray_resources(
    #     {
    #         "mapper": FLAGS.num_mappers,
    #         "reducer": FLAGS.num_reducers,
    #     }
    # )


if __name__ == "__main__":
    app.run(main)
