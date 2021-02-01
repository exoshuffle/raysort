import datetime
import string
import subprocess

from absl import app
from absl import flags
from absl import logging
import ray


FLAGS = flags.FLAGS
flags.DEFINE_string(
    "cluster_config_template",
    "config/raysort-cluster.yaml.template",
    "path to the cluster config file relative to repository root",
)
flags.DEFINE_integer(
    "num_workers",
    4,
    "number of worker nodes",
    short_name="n",
)
flags.DEFINE_string(
    "worker_type",
    "m5.xlarge",
    "worker instance type",
    short_name="w",
)
flags.DEFINE_integer(
    "object_store_memory",
    100 * 1024 * 1024,
    "memory reserved for object store per worker",
)
flags.DEFINE_bool(
    "preallocate",
    True,
    "if set, allocate all worker nodes at startup",
)


def run(cmd, **kwargs):
    logging.info("$ " + cmd)
    return subprocess.run(cmd, shell=True, **kwargs)


def get_run_id():
    now = datetime.datetime.now()
    return now.isoformat()


def write_cluster_config():
    template_path = FLAGS.cluster_config_template
    assert template_path.endswith(".yaml.template"), template_path
    with open(template_path) as fin:
        template = fin.read()
    template = string.Template(template)
    conf = template.substitute(
        {
            "MIN_WORKERS": FLAGS.num_workers if FLAGS.preallocate else 0,
            "MAX_WORKERS": FLAGS.num_workers,
            "OBJECT_STORE_MEMORY": FLAGS.object_store_memory,
            "WORKER_TYPE": FLAGS.worker_type,
            "RUN_ID": get_run_id(),
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


if __name__ == "__main__":
    app.run(main)