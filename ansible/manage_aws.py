from typing import Dict, List

from absl import app
from absl import flags
from absl import logging
import boto3

FLAGS = flags.FLAGS
flags.DEFINE_string(
    "instance_name_pattern",
    "raysort-worker-*",
    "instance name pattern",
)
flags.DEFINE_enum(
    "action",
    "stop",
    ["reboot", "start", "stop"],
    "action to perform on the instances",
)


def get_instances(pattern: str) -> List[Dict]:
    ec2 = boto3.client("ec2")
    paginator = ec2.get_paginator("describe_instances")
    ret = []
    for page in paginator.paginate(
        Filters=[
            {
                "Name": "tag:Name",
                "Values": [pattern],
            },
        ],
    ):
        ret.extend(page["Reservations"])
    return [item["Instances"][0] for item in ret]


def perform_action(action: str, instances: List[Dict]):
    ids = [inst["InstanceId"] for inst in instances]
    ec2 = boto3.client("ec2")
    action_to_fn = {
        "reboot": ec2.reboot_instances,
        "start": ec2.start_instances,
        "stop": ec2.stop_instances,
    }
    fn = action_to_fn[action]
    resp = fn(InstanceIds=ids)
    logging.info(resp)


def main(argv):
    del argv  # Unused.
    instances = get_instances(FLAGS.instance_name_pattern)
    perform_action(FLAGS.action, instances)
    logging.info(len(instances))


if __name__ == "__main__":
    app.run(main)
