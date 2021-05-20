import datetime
import json
import logging
import random

import ray
import redis
import wandb

from raysort import constants


def init():
    fmt = "%(levelname)s %(asctime)s %(filename)s:%(lineno)s] %(message)s"
    logging.basicConfig(
        format=fmt,
        level=logging.INFO,
    )


def wandb_init(args):
    wandb.init(project=constants.WANDB_PROJECT)
    wandb.config.update(args)
    resources = ray.cluster_resources()
    for key in ["CPU", "memory", "object_store_memory"]:
        wandb.config.update({key: resources.get(key)})
    worker_resources = {k: v for k, v in resources.items() if k.startswith("worker:")}
    wandb.config.update(worker_resources)
    wandb.config.num_workers = sum(worker_resources.values())
    logging.info(f"Cluster config %s", wandb.config)


def get_redis():
    head_addr = ray.worker._global_node.address
    head_ip, _ = head_addr.split(":")
    return redis.Redis(head_ip, constants.APPLICATION_REDIS_PORT)


def log_metric(event, args, sample=False):
    if sample and random.random() > constants.APPLICATION_METRIC_SAMPLE_RATE:
        return
    args = json.dumps(args)
    get_redis().lpush(event, args)
    logging.debug(f"{event} {args}")


def wandb_log(args, log_to_wandb=True):
    logging.info(args)
    if log_to_wandb:
        wandb.log(args)


def log_benchmark_result(args, exec_time):
    total_size = args.num_records * constants.RECORD_SIZE / 10 ** 9
    logging.info(
        f"Sorting {args.num_records:,} records ({total_size} GiB) took {exec_time:.3f} seconds."
    )
    wandb_log(
        {
            "num_records": args.num_records,
            "data_size": args.num_records * constants.RECORD_SIZE / 10 ** 9,
            "execution_time": exec_time,
        }
    )


def export_timeline():
    timestr = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    filename = f"/tmp/timeline-{timestr}.json"
    ray.timeline(filename=filename)
    logging.info(f"Exported timeline to {filename}")
    try:
        wandb.save(filename)
    except wandb.Error:
        pass
