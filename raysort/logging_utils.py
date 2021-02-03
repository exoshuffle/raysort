import logging

import ray
import wandb

from raysort import constants


def init():
    logging.basicConfig(
        format="%(levelname)s %(asctime)s %(filename)s:%(lineno)s] %(message)s",
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


def wandb_log(args):
    logging.info(f"Logging metric: {args}")
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
