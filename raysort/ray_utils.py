import logging
import time

import ray

from raysort import constants


def get_ray_options(args):
    total_data_size = args.num_records * constants.RECORD_SIZE  # bytes
    return {
        "object_store_memory": total_data_size,
    }


def get_required_resources(args):
    cpu = (args.num_mappers + args.num_reducers) * constants.NODE_CPUS
    total_data_size = args.num_records * constants.RECORD_SIZE
    obj_mem = total_data_size / constants.RAY_MEMORY_UNIT  # in 50MB units
    return {
        "CPU": cpu,
        "object_store_memory": obj_mem,
    }


def check_ray_resources_impl(required_resources, ray_resources):
    for key, val in required_resources.items():
        ray_val = ray_resources.get(key, 0)
        if ray_val < val:
            return False
    return True


def check_ray_resources(args, num_tries=10, wait_time=10):
    required_resources = get_required_resources(args)
    while num_tries > 0:
        ray_resources = ray.available_resources()
        ready = check_ray_resources_impl(required_resources, ray_resources)
        if ready:
            logging.info(f"Ray cluster is ready: {ray_resources}")
            return
        logging.info(f"Ray cluster is not ready yet, sleeping for {wait_time} secs: required={required_resources}, actual={ray_resources}")
        time.sleep(wait_time)
        num_tries -= 1
    raise RuntimeError("Ray cluster is not ready")
