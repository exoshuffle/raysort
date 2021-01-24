import logging
import time

import ray

from raysort import constants


def get_required_memory(args):
    total_data_size = args.num_records * constants.RECORD_SIZE  # bytes
    obj_mem_bytes = total_data_size + 10 ** 8  # 100MB extra
    return obj_mem_bytes / constants.RAY_MEMORY_UNIT  # in 50MB units


def get_ray_options(args):
    return {
        "object_store_memory": get_required_memory(args),
    }


def get_required_resources(args):
    # This needs revisiting.
    # If we rely on object spilling then no need to require obj_mem.
    # If we run multiple waves of mapper/reducer tasks then no need
    # to require mapper/reducer.
    obj_mem = get_required_memory(args)
    return {
        "mapper": args.num_mappers,
        "reducer": args.num_reducers,
        "object_store_memory": obj_mem,
    }


def check_ray_resources_impl(required_resources, ray_resources):
    for key, val in required_resources.items():
        ray_val = ray_resources.get(key, 0)
        if ray_val < val:
            return False
    return True


def check_ray_resources(args, num_tries=6, wait_time=10):
    if isinstance(args, dict):
        res = args
    else:
        res = get_required_resources(args)
    while num_tries > 0:
        ray_resources = ray.cluster_resources()
        ready = check_ray_resources_impl(res, ray_resources)
        if ready:
            logging.info(
                f"Ray cluster is ready: required={res}, actual={ray_resources}"
            )
            return
        logging.info(
            f"Ray cluster is not ready yet, sleeping for {wait_time} secs: required={res}, actual={ray_resources}"
        )
        time.sleep(wait_time)
        num_tries -= 1
    raise RuntimeError("Ray cluster is not ready")


def get_node_resources():
    """Returns a list of Ray worker node labels."""
    res = ray.available_resources()
    return [k for k in res if k.startswith("node:")]


class NodeAllocator:
    def __init__(self, args):
        self.next_node_idx = 0
        self.nodes = get_node_resources()
        assert len(self.nodes) >= args.num_mappers + args.num_reducers, (
            "Not enough worker nodes",
            self.nodes,
        )

    def get(self):
        node = self.nodes[self.next_node_idx]
        self.next_node_idx += 1
        if self.next_node_idx == len(self.nodes) + 1:
            raise RuntimeError("Running out of worker nodes to allocate")
        return {node: 1}
