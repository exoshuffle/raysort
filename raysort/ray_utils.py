import collections
import logging
import time

import ray
import ray.autoscaler.sdk


def get_required_resources(args):
    num_workers = max(args.num_mappers, args.num_reducers)
    return [{"worker": 1}] * num_workers


def aggregate_bundles(bundles):
    ret = collections.defaultdict(int)
    for bundle in bundles:
        for k, v in bundle.items():
            ret[k] += v
    return ret


def check_ray_resources_impl(bundles, ray_resources):
    resource_requirements = aggregate_bundles(bundles)
    for key, val in resource_requirements.items():
        ray_val = ray_resources.get(key, 0)
        if ray_val < val:
            return False
    return True


def request_resources(args, num_tries=6, wait_time=30):
    res = get_required_resources(args)
    autoscaler_requested = False
    while num_tries > 0:
        ray_resources = ray.cluster_resources()
        ready = check_ray_resources_impl(res, ray_resources)
        if ready:
            logging.info(
                f"Ray cluster is ready: required={res}, actual={ray_resources}"
            )
            return
        if not autoscaler_requested:
            ray.autoscaler.sdk.request_resources(bundles=res)
            logging.info(f"Requested {res} from the autoscaler")
            autoscaler_requested = True
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
