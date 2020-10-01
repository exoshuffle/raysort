import logging

import ray


def logger():
    logging.basicConfig(format=ray.ray_constants.LOGGER_FORMAT, level=logging.DEBUG)
    log = logging.getLogger(__name__)
    return log
