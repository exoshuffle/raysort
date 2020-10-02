import logging


def logger():
    logging.basicConfig(
        format="%(levelname)s %(asctime)s %(filename)s:%(lineno)s] %(message)s",
        level=logging.DEBUG,
    )
    log = logging.getLogger(__name__)
    return log
