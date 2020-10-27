import logging


def init():
    logging.basicConfig(
        format="%(levelname)s %(asctime)s %(filename)s:%(lineno)s] %(message)s",
        level=logging.INFO,
    )
