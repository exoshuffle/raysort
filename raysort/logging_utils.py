import logging


def init():
    fmt = "%(levelname)s %(asctime)s %(filename)s:%(lineno)s] %(message)s"
    logging.basicConfig(
        format=fmt,
        level=logging.INFO,
    )
    logging.getLogger("azure").setLevel(logging.WARNING)
    logging.getLogger("botocore.credentials").setLevel(logging.WARNING)
