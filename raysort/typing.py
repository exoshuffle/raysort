import argparse
import enum
from typing import NamedTuple, Tuple

Args = argparse.Namespace
ByteCount = int
PartId = int
Path = str
RecordCount = int

BlockInfo = Tuple[int, int]


class SpillingMode(enum.Enum):
    RAY = "ray"
    DISK = "disk"
    S3 = "s3"


class PartInfo(NamedTuple):
    node: str
    path: Path

    def __repr__(self):
        return f"Part({self.node}:{self.path})"
