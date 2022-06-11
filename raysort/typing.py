import enum
from typing import NamedTuple, Optional, Tuple

ByteCount = int
PartId = int
Path = str
RecordCount = int

BlockInfo = Tuple[int, int]


class AppStep(enum.Enum):
    GENERATE_INPUT = "generate_input"
    SORT = "sort"
    VALIDATE_OUTPUT = "validate_output"


class PartInfo(NamedTuple):
    node: Optional[str]
    bucket: Optional[str]
    path: Path

    def __repr__(self):
        ret = ""
        if self.node:
            ret += f"{self.node}:"
        if self.bucket:
            ret += f"{self.bucket}:"
        ret += self.path
        return ret


class SpillingMode(enum.Enum):
    RAY = "ray"
    DISK = "disk"
    S3 = "s3"
