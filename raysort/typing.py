import enum
from typing import NamedTuple, Tuple

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
    node: str
    path: Path

    def __repr__(self):
        return f"{self.node}:{self.path}"


class SpillingMode(enum.Enum):
    RAY = "ray"
    DISK = "disk"
    S3 = "s3"
