import enum
from typing import NamedTuple, Optional, Tuple  # pylint: disable=import-self

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
    part_id: int
    node: Optional[str]
    bucket: Optional[str]
    path: Path

    def __repr__(self):
        ret = f"{self.part_id:010x}@"
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


class InstanceLifetime(enum.Enum):
    DEDICATED = "dedicated"
    SPOT = "spot"
