import os
from dataclasses import dataclass, field, InitVar
from enum import Enum
from typing import Dict, Optional, List

CONFIG_NAME_ENV_VAR = "CONFIG"

KiB = 1024
MiB = KiB * 1024
GiB = MiB * 1024


class SpillingMode(Enum):
    RAY = "ray"
    DISK = "disk"
    S3 = "s3"


class Step(Enum):
    GENERATE_INPUT = "generate_input"
    SORT = "sort"
    VALIDATE_OUTPUT = "validate_output"


@dataclass
class InstanceType:
    name: str
    cpu: int
    memory_gib: float
    memory_bytes: int = field(init=False)
    instance_disk_count: int = 0
    disk_count: int = field(init=False)
    disk_device_offset: int = 0
    hdd: bool = False

    def __post_init__(self):
        self.memory_bytes = int(self.memory_gib * GiB)
        self.disk_count = self.instance_disk_count


@dataclass
class ClusterConfig:
    instance_count: int
    instance_type: InstanceType
    ebs: bool

    def __post_init__(self):
        if self.ebs:
            self.instance_type.disk_count = self.instance_type.instance_disk_count + 1


@dataclass
class SystemConfig:
    _cluster: InitVar[ClusterConfig]
    object_spilling_threshold: float = 0.8
    object_store_memory_percent: float = 0.45
    object_store_memory_bytes: int = field(init=False)
    s3_spill: int = 0

    def __post_init__(self, cluster: ClusterConfig):
        self.object_store_memory_bytes = int(
            cluster.instance_type.memory_bytes * self.object_store_memory_percent
        )


@dataclass
class AppConfig:
    _cluster: InitVar[ClusterConfig]

    total_gb: int
    total_data_size: int = field(init=False)
    input_part_gb: int
    input_part_size: int = field(init=False)

    num_concurrent_rounds: int = 1
    merge_factor: int = 2
    map_parallelism_multiplier: float = 0.5
    map_parallelism: int = field(init=False)
    reduce_parallelism_multiplier: float = 0.5
    reduce_parallelism: int = field(init=False)

    io_size: int = 256 * KiB
    io_parallelism: int = 0

    skip_sorting: bool = False
    skip_input: bool = False
    skip_output: bool = False
    skip_final_reduce: bool = False

    spilling: SpillingMode = SpillingMode.RAY

    free_scheduling: bool = False
    use_put: bool = False

    simple_shuffle: bool = False
    riffle: bool = False
    magnet: bool = False

    s3_bucket: Optional[str] = None

    local: bool = False
    ray_spill_path: Optional[str] = None

    fail_node: Optional[str] = None
    fail_time: int = 45

    steps: List[Step] = field(
        default_factory=lambda: ["generate_input", "sort", "validate_output"],
    )

    def __post_init__(self, cluster: ClusterConfig):
        self.total_data_size = self.total_gb * GiB
        self.input_part_size = self.input_part_gb * GiB
        self.map_parallelism = int(
            self.map_parallelism_multiplier * cluster.instance_type.cpu
        )
        self.reduce_parallelism = int(
            self.reduce_parallelism_multiplier * cluster.instance_type.cpu
        )


@dataclass
class JobConfig:
    cluster: ClusterConfig
    system: SystemConfig
    app: AppConfig

    def __init__(self, cluster: Dict, system: Dict, app: Dict):
        self.cluster = ClusterConfig(**cluster)
        self.system = SystemConfig(**system, _cluster=self.cluster)
        self.app = AppConfig(**app, _cluster=self.cluster)

    def __post_init__(self):
        pass


# ------------------------------------------------------------
#     Configurations
# ------------------------------------------------------------


r6i_2xl = InstanceType(
    name="r6i.2xlarge",
    cpu=8,
    memory_gib=61.8,
)


__config__ = {
    "ManualS3_1TB": JobConfig(
        cluster=dict(
            instance_count=10,
            instance_type=r6i_2xl,
            ebs=False,
        ),
        system=dict(
            object_spilling_threshold=1,
        ),
        app=dict(
            total_gb=1000,
            input_part_gb=2,
        ),
    )
}


def get(config_name: Optional[str] = None) -> JobConfig:
    if config_name is None:
        config_name = os.getenv(CONFIG_NAME_ENV_VAR)
    assert config_name, f"${CONFIG_NAME_ENV_VAR} not set"
    assert config_name in __config__, f"Unknown configuration: {config_name}"
    return __config__[config_name]


cfg = get()
