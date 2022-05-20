import math
import os
from dataclasses import dataclass, field, InitVar
from typing import Dict, Optional, List, Tuple

from raysort.typing import AppStep, SpillingMode

CLUSTER_NAME = os.getenv("CLUSTER_NAME")
S3_BUCKET = os.getenv("S3_BUCKET")

CONFIG_NAME_ENV_VAR = "CONFIG"
APP_STEPS_ENV_VAR = "STEPS"

KiB = 1024
MiB = KiB * 1024
GiB = MiB * 1024
KB = 1000
MB = KB * 1000
GB = MB * 1000


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
    cluster_name: str = CLUSTER_NAME
    ebs: bool = False
    local: bool = False

    def __post_init__(self):
        if self.ebs:
            self.instance_type.disk_count = self.instance_type.instance_disk_count + 1


@dataclass
class SystemConfig:
    _cluster: InitVar[ClusterConfig]
    object_spilling_threshold: float = 0.8
    object_store_memory_percent: float = 0.45
    object_store_memory_bytes: int = field(init=False)
    ray_storage: Optional[str] = f"s3://{S3_BUCKET}" if S3_BUCKET else None
    s3_spill: int = 0

    def __post_init__(self, cluster: ClusterConfig):
        self.object_store_memory_bytes = int(
            cluster.instance_type.memory_bytes * self.object_store_memory_percent
        )


@dataclass
class AppConfig:
    _cluster: InitVar[ClusterConfig]

    total_gb: InitVar[float]
    input_part_gb: InitVar[float]
    total_data_size: int = field(init=False)
    input_part_size: int = field(init=False)

    num_workers: int = field(init=False)
    num_mappers: int = field(init=False)
    num_mappers_per_worker: int = field(init=False)
    num_mergers_per_worker: int = field(init=False)
    num_reducers: int = field(init=False)
    num_reducers_per_worker: int = field(init=False)

    num_concurrent_rounds: int = 1
    merge_factor: int = 2
    map_parallelism_multiplier: InitVar[float] = 0.5
    reduce_parallelism_multiplier: InitVar[float] = 0.5
    map_parallelism: int = field(init=False)
    merge_parallelism: int = field(init=False)
    reduce_parallelism: int = field(init=False)

    io_size: int = 256 * KiB
    io_parallelism: int = 0
    merge_io_parallelism: int = field(init=False)
    reduce_io_parallelism: int = field(init=False)

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

    fail_node: Optional[str] = None
    fail_time: int = 45

    generate_input: bool = False
    sort: bool = False
    validate_output: bool = False

    # Runtime Context
    worker_ips: List[str] = field(default_factory=list)
    data_dirs: List[str] = field(default_factory=list)

    def __post_init__(
        self,
        cluster: ClusterConfig,
        total_gb: float,
        input_part_gb: float,
        map_parallelism_multiplier: float,
        reduce_parallelism_multiplier: float,
    ):
        self.total_data_size = int(total_gb * GB)
        self.input_part_size = int(input_part_gb * GB)
        self.map_parallelism = int(
            map_parallelism_multiplier * cluster.instance_type.cpu
        )
        self.reduce_parallelism = int(
            reduce_parallelism_multiplier * cluster.instance_type.cpu
        )

        self.num_workers = cluster.instance_count
        self.num_mappers = int(math.ceil(self.total_data_size / self.input_part_size))
        assert self.num_mappers % self.num_workers == 0, self
        self.num_mappers_per_worker = self.num_mappers // self.num_workers
        if self.riffle:
            assert self.merge_factor % self.map_parallelism == 0, self
            self.merge_parallelism = 1
        else:
            assert self.map_parallelism % self.merge_factor == 0, self
            self.merge_parallelism = self.map_parallelism // self.merge_factor
        self.num_rounds = int(
            math.ceil(self.num_mappers / self.num_workers / self.map_parallelism)
        )
        self.num_mergers_per_worker = self.num_rounds * self.merge_parallelism
        self.num_reducers = self.num_mappers
        assert self.num_reducers % self.num_workers == 0, self
        self.num_reducers_per_worker = self.num_reducers // self.num_workers

        self.merge_io_parallelism = self.io_parallelism // self.merge_parallelism
        self.reduce_io_parallelism = self.io_parallelism // self.reduce_parallelism


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


def get_steps(steps: List[AppStep] = []) -> Dict:
    """
    Return a dictionary of steps to run for AppConfig.
    """
    if not steps:
        steps_str = os.getenv(APP_STEPS_ENV_VAR)
        if steps_str:
            steps = [AppStep[step] for step in steps_str.split(",")]
        if not steps:
            steps = [AppStep.GENERATE_INPUT, AppStep.SORT, AppStep.VALIDATE_OUTPUT]
    return {step.value: True for step in steps}


# ------------------------------------------------------------
#     Configurations
# ------------------------------------------------------------

local_cluster = dict(
    instance_count=os.cpu_count() // 2,
    instance_type=InstanceType(
        name="local",
        cpu=2,
        memory_gib=0,
    ),
    local=True,
)


r6i_2xl = InstanceType(
    name="r6i.2xlarge",
    cpu=8,
    memory_gib=61.8,
)

local_base_app_config = dict(
    **get_steps(),
    map_parallelism_multiplier=1,
    reduce_parallelism_multiplier=1,
)

local_micro_app_config = dict(
    **local_base_app_config,
    total_gb=0.16,
    input_part_gb=0.01,
)

local_mini_app_config = dict(
    **local_base_app_config,
    total_gb=0.16,
    input_part_gb=0.001,
)

local_app_config = dict(
    **local_base_app_config,
    total_gb=1.024,
    input_part_gb=0.004,
)


__config__ = {
    # ------------------------------------------------------------
    #     Local experiments
    # ------------------------------------------------------------
    "LocalSimple": JobConfig(
        cluster=local_cluster,
        system=dict(),
        app=dict(
            **local_app_config,
            simple_shuffle=True,
        ),
    ),
    "LocalManualSpillingDisk": JobConfig(
        cluster=local_cluster,
        system=dict(),
        app=dict(
            **local_app_config,
            spilling=SpillingMode.DISK,
        ),
    ),
    "LocalManualSpillingDiskParallel": JobConfig(
        cluster=local_cluster,
        system=dict(),
        app=dict(
            **local_app_config,
            spilling=SpillingMode.DISK,
            io_parallelism=2,
        ),
    ),
    "LocalNative": JobConfig(
        cluster=local_cluster,
        system=dict(),
        app=dict(**local_app_config),
    ),
    "LocalNativePut": JobConfig(
        cluster=local_cluster,
        system=dict(),
        app=dict(
            **local_app_config,
            use_put=True,
        ),
    ),
    "LocalMagnet": JobConfig(
        cluster=local_cluster,
        system=dict(),
        app=dict(
            **local_app_config,
            magnet=True,
        ),
    ),
    "LocalRiffle": JobConfig(
        cluster=local_cluster,
        system=dict(),
        app=dict(
            **local_app_config,
            riffle=True,
            merge_factor=8,
        ),
    ),
    # ------------------------------------------------------------
    #     Local fault tolerance experiments
    # ------------------------------------------------------------
    # ------------------------------------------------------------
    #     Local S3 spilling experiments
    # ------------------------------------------------------------
    "LocalS3Spilling": JobConfig(
        cluster=local_cluster,
        system=dict(
            s3_spill=4,
        ),
        app=dict(
            **local_mini_app_config,
        ),
    ),
    "LocalS3IO": JobConfig(
        cluster=local_cluster,
        system=dict(),
        app=dict(
            **local_mini_app_config,
            s3_bucket=S3_BUCKET,
        ),
    ),
    "LocalS3IOAndSpilling": JobConfig(
        cluster=local_cluster,
        system=dict(
            s3_spill=4,
        ),
        app=dict(
            **local_mini_app_config,
            s3_bucket=S3_BUCKET,
        ),
    ),
    "LocalS3IOManualSpillingS3": JobConfig(
        cluster=local_cluster,
        system=dict(),
        app=dict(
            **local_micro_app_config,
            s3_bucket=S3_BUCKET,
            spilling=SpillingMode.S3,
        ),
    ),
    "LocalS3IOManualSpillingS3Parallel": JobConfig(
        cluster=local_cluster,
        system=dict(),
        app=dict(
            **local_micro_app_config,
            s3_bucket=S3_BUCKET,
            spilling=SpillingMode.S3,
            io_parallelism=4,
        ),
    ),
    # ------------------------------------------------------------
    #     10 nodes 1TB experiments
    # ------------------------------------------------------------
    "10-1tb-s3-manual-s3": JobConfig(
        cluster=dict(
            instance_count=10,
            instance_type=r6i_2xl,
        ),
        system=dict(
            object_spilling_threshold=1,
        ),
        app=dict(
            **get_steps(),
            total_gb=1000,
            input_part_gb=2,
            s3_bucket=S3_BUCKET,
            spilling=SpillingMode.S3,
            io_parallelism=32,
        ),
    ),
}


def get(config_name: Optional[str] = None) -> Tuple[JobConfig, str]:
    if config_name is None:
        config_name = os.getenv(CONFIG_NAME_ENV_VAR)
    assert config_name, f"No configuration specified, please set ${CONFIG_NAME_ENV_VAR}"
    assert config_name in __config__, f"Unknown configuration: {config_name}"
    return __config__[config_name], config_name


cfg, cfg_name = get()
