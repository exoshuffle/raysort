# pylint: disable=use-dict-literal
from raysort.config.common import Cloud, InstanceType, JobConfig, get_steps

# ------------------------------------------------------------
#     VM Types
# ------------------------------------------------------------

l8s_v3 = InstanceType(
    name="Standard_L8s_v3",
    cpu=8,
    memory_gib=62.8,
    disk_count=1,
    disk_device_offset=0,
    cloud=Cloud.AZURE,
)

configs = [
    # ------------------------------------------------------------
    #     L8s_v3 Local SSD
    # ------------------------------------------------------------
    JobConfig(
        # TODO
        name="1tb-2gb-l8s",
        cluster=dict(
            instance_count=10,
            instance_type=l8s_v3,
        ),
        system=dict(),
        app=dict(
            **get_steps(),
            total_gb=1000,
            input_part_gb=2,
            reduce_parallelism_multiplier=1,
            sort_optimized=True,
        ),
    ),
]
