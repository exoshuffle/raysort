from raysort.config.common import Cloud, InstanceType, JobConfig, get_steps

# ------------------------------------------------------------
#     VM Types
# ------------------------------------------------------------

n2_highmem_8 = InstanceType(
    name="n2-highmem-8",
    cpu=8,
    memory_gib=64,
    disk_count=4,
    disk_device_offset=1,
    cloud=Cloud.GCP,
)

configs = [
    # ------------------------------------------------------------
    #     n2_highmem_8 Local SSD
    # ------------------------------------------------------------
    JobConfig(
        name="1tb-2gb-n2-highmem-8",
        cluster=dict(
            instance_count=10,
            instance_type=n2_highmem_8,
        ),
        system=dict(),
        app=dict(
            **get_steps(),
            total_gb=1000,
            input_part_gb=2,
            reduce_parallelism_multiplier=1,
        ),
    ),
]
