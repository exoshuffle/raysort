import argparse

import numpy as np

from raysort.typing import Args, ByteCount, SpillingMode

STEPS = ["generate_input", "sort", "validate_output"]


def get_args(*args, **kwargs):
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--total_gb",
        default=1000,
        type=float,
        help="total data size in GB (10^9 bytes)",
    )
    parser.add_argument(
        "--input_part_size",
        default=1000 * 1000 * 1000,
        type=ByteCount,
        help="size in bytes of each map partition",
    )
    parser.add_argument(
        "--num_concurrent_rounds",
        default=2,
        type=int,
        help="how many rounds of tasks to run concurrently (1 or 2)",
    )
    parser.add_argument(
        "--map_parallelism",
        default=4,
        type=int,
        help="each round has `map_parallelism` map tasks per node",
    )
    parser.add_argument(
        "--merge_factor",
        default=2,
        type=int,
        help="each round has `map_parallelism / merge_factor` per node",
    )
    parser.add_argument(
        "--reduce_parallelism",
        default=4,
        type=int,
        help="number of reduce tasks to run in parallel per node",
    )
    parser.add_argument(
        "--io_size",
        default=256 * 1024,
        type=ByteCount,
        help="disk I/O buffer size",
    )
    parser.add_argument(
        "--skip_sorting",
        default=False,
        action="store_true",
        help="if set, no sorting is actually performed",
    )
    parser.add_argument(
        "--skip_input",
        default=False,
        action="store_true",
        help="if set, mappers will not read data from disk",
    )
    parser.add_argument(
        "--skip_output",
        default=False,
        action="store_true",
        help="if set, reducers will not write out results to disk",
    )
    parser.add_argument(
        "--skip_final_reduce",
        default=False,
        action="store_true",
        help="if set, will skip the second stage reduce tasks",
    )
    parser.add_argument(
        "--output_consume_time",
        default=0,
        type=float,
        help="output will be consumed instead of being written to disk",
    )
    parser.add_argument(
        "--spilling",
        default=SpillingMode.RAY,
        type=SpillingMode,
        help="can be 'ray' (default), 'disk' or 's3'",
    )
    parser.add_argument(
        "--use_put",
        default=False,
        action="store_true",
        help="if set, will return ray.put() references instead of objects directly",
    )
    parser.add_argument(
        "--simple_shuffle",
        default=False,
        action="store_true",
        help="if set, will use the simple map-reduce version",
    )
    parser.add_argument(
        "--magnet",
        default=False,
        action="store_true",
        help="if set, will keep map results in scope until all reduce tasks are scheduled",
    )
    parser.add_argument(
        "--riffle",
        default=False,
        action="store_true",
        help="if set, will run Riffle-style map-side merge",
    )
    parser.add_argument(
        "--s3_bucket",
        default=None,
        type=str,
        help="if set, will use this S3 bucket for input and output data",
    )
    parser.add_argument(
        "--local",
        default=False,
        action="store_true",
        help="if set, will use a locally emulated Ray cluster",
    )
    parser.add_argument(
        "--ray_spill_path",
        default=None,
        type=str,
        help="[only used in local cluster setup] can be a local path or S3",
    )
    parser.add_argument(
        "--fail_node",
        default="",
        type=str,
        help="empty means no failure; for local setup set an index between 0 and num_workers; for multi-node set a worker IP",
    )
    parser.add_argument(
        "--fail_time",
        default=45,
        type=float,
        help="fail a node this many seconds into execution if fail_node is nonempty",
    )
    # Which steps to run?
    steps_grp = parser.add_argument_group(
        "steps to run", "if none is specified, will run all steps"
    )
    for step in STEPS:
        steps_grp.add_argument(f"--{step}", action="store_true")
    return parser.parse_args(*args, **kwargs)


def derive_app_args(args: Args):
    # If no steps are specified, run all steps.
    args_dict = vars(args)
    if not any(args_dict[step] for step in STEPS):
        for step in STEPS:
            args_dict[step] = True

    assert args.local or args.ray_spill_path is None, args
    args.total_data_size = args.total_gb * 10**9
    args.num_mappers = int(np.ceil(args.total_data_size / args.input_part_size))
    assert args.num_mappers % args.num_workers == 0, args
    args.num_mappers_per_worker = args.num_mappers // args.num_workers
    if args.riffle:
        assert args.merge_factor % args.map_parallelism == 0, args
        args.merge_parallelism = 1
    else:
        assert args.map_parallelism % args.merge_factor == 0, args
        args.merge_parallelism = args.map_parallelism // args.merge_factor
    args.num_rounds = int(
        np.ceil(args.num_mappers / args.num_workers / args.map_parallelism)
    )
    args.num_mergers_per_worker = args.num_rounds * args.merge_parallelism
    args.num_reducers = args.num_mappers
    assert args.num_reducers % args.num_workers == 0, args
    args.num_reducers_per_worker = args.num_reducers // args.num_workers
