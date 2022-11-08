"""This script tests merger performance.

We observed that the merger gets slower when the number of sorted blocks
increases (as the number of workers increases).
"""


import time

import numpy as np
import pandas as pd

from raysort import config, constants, sortlib


def get_boundaries(
    num_map_returns: int, num_merge_returns: int = -1
) -> tuple[list[int], list[list[int]]]:
    if num_merge_returns == -1:
        return sortlib.get_boundaries(num_map_returns), []
    merge_bounds_flat = sortlib.get_boundaries(num_map_returns * num_merge_returns)
    merge_bounds = (
        np.array(merge_bounds_flat, dtype=sortlib.KeyT)
        .reshape(num_map_returns, num_merge_returns)
        .tolist()
    )
    map_bounds = [b[0] for b in merge_bounds]
    return map_bounds, merge_bounds


def _merge_blocks(blocks: list[np.ndarray], bounds: list[int]) -> pd.Series:
    total_bytes = sum(b.size for b in blocks)
    num_records = constants.bytes_to_records(total_bytes / len(bounds) * 2)
    get_block = lambda i, d: blocks[i] if d == 0 else None
    merger = sortlib.merge_partitions(
        len(blocks), get_block, num_records, False, bounds
    )
    ret = []
    start = time.perf_counter()
    for _ in merger:
        ret.append(time.perf_counter() - start)
        start = time.perf_counter()
    return pd.Series(ret)


def _make_block(size: int, lower_limit: int, upper_limit: int) -> np.ndarray:
    num_records = size // constants.RECORD_SIZE
    keys = np.sort(
        np.random.randint(lower_limit, upper_limit, size=num_records, dtype=sortlib.KeyT)
    )
    ret = np.empty((num_records, 100), dtype=np.uint8)
    ret[:, :8] = keys.byteswap().view(np.uint8).reshape(-1, 8)
    ret = ret.flatten()
    return ret


def _make_blocks(
    num_blocks: int,
    total_bytes: int,
    lower_limit: int,
    upper_limit: int,
    fully_sorted: bool = False,
) -> list[np.ndarray]:
    block_bytes = total_bytes // num_blocks
    if fully_sorted:
        block = _make_block(total_bytes, lower_limit, upper_limit)
        offset = 0
        ret = []
        for _ in range(num_blocks):
            ret.append(block[offset : offset + block_bytes])
            offset += block_bytes
        return ret
    return [
        _make_block(block_bytes, lower_limit, upper_limit) for _ in range(num_blocks)
    ]


def test_config(config_name, merger_id: int = 0):
    job_cfg = config.get(config_name)
    cfg = job_cfg.app
    map_bounds, merge_bounds = get_boundaries(
        cfg.num_workers, cfg.num_reducers_per_worker
    )
    print(config_name)
    merge_limit = cfg.merge_factor * cfg.num_workers
    bounds = merge_bounds[merger_id]
    start = time.perf_counter()
    blocks = _make_blocks(
        merge_limit,
        cfg.input_part_size,
        map_bounds[merger_id],
        map_bounds[merger_id + 1],
    )
    duration = time.perf_counter() - start
    print(f"Generated {len(blocks)} blocks in {duration:.2f} seconds")
    start = time.perf_counter()
    ss = _merge_blocks(blocks, bounds)
    duration = time.perf_counter() - start
    print(f"Merged {len(blocks)} blocks in {duration:.2f} seconds")
    # print(ss.describe())


def main():
    test_config("2tb-2gb-i4i4x-s3")
    test_config("6tb-2gb-i4i4x-s3")
    test_config("10tb-2gb-i4i4x-s3")
    test_config("100tb-2gb-i4i4x-s3")


if __name__ == "__main__":
    main()
