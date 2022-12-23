import numpy as np

from raysort import constants, sortlib


def _test_merge(blocks: list[np.ndarray], num_output_blocks: int = 4):
    num_output_blocks = 4
    bounds = sortlib.get_boundaries(num_output_blocks)
    total_bytes = sum(b.size for b in blocks)
    num_records = max(1, constants.bytes_to_records(total_bytes))
    get_block = lambda i, d: blocks[i] if d == 0 else None
    merger = sortlib.merge_partitions(
        len(blocks), get_block, num_records, False, bounds
    )
    ret = list(merger)
    sizes = [b.size for b in ret]
    assert len(ret) == num_output_blocks, sizes
    assert sum(sizes) == total_bytes, sizes


def test_merge_nothing():
    _test_merge([np.zeros(0, dtype=np.uint8), np.zeros(0, dtype=np.uint8)])


def test_merge_first_only():
    _test_merge([np.zeros(constants.RECORD_SIZE * 100, dtype=np.uint8)])


def test_merge_first_and_last():
    _test_merge(
        [
            np.full(constants.RECORD_SIZE * 100, 0x00, dtype=np.uint8),
            np.full(constants.RECORD_SIZE * 100, 0xFF, dtype=np.uint8),
        ]
    )


def test_merge_all_ranges():
    _test_merge(
        [
            np.full(constants.RECORD_SIZE * 100, 0x00, dtype=np.uint8),
            np.full(constants.RECORD_SIZE * 100, 0x44, dtype=np.uint8),
            np.full(constants.RECORD_SIZE * 100, 0x88, dtype=np.uint8),
            np.full(constants.RECORD_SIZE * 100, 0xCC, dtype=np.uint8),
        ]
    )
