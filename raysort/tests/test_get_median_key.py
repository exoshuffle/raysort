import numpy as np

from raysort import sort_utils


def test_get_median_key():
    NUM_RECORDS = 50
    TOTAL_BYTES = NUM_RECORDS * 100
    part = np.zeros(TOTAL_BYTES, dtype=np.uint8)
    for i in range(0, NUM_RECORDS):
        idx = i * 100
        part[idx] = i

    expected_median = np.median(range(0, NUM_RECORDS))

    assert sort_utils.get_median_key(part) == expected_median
