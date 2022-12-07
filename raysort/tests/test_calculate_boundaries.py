from raysort import sort_utils


def test_calculate_boundaries_uniform():
    # samples = [0, 64, 196, 255]
    samples = [
        b"\x00" * 100,
        b"\x40" + b"\x00" * 99,
        b"\xC4" + b"\x00" * 99,
        b"\xFF" + b"\x00" * 99,
    ]
    assert sort_utils.calculate_boundaries(samples, 2, bytes_for_bounds=1) == [
        0,
        130,
        255,
    ]
    # Ensure that bounds overlap with when number of partitions increases by a multiple
    assert sort_utils.calculate_boundaries(samples, 4, bytes_for_bounds=1) == [
        0,
        16,
        130,
        240,
        255,
    ]


def test_calculate_boundaries_skewed():
    # samples = [1, 2]
    samples = [b"\x01" + b"\x00" * 99, b"\x02" + b"\x00" * 99]
    assert sort_utils.calculate_boundaries(samples, 2, bytes_for_bounds=1) == [
        0,
        1,
        255,
    ]
    # Ensure that bounds overlap with when number of partitions increases by a multiple
    assert sort_utils.calculate_boundaries(samples, 4, bytes_for_bounds=1) == [
        0,
        0,
        1,
        65,
        255,
    ]
