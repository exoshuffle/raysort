import typing

import main
import numpy as np
import sort_utils
import sortlib


def get_blocks_for_bounds(bounds):
    map_bounds, merge_bounds = main.get_boundaries(
        bounds, -1
    )

    path = 'skew-gensort'
    checksum = sort_utils._run_gensort(0, 2000, path)
    pinfo = typing.PartInfo(0, 0, 0, path, checksum)

    with open(pinfo.path, "rb", buffering=256 * 1024) as fin:
        part =  np.fromfile(fin, dtype=np.uint8)

    blocks = sortlib.sort_and_partition(part, map_bounds)
    return blocks

num_mappers = [10, 50, 100]
for num in num_mappers:
    print(f"{num}  MAPPERS: ", sorted(get_blocks_for_bounds(num), key=lambda x: -x[1]), "\n")
