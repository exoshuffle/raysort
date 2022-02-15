import csv
import os
import random
import logging
import subprocess
import tempfile
from typing import Dict, List

import numpy as np
import ray

from raysort import constants
from raysort import logging_utils
from raysort.typing import Args, PartId, PartInfo, Path, RecordCount


def load_manifest(args: Args, path: Path) -> List[PartInfo]:
    if args.skip_input:
        return [
            PartInfo(i, args.worker_ips[i % args.num_workers], None)
            for i in range(args.num_mappers)
        ]
    with open(path) as fin:
        reader = csv.reader(fin)
        return [PartInfo(int(part_id), node, path) for part_id, node, path in reader]


@ray.remote
def make_data_dirs(args: Args):
    for dir in args.data_dirs:
        for dir_fmt in constants.DATA_DIR_FMT.values():
            dirpath = dir_fmt.format(dir=dir)
            os.makedirs(dirpath, exist_ok=True)


def init(args: Args):
    os.makedirs(constants.WORK_DIR, exist_ok=True)
    tasks = [
        make_data_dirs.options(**_node_res(node)).remote(args)
        for node in args.worker_ips
    ]
    ray.get(tasks)
    logging.info("Created data directories on all worker nodes")


# ------------------------------------------------------------
#     Generate Input
# ------------------------------------------------------------


def _node_res(node: str) -> Dict[str, float]:
    return {"resources": {f"node:{node}": 1e-3}}


def _validate_input_manifest(args: Args) -> bool:
    try:
        parts = load_manifest(args, constants.INPUT_MANIFEST_FILE)
    except FileNotFoundError:
        return False
    parts = parts[: args.num_mappers]
    if len(parts) < args.num_mappers:
        return False
    for _, _, path in parts:
        if not os.path.exists(path):
            return False
        if os.path.getsize(path) != args.input_part_size:
            return False
    logging.info("Found existing input manifest, skipping generating input.")
    return True


def part_info(args: Args, part_id: PartId, *, kind="input") -> PartInfo:
    node = ray.util.get_node_ip_address()
    dir = random.choice(args.data_dirs)
    filepath = _get_part_path(dir, part_id, kind)
    return PartInfo(part_id, node, filepath)


def _get_part_path(dir: Path, part_id: PartId, kind="input") -> Path:
    assert kind in {"input", "output", "temp"}
    dir_fmt = constants.DATA_DIR_FMT[kind]
    dirpath = dir_fmt.format(dir=dir)
    filename_fmt = constants.FILENAME_FMT[kind]
    filename = filename_fmt.format(part_id=part_id)
    filepath = os.path.join(dirpath, filename)
    return filepath


@ray.remote
def generate_part(
    args: Args, part_id: PartId, size: RecordCount, offset: RecordCount
) -> PartInfo:
    logging_utils.init()
    pinfo = part_info(args, part_id)
    subprocess.run(
        [constants.GENSORT_PATH, f"-b{offset}", f"{size}", pinfo.path], check=True
    )
    logging.info(f"Generated input {pinfo}")
    return pinfo, size


def generate_input(args: Args):
    if args.skip_input or _validate_input_manifest(args):
        return
    total_size = constants.bytes_to_records(args.total_data_size)
    size = constants.bytes_to_records(args.input_part_size)
    offset = 0
    tasks = []
    for part_id in range(args.num_mappers):
        node = args.worker_ips[part_id % args.num_workers]
        tasks.append(
            generate_part.options(**_node_res(node)).remote(
                args, part_id, min(size, total_size - offset), offset
            )
        )
        offset += size
    logging.info(f"Generating {len(tasks)} partitions")
    parts = ray.get(tasks)
    assert sum([s for _, s in parts]) == total_size, (parts, args)
    with open(constants.INPUT_MANIFEST_FILE, "w") as fout:
        writer = csv.writer(fout)
        writer.writerows([p for p, _ in parts])


def generate_partition(part_size: int) -> np.ndarray:
    num_records = part_size // 100
    mat = np.empty((num_records, 100), dtype=np.uint8)
    mat[:, :10] = np.frombuffer(
        np.random.default_rng().bytes(num_records * 10), dtype=np.uint8
    ).reshape((num_records, -1))
    return mat.flatten()


# ------------------------------------------------------------
#     Validate Output
# ------------------------------------------------------------


def _run_valsort(args: List[str]):
    proc = subprocess.run([constants.VALSORT_PATH] + args, capture_output=True)
    if proc.returncode != 0:
        logging.critical("\n" + proc.stderr.decode("ascii"))
        raise RuntimeError(f"Validation failed: {args}")


@ray.remote
def validate_part(path: Path):
    logging_utils.init()
    sum_path = path + ".sum"
    _run_valsort(["-o", sum_path, path])
    logging.info(f"Validated output {path}")
    with open(sum_path, "rb") as fin:
        return os.path.getsize(path), fin.read()


def validate_output(args: Args):
    if args.skip_sorting or args.skip_output:
        return
    partitions = load_manifest(args, constants.OUTPUT_MANIFEST_FILE)
    results = []
    for _, node, path in partitions:
        results.append(validate_part.options(**_node_res(node)).remote(path))
    logging.info(f"Validating {len(results)} partitions")
    results = ray.get(results)
    total = sum(sz for sz, _ in results)
    assert total == args.total_data_size, total - args.total_data_size
    all_checksum = b"".join(chksm for _, chksm in results)
    with tempfile.NamedTemporaryFile() as fout:
        fout.write(all_checksum)
        fout.flush()
        _run_valsort(["-s", fout.name])
    logging.info("All OK!")
