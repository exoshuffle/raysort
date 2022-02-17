import csv
import io
import logging
import os
import random
import subprocess
import tempfile
from typing import Dict, Iterable, List, Optional, Tuple

import boto3
import numpy as np
import ray

from raysort import constants
from raysort import logging_utils
from raysort.typing import Args, PartId, PartInfo, Path, RecordCount


# ------------------------------------------------------------
#     Loading and Saving Partitions
# ------------------------------------------------------------


def get_manifest_file(args: Args, kind: str = "input") -> Path:
    suffix = "s3" if args.use_s3 else "fs"
    return constants.MANIFEST_FMT.format(kind=kind, suffix=suffix)


def load_manifest(args: Args, kind: str = "input") -> List[PartInfo]:
    if args.skip_input and kind == "input":
        return [
            PartInfo(args.worker_ips[i % args.num_workers], None)
            for i in range(args.num_mappers)
        ]
    path = get_manifest_file(args, kind=kind)
    with open(path) as fin:
        reader = csv.reader(fin)
        ret = [PartInfo(node, path) for node, path in reader]
        assert len(ret) == args.num_mappers, (len(ret), args)
        return ret


def load_partition(args: Args, path: Path) -> np.ndarray:
    if args.skip_input:
        return generate_partition(args.input_part_size)
    if args.use_s3:
        s3 = boto3.client("s3")
        buf = io.BytesIO()
        s3.download_fileobj(constants.S3_BUCKET, path, buf)
        return np.frombuffer(buf.getbuffer(), dtype=np.uint8)
    return np.fromfile(path, dtype=np.uint8)


def save_partition(args: Args, path: Path, producer: Iterable[np.ndarray]) -> None:
    if args.use_s3:
        # TODO(@lsf): use multipart upload
        s3 = boto3.client("s3")
        buf = io.BytesIO()
        for datachunk in producer:
            buf.write(datachunk)
        buf.seek(0)
        s3.upload_fileobj(buf, constants.S3_BUCKET, path)
    else:
        with open(path, "wb", buffering=args.io_size) as fout:
            for datachunk in producer:
                fout.write(datachunk)


# ------------------------------------------------------------
#     Initialization
# ------------------------------------------------------------


@ray.remote
def make_data_dirs(args: Args):
    for prefix in args.data_dirs:
        for kind in constants.FILENAME_FMT.keys():
            os.makedirs(os.path.join(prefix, kind), exist_ok=True)


def init(args: Args):
    tasks = [
        make_data_dirs.options(**_node_res(node)).remote(args)
        for node in args.worker_ips
    ]
    ray.get(tasks)
    logging.info("Created data directories on all worker nodes")


# ------------------------------------------------------------
#     Generate Input
# ------------------------------------------------------------

# TODO(@lsf): put these in one place in ray_utils
def _node_res(node: str) -> Dict[str, float]:
    return {"resources": {f"node:{node}": 1e-3}}


def _validate_input_manifest(args: Args) -> bool:
    try:
        parts = load_manifest(args)
    except FileNotFoundError:
        return False
    parts = parts[: args.num_mappers]
    if len(parts) < args.num_mappers:
        return False
    for part in parts:
        if not os.path.exists(part.path):
            return False
        if os.path.getsize(part.path) != args.input_part_size:
            return False
    logging.info("Found existing input manifest, skipping generating input.")
    return True


def part_info(args: Args, part_id: PartId, *, kind="input", s3=False) -> PartInfo:
    if s3:
        shard = part_id // constants.S3_SHARD_FACTOR
        path = _get_part_path(part_id, shard=shard, kind=kind)
        return PartInfo(None, path)
    if len(args.data_dirs) > 1:
        prefix = random.choice(args.data_dirs)
    else:
        prefix = args.data_dirs[0]
    filepath = _get_part_path(part_id, prefix=prefix, kind=kind)
    node = ray.util.get_node_ip_address()
    return PartInfo(node, filepath)


def _get_part_path(
    part_id: PartId,
    *,
    prefix: Path = "",
    shard: Optional[int] = None,
    kind: str = "input",
) -> Path:
    filename_fmt = constants.FILENAME_FMT[kind]
    filename = filename_fmt.format(part_id=part_id)
    shard_str = f"{shard:08x}" if shard is not None else ""
    return os.path.join(prefix, kind, shard_str, filename)


def _upload_s3(src: Path, dst: Path, *, delete_src: bool = True) -> None:
    s3 = boto3.client("s3")
    s3.upload_file(src, constants.S3_BUCKET, dst)
    if delete_src:
        os.remove(src)


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
    if args.use_s3:
        s3_info = part_info(args, part_id, s3=True)
        _upload_s3(pinfo.path, s3_info.path)
        logging.info(f"Uploaded input {s3_info}")
        return s3_info
    return pinfo


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
    with open(get_manifest_file(args), "w") as fout:
        writer = csv.writer(fout)
        writer.writerows(parts)


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


def _validate_part_impl(path: Path) -> Tuple[int, bytes]:
    sum_path = path + ".sum"
    _run_valsort(["-o", sum_path, path])
    with open(sum_path, "rb") as fin:
        return os.path.getsize(path), fin.read()


@ray.remote
def validate_part(args: Args, path: Path) -> Tuple[int, bytes]:
    logging_utils.init()
    logging.info(f"Validating output {path}")
    if args.use_s3:
        s3 = boto3.client("s3")
        with tempfile.NamedTemporaryFile(delete=False) as f:
            s3.download_fileobj(constants.S3_BUCKET, path, f)
        ret = _validate_part_impl(f.name)
        os.remove(f.name)
        return ret
    return _validate_part_impl(path)


def validate_output(args: Args):
    if args.skip_sorting or args.skip_output:
        return
    parts = load_manifest(args, kind="output")
    results = []
    for part in parts:
        node_res = _node_res(part.node) if part.node else {}
        results.append(validate_part.options(**node_res).remote(args, part.path))
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
