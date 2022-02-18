import csv
import io
import logging
import os
import random
import subprocess
import tempfile
from typing import Iterable, List, Optional, Tuple

import boto3
import numpy as np
import ray

from raysort import constants
from raysort import logging_utils
from raysort import ray_utils
from raysort.typing import Args, PartId, PartInfo, Path, RecordCount


# ------------------------------------------------------------
#     Loading and Saving Partitions
# ------------------------------------------------------------


def get_manifest_file(args: Args, kind: str = "input") -> Path:
    suffix = "s3" if args.s3_bucket else "fs"
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
    if args.s3_bucket:
        s3 = boto3.client("s3")
        buf = io.BytesIO()
        s3.download_fileobj(args.s3_bucket, path, buf)
        return np.frombuffer(buf.getbuffer(), dtype=np.uint8)
    return np.fromfile(path, dtype=np.uint8)


def save_partition(args: Args, path: Path, producer: Iterable[np.ndarray]) -> None:
    if args.s3_bucket:
        # TODO(@lsf): use multipart upload
        s3 = boto3.client("s3")
        buf = io.BytesIO()
        for datachunk in producer:
            buf.write(datachunk)
        buf.seek(0)
        s3.upload_fileobj(buf, args.s3_bucket, path)
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
        make_data_dirs.options(**ray_utils.node_res(node)).remote(args)
        for node in args.worker_ips
    ]
    ray.get(tasks)
    logging.info("Created data directories on all worker nodes")


# ------------------------------------------------------------
#     Generate Input
# ------------------------------------------------------------


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
        shard = part_id & constants.S3_SHARD_MASK
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
    shard_str = constants.SHARD_FMT.format(shard=shard) if shard is not None else ""
    return os.path.join(prefix, kind, shard_str, filename)


def _upload_s3(args: Args, src: Path, dst: Path, *, delete_src: bool = True) -> None:
    s3 = boto3.client("s3")
    s3.upload_file(src, args.s3_bucket, dst)
    if delete_src:
        os.remove(src)


def _run_gensort(offset: int, size: int, path: str, buf: bool = False):
    # gensort by default uses direct I/O unless `,buf` is specified.
    # tmpfs does not support direct I/O so we must disabled it.
    if buf:
        path += ",buf"
    subprocess.run(
        f"{constants.GENSORT_PATH} -b{offset} {size} {path}", shell=True, check=True
    )


@ray.remote
def generate_part(
    args: Args, part_id: PartId, size: RecordCount, offset: RecordCount
) -> PartInfo:
    logging_utils.init()
    if args.s3_bucket:
        pinfo = part_info(args, part_id, s3=True)
        path = f"/dev/shm/{part_id}"
    else:
        pinfo = part_info(args, part_id)
        path = pinfo.path
    _run_gensort(offset, size, path, args.s3_bucket)
    if args.s3_bucket:
        _upload_s3(args, path, pinfo.path)
    logging.info(f"Generated input {pinfo}")
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
            generate_part.options(**ray_utils.node_res(node)).remote(
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


def _run_valsort(argstr: str):
    proc = subprocess.run(
        f"{constants.VALSORT_PATH} {argstr}", shell=True, capture_output=True
    )
    if proc.returncode != 0:
        logging.critical("\n" + proc.stderr.decode("ascii"))
        raise RuntimeError(f"Validation failed: {argstr}")


def _validate_part_impl(path: Path, buf: bool = False) -> Tuple[int, bytes]:
    sum_path = path + ".sum"
    argstr = f"-o {sum_path} {path}"
    if buf:
        argstr += ",buf"
    _run_valsort(argstr)
    with open(sum_path, "rb") as fin:
        return os.path.getsize(path), fin.read()


@ray.remote
def validate_part(args: Args, path: Path) -> Tuple[int, bytes]:
    logging_utils.init()
    if args.s3_bucket:
        s3 = boto3.client("s3")
        tmp_path = "/dev/shm/" + os.path.basename(path)
        s3.download_file(args.s3_bucket, path, tmp_path)
        ret = _validate_part_impl(tmp_path, buf=True)
        os.remove(tmp_path)
    else:
        ret = _validate_part_impl(path)
    logging.info(f"Validated output {path}")
    return ret


def validate_output(args: Args):
    if args.skip_sorting or args.skip_output:
        return
    parts = load_manifest(args, kind="output")
    results = []
    for part in parts:
        opt = ray_utils.node_res(part.node) if part.node else {}
        results.append(validate_part.options(**opt).remote(args, part.path))
    logging.info(f"Validating {len(results)} partitions")
    results = ray.get(results)
    total = sum(sz for sz, _ in results)
    assert total == args.total_data_size, total - args.total_data_size
    all_checksum = b"".join(chksm for _, chksm in results)
    with tempfile.NamedTemporaryFile() as fout:
        fout.write(all_checksum)
        fout.flush()
        _run_valsort(f"-s {fout.name}")
    logging.info("All OK!")
