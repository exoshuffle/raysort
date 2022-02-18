import csv
import io
import logging
import os
import random
import subprocess
import tempfile
import time
from typing import Iterable, List, Optional, Tuple

import boto3
import numpy as np
import ray

from raysort import constants
from raysort import logging_utils
from raysort import ray_utils
from raysort import tracing_utils
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
        return ret


def load_partition(args: Args, path: Path) -> np.ndarray:
    if args.skip_input:
        return create_partition(args.input_part_size)
    if args.s3_bucket:
        s3 = boto3.client("s3")
        buf = io.BytesIO()
        s3.download_fileobj(args.s3_bucket, path, buf)
        return np.frombuffer(buf.getbuffer(), dtype=np.uint8)
    return np.fromfile(path, dtype=np.uint8)


def _multipart_upload(args: Args, path: Path, merger: Iterable[np.ndarray]) -> None:
    s3 = boto3.client("s3")
    mpu = s3.create_multipart_upload(Bucket=args.s3_bucket, Key=path)
    mpu_parts = []
    mpu_part_id = 1

    def upload_part(data):
        nonlocal mpu_part_id
        resp = s3.upload_part(
            Body=data,
            Bucket=args.s3_bucket,
            Key=path,
            PartNumber=mpu_part_id,
            UploadId=mpu["UploadId"],
        )
        mpu_parts.append(
            {
                "ETag": resp["ETag"],
                "PartNumber": mpu_part_id,
            }
        )
        mpu_part_id += 1

    tail = io.BytesIO()
    for datachunk in merger:
        if datachunk.size >= constants.S3_MIN_CHUNK_SIZE:
            # There should never be large chunks once we start seeing
            # small chunks towards the end.
            assert tail.getbuffer().nbytes == 0
            upload_part(datachunk.tobytes())
        else:
            tail.write(datachunk)

    if tail.getbuffer().nbytes > 0:
        tail.seek(0)
        upload_part(tail)

    s3.complete_multipart_upload(
        Bucket=args.s3_bucket,
        Key=path,
        MultipartUpload={"Parts": mpu_parts},
        UploadId=mpu["UploadId"],
    )


def save_partition(args: Args, path: Path, merger: Iterable[np.ndarray]) -> None:
    if args.skip_output:
        first_chunk = True
        for datachunk in merger:
            if first_chunk:
                first_chunk = False
                tracing_utils.record_value(
                    "output_time",
                    time.time(),
                    relative_to_start=True,
                    echo=True,
                    log_to_wandb=True,
                )
            del datachunk
    elif args.s3_bucket:
        _multipart_upload(args, path, merger)
    else:
        with open(path, "wb", buffering=args.io_size) as fout:
            for datachunk in merger:
                fout.write(datachunk)


# ------------------------------------------------------------
#     Initialization
# ------------------------------------------------------------


@ray.remote
def make_data_dirs(args: Args):
    os.makedirs(constants.TMPFS_PATH, exist_ok=True)
    for prefix in args.data_dirs:
        for kind in constants.FILENAME_FMT.keys():
            os.makedirs(os.path.join(prefix, kind), exist_ok=True)


def init(args: Args):
    opts = [ray_utils.node_res(node) for node in args.worker_ips]
    opts.append({"resources": {"head": 1}})
    tasks = [make_data_dirs.options(**opt).remote(args) for opt in opts]
    ray.get(tasks)
    logging.info("Created data directories on all nodes")


# ------------------------------------------------------------
#     Generate Input
# ------------------------------------------------------------


def _validate_input_manifest(args: Args) -> bool:
    try:
        parts = load_manifest(args)
    except FileNotFoundError:
        return False
    logging.info("Found existing input manifest")
    parts = parts[: args.num_mappers]
    if len(parts) < args.num_mappers:
        logging.warning("Too few input partitions in manifest")
        return False
    logging.info("Validated manifest, skipping generating input")
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
        path = os.path.join(constants.TMPFS_PATH, f"{part_id:010x}")
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


def create_partition(part_size: int) -> np.ndarray:
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
def validate_part(args: Args, pinfo: PartInfo) -> Tuple[int, bytes]:
    logging_utils.init()
    if args.s3_bucket:
        s3 = boto3.client("s3")
        tmp_path = os.path.join(constants.TMPFS_PATH, os.path.basename(pinfo.path))
        s3.download_file(args.s3_bucket, pinfo.path, tmp_path)
        ret = _validate_part_impl(tmp_path, buf=True)
        os.remove(tmp_path)
    else:
        ret = _validate_part_impl(pinfo.path)
    logging.info(f"Validated output {pinfo}")
    return ret


def validate_output(args: Args):
    if args.skip_sorting or args.skip_output:
        return
    parts = load_manifest(args, kind="output")
    assert len(parts) == args.num_reducers, (len(parts), args)
    results = []
    for pinfo in enumerate(parts):
        opt = (
            ray_utils.node_res(pinfo.node)
            if pinfo.node
            else {"resources": {"worker": 1e-3}}
        )
        results.append(validate_part.options(**opt).remote(args, pinfo))
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
