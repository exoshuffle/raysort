import csv
import logging
import os
import subprocess
import tempfile
import time
from typing import Iterable, List, Optional, Tuple

import numpy as np
import ray

from raysort import constants, logging_utils, ray_utils, s3_utils, tracing_utils
from raysort.config import AppConfig
from raysort.typing import PartId, PartInfo, Path, RecordCount, SpillingMode

# ------------------------------------------------------------
#     Loading and Saving Partitions
# ------------------------------------------------------------


def get_manifest_file(cfg: AppConfig, kind: str = "input") -> Path:
    suffix = "s3" if cfg.s3_buckets else "fs"
    return constants.MANIFEST_FMT.format(kind=kind, suffix=suffix)


def load_manifest(cfg: AppConfig, kind: str = "input") -> List[PartInfo]:
    if cfg.skip_input and kind == "input":
        return [
            PartInfo(i, cfg.worker_ips[i % cfg.num_workers], None, None)
            for i in range(cfg.num_mappers)
        ]
    path = get_manifest_file(cfg, kind=kind)
    with open(path) as fin:
        reader = csv.reader(fin)
        ret = [PartInfo.from_csv_row(row) for row in reader]
        return ret


def load_partition(cfg: AppConfig, pinfo: PartInfo) -> np.ndarray:
    if cfg.skip_input:
        size = cfg.input_part_size * (cfg.merge_factor if cfg.skip_first_stage else 1)
        return create_partition(size)
    if cfg.s3_buckets:
        return s3_utils.download_s3(pinfo)
    with open(pinfo.path, "rb", buffering=cfg.io_size) as fin:
        return np.fromfile(fin, dtype=np.uint8)


def save_partition(
    cfg: AppConfig, pinfo: PartInfo, merger: Iterable[np.ndarray]
) -> List[PartInfo]:
    if cfg.skip_output:
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
        return [pinfo]
    if cfg.s3_buckets:
        return s3_utils.multipart_upload(cfg, pinfo, merger)
    with open(pinfo.path, "wb", buffering=cfg.io_size) as fout:
        for datachunk in merger:
            fout.write(datachunk)
    return [pinfo]


# ------------------------------------------------------------
#     Initialization
# ------------------------------------------------------------


@ray.remote
def make_data_dirs(cfg: AppConfig):
    os.makedirs(constants.TMPFS_PATH, exist_ok=True)
    if cfg.s3_buckets and cfg.spilling == SpillingMode.S3:
        return
    for prefix in cfg.data_dirs:
        for kind in constants.FILENAME_FMT.keys():
            os.makedirs(os.path.join(prefix, kind), exist_ok=True)


def init(cfg: AppConfig):
    ray.get(ray_utils.run_on_all_workers(cfg, make_data_dirs, include_current=True))
    logging.info("Created data directories on all nodes")


# ------------------------------------------------------------
#     Generate Input
# ------------------------------------------------------------


def part_info(
    cfg: AppConfig,
    part_id: PartId,
    *,
    kind: str = "input",
    s3: bool = False,
) -> PartInfo:
    if s3:
        shard = hash(str(part_id)) & constants.S3_SHARD_MASK
        path = _get_part_path(part_id, shard=shard, kind=kind)
        bucket = cfg.s3_buckets[shard % len(cfg.s3_buckets)]
        return PartInfo(part_id, None, bucket, path)
    data_dir_idx = part_id % len(cfg.data_dirs)
    prefix = cfg.data_dirs[data_dir_idx]
    filepath = _get_part_path(part_id, prefix=prefix, kind=kind)
    node = (
        cfg.worker_ips[part_id % cfg.num_workers]
        if cfg.is_local_cluster
        else ray.util.get_node_ip_address()
    )
    return PartInfo(part_id, node, None, filepath)


def _get_part_path(
    part_id: PartId,
    *,
    prefix: Path = "",
    shard: Optional[int] = None,
    kind: str = "input",
) -> Path:
    filename_fmt = constants.FILENAME_FMT[kind]
    filename = filename_fmt.format(part_id=part_id)
    parts = [prefix, kind]
    if shard is not None:
        parts.append(constants.SHARD_FMT.format(shard=shard))
    parts.append(filename)
    return os.path.join(*parts)


def _run_gensort(offset: int, size: int, path: str, buf: bool = False):
    # Add `,buf` to use buffered I/O instead of direct I/O (for tmpfs).
    if buf:
        path += ",buf"
    subprocess.run(
        f"{constants.GENSORT_PATH} -b{offset} {size} {path}", shell=True, check=True
    )


@ray.remote
def generate_part(
    cfg: AppConfig,
    part_id: PartId,
    size: RecordCount,
    offset: RecordCount,
) -> PartInfo:
    with tracing_utils.timeit("generate_part"):
        assert size > 0, (cfg, size)
        logging_utils.init()
        if cfg.s3_buckets:
            pinfo = part_info(cfg, part_id, s3=True)
            path = os.path.join(constants.TMPFS_PATH, f"{part_id:010x}")
        else:
            pinfo = part_info(cfg, part_id)
            path = pinfo.path
        _run_gensort(offset, size, path, bool(cfg.s3_buckets))
        if cfg.s3_buckets:
            s3_utils.upload_s3(
                path,
                pinfo,
                max_concurrency=max(1, cfg.io_parallelism // cfg.map_parallelism),
            )
        logging.info("Generated input %s", pinfo)
        return pinfo


@ray.remote
def drop_fs_cache(_: AppConfig):
    subprocess.run(
        "sudo bash -c 'sync; echo 3 > /proc/sys/vm/drop_caches'", check=True, shell=True
    )
    logging.info("Dropped filesystem cache")


def generate_input(cfg: AppConfig):
    if cfg.skip_input:
        return
    total_size = constants.bytes_to_records(cfg.total_data_size)
    size = constants.bytes_to_records(cfg.input_part_size)
    offset = 0
    tasks = []
    for m in range(cfg.num_mappers_per_worker):
        for w in range(cfg.num_workers):
            part_id = constants.merge_part_ids(w, m)
            tasks.append(
                generate_part.options(**ray_utils.node_i(cfg, w)).remote(
                    cfg, part_id, min(size, total_size - offset), offset
                )
            )
            offset += size
    logging.info("Generating %d partitions", len(tasks))
    parts = ray.get(tasks)
    with open(get_manifest_file(cfg), "w") as fout:
        writer = csv.writer(fout)
        for pinfo in parts:
            writer.writerow(pinfo.to_csv_row())
    if not cfg.s3_buckets:
        ray.get(ray_utils.run_on_all_workers(cfg, drop_fs_cache))


def create_partition(part_size: int) -> np.ndarray:
    # TODO(@lsf): replace this with gensort
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
        f"{constants.VALSORT_PATH} {argstr}",
        check=True,
        shell=True,
        capture_output=True,
    )
    if proc.returncode != 0:
        logging.critical("\n%s", proc.stderr.decode("ascii"))
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
def validate_part(cfg: AppConfig, pinfo: PartInfo) -> Tuple[int, bytes]:
    logging_utils.init()
    if cfg.s3_buckets:
        tmp_path = os.path.join(constants.TMPFS_PATH, os.path.basename(pinfo.path))
        s3_utils.download_s3(pinfo, tmp_path)
        ret = _validate_part_impl(tmp_path, buf=True)
        os.remove(tmp_path)
    else:
        ret = _validate_part_impl(pinfo.path)
    logging.info("Validated output %s", pinfo)
    return ret


def validate_output(cfg: AppConfig):
    if cfg.skip_sorting or cfg.skip_output:
        return
    parts = load_manifest(cfg, kind="output")
    results = []
    for pinfo in parts:
        opt = (
            ray_utils.node_ip_aff(cfg, pinfo.node)
            if pinfo.node
            else {"resources": {constants.WORKER_RESOURCE: 1e-3}}
        )
        results.append(validate_part.options(**opt).remote(cfg, pinfo))
    logging.info("Validating %d partitions", len(results))
    results = ray.get(results)
    total = sum(sz for sz, _ in results)
    assert total == cfg.total_data_size, total - cfg.total_data_size
    all_checksum = b"".join(chksm for _, chksm in results)
    with tempfile.NamedTemporaryFile() as fout:
        fout.write(all_checksum)
        fout.flush()
        _run_valsort(f"-s {fout.name}")
    logging.info("All OK!")
