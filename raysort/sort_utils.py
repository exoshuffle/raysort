import csv
import logging
import os
import subprocess
import tempfile
import time
from typing import Iterable, List, Optional, Tuple

import numpy as np
import ray

from raysort import (
    azure_utils,
    constants,
    logging_utils,
    ray_utils,
    s3_utils,
    tracing_utils,
)
from raysort.config import AppConfig
from raysort.typing import PartId, PartInfo, Path, RecordCount, SpillingMode

# ------------------------------------------------------------
#     Loading and Saving Partitions
# ------------------------------------------------------------


def get_manifest_file(cfg: AppConfig, kind: str = "input") -> Path:
    suffix = "cloud" if cfg.cloud_storage else "fs"
    return constants.MANIFEST_FMT.format(kind=kind, suffix=suffix)


def load_manifest(cfg: AppConfig, kind: str = "input") -> List[PartInfo]:
    if cfg.skip_input and kind == "input":
        return [
            PartInfo(
                i,
                cfg.worker_ips[i % cfg.num_workers],
                None,
                "",
                cfg.input_part_size,
                None,
            )
            for i in range(cfg.num_mappers)
        ]
    path = get_manifest_file(cfg, kind=kind)
    with open(path) as fin:
        reader = csv.reader(fin)
        ret = [PartInfo.from_csv_row(row) for row in reader]
        return ret


def load_partitions(cfg: AppConfig, pinfolist: List[PartInfo]) -> np.ndarray:
    if len(pinfolist) == 1:
        return load_partition(cfg, pinfolist[0])
    if cfg.s3_buckets:
        return s3_utils.download_parallel(
            pinfolist, cfg.input_shard_size, cfg.map_io_parallelism
        )
    if cfg.azure_containers:
        # TODO(@lsf): not necessary to implement for now.
        pass
    return np.concatenate([load_partition(cfg, pinfo) for pinfo in pinfolist])


def load_partition(cfg: AppConfig, pinfo: PartInfo) -> np.ndarray:
    if cfg.skip_input:
        size = cfg.input_part_size * (cfg.merge_factor if cfg.skip_first_stage else 1)
        return create_partition(size)
    if cfg.s3_buckets:
        return s3_utils.download(
            pinfo,
            size=cfg.input_part_size,
            max_concurrency=cfg.map_io_parallelism,
        )
    if cfg.azure_containers:
        return azure_utils.download(pinfo)
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
    if cfg.azure_containers:
        return azure_utils.multipart_upload(cfg, pinfo, merger)
    bytes_count = 0
    with open(pinfo.path, "wb", buffering=cfg.io_size) as fout:
        for datachunk in merger:
            fout.write(datachunk)
            bytes_count += datachunk.size
    pinfo.size = bytes_count
    return [pinfo]


# ------------------------------------------------------------
#     Initialization
# ------------------------------------------------------------


@ray.remote
def make_data_dirs(cfg: AppConfig):
    os.makedirs(constants.TMPFS_PATH, exist_ok=True)
    if cfg.cloud_storage and cfg.spilling == SpillingMode.S3:
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
    cloud: bool = False,
) -> PartInfo:
    if cloud:
        shard = hash(str(part_id)) & constants.S3_SHARD_MASK
        path = _get_part_path(part_id, shard=shard, kind=kind)
        if cfg.s3_buckets:
            bucket = cfg.s3_buckets[shard % len(cfg.s3_buckets)]
        elif cfg.azure_containers:
            bucket = cfg.azure_containers[shard % len(cfg.azure_containers)]
        else:
            raise ValueError("No cloud storage configured")
        return PartInfo(part_id, None, bucket, path, 0, None)
    data_dir_idx = part_id % len(cfg.data_dirs)
    prefix = cfg.data_dirs[data_dir_idx]
    filepath = _get_part_path(part_id, prefix=prefix, kind=kind)
    node = (
        cfg.worker_ips[part_id % cfg.num_workers]
        if cfg.is_local_cluster
        else ray.util.get_node_ip_address()
    )
    return PartInfo(part_id, node, None, filepath, 0, None)


def _get_part_path(
    part_id: PartId,
    *,
    prefix: Path = "",
    shard: Optional[int] = None,
    kind: str = "input",
) -> Path:
    filename_fmt = constants.FILENAME_FMT[kind]
    filename = filename_fmt.format(part_id=part_id)
    parts = [prefix]
    if shard is not None:
        parts.append(constants.SHARD_FMT.format(shard=shard))
    parts.append(filename)
    return os.path.join(*parts)


def _run_gensort(offset: int, size: int, path: str, buf: bool = False) -> str:
    # Add `,buf` to use buffered I/O instead of direct I/O (for tmpfs).
    if buf:
        path += ",buf"
    proc = subprocess.run(
        f"{constants.GENSORT_PATH} -c -b{offset} {size} {path}",
        shell=True,
        check=True,
        stderr=subprocess.PIPE,
        text=True,
    )
    return proc.stderr.strip()


@ray.remote
def generate_part(
    cfg: AppConfig,
    part_id: PartId,
    size: RecordCount,
    offset: RecordCount,
) -> PartInfo:
    with tracing_utils.timeit("generate_part"):
        logging_utils.init()
        if cfg.cloud_storage:
            pinfo = part_info(cfg, part_id, cloud=True)
            path = os.path.join(constants.TMPFS_PATH, f"{part_id:010x}")
        else:
            pinfo = part_info(cfg, part_id)
            path = pinfo.path
        pinfo.size = size * constants.RECORD_SIZE
        pinfo.checksum = _run_gensort(offset, size, path, cfg.cloud_storage)
        if cfg.s3_buckets:
            s3_utils.upload(
                path,
                pinfo,
                max_concurrency=max(1, cfg.io_parallelism // cfg.map_parallelism),
            )
        elif cfg.azure_containers:
            azure_utils.upload(path, pinfo)
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
    size = constants.bytes_to_records(cfg.input_shard_size)
    offset = 0
    tasks = []
    for m in range(cfg.num_mappers_per_worker):
        for w in range(cfg.num_workers):
            for i in range(cfg.num_shards_per_mapper):
                if offset >= total_size:
                    break
                part_id = constants.merge_part_ids(w, m, i)
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
    if not cfg.cloud_storage:
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


def _run_valsort(argstr: str) -> str:
    proc = subprocess.run(
        f"{constants.VALSORT_PATH} {argstr}",
        check=True,
        shell=True,
        capture_output=True,
        text=True,
    )
    if proc.returncode != 0:
        logging.critical("\n%s", proc.stderr.decode("ascii"))
        raise RuntimeError(f"Validation failed: {argstr}")
    return proc.stderr


def _validate_part_impl(pinfo: PartInfo, path: Path, buf: bool = False) -> bytes:
    filesize = os.path.getsize(path)
    assert filesize == pinfo.size, (pinfo, filesize)
    sum_path = path + ".sum"
    argstr = f"-o {sum_path} {path}"
    if buf:
        argstr += ",buf"
    _run_valsort(argstr)
    with open(sum_path, "rb") as fin:
        return fin.read()


@ray.remote
def validate_part(cfg: AppConfig, pinfo: PartInfo) -> bytes:
    logging_utils.init()
    with tracing_utils.timeit("validate_part"):
        if cfg.cloud_storage:
            tmp_path = os.path.join(constants.TMPFS_PATH, os.path.basename(pinfo.path))
            if cfg.s3_buckets:
                s3_utils.download(pinfo, filename=tmp_path)
            elif cfg.azure_containers:
                azure_utils.download(pinfo, filename=tmp_path)
            ret = _validate_part_impl(pinfo, tmp_path, buf=True)
            os.remove(tmp_path)
        else:
            ret = _validate_part_impl(pinfo, pinfo.path)
        logging.info("Validated output %s", pinfo)
        return ret


def compare_checksums(input_checksums: List[str], output_summary: str) -> bool:
    assert "Checksum: " in output_summary, output_summary
    checksum_line = output_summary.split("Checksum: ")[1]
    output_checksum = checksum_line.split()[0]
    input_checksum = sum(input_checksums)
    input_checksum = str(hex(input_checksum))[2:]
    input_checksum = input_checksum[-len(output_checksum) :]
    assert (
        input_checksum == output_checksum
    ), f"Mismatched checksums: {input_checksum} {output_checksum} ||| {str(hex(sum(input_checksums)))} ||| {output_summary}"
    logging.info(output_summary)


def validate_output(cfg: AppConfig):
    if cfg.skip_sorting or cfg.skip_output:
        return
    parts = load_manifest(cfg, kind="output")
    total_bytes = sum(p.size for p in parts)
    assert total_bytes == cfg.total_data_size, total_bytes - cfg.total_data_size

    results = []
    for pinfo in parts:
        opt = (
            ray_utils.node_ip_aff(cfg, pinfo.node)
            if pinfo.node
            else {"resources": {constants.WORKER_RESOURCE: 1e-3}}
        )
        opt["num_cpus"] = int(np.ceil(cfg.output_part_size / 2_000_000_000))
        results.append(validate_part.options(**opt).remote(cfg, pinfo))
    logging.info("Validating %d partitions", len(results))
    results = ray.get(results)

    all_checksum = b"".join(results)
    with open(get_manifest_file(cfg), "r") as fin:
        reader = csv.reader(fin)
        input_checksums = [int(row[-1], 16) for row in reader]

    with tempfile.NamedTemporaryFile() as fout:
        fout.write(all_checksum)
        fout.flush()
        output_summary = _run_valsort(f"-s {fout.name}")
        compare_checksums(input_checksums, output_summary)

    logging.info("All OK!")
