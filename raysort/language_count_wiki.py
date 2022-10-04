# pylint: disable=too-many-instance-attributes
import collections
import dataclasses
import io

import pandas as pd
import ray

from raysort import s3_utils, tracing_utils
from raysort.shuffle_lib import ShuffleConfig, ShuffleStrategy, shuffle

NUM_PARTITIONS = 32
PARTITION_PATHS = [
    "wikimedia/pagecounts-20160101-000000",
    "wikimedia/pagecounts-20160101-010000",
    "wikimedia/pagecounts-20160101-020000",
    "wikimedia/pagecounts-20160101-030000",
    "wikimedia/pagecounts-20160101-040000",
    "wikimedia/pagecounts-20160101-050000",
    "wikimedia/pagecounts-20160101-060000",
    "wikimedia/pagecounts-20160101-070000",
    "wikimedia/pagecounts-20160101-080000",
    "wikimedia/pagecounts-20160101-090000",
    "wikimedia/pagecounts-20160101-100000",
    "wikimedia/pagecounts-20160101-110000",
    "wikimedia/pagecounts-20160101-120000",
    "wikimedia/pagecounts-20160101-130000",
    "wikimedia/pagecounts-20160101-140000",
    "wikimedia/pagecounts-20160101-150000",
    "wikimedia/pagecounts-20160101-160000",
    "wikimedia/pagecounts-20160101-170000",
    "wikimedia/pagecounts-20160101-180000",
    "wikimedia/pagecounts-20160101-190000",
    "wikimedia/pagecounts-20160101-200000",
    "wikimedia/pagecounts-20160101-210000",
    "wikimedia/pagecounts-20160101-220000",
    "wikimedia/pagecounts-20160101-230000",
    "wikimedia/pagecounts-20160102-000000",
    "wikimedia/pagecounts-20160102-010000",
    "wikimedia/pagecounts-20160102-020000",
    "wikimedia/pagecounts-20160102-030000",
    "wikimedia/pagecounts-20160102-040000",
    "wikimedia/pagecounts-20160102-050000",
    "wikimedia/pagecounts-20160102-060000",
    "wikimedia/pagecounts-20160102-070000"
]
TOP_LANGUAGES = (
    "en en.mw ja.mw de.mw ru de es.mw ja fr zh fr.mw ru.mw es it.mw commons.m it pl pt.mw www.wd pt".split()
)


@dataclasses.dataclass
class AppConfig:
    shuffle: ShuffleConfig

    fail_node: bool = False
    top_k: int = 20
    s3_bucket: str = "lsf-berkeley-edu"


def download_s3(cfg: AppConfig, url: str) -> io.BytesIO:
    buf = io.BytesIO()
    s3_utils.s3().download_fileobj(cfg.s3_bucket, url, buf)
    buf.seek(0)
    return buf


def load_partition(cfg: AppConfig, part_id: int) -> pd.DataFrame:
    buf = download_s3(cfg, PARTITION_PATHS[part_id])
    df = pd.read_csv(buf, sep=" ", names=["language", "title", "requests", "size"])
    return df


def get_reducer_id_for_language(cfg: AppConfig, language: str) -> int:
    ch = ord(language[0]) if len(language) > 0 else 0
    return ch % cfg.shuffle.num_reducers


M = pd.Series
R = collections.Counter[str, int]
S = pd.Series


def mapper(cfg: AppConfig, mapper_id: int) -> list[M]:
    if mapper_id >= cfg.shuffle.num_mappers:
        return [pd.Series(dtype=str) for _ in range(cfg.shuffle.num_reducers)]
    languages = load_partition(cfg, mapper_id)
    grouped = languages.groupby(languages["language"])["requests"].sum()
    grouped = grouped.groupby(lambda language: get_reducer_id_for_language(cfg, language))
    assert len(grouped) == cfg.shuffle.num_reducers, grouped
    return [group for _, group in grouped]


def reducer(_cfg: AppConfig, state: R, *map_results: list[M]) -> R:
    if state is None:
        state = collections.Counter()
    for map_result in map_results:
        state += map_result.to_dict()
    return state


def top_languages_map(cfg: AppConfig, state: R) -> S:
    if state is None:
        return pd.Series(dtype=str)
    language_counts = state.most_common(cfg.top_k)
    return pd.Series(dict(language_counts))


def top_languages_reduce(cfg: AppConfig, most_commons: list[S]) -> S:
    return pd.concat(most_commons).sort_values(ascending=False)[: cfg.top_k]


def top_languages_print(_cfg: AppConfig, summary: S):
    num_correct = 0
    correct_so_far = True
    for (language, count), truth_language in zip(summary.items(), TOP_LANGUAGES):
        print(language, count, end=" " * 4)
        if correct_so_far and language == truth_language:
            num_correct += 1
        else:
            correct_so_far = False
    print(f"\nTop {num_correct} languages are correct")


def word_count_main():
    shuffle_cfg = ShuffleConfig(
        num_mappers=NUM_PARTITIONS,
        num_mappers_per_round=32,
        num_reducers=8,
        map_fn=mapper,
        reduce_fn=reducer,
        summary_map_fn=top_languages_map,
        summary_reduce_fn=top_languages_reduce,
        summary_print_fn=top_languages_print,
        strategy=ShuffleStrategy.SIMPLE,
        # strategy=ShuffleStrategy.STREAMING,
    )
    app_cfg = AppConfig(shuffle=shuffle_cfg)
    # app_cfg.fail_node = True
    print(app_cfg)
    if shuffle_cfg.is_cluster:
        ray.init("auto")
    else:
        ray.init()
    tracker = tracing_utils.create_progress_tracker(app_cfg)
    with tracing_utils.timeit("language_count"):
        shuffle(shuffle_cfg, app_cfg)
    ray.get(tracker.performance_report.remote())


def main():
    word_count_main()


if __name__ == "__main__":
    main()
