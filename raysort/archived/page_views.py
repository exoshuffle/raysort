# pylint: disable=too-many-instance-attributes
import collections
import csv
import dataclasses
import io
import json

import pandas as pd
import ray

from raysort import s3_utils, tracing_utils
from raysort.shuffle_lib import ShuffleConfig, ShuffleStrategy, shuffle


def get_partition_paths():
    partitions = []
    for i in range(1, 6, 2):
        for j in range(1, 32):
            for k in range(0, 24):
                partitions.append(f"wikimedia/pagecounts-20160{i}{j:02d}-{k:02d}0000")
    return partitions


NUM_PARTITIONS = 2232
PARTITION_PATHS = get_partition_paths()
TOP_LANGUAGES = (
    "en ja es de ru fr it zh pt pl commons nl tr www ar id sv fa ko cs".split()
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
    df = pd.read_csv(
        buf,
        sep=" ",
        names=["language", "title", "requests", "size"],
        quoting=csv.QUOTE_NONE,
    )
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
    df = load_partition(cfg, mapper_id)
    grouped = df.groupby(df["language"])["requests"].sum()
    grouped = grouped.groupby(lambda language: language.split(".")[0]).sum()
    grouped = grouped.groupby(
        lambda language: get_reducer_id_for_language(cfg, language)
    )
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


def top_languages_proportions_reduce(_cfg: AppConfig, most_commons: list[S]) -> S:
    return pd.concat(most_commons).sort_values(ascending=False)[:]


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


def top_languages_proportions_print(_cfg: AppConfig, summary: S):
    total = sum(summary.values())
    language_proportions = {}

    language_proportions = {lang: count / total for lang, count in summary.items()}
    print(language_proportions)

    with open("/tmp/language_proportions.json", "a") as fp:
        print(json.dumps(language_proportions), file=fp)


def page_views_main():
    shuffle_cfg = ShuffleConfig(
        num_mappers=NUM_PARTITIONS,
        num_mappers_per_round=40,
        num_reducers=10,
        map_fn=mapper,
        reduce_fn=reducer,
        summary_map_fn=top_languages_map,
        summary_reduce_fn=top_languages_proportions_reduce,
        summary_print_fn=top_languages_proportions_print,
        # strategy=ShuffleStrategy.SIMPLE,
        strategy=ShuffleStrategy.STREAMING,
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
    page_views_main()


if __name__ == "__main__":
    main()
