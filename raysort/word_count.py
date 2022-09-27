# pylint: disable=too-many-instance-attributes
import collections
import dataclasses
import io

import pandas as pd
import ray

from raysort import s3_utils, tracing_utils
from raysort.shuffle_lib import ShuffleConfig, ShuffleStrategy, shuffle

NUM_PARTITIONS = 646
PARTITION_PATHS = [
    f"wiki/wiki-{part_id:04d}.parquet" for part_id in range(NUM_PARTITIONS)
]
TOP_WORDS = (
    "the of in and a to was is for as on by with from he at that his it an".split()
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


def load_partition(cfg: AppConfig, part_id: int) -> pd.Series:
    buf = download_s3(cfg, PARTITION_PATHS[part_id])
    df = pd.read_parquet(buf)
    return df["text"].str.lower().str.split().explode().rename()


def get_reducer_id_for_word(cfg: AppConfig, word: str) -> int:
    ch = ord(word[0]) if len(word) > 0 else 0
    return ch % cfg.shuffle.num_reducers


M = pd.Series
R = collections.Counter[str, int]
S = pd.Series


def mapper(cfg: AppConfig, mapper_id: int) -> list[M]:
    if mapper_id >= cfg.shuffle.num_mappers:
        return [pd.Series(dtype=str) for _ in range(cfg.shuffle.num_reducers)]
    words = load_partition(cfg, mapper_id)
    word_counts = words.value_counts(sort=False)
    grouped = word_counts.groupby(lambda word: get_reducer_id_for_word(cfg, word))
    assert len(grouped) == cfg.shuffle.num_reducers, grouped
    return [group for _, group in grouped]


def reducer(_cfg: AppConfig, state: R, *map_results: list[M]) -> R:
    if state is None:
        state = collections.Counter()
    for map_result in map_results:
        state += map_result.to_dict()
    return state


def top_words_map(cfg: AppConfig, state: R) -> S:
    if state is None:
        return pd.Series(dtype=str)
    word_counts = state.most_common(cfg.top_k)
    return pd.Series(dict(word_counts))


def top_words_reduce(cfg: AppConfig, most_commons: list[S]) -> S:
    return pd.concat(most_commons).sort_values(ascending=False)[: cfg.top_k]


def top_words_print(_cfg: AppConfig, summary: S):
    num_correct = 0
    correct_so_far = True
    for (word, count), truth_word in zip(summary.items(), TOP_WORDS):
        print(word, count, end=" " * 4)
        if correct_so_far and word == truth_word:
            num_correct += 1
        else:
            correct_so_far = False
    print(f"\nTop {num_correct} words are correct")


def word_count_main():
    shuffle_cfg = ShuffleConfig(
        num_mappers=NUM_PARTITIONS,
        num_mappers_per_round=32,
        num_reducers=8,
        map_fn=mapper,
        reduce_fn=reducer,
        summary_map_fn=top_words_map,
        summary_reduce_fn=top_words_reduce,
        summary_print_fn=top_words_print,
        strategy=ShuffleStrategy.SIMPLE,
        # strategy=ShuffleStrategy.STREAMING,
    )
    app_cfg = AppConfig(shuffle=shuffle_cfg)
    #app_cfg.fail_node = True
    print(app_cfg)
    if shuffle_cfg.is_cluster:
        ray.init("auto")
    else:
        ray.init()
    tracker = tracing_utils.create_progress_tracker(app_cfg)
    with tracing_utils.timeit("word_count"):
        shuffle(shuffle_cfg, app_cfg)
    ray.get(tracker.performance_report.remote())


def main():
    word_count_main()


if __name__ == "__main__":
    main()
