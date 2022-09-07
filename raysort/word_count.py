# pylint: disable=too-many-instance-attributes
import collections
import dataclasses
import heapq
import io

import pandas as pd
import ray

from raysort import s3_utils, tracing_utils
from raysort.shuffle_lib import ShuffleConfig, ShuffleStrategy, shuffle

NUM_PARTITIONS = 646
PARTITION_PATHS = [
    f"wiki/wiki-{part_id:04d}.parquet" for part_id in range(NUM_PARTITIONS)
]
TOP_WORDS = "the of is in and as for was with a he had at also from american on were would to".split()


@dataclasses.dataclass
class AppConfig:
    shuffle: ShuffleConfig

    top_k: int = 20
    s3_bucket: str = "lsf-berkeley-edu"


def flatten(xss: list[list]) -> list:
    return [x for xs in xss for x in xs]


def download_s3(cfg: AppConfig, url: str) -> io.BytesIO:
    buf = io.BytesIO()
    s3_utils.s3().download_fileobj(cfg.s3_bucket, url, buf)
    buf.seek(0)
    return buf


def load_partition(cfg: AppConfig, part_id: int) -> pd.Series:
    with tracing_utils.timeit("load_partition", report_completed=False):
        with tracing_utils.timeit("s3_download", report_completed=False):
            buf = download_s3(cfg, PARTITION_PATHS[part_id])
        df = pd.read_parquet(buf)
        return df["text"].str.lower().str.split().explode()


def get_reducer_id_for_word(cfg: AppConfig, word: str) -> int:
    ch = ord(word[0]) if len(word) > 0 else 0
    return ch % cfg.shuffle.num_reducers


M = collections.Counter[str]
R = collections.Counter[str]
S = tuple[str, int]


def mapper(cfg: AppConfig, mapper_id: int) -> list[M]:
    if mapper_id >= cfg.shuffle.num_mappers:
        return [None for _ in range(cfg.shuffle.num_reducers)]
    with tracing_utils.timeit("map"):
        words = load_partition(cfg, mapper_id)
        word_counts = words.value_counts()
        counters = [collections.Counter() for _ in range(cfg.shuffle.num_reducers)]
        for word, cnt in word_counts.iteritems():
            idx = get_reducer_id_for_word(cfg, word)
            counters[idx][word] += cnt
        return counters


def reducer(_cfg: AppConfig, state: R, *map_results: list[M]) -> R:
    with tracing_utils.timeit("reduce"):
        if state is None:
            state = collections.Counter()
        for map_result in map_results:
            if map_result:
                state += map_result
        return state


def top_words_map(cfg: AppConfig, state: R) -> list[S]:
    return state.most_common(cfg.top_k)


def top_words_reduce(cfg: AppConfig, most_commons: list[S]) -> list[S]:
    heap = []
    for most_common in most_commons:
        for word, count in most_common:
            heapq.heappush(heap, (-count, word))
    return [(word, -neg_count) for neg_count, word in heap[: cfg.top_k]]


def top_words_print(_cfg: AppConfig, summary: list[S]):
    num_correct = 0
    correct_so_far = True
    for (word, count), truth_word in zip(summary, TOP_WORDS):
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
    )
    app_cfg = AppConfig(shuffle=shuffle_cfg)
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
