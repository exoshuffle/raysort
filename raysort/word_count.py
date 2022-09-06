# pylint: disable=too-many-instance-attributes
import collections
import dataclasses
import heapq
import io

import numpy as np
import pandas as pd
import ray

from raysort import s3_utils
from raysort import tracing_utils
from raysort.shuffle_lib import streaming_shuffle

TOP_WORDS = "the of is in and as for was with a he had at also from american on were would to".split()


@dataclasses.dataclass
class AppConfig:
    num_mappers: int = 646
    num_mappers_per_round: int = 32
    num_rounds: int = dataclasses.field(init=False)
    num_reducers: int = 8
    top_k: int = 20
    local_mode: bool = False

    start_time: float = 0
    round_start_time: float = 0

    s3_bucket: str = "lsf-berkeley-edu"

    def __post_init__(self):
        self.num_rounds = int(np.ceil(self.num_mappers / self.num_mappers_per_round))


def flatten(xss: list[list]) -> list:
    return [x for xs in xss for x in xs]


def download_s3(cfg: AppConfig, url: str) -> io.BytesIO:
    buf = io.BytesIO()
    s3_utils.s3().download_fileobj(cfg.s3_bucket, url, buf)
    buf.seek(0)
    return buf


def load_partition(cfg: AppConfig, part_id: int) -> list[str]:
    with tracing_utils.timeit("load_partition", report_completed=False):
        with tracing_utils.timeit("s3_download", report_completed=False):
            buf = download_s3(cfg, f"wiki/wiki-{part_id:04d}.parquet")
        df = pd.read_parquet(buf)
        words = df["text"].str.lower().str.split().tolist()
        return flatten(words)


def get_reducer_id_for_word(cfg: AppConfig, word: str) -> int:
    ch = ord(word[0]) if len(word) > 0 else 0
    return ch % cfg.num_reducers


M = collections.Counter[str]
R = collections.Counter[str]
S = tuple[str, int]


def mapper(cfg: AppConfig, mapper_id: int) -> list[M]:
    if mapper_id >= cfg.num_mappers:
        return [None for _ in range(cfg.num_reducers)]
    # print(ray.util.get_node_ip_address())
    with tracing_utils.timeit("map"):
        words = load_partition(cfg, mapper_id)
        counters = [collections.Counter() for _ in range(cfg.num_reducers)]
        for word in words:
            idx = get_reducer_id_for_word(cfg, word)
            counters[idx][word] += 1
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
    cfg = AppConfig()
    if cfg.local_mode:
        ray.init()
    else:
        ray.init("auto")
    tracker = tracing_utils.create_progress_tracker(cfg)
    streaming_shuffle(
        cfg,
        mapper,
        reducer,
        top_words_map,
        top_words_reduce,
        top_words_print,
    )
    ray.get(tracker.performance_report.remote())


def main():
    word_count_main()


if __name__ == "__main__":
    main()
