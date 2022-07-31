import collections
import dataclasses
import heapq
import io

import numpy as np
import pyarrow.compute as pc
import pyarrow.parquet as pq
import ray

from raysort import s3_utils


@dataclasses.dataclass
class AppConfig:
    num_mappers: int = 646
    num_mappers_per_round: int = 8
    num_rounds: int = dataclasses.field(init=False)
    num_reducers: int = 8
    top_k: int = 15

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
    buf = download_s3(cfg, f"wiki/wiki-{part_id:04d}.parquet")
    table = pq.read_table(buf)
    text = table[3]
    wordlists = pc.split_pattern_regex(pc.utf8_lower(text), r"\W+")
    return [w.as_py() for words in wordlists for w in words]


def get_reducer_id_for_word(cfg: AppConfig, word: str) -> int:
    return ord(word[0]) % cfg.num_reducers if len(word) > 0 else 0


@ray.remote
def mapper(cfg: AppConfig, mapper_id: int, reducers: list[ray.actor.ActorHandle]):
    if mapper_id >= cfg.num_mappers:
        return
    words = load_partition(cfg, mapper_id)
    counters = [collections.Counter() for _ in reducers]
    for word in words:
        idx = get_reducer_id_for_word(cfg, word)
        counters[idx][word] += 1
    tasks = []
    for reducer, counter in zip(reducers, counters):
        tasks.append(reducer.add_map_results.remote([counter]))
    ray.get(tasks)


@ray.remote
class Reducer:
    def __init__(self, cfg: AppConfig, reducer_id: int):
        self.cfg = cfg
        self.reducer_id = reducer_id
        self.counter = collections.Counter()

    def add_map_results(self, map_results: list[dict]):
        for counter in map_results:
            self.counter += counter

    def get_top_words(self) -> list[tuple[str, int]]:
        return self.counter.most_common(self.cfg.top_k)


def final_reduce(
    cfg: AppConfig, most_commons: list[tuple[str, int]]
) -> list[tuple[str, int]]:
    heap = []
    for most_common in most_commons:
        for word, count in most_common:
            heapq.heappush(heap, (-count, word))
    return [(word, -neg_count) for neg_count, word in heap[: cfg.top_k]]


def print_top_words(cfg: AppConfig, reducers: list[ray.actor.ActorHandle]):
    top_words_list = ray.get([reducer.get_top_words.remote() for reducer in reducers])
    top_words = final_reduce(cfg, top_words_list)
    for word, count in top_words:
        print(word, count, end=" " * 4)
    print()


def mpo_main():
    cfg = AppConfig()
    reducers = [
        Reducer.remote(cfg, reducer_id) for reducer_id in range(cfg.num_reducers)
    ]
    for rnd in range(cfg.num_rounds):
        print(f"===== round {rnd} =====")
        tasks = [
            mapper.remote(cfg, cfg.num_mappers_per_round * rnd + i, reducers)
            for i in range(cfg.num_mappers_per_round)
        ]
        ray.get(tasks)
        print_top_words(cfg, reducers)

    print("===== final result =====")
    print_top_words(cfg, reducers)


def main():
    ray.init()
    mpo_main()


if __name__ == "__main__":
    main()
