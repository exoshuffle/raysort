import collections
import dataclasses
import heapq
import io
from typing import TypeVar

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


V = TypeVar("V")


@ray.remote
def mapper(cfg: AppConfig, mapper_id: int) -> list[V]:
    if mapper_id >= cfg.num_mappers:
        return [None for _ in range(cfg.num_reducers)]
    words = load_partition(cfg, mapper_id)
    counters = [collections.Counter() for _ in range(cfg.num_reducers)]
    for word in words:
        idx = get_reducer_id_for_word(cfg, word)
        counters[idx][word] += 1
    return counters


@ray.remote
def reduce(_cfg: AppConfig, state: V, *map_results: list[V]) -> V:
    if state is None:
        state = collections.Counter()
    for map_result in map_results:
        state += map_result
    return state


@ray.remote
def get_top_words(cfg: AppConfig, state: V) -> list[tuple[str, int]]:
    return state.most_common(cfg.top_k)


def final_reduce(
    cfg: AppConfig, most_commons: list[tuple[str, int]]
) -> list[tuple[str, int]]:
    heap = []
    for most_common in most_commons:
        for word, count in most_common:
            heapq.heappush(heap, (-count, word))
    return [(word, -neg_count) for neg_count, word in heap[: cfg.top_k]]


def print_top_words(cfg: AppConfig, reduce_states: list[ray.ObjectRef]):
    top_words_list = ray.get(
        [get_top_words.remote(cfg, state) for state in reduce_states]
    )
    top_words = final_reduce(cfg, top_words_list)
    for word, count in top_words:
        print(word, count, end=" " * 4)
    print()


def mpo_main():
    cfg = AppConfig()
    reduce_states = [None for _ in range(cfg.num_reducers)]
    for rnd in range(cfg.num_rounds):
        print(f"===== round {rnd} =====")
        map_results = np.array(
            [
                mapper.options(num_returns=cfg.num_reducers).remote(
                    cfg, cfg.num_mappers_per_round * rnd + i
                )
                for i in range(cfg.num_mappers_per_round)
            ]
        )
        for r, reduce_state in enumerate(reduce_states):
            reduce_states[r] = reduce.remote(cfg, reduce_state, *map_results[:, r].tolist())
        print_top_words(cfg, reduce_states)

    print("===== final result =====")
    print_top_words(cfg, reduce_states)


def main():
    ray.init()
    mpo_main()


if __name__ == "__main__":
    main()
