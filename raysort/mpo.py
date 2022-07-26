import collections
import dataclasses
import heapq

import numpy as np
import ray


@dataclasses.dataclass
class AppConfig:
    num_mappers: int = 100
    num_mappers_per_round: int = 8
    num_rounds: int = dataclasses.field(init=False)
    num_reducers: int = 8
    top_k: int = 10

    def __post_init__(self):
        self.num_rounds = int(np.ceil(self.num_mappers / self.num_mappers_per_round))


def load_partition(part_id: int) -> list[str]:
    with open(f"/mnt/data0/words/words-{part_id:03d}.txt") as fin:
        return [line.strip() for line in fin]


def get_reducer_id_for_word(cfg: AppConfig, word: str) -> int:
    return ord(word[0]) % cfg.num_reducers


@ray.remote
def mapper(cfg: AppConfig, mapper_id: int, reducers: list[ray.actor.ActorHandle]):
    words = load_partition(mapper_id)
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
            mapper.remote(cfg, i, reducers) for i in range(cfg.num_mappers_per_round)
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
