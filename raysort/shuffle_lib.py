import time
from typing import Callable, TypeVar

import numpy as np
import ray

AppConfig = TypeVar("AppConfig")
M = TypeVar("M")  # Map output type
R = TypeVar("R")  # Reduce output type
S = TypeVar("S")  # Reduce summary type


def _print_partial_state(
    cfg: AppConfig,
    reduce_states: list[ray.ObjectRef],
    summary_map_fn: Callable[[AppConfig, R], S],
    summary_reduce_fn: Callable[[AppConfig, list[S]], S],
    summary_print_fn: Callable[[AppConfig, S], None],
):
    map_remote = ray.remote(summary_map_fn)
    summaries = ray.get([map_remote.remote(cfg, state) for state in reduce_states])
    summary = summary_reduce_fn(cfg, summaries)
    now = time.time()
    print(
        f"this round: {now - cfg.round_start_time:.1f}s, total: {now - cfg.start_time:.1f}s"
    )
    summary_print_fn(cfg, summary)
    print()
    cfg.round_start_time = now


def streaming_shuffle(
    cfg: AppConfig,
    map_fn: Callable[[AppConfig, int], list[M]],
    reduce_fn: Callable[[AppConfig, R, list[M]], R],
    summary_map_fn: Callable[[AppConfig, R], S],
    summary_reduce_fn: Callable[[AppConfig, list[S]], S],
    summary_print_fn: Callable[[S], None],
):
    map_remote = ray.remote(num_returns=cfg.num_reducers)(map_fn)
    reduce_remote = ray.remote(reduce_fn)
    print_partial_state = lambda: _print_partial_state(
        cfg,
        reduce_states,
        summary_map_fn,
        summary_reduce_fn,
        summary_print_fn,
    )

    reduce_states = [None for _ in range(cfg.num_reducers)]
    cfg.start_time = time.time()
    cfg.round_start_time = cfg.start_time
    for rnd in range(cfg.num_rounds):
        print(f"===== round {rnd} =====")
        map_results = np.array(
            [
                map_remote.remote(cfg, cfg.num_mappers_per_round * rnd + i)
                for i in range(cfg.num_mappers_per_round)
            ]
        )
        for r, reduce_state in enumerate(reduce_states):
            reduce_states[r] = reduce_remote.remote(
                cfg, reduce_state, *map_results[:, r].tolist()
            )
        print_partial_state()

    print("===== final result =====")
    print_partial_state()
