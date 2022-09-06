# pylint: disable=too-many-instance-attributes
import dataclasses
import enum
import time
from typing import Callable, Optional, TypeVar

import numpy as np
import ray

AppConfig = TypeVar("AppConfig")
M = TypeVar("M")  # Map output type
R = TypeVar("R")  # Reduce output type
S = TypeVar("S")  # Reduce summary type


class ShuffleStrategy(enum.Enum):
    SIMPLE = enum.auto()
    STREAMING = enum.auto()
    PUSH_BASED = enum.auto()


@dataclasses.dataclass
class ShuffleConfig:
    num_mappers: int
    num_mappers_per_round: int
    num_reducers: int
    num_rounds: int = dataclasses.field(init=False)

    map_fn: Callable[[AppConfig, int], list[M]]
    reduce_fn: Callable[[AppConfig, R, list[M]], R]

    # Summary functions are only used for the streaming shuffle.
    summary_map_fn: Optional[Callable[[AppConfig, R], S]] = None
    summary_reduce_fn: Optional[Callable[[AppConfig, list[S]], S]] = None
    summary_print_fn: Optional[Callable[[S], None]] = None

    strategy: ShuffleStrategy = ShuffleStrategy.SIMPLE

    start_time: float = 0
    round_start_time: float = 0

    def __post_init__(self):
        self.num_rounds = int(np.ceil(self.num_mappers / self.num_mappers_per_round))


def _print_partial_state(
    cfg: AppConfig,
    reduce_states: list[ray.ObjectRef],
    summary_map_fn: Callable[[AppConfig, R], S],
    summary_reduce_fn: Callable[[AppConfig, list[S]], S],
    summary_print_fn: Callable[[AppConfig, S], None],
):
    if summary_map_fn is None or summary_reduce_fn is None or summary_print_fn is None:
        return
    map_remote = _ray_remote(cfg, summary_map_fn)
    summaries = ray.get([map_remote.remote(cfg, state) for state in reduce_states])
    summary = summary_reduce_fn(cfg, summaries)
    now = time.time()
    print(
        f"this round: {now - cfg.round_start_time:.1f}s, total: {now - cfg.start_time:.1f}s"
    )
    summary_print_fn(cfg, summary)
    print()
    cfg.round_start_time = now


def _ray_remote(cfg: AppConfig, fn: Callable, **kwargs: dict) -> Callable:
    if not cfg.local_mode:
        kwargs["resources"] = {"worker": 1e-3}
        kwargs["scheduling_strategy"] = "SPREAD"
    if len(kwargs) == 0:
        return ray.remote(fn)
    return ray.remote(**kwargs)(fn)


def _streaming_shuffle(app_cfg: AppConfig, shuffle_cfg: ShuffleConfig):
    map_remote = _ray_remote(
        app_cfg, shuffle_cfg.map_fn, num_returns=shuffle_cfg.num_reducers
    )
    reduce_remote = _ray_remote(app_cfg, shuffle_cfg.reduce_fn)
    print_partial_state = lambda: _print_partial_state(
        app_cfg,
        reduce_states,
        shuffle_cfg.summary_map_fn,
        shuffle_cfg.summary_reduce_fn,
        shuffle_cfg.summary_print_fn,
    )

    reduce_states = [None for _ in range(shuffle_cfg.num_reducers)]
    shuffle_cfg.start_time = time.time()
    shuffle_cfg.round_start_time = shuffle_cfg.start_time
    for rnd in range(shuffle_cfg.num_rounds):
        print(f"===== round {rnd} =====")
        map_results = np.array(
            [
                map_remote.remote(app_cfg, shuffle_cfg.num_mappers_per_round * rnd + i)
                for i in range(shuffle_cfg.num_mappers_per_round)
            ]
        )
        for r, reduce_state in enumerate(reduce_states):
            reduce_states[r] = reduce_remote.remote(
                app_cfg, reduce_state, *map_results[:, r].tolist()
            )
        print_partial_state()

    print("===== final result =====")
    print_partial_state()


def shuffle(app_cfg: AppConfig, shuffle_cfg: ShuffleConfig):
    if shuffle_cfg.strategy == ShuffleStrategy.STREAMING:
        return _streaming_shuffle(app_cfg, shuffle_cfg)
    raise NotImplementedError
