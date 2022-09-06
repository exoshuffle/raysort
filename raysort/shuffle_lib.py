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
    summary_print_fn: Optional[Callable[[AppConfig, S], None]] = None

    strategy: ShuffleStrategy = ShuffleStrategy.SIMPLE

    # Runtime states.
    start_time: float = 0
    round_start_time: float = 0
    reduce_states: list[R] = dataclasses.field(default_factory=list)

    def __post_init__(self):
        self.num_rounds = int(np.ceil(self.num_mappers / self.num_mappers_per_round))
        self.reduce_states = [None] * self.num_reducers


def _print_partial_state(app_cfg: AppConfig, shuffle_cfg: ShuffleConfig):
    if (
        shuffle_cfg.summary_map_fn is None
        or shuffle_cfg.summary_reduce_fn is None
        or shuffle_cfg.summary_print_fn is None
    ):
        return
    map_remote = _ray_remote(app_cfg, shuffle_cfg.summary_map_fn)
    summaries = ray.get(
        [map_remote.remote(app_cfg, state) for state in shuffle_cfg.reduce_states]
    )
    summary = shuffle_cfg.summary_reduce_fn(app_cfg, summaries)
    now = time.time()
    print(
        f"this round: {now - shuffle_cfg.round_start_time:.1f}s, total: {now - shuffle_cfg.start_time:.1f}s"
    )
    shuffle_cfg.summary_print_fn(app_cfg, summary)
    print()
    shuffle_cfg.round_start_time = now


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
        shuffle_cfg,
    )

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
        for r, reduce_state in enumerate(shuffle_cfg.reduce_states):
            shuffle_cfg.reduce_states[r] = reduce_remote.remote(
                app_cfg, reduce_state, *map_results[:, r].tolist()
            )
        print_partial_state()

    print("===== final result =====")
    print_partial_state()


def shuffle(app_cfg: AppConfig, shuffle_cfg: ShuffleConfig):
    if shuffle_cfg.strategy == ShuffleStrategy.STREAMING:
        return _streaming_shuffle(app_cfg, shuffle_cfg)
    raise NotImplementedError
