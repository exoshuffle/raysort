# pylint: disable=too-many-instance-attributes
import dataclasses
import enum
import time
from typing import Callable, Optional, TypeVar

import numpy as np
import ray

from raysort import tracing_utils

AppConfig = TypeVar("AppConfig")
M = TypeVar("M")  # Map output type
R = TypeVar("R")  # Reduce output type
S = TypeVar("S")  # Reduce summary type


class ShuffleStrategy(enum.Enum):
    SIMPLE = "SIMPLE"
    STREAMING = "STREAMING"
    PUSH_BASED = "PUSH_BASED"


@dataclasses.dataclass
class ShuffleConfig:
    num_mappers: int
    num_mappers_per_round: int
    num_reducers: int
    num_rounds: int = dataclasses.field(init=False)

    map_fn: Callable[[AppConfig, int], list[M]]
    reduce_fn: Callable[[AppConfig, R, list[M]], R]

    summary_map_fn: Optional[Callable[[AppConfig, R], S]] = None
    summary_reduce_fn: Optional[Callable[[AppConfig, list[S]], S]] = None
    summary_print_fn: Optional[Callable[[AppConfig, S], None]] = None

    strategy: ShuffleStrategy = ShuffleStrategy.SIMPLE
    is_cluster: bool = True

    def __post_init__(self):
        self.num_rounds = int(np.ceil(self.num_mappers / self.num_mappers_per_round))


def _ray_remote(
    fn: Optional[Callable],
    is_cluster: bool = True,
    *,
    timeit: bool = True,
    **kwargs: dict,
) -> Callable:
    if fn is None:
        return None
    if is_cluster:
        kwargs["resources"] = {"worker": 1e-3}
        kwargs["scheduling_strategy"] = "SPREAD"
    if timeit:
        fn = tracing_utils.timeit_wrapper(fn)
    if len(kwargs) == 0:
        return ray.remote(fn)
    return ray.remote(**kwargs)(fn)


class ShuffleManager:
    def __init__(self, cfg: ShuffleConfig, app_cfg: AppConfig):
        self.cfg = cfg
        self.app_cfg = app_cfg

        self.map_remote = _ray_remote(
            cfg.map_fn,
            cfg.is_cluster,
            num_returns=cfg.num_reducers,
        )
        self.reduce_remote = _ray_remote(
            cfg.reduce_fn,
            cfg.is_cluster,
        )
        self.summary_map_remote = _ray_remote(
            cfg.summary_map_fn, cfg.is_cluster, timeit=False
        )
        self.summarize_remote = _ray_remote(self._summarize, False)

        self.start_time = time.time()
        self.rounds_completed = 0

    def run(self):
        if self.cfg.strategy == ShuffleStrategy.SIMPLE:
            return self._simple_shuffle()
        if self.cfg.strategy == ShuffleStrategy.STREAMING:
            return self._streaming_shuffle()
        raise NotImplementedError

    def _simple_shuffle(self):
        map_results = np.empty(
            (self.cfg.num_mappers, self.cfg.num_reducers), dtype=object
        )
        reduce_states = [None] * self.cfg.num_reducers
        for i in range(self.cfg.num_mappers):
            map_results[i, :] = self.map_remote.remote(self.app_cfg, i)
        for r, reduce_state in enumerate(reduce_states):
            reduce_states[r] = self.reduce_remote.remote(
                self.app_cfg, reduce_state, *map_results[:, r].tolist()
            )
        ray.get(self.summarize_remote.remote(reduce_states))

    def _streaming_shuffle(self):
        reduce_states = [None] * self.cfg.num_reducers
        for rnd in range(self.cfg.num_rounds):
            map_results = np.array(
                [
                    self.map_remote.remote(
                        self.app_cfg, self.cfg.num_mappers_per_round * rnd + i
                    )
                    for i in range(self.cfg.num_mappers_per_round)
                ]
            )
            to_wait = [r for r in reduce_states if r is not None]
            ray.wait(to_wait, num_returns=len(to_wait), fetch_local=False)
            for r, reduce_state in enumerate(reduce_states):
                reduce_states[r] = self.reduce_remote.remote(
                    self.app_cfg, reduce_state, *map_results[:, r].tolist()
                )
            self.summarize_remote.remote(reduce_states)
        ray.get(self.summarize_remote.remote(reduce_states))

    def _summarize(self, reduce_states: list[R]):
        if self.summary_map_remote is None:
            return
        summaries = ray.get(
            [
                self.summary_map_remote.remote(self.app_cfg, state)
                for state in reduce_states
            ]
        )
        summary = self.cfg.summary_reduce_fn(self.app_cfg, summaries)
        now = time.time()
        print("===== partial summary =====")
        print(f"time elapsed: {now - self.start_time:.1f}s")
        self.cfg.summary_print_fn(self.app_cfg, summary)
        print()


def shuffle(cfg: ShuffleConfig, app_cfg: AppConfig):
    mgr = ShuffleManager(cfg, app_cfg)
    return mgr.run()
