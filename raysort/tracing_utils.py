import collections
import functools
import json
import logging
import os
import re
import time
from typing import Dict

import ray
from ray.util import metrics
import wandb

from raysort import constants
from raysort import logging_utils

Span = collections.namedtuple(
    "Span",
    ["time", "duration", "event", "address", "pid"],
)


def timeit(
    event: str,
    report_in_progress=True,
    report_completed=True,
    log_to_wandb=False,
):
    def decorator(f):
        @functools.wraps(f)
        def wrapped_f(*args, **kwargs):
            tracker = get_progress_tracker()
            tracker.inc.remote(
                f"{event}_in_progress",
                echo=report_in_progress,
            )
            if event == "sort":
                tracker.record_start_time.remote()
            try:
                begin = time.time()
                ret = f(*args, **kwargs)
                duration = time.time() - begin
                tracker.record_span.remote(
                    Span(
                        begin,
                        duration,
                        event,
                        ray.util.get_node_ip_address(),
                        os.getpid(),
                    ),
                    log_to_wandb=log_to_wandb,
                )
                tracker.inc.remote(
                    f"{event}_completed",
                    echo=report_completed,
                )
                return ret
            finally:
                tracker.dec.remote(f"{event}_in_progress")

        return wrapped_f

    return decorator


def record_value(*args, **kwargs):
    progress_tracker = get_progress_tracker()
    progress_tracker.record_value.remote(*args, **kwargs)


def get_progress_tracker():
    return ray.get_actor(constants.PROGRESS_TRACKER_ACTOR)


def create_progress_tracker(args):
    return ProgressTracker.options(name=constants.PROGRESS_TRACKER_ACTOR).remote(args)


def _make_trace_event(span: Span):
    return {
        "cat": span.event,
        "name": span.event,
        "pid": span.address,
        "tid": span.pid,
        "ts": span.time * 1_000_000,
        "dur": span.duration * 1_000_000,
        "ph": "X",
        "args": {},
    }


def _get_spilling_stats(print_ray_stats: bool = False) -> Dict[str, float]:
    summary = ray.internal.internal_api.memory_summary(
        ray.worker._global_node.address, stats_only=True
    )
    if print_ray_stats:
        print(summary)

    def mib_to_gb(x: float) -> float:
        return x * 1024 * 1024 / 1000**3

    def extract_gb(regex: str) -> float:
        matches = re.findall(regex, summary)
        return mib_to_gb(float(matches[0])) if len(matches) > 0 else 0

    return {
        "spilled_gb": extract_gb(r"Spilled (\d+) MiB"),
        "restored_gb": extract_gb(r"Restored (\d+) MiB"),
    }


@ray.remote(resources={"head": 1e-3})
class ProgressTracker:
    def __init__(self, args):
        self.run_args = args
        gauges = [
            "map_in_progress",
            "merge_in_progress",
            "reduce_in_progress",
            "sort_in_progress",
            "map_completed",
            "merge_completed",
            "reduce_completed",
            "sort_completed",
        ]
        self.counts = {m: 0 for m in gauges}
        self.gauges = {m: metrics.Gauge(m) for m in gauges}
        self.series = collections.defaultdict(list)
        self.spans = []
        self.reset_gauges()
        self.initial_spilling_stats = _get_spilling_stats()
        self.start_time = None
        logging_utils.init()
        logging.info(args)
        try:
            wandb.init(entity="raysort", project="raysort")
        except wandb.errors.UsageError as e:
            if "call wandb.login" in e.message:
                wandb.init(mode="offline")
            else:
                raise e
        wandb.config.update(args)
        wandb.config.update(
            {k: v for k, v in os.environ.items() if k.startswith("RAY_")}
        )

    def reset_gauges(self):
        for g in self.gauges.values():
            g.set(0)

    def inc(self, metric_name, value=1, echo=False, log_to_wandb=False):
        gauge = self.gauges.get(metric_name)
        if gauge is None:
            logging.warning(f"No such gauge: {metric_name}")
            return
        self.counts[metric_name] += value
        gauge.set(self.counts[metric_name])
        if echo:
            logging.info(f"{metric_name} {self.counts[metric_name]}")
        if log_to_wandb:
            wandb.log({metric_name: self.counts[metric_name]})

    def dec(self, metric_name, value=1, echo=False):
        return self.inc(metric_name, -value, echo)

    def record_value(
        self,
        metric_name,
        value,
        relative_to_start=False,
        echo=False,
        log_to_wandb=False,
    ):
        if relative_to_start and self.start_time:
            value -= self.start_time
        self.series[metric_name].append(value)
        if echo:
            logging.info(f"{metric_name} {value}")
        if log_to_wandb:
            wandb.log({metric_name: value})

    def record_span(self, span: Span, record_value=True, log_to_wandb=False):
        self.spans.append(span)
        if record_value:
            self.record_value(span.event, span.duration, log_to_wandb=log_to_wandb)

    def record_start_time(self):
        self.start_time = time.time()

    def report(self):
        import pandas as pd

        ret = []
        for key, values in self.series.items():
            ss = pd.Series(values)
            ret.append(
                [
                    key,
                    ss.mean(),
                    ss.std(),
                    ss.max(),
                    ss.min(),
                    ss.count(),
                ]
            )
        df = pd.DataFrame(ret, columns=["task", "mean", "std", "max", "min", "count"])
        print(self.series.get("output_time"))
        print(df.set_index("task"))
        print(self.run_args)
        wandb.log({"performance_summary": wandb.Table(dataframe=df)})
        wandb.finish()

    def report_spilling(self):
        stats = _get_spilling_stats(print_ray_stats=True)
        for k, v in stats.items():
            v0 = self.initial_spilling_stats.get(k, 0)
            wandb.log({k: v - v0})

    def save_trace(self):
        self.spans.sort(key=lambda span: span.time)
        ret = [_make_trace_event(span) for span in self.spans]
        filename = f"/tmp/raysort-{self.start_time}.json"
        with open(filename, "w") as fout:
            json.dump(ret, fout)
        wandb.save(filename, base_path="/tmp")

    def performance_report(self):
        self.save_trace()
        self.report_spilling()
        self.report()
