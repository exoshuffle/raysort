import ray

from raysort import tracing_utils


def test_get_spilling_stats():
    try:
        ray.init()
        ret = tracing_utils._get_spilling_stats()  # pylint: disable=protected-access
        assert ret["spilled_gb"] == 0, ret
        assert ret["restored_gb"] == 0, ret
    finally:
        ray.shutdown()
