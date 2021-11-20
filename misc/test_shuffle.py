import numpy as np
import ray

ray.init("auto")

M = 16
R = 4
REDUCER_BYTES = 1024 * 1024 * 1024 * 6


@ray.remote(resources={"worker": 0.1 / 4})
def mapper(m, reducers):
    n_bytes = int(REDUCER_BYTES / R)
    buffers = [
        np.frombuffer(np.random.bytes(n_bytes), dtype=np.uint8)
        for _ in reducers
    ]
    return [r.inc.remote(m, b) for r, b in zip(reducers, buffers)]


@ray.remote(resources={"worker": 0.9})
class Reducer:
    def __init__(self, r):
        self.count = 0
        self.r = r

    def get_count(self):
        return self.count

    def inc(self, m, _payload):
        print(f"receive_block r={self.r}, m={m}")
        self.count += 1
        if self.count == M:
            print("OK")
        return self.count


reducers = [Reducer.remote(r) for r in range(R)]
mapper_results = [mapper.remote(m, reducers) for m in range(M)]

mapper_results = ray.get(mapper_results)
mapper_results = [ray.get(results) for results in mapper_results]
reducer_results = ray.get([r.get_count.remote() for r in reducers])
print(reducer_results)
