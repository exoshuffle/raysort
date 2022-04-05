import collections

import ray


@ray.remote
def mapper():
    return [
        {"apple": 3, "banana": 1},
        {"hello": 2, "world": 5},
    ]


@ray.remote
class Reducer:
    def __init__(self):
        self.heap = collections.defaultdict(int)

    def add(self, map_result):
        for key, value in map_result.items():
            self.heap[key] += value
        print(self.topk())

    def topk(self, k=3):
        return sorted(self.heap.items(), key=lambda x: x[1], reverse=True)[:k]


def main():
    num_mappers = 2
    num_reducers = 2

    all_map_out = [
        mapper.options(num_returns=num_reducers).remote() for _ in range(num_mappers)
    ]
    reducers = [Reducer.remote() for _ in range(num_reducers)]
    map_out_to_id = {r[0]: i for i, r in enumerate(all_map_out)}

    map_out_remaining = list(map_out_to_id)
    while len(map_out_remaining) > 0:
        ready, map_out_remaining = ray.wait(map_out_remaining)
        map_out = all_map_out[map_out_to_id[ready[0]]]
        for result, reducer in zip(map_out, reducers):
            reducer.add.remote(result)

    print("OK")


if __name__ == "__main__":
    main()
