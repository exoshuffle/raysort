from collections import OrderedDict
import argparse
import os
import json
import ray
import time
import timeit

import torch.optim as optim
import numpy as np
import torch
import horovod.torch as hvd
from horovod.ray import RayExecutor

from ray_shuffling_data_loader.data_generation import DATA_SPEC
from ray_shuffling_data_loader.embedding_model import MyModel, annotation, huber_loss
from ray.data.dataset_pipeline import DatasetPipeline

# Training settings
parser = argparse.ArgumentParser(description="Dataset ingestion Example")
parser.add_argument(
    "--batch-size",
    type=int,
    default=250000,
    metavar="N",
    help="input batch size for training (default: 64)",
)
parser.add_argument(
    "--epochs",
    type=int,
    default=10,
    metavar="N",
    help="number of epochs to train (default: 10)",
)
parser.add_argument("--debug", action="store_true", default=False, help="disables hvd")
parser.add_argument(
    "--seed", type=int, default=42, metavar="S", help="random seed (default: 42)"
)
parser.add_argument(
    "--log-interval",
    type=int,
    default=10,
    metavar="N",
    help=("how many batches to wait before logging training " "status"),
)
parser.add_argument("--num-workers", type=int, default=10)
parser.add_argument("--num-files", type=int, default=30)
parser.add_argument("--num-windows", type=int, default=1)
parser.add_argument("--manual-windows", type=bool, default=False)
parser.add_argument("--parallelism", type=int, default=400)
parser.add_argument(
    "--no-shuffle",
    action="store_true",
    default=False,
    help="disables per-epoch shuffle",
)
parser.add_argument("--local", action="store_true", default=False, help="run locally")
parser.add_argument("--num-batches", type=int, default=-1)
parser.add_argument("--mock-sleep-time", type=int, default=-1)

SIZE_50_G = 30  # 49.17GB
SIZE_100_G = 62  # 101.62GB
SIZE_500_G = 305  # 499.93GB
SIZE_1500_G = 915


def construct_optimizers(model):
    sparse_params = []
    dense_params = []
    for k, v in model.named_parameters():
        if "input.embeddings.embeddings" in k:
            sparse_params.append((k, v))
        else:
            dense_params.append((k, v))

    optimizers = []
    if len(dense_params) > 0:
        opt = optim.Adam([v for _, v in dense_params], lr=0.001)
        opt = hvd.DistributedOptimizer(opt, dense_params)
        optimizers.append(opt)
    if len(sparse_params) > 0:
        opt = optim.SparseAdam([v for _, v in sparse_params], lr=0.001)
        opt = hvd.DistributedOptimizer(opt, sparse_params)
        optimizers.append(opt)

    if hvd.rank() == 0:
        print(optimizers)

    return optimizers


def train_main(args, splits):
    # Horovod: initialize library.
    hvd.init()
    torch.manual_seed(args.seed)

    if torch.cuda.is_available():
        # Horovod: pin GPU to local rank.
        torch.cuda.set_device(hvd.local_rank())
        torch.cuda.manual_seed(args.seed)

    # Horovod: limit # of CPU threads to be used per worker.
    torch.set_num_threads(1)
    rank = hvd.rank()

    if args.mock_sleep_time < 0:
        model = MyModel(annotation, use_bn=False)
        # By default, Adasum doesn"t need scaling up learning rate.
        if torch.cuda.is_available():
            # Move model to GPU.
            model.cuda()

        optimizers = construct_optimizers(model)
        loss_function = huber_loss
        # Horovod: broadcast parameters & optimizer state.
        hvd.broadcast_parameters(model.state_dict(), root_rank=0)
        for opt in optimizers:
            hvd.broadcast_optimizer_state(opt, root_rank=0)

    def _train(epoch, train_dataset):
        if args.mock_sleep_time < 0:
            model.train()
        # Horovod: set epoch to sampler for shuffling.
        # train_dataset.set_epoch(epoch)
        start_epoch = timeit.default_timer()
        last_batch_time = start_epoch
        batch_wait_times = []
        print("torch.cuda.is_available?", torch.cuda.is_available())
        for batch_idx, (data, target) in enumerate(train_dataset):
            print(f"Processing batch {batch_idx} in epoch {epoch} on worker {rank}.")
            if args.num_batches > 0 and batch_idx > args.num_batches:
                break

            batch_wait_times.append(timeit.default_timer() - last_batch_time)
            batch_wait_time = timeit.default_timer() - last_batch_time
            print(f"Batch wait time: {batch_wait_time}")
            batch_wait_times.append(batch_wait_time)

            batch_start_time = timeit.default_timer()
            if torch.cuda.is_available():
                data = data.cuda()
                target = target.cuda()
            if args.mock_sleep_time < 0:
                for opt in optimizers:
                    opt.zero_grad()
            batch = OrderedDict()
            batch["embeddings"] = OrderedDict()
            batch["one_hot"] = OrderedDict()
            for i, name in enumerate(annotation["embeddings"]):
                batch["embeddings"][name] = data[:, i : i + 1]
            batch["one_hot"]["hot0"] = data[:, -2:-1]
            batch["one_hot"]["hot1"] = data[:, -1:]

            if args.mock_sleep_time > 0:
                time.sleep(args.mock_sleep_time)
                last_batch_time = timeit.default_timer()
                continue

            batch_pred = model(batch)
            forward_end_time = timeit.default_timer()
            print("Forward pass time:", forward_end_time - batch_start_time)

            loss = loss_function(batch_pred, target, delta=60)
            loss.mean().backward()
            for opt in optimizers:
                opt.step()
            print("Optimize time: ", timeit.default_timer() - forward_end_time)

            last_batch_time = timeit.default_timer()

            if batch_idx % args.log_interval == 0:
                print("Total batch time:", timeit.default_timer() - batch_start_time)
        epoch_duration = timeit.default_timer() - start_epoch
        print("epoch duration", epoch_duration)
        avg_batch_wait_time = np.mean(batch_wait_times)
        std_batch_wait_time = np.std(batch_wait_times)
        max_batch_wait_time = np.max(batch_wait_times)
        min_batch_wait_time = np.min(batch_wait_times)
        print(
            f"\nEpoch {epoch}, worker {rank} stats over "
            f"{len(batch_wait_times)} steps: {epoch_duration:.3f}"
        )
        print(
            f"Mean batch wait time: {avg_batch_wait_time:.3f}s +- "
            f"{std_batch_wait_time}"
        )
        print(f"Max batch wait time: {max_batch_wait_time:.3f}s")
        print(f"Min batch wait time: {min_batch_wait_time:.3f}s")
        return batch_wait_times

    print(f"Starting training on worker {rank}.")
    batch_wait_times = []
    for epoch, split_ds in enumerate(splits[rank].iter_epochs()):
        train_dataset = create_torch_iterator(split_ds, args.batch_size, rank)
        new_batch_times = _train(epoch, train_dataset)
        new_batch_times.pop(0)
        batch_wait_times.extend(new_batch_times)
    print(f"Done training on worker {rank}.")
    avg_batch_wait_time = np.mean(batch_wait_times)
    std_batch_wait_time = np.std(batch_wait_times)
    max_batch_wait_time = np.max(batch_wait_times)
    min_batch_wait_time = np.min(batch_wait_times)
    print(f"\nWorker {rank} training stats over {args.epochs} epochs:")
    print(
        f"Mean batch wait time: {avg_batch_wait_time:.3f}s +- " f"{std_batch_wait_time}"
    )
    print(f"Max batch wait time: {max_batch_wait_time:.3f}s")
    print(f"Min batch wait time: {min_batch_wait_time:.3f}s")


######################################################

numpy_to_torch_dtype = {
    np.bool: torch.bool,
    np.uint8: torch.uint8,
    np.int8: torch.int8,
    np.int16: torch.int16,
    np.int32: torch.int32,
    np.int64: torch.int64,
    np.float16: torch.float16,
    np.float32: torch.float32,
    np.float64: torch.float64,
    np.complex64: torch.complex64,
    np.complex128: torch.complex128,
}


def create_torch_iterator(split, batch_size, rank=None):
    print(
        f"Creating Torch shuffling dataset for worker {rank} with "
        f"{batch_size} batch size."
    )
    feature_columns = list(DATA_SPEC.keys())
    feature_types = [numpy_to_torch_dtype[dtype] for _, _, dtype in DATA_SPEC.values()]
    label_column = feature_columns.pop()
    label_type = feature_types.pop()

    # TODO: print first 10 elements of batch (should be different each epoch
    # if shuffle, else same)

    torch_iterator = split.to_torch(
        label_column=label_column,
        feature_columns=feature_columns,
        label_column_dtype=label_type,
        feature_column_dtypes=feature_types[0],
        batch_size=batch_size,
        # prefetch_blocks: int = 0,
        # drop_last: bool = False
    )
    return torch_iterator


def create_dataset(
    files,
    num_workers=4,
    epochs=50,
    num_windows=1,
    manual_windowing=False,
    parallelism=400,
    shuffle=True,
):
    if num_windows > 1 and manual_windowing:
        num_rows = ray.data.read_parquet(
            files
        ).count()  # This should only read Parquet metadata.
        file_splits = np.array_split(files, num_windows)

        class Windower:
            def __init__(self):
                self.i = 0
                self.iterations = epochs * num_windows

            def __iter__(self):
                return self

            def __next__(self):
                if self.i >= self.iterations:
                    raise StopIteration()
                split = file_splits[self.i % num_windows]
                self.i += 1
                return lambda: ray.data.read_parquet(list(split))

        pipe = DatasetPipeline.from_iterable(Windower())
        split_indices = [
            i * num_rows // num_windows // num_workers for i in range(1, num_workers)
        ]
        if shuffle:
            pipe = pipe.random_shuffle_each_window()
        pipe_shards = pipe.split_at_indices(split_indices)
    else:
        ds = ray.data.read_parquet(files, parallelism=parallelism)
        if num_windows > 1:
            window_size = max(ds.num_blocks() // num_windows, 1)
            ds = ds.window(blocks_per_window=window_size)
        pipe = ds.repeat(epochs)
        if shuffle:
            pipe = pipe.random_shuffle_each_window()
        pipe_shards = pipe.split(num_workers, equal=True)
    return pipe_shards


@ray.remote
def consume(split, rank=None, batch_size=None):
    torch_iterator = create_torch_iterator(split, batch_size=batch_size, rank=rank)
    start = time.perf_counter()
    batch_start = start
    batch_wait_time = []
    num_batches = 0
    for i, (x, y) in enumerate(torch_iterator):
        num_batches += 1
        batch_wait = time.perf_counter() - batch_start
        batch_wait_time.append(batch_wait)
        if i % 10 == 0:
            print(f"Consumer #{rank} finishes batch #{i}")
        batch_start = time.perf_counter()

    duration = time.perf_counter() - start
    t50 = np.quantile(batch_wait_time, 0.5)
    t95 = np.quantile(batch_wait_time, 0.95)
    tmax = np.max(batch_wait_time)
    print(
        f"Consumer #{rank} total time: {duration}, total batches: {num_batches}, "
        f"P50/P95/Max batch wait time (s): {t50}/{t95}/{tmax}."
    )

    return


if __name__ == "__main__":
    args = parser.parse_args()
    import ray

    print("Connecting to Ray cluster...")
    num_files = (
        args.num_files // 10
    ) * args.num_workers  # scale data for number of workers
    if args.local:
        ray.init(num_gpus=1)
    else:
        ray.init(address="auto")
        num_files = args.num_files

    files = [
        f"s3://ray-shuffling-data-loader/data/r10_000_000_000-f1000"
        f"/input_data_{i}.parquet.snappy"
        for i in range(num_files)
    ]

    start = time.time()

    print("Shuffle set to", (not args.no_shuffle))

    splits = create_dataset(
        files,
        num_workers=args.num_workers,
        epochs=args.epochs,
        num_windows=args.num_windows,
        manual_windowing=args.manual_windows,
        parallelism=args.parallelism,
        shuffle=(not args.no_shuffle),
    )

    print("Created splits")

    if args.debug:
        tasks = [
            consume.options(num_gpus=1, num_cpus=0).remote(
                split, rank=idx, batch_size=args.batch_size
            )
            for idx, split in enumerate(splits)
        ]
        ray.get(tasks)
    else:
        print("Create Ray executor")
        settings = RayExecutor.create_settings(timeout_s=30)
        executor = RayExecutor(settings, num_workers=args.num_workers, use_gpu=True)
        print("Starting executor")
        executor.start()
        executor.run(train_main, args=[args, splits])
        executor.shutdown()

    delta = time.time() - start
    print(f"success! total time {delta}")
    with open(os.environ.get("TEST_OUTPUT_JSON", "output.json"), "w") as f:
        f.write(json.dumps({"ingest_time": delta, "success": 1}))
