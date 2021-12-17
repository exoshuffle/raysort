# RaySort

## Setup

If you created your VM using the `raysort-general` image, you should already have a Conda environment ready. Otherwise, install [Miniconda3](https://docs.conda.io/en/latest/miniconda.html) and run the following:

```
conda create -n raysort python=3.9.7
pip install -Ur requirements/dev.txt
pip install -Ur requirements/worker.txt
pip install -e .
pushd raysort/sortlib && python setup.py build_ext --inplace && popd
scripts/install_binaries.sh
```

## Running Locally

A full end-to-end smoke run:

```bash
python raysort/main.py --total_tb=0.01 --input_part_size=100_000_000 --ray_address= 2>&1 | tee local.log
```

A quicker run, skipping input:

```bash
python raysort/main.py --total_tb=0.01 --input_part_size=100_000_000 --use_object_store --skip_input --sort --ray_address= 2>&1 | tee local.log
```

Notes:
* Run `python raysort/main.py --help` to see description of arguments.
* Specifying `--ray_address=` (empty address) will make Raysort launch a new (local) Ray cluster to start the run. If not, it will try to connect to an existing one using `ray.init("auto")`.
* By default, Raysort calls `gensort` to generate input data and `valsort` to validate output data on disk. If you want to skip both or either steps, set `--skip_input` or `--skip_output`. If input is skipped, Raysort will generate input data on the fly using `np.random`.


# Misc


## FIO test

Code for testing disk bandwidth.

```
sudo fio --directory=. --ioengine=psync --name fio_test_file --direct=1 --rw=write --bs=1m --size=1g --numjobs=8
sudo fio --directory=. --ioengine=psync --name fio_test_file --direct=1 --rw=read --bs=1m --size=1g --numjobs=8
```
