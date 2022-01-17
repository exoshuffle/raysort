# RaySort

## Development Setup

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

## Starting up a Cluster

1. Install Terraform: `$RAYSORT_ROOT/terraform/setup.sh`.
2. Go to `$RAYSORT_ROOT/terraform/aws`. If you don't have [direnv](https://direnv.net/), manually run the content of `.envrc`.
3. In `$RAYSORT_ROOT/terraform/aws`, run `terraform apply` to launch a cluster of AWS instances.
4. Run Ansible to set up the worker nodes: `$RAYSORT_ROOT/ansible/setup.py`.
5. Start Ray: `$RAYSORT_ROOT/ansible/start_ray.sh`.
6. Run a test run on the cluster: `python raysort/main.py --total_tb=0.1 --use_object_store 2>&1 | tee main.log`


## Misc

### FIO test

Code for testing disk bandwidth.

```
sudo fio --directory=. --ioengine=psync --name fio_test_file --direct=1 --rw=write --bs=1m --size=1g --numjobs=8
sudo fio --directory=. --ioengine=psync --name fio_test_file --direct=1 --rw=read --bs=1m --size=1g --numjobs=8
```
