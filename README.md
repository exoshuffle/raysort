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

A full end-to-end test run:

```bash
python raysort/main.py --total_gb=10 --input_part_size=100_000_000 --ray_address= 2>&1 | tee local.log
```

A quicker run, skipping input:

```bash
python raysort/main.py --total_gb=10 --input_part_size=100_000_000 --skip_input --sort --ray_address= 2>&1 | tee local.log
```

Notes:

- Run `python raysort/main.py --help` to see description of arguments.
- Specifying `--ray_address=` (empty address) will make Raysort launch a new (local) Ray cluster to start the run. If not, it will try to connect to an existing one using `ray.init("auto")`.
- By default, Raysort calls `gensort` to generate input data and `valsort` to validate output data on disk. If you want to skip both or either steps, set `--skip_input` or `--skip_output`. If input is skipped, Raysort will generate input data on the fly using `np.random`.

## Starting up a Cluster

1. Install Terraform: `$RAYSORT_ROOT/terraform/setup.sh`
2. Go to `$RAYSORT_ROOT/terraform/aws`. If you don't have [direnv](https://direnv.net/), manually run the content of `.envrc`.
3. In `$RAYSORT_ROOT/terraform/aws`, run `terraform apply` to launch a cluster of AWS instances.
4. Make sure you can ssh into one of the worker nodes using `ssh -i ~/.aws/login-us-west-2.pem -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null <worker_ip>`. If you cannot, it's likely because your current VM is not in the same security group as the worker nodes (which are in the `default` security group). The easiest solution is to find your instance on the AWS EC2 UI, right click "Security -> Change Security Groups", and add your instance to the `default` security group. TODO: this might be possible to automate in Terraform.
5. Run Ansible to set up the worker nodes: `$RAYSORT_ROOT/ansible/setup.sh`
6. Start Ray: `$RAYSORT_ROOT/ansible/start_ray.sh`
7. Run a test run on the cluster: `python raysort/main.py --total_gb=256 2>&1 | tee main.log`

## Misc

### Configuring Ray

* All of Ray's system configuration parameters can be found in [`ray_config_defs.h`](https://github.com/ray-project/ray/blob/master/src/ray/common/ray_config_def.h)
* You only need to specify the config on the head node. All worker nodes will use the same config.
* There are two ways to specify a configuration value. Suppose you want to set [`min_spilling_size`](https://github.com/ray-project/ray/blob/master/src/ray/common/ray_config_def.h#L409) to 0, then:
  1. You can set it in Python, where you do `ray.init(..., _system_config={"min_spilling_size": 0, ...})`
  2. You can set it in the environment variable by running `export RAY_min_spilling_size=0` before running your `ray start` command or your Python program that calls `ray.init()`. This is preferred as our experiment tracker will automatically pick up these environment variables and log them in the W&B trials. Again, it suffices to only set this environment variable on the head node.

### Troubleshooting

#### Package is missing

Make sure the Conda environment is running. Install missing packages with:

```
pip install package_name
```

#### Missing AWS credentials

Install AWS's CLI and set credentials with:

```
pip install awscli
aws configure
```

#### Cluster missing packages/version mismatch

Verify that the image the nodes are being created from matches expectations.
This image [`raysort-hadoop-spark-conda`](https://us-west-2.console.aws.amazon.com/ec2/v2/home?region=us-west-2#ImageDetails:imageId=ami-0da5da6db44aaf267) is currently being used.

### FIO test

Code for testing disk bandwidth.

```
sudo fio --directory=. --ioengine=psync --name fio_test_file --direct=1 --rw=write --bs=1m --size=1g --numjobs=8
sudo fio --directory=. --ioengine=psync --name fio_test_file --direct=1 --rw=read --bs=1m --size=1g --numjobs=8
```
