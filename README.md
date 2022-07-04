# RaySort

## Development Setup

If you created your VM using the `raysort` AMI, you should already have a Conda environment ready. Otherwise, install [Miniconda3](https://docs.conda.io/en/latest/miniconda.html) and run `conda create -n raysort python=3.9.7`. Then run:

```
conda activate raysort
pip install -Ur requirements/dev.txt
pip install -Ur requirements/worker.txt
pip install -e .
pushd raysort/sortlib && python setup.py build_ext --inplace && popd
scripts/installers/install_binaries.sh
```

Edit `.envrc` and change `CLUSTER_NAME` and `S3_BUCKET` to your own. Set up [direnv](https://direnv.net/) so that the `.envrc` files are sourced automatically when you `cd` into a directory. Otherwise, manually `source .envrc`.

## Running Locally

A good first step to sanity check your setup is to run raysort on a single node with:

```
CONFIG=LocalNative python raysort/main.py
```

It should complete without errors with an `All OK!` message. The detailed configuration is in [config.py](https://github.com/franklsf95/raysort/blob/master/raysort/config.py).

## Starting up a Cluster

1. Install Terraform: `scripts/installers/install_terraform.sh`
2. Run `export CONFIG=1tb-1gb-s3-native-s3 && python scripts/cls.py up --ray` to launch a Ray cluster, or `--yarn` to launch a YARN cluster for Spark
3. Run a test run on the cluster: `python raysort/main.py 2>&1 | tee main.log`

The `1tb-1gb-s3-native-s3` config launches 10 `r6i.2xlarge` nodes, and runs a 1TB sort with 1GB partitions using S3 for I/O and for shuffle spilling.

## Cluster Management

`scripts/cls.py` is the centralized place for cluster management code.

- `python scripts/cls.py up` launches a cluster via Terraform and configures it via Ansible. Add `--ray` or `--yarn` to start a Ray or a YARN cluster.
- `python scripts/cls.py setup` skips Terraform and only runs Ansible for software setup. Add `--ray` or `--yarn` to start a Ray or a YARN cluster.
- `python scripts/cls.py down` terminates the cluster via Terraform. Tip: when you're done for the day, run `python scripts/cls.py down && sudo shutdown -h now` to terminate the cluster and stop your head node.
- `python scripts/cls.py start/stop/reboot` calls the AWS CLI tool to start/stop/reboot all your machines in the cluster. Useful when you want to stop the cluster but not terminate the machines.

## Autoscaler Management

While `scripts/cls.py` uses Terraform to manage the cluster, `scripts/autoscaler.py` uses the [Ray autoscaler](https://docs.ray.io/en/latest/cluster/sdk.html) to manage the cluster.

- `python scripts/autoscaler.py up -y` launches a cluster via the Ray autoscaler.
- `python scripts/autoscaler.py submit scripts/main.py` submits a job to be executed by the Ray autoscaler that has been launched.
- `python scripts/autoscaler.py down -y` terminates the cluster.

## Misc

### Configuring Ray

- All of Ray's system configuration parameters can be found in [`ray_config_defs.h`](https://github.com/ray-project/ray/blob/master/src/ray/common/ray_config_def.h).
- You only need to specify the config on the head node. All worker nodes will use the same config.
- There are two ways to specify a configuration value. Suppose you want to set [`min_spilling_size`](https://github.com/ray-project/ray/blob/master/src/ray/common/ray_config_def.h#L409) to 0, then:
  1. You can set it in Python, where you do `ray.init(..., _system_config={"min_spilling_size": 0, ...})`
  2. You can set it in the environment variable by running `export RAY_min_spilling_size=0` before running your `ray start` command or your Python program that calls `ray.init()`. This is preferred as our experiment tracker will automatically pick up these environment variables and log them in the W&B trials. Again, it suffices to only set this environment variable on the head node.

Useful Ray environment variables:

```bash
# Enable debug logging for all raylets and workers
export RAY_BACKEND_LOG_LEVEL=debug
```

### Mounting a new volume

- Create a new volume in the same region as your machine on the [AWS Dashboard](https://us-west-2.console.aws.amazon.com/ec2/v2/home?region=us-west-2#Volumes:).
- Attach it to your machine and format it as follows:
  - Create a partition by running `sudo parted /dev/nvme1n1`, where `nvme1n1` is the device name which you can find with `lsblk`.
  - If a partition table does not exist, create it with `mklabel gpt`.
  - Run `mkpart part0 ext4 0% 100%`. Make sure no warnings appear.
  - Exit `parted` and run `sudo mkfs.ext4 /dev/nvme1n1p1`. Note the extra **`p1`**.
  - Run `sudo mount -o sync path_to_volume /mnt/data0`. Only use `-o sync` if you are running microbenchmarks.
- Verify that the mounting worked with `lsblk`.
  - If the desired volume is not mounted, edit `/etc/fstab` to remove any conflicting lines. Then, restart your machine and remount.

### Setting up Grafana

- After launching a cluster via `cls.py up`, forward port 3000 from the head node to your laptop, then go to http://localhost:3000/.
- Default login is username `admin` and password `admin`.
- Add a new data source here: http://localhost:3000/datasources/new. Select `Prometheus`, set URL to be `http://localhost:9090`, then select `Save & Test`.
- Import the dashboard here: http://localhost:3000/dashboard/import. Use the JSON file here: https://github.com/franklsf95/raysort/tree/master/scripts/config/grafana/Exoshuffle-AWS.json. Select the Prometheus data source in Options.
- You should see CPU, memory, disk, network activities in the dashboard. The Application Progress only shows up when you are running Exoshuffle jobs.

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

#### Cannot connect to worker node

First, try manually connect: `ssh -i ~/.aws/login-us-west-2.pem -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null <worker_ip>`. If this doesn't work, it's likely because your current VM is not in the same security group as the worker nodes (which are in the `default` security group). The easiest solution is to find your instance on the AWS EC2 UI, right click "Security -> Change Security Groups", and add your instance to the `default` security group. TODO: this might be possible to automate in Terraform.

### FIO test

Code for testing disk bandwidth.

```
sudo fio --directory=. --ioengine=psync --name fio_test_file --direct=1 --rw=write --bs=1m --size=1g --numjobs=8
sudo fio --directory=. --ioengine=psync --name fio_test_file --direct=1 --rw=read --bs=1m --size=1g --numjobs=8
```
