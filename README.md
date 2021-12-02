# RaySort

## Setup

```
conda create -n raysort python=3.9.7
pip install -Ur requirements/dev.txt
pip install -Ur requirements/worker.txt
pip install -e .
pushd raysort/sortlib && python setup.py build_ext --inplace && popd
scripts/install_binaries.sh
```


## FIO test

```
sudo fio --directory=. --ioengine=psync --name fio_test_file --direct=1 --rw=write --bs=1m --size=1g --numjobs=8
sudo fio --directory=. --ioengine=psync --name fio_test_file --direct=1 --rw=read --bs=1m --size=1g --numjobs=8
```
