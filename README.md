# RaySort

## Setup

```
conda create -n raysort python=3.9
pip install -Ur requirements.txt
pip install -e .
pushd raysort/sortlib && python setup.py build_ext --inplace && popd
scripts/install_binaries.sh
```
