#!/bin/bash

ROOT=$(pwd)

echo "Installing Python dependencies"
pip install -U -r requirements.txt

echo "Installing Python projects"
pip install -e .
cd raysort/sortlib && python setup.py build_ext --inplace
cd $ROOT

echo "Downloading gensort"
GENSORT_DIR=bin/gensort
mkdir -p $GENSORT_DIR && cd $GENSORT_DIR
wget http://www.ordinal.com/try.cgi/gensort-linux-1.5.tar.gz
tar xzvf gensort-linux-1.5.tar.gz
cd $ROOT

echo "Login W&B"
wandb login
