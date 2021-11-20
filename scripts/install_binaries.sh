#!/bin/bash

set -x

BIN_DIR=raysort/bin

install_gensort() {
    DIR=$BIN_DIR/gensort
    mkdir -p $DIR || exit $?
    pushd $DIR
    TARFILE=gensort-linux-1.5.tar.gz
    wget http://www.ordinal.com/try.cgi/$TARFILE || exit $?
    tar xvf $TARFILE || exit $?
    popd
}

_install_github_binary() {
    PROJ=$1
    BIN=$2
    VER=$3
    SEP=$4
    DIR=$BIN_DIR
    mkdir -p $DIR || exit $?
    pushd $DIR
    TARNAME=${BIN}-${VER}${SEP}linux-amd64
    TARFILE=$TARNAME.tar.gz
    wget https://github.com/$PROJ/$BIN/releases/download/v$VER/$TARFILE || exit $?
    tar xvf $TARFILE || exit $?
    mv $TARNAME $BIN || exit $?
    popd
}

install_prometheus() {
    _install_github_binary prometheus prometheus 2.31.1 .
}

install_node_exporter() {
    _install_github_binary prometheus node_exporter 1.3.0 .
}

install_jaeger() {
    _install_github_binary jaegertracing jaeger 1.22.0 -
}

cleanup() {
    find . -type f -name '*.tar.gz' -delete
}

show_tree() {
    tree $BIN_DIR
}

install_gensort
install_prometheus
install_node_exporter
# install_jaeger
cleanup
show_tree
