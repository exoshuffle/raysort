#!/bin/bash

set -ex

BIN_DIR=raysort/bin

prepare() {
    rm -rf $BIN_DIR
    mkdir -p $BIN_DIR
}

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

show_files() {
    find $BIN_DIR
}

show_help() {
    set +x
    echo "Usage: $0 [binary1] [binary2]..."
    echo "  where [binary#] can be {gensort,prometheus,node_exporter,jaeger}."
    echo "  If no argument is supplied, will install all binaries."
    set -x
}

args="$@"
if [ -z "$args" ]; then
    args="gensort prometheus node_exporter"
fi

prepare
for arg in $args
do
    "install_$arg"
done
cleanup
show_files
