#!/bin/bash

SNAPPY_DIR=/usr/local/snappy
LEVELDB_DIR=/usr/local/leveldb

ROOT_DIR=$(pwd)

BUILD_DIR=/tmp/build_leveldb

mkdir -p $BUILD_DIR

cd $BUILD_DIR

if [ ! -f $SNAPPY_DIR/lib/libsnappy.a ]; then
    (git clone git@github.com:siddontang/snappy.git && \
        cd ./snappy && \
        ./configure --prefix=$SNAPPY_DIR && \
        make && \
        make install && \
        cd ..)
else
    echo "skip install snappy"
fi

cd $BUILD_DIR

if [ ! -f $LEVELDB_DIR/lib/libleveldb.a ]; then
    (git clone git@github.com:siddontang/leveldb.git && \
        cd ./leveldb && \
        echo "echo \"PLATFORM_CFLAGS+=-I$SNAPPY_DIR/include\" >> build_config.mk" >> build_detect_platform &&
        echo "echo \"PLATFORM_CXXFLAGS+=-I$SNAPPY_DIR/include\" >> build_config.mk" >> build_detect_platform &&
        echo "echo \"PLATFORM_LDFLAGS+=-L $SNAPPY_DIR/lib -lsnappy\" >> build_config.mk" >> build_detect_platform &&
        make SNAPPY=1 && \
        make && \
        mkdir -p $LEVELDB_DIR/include/leveldb && \
        install include/leveldb/*.h $LEVELDB_DIR/include/leveldb && \
        mkdir -p $LEVELDB_DIR/lib && \
        cp -f libleveldb.* $LEVELDB_DIR/lib &&\
        cd ..)
else
    echo "skip install leveldb"
fi

cd $ROOT_DIR


function add_path()
{
  # $1 path variable
  # $2 path to add
  if [ -d "$2" ] && [[ ":$1:" != *":$2:"* ]]; then
    echo "$1:$2"
  else
    echo "$1"
  fi
}

export CGO_CFLAGS="-I$LEVELDB_DIR/include -I$SNAPPY_DIR/include"
export CGO_CXXFLAGS="-I$LEVELDB_DIR/include -I$SNAPPY_DIR/include"
export CGO_LDFLAGS="-L$LEVELDB_DIR/lib -L$SNAPPY_DIR/lib -lsnappy"
export LD_LIBRARY_PATH=$(add_path $LD_LIBRARY_PATH $SNAPPY_DIR/lib)
export LD_LIBRARY_PATH=$(add_path $LD_LIBRARY_PATH $LEVELDB_DIR/lib)
