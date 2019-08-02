#!/bin/bash

protodir="src/test/proto"
descname="messages.desc"

mkdir -p build

protoc --include_imports --descriptor_set_out=./build/${descname} ${protodir}/*.proto
pushd build

python -m SimpleHTTPServer

popd
