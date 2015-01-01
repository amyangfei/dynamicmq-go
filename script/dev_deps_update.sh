#!/bin/bash

# install dependcies for local developing

cur=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
src_dst=$GOPATH/src/github.com/amyangfei/dynamicmq-go
echo $src_dst

mkdir -p $src_dst
cp -r $cur/../dynamicmq $src_dst
cp -r $cur/../sdk $src_dst

cd $src_dst/dynamicmq $src_dst
go install
cd $src_dst/sdk
go install
