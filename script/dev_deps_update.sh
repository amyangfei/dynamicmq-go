#!/bin/bash

# install dependcies for local developing

cur=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
src_dst=$GOPATH/src/github.com/amyangfei/dynamicmq-go

rm -rf $src_dst
mkdir -p $src_dst
cp -r $cur/../dynamicmq $src_dst
cp -r $cur/../sdk $src_dst

cd $src_dst/dynamicmq
echo "installing dynamicmq-go/dynamicmq"
go install

echo "installing dynamicmq-go/sdk"
cd $src_dst/sdk
go install

echo "done!"
