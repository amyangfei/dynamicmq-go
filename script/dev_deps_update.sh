#!/bin/bash

# install dependcies for local developing

cur=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
src_dst=$GOPATH/src/github.com/amyangfei/dynamicmq-go

rm -rf $src_dst
mkdir -p $src_dst
cp -r $cur/../dynamicmq $src_dst
cp -r $cur/../sdk $src_dst
cp -r $cur/../chord $src_dst

cd $src_dst/dynamicmq
echo "installing dynamicmq-go/dynamicmq"
go install

cd $src_dst/sdk
echo "installing dynamicmq-go/sdk"
go install

cd $src_dst/chord
echo "installing dynamicmq-go/chord"
go install

echo "done!"
