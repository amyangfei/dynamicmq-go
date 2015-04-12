#!/bin/bash

cur=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
subbin=$cur/subscriber

if [ $# -ge 1 ] && [[ $1 =~ ^-?[0-9]+$ ]]; then
    cnt=$1
else
    cnt=10
fi

if [ ! -f "$subbin" ]; then
    cd $cur
    go build
fi

for i in $(seq 1 $cnt); do
    $subbin &
done

# for i in $(ps aux|grep '[s]ubscriber'|awk '{print $2}'); do kill $i; done]')
