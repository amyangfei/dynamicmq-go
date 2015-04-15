#!/bin/bash

cur=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
wdir=$cur/../build
src=$cur/..
machine="01"
ip="127.0.0.1"
sep="----------------------------------------------------"
red='\e[0;31m'
NC='\e[0m' # No Color

port_shift() {
    port=$1
    sft=$2
    echo $((10#${port}+10#${sft}))
}

prepare() {
    cp $src/match/datanode/datanode $wdir/datanode
}

start_datanode_single() {
    seq_id=$1
    starthash=$2
    if [ $# -gt 2 ]; then
        entrypoint=$3
    else
        entrypoint=""
    fi

    fmt_seq_id=$(echo $seq_id|awk '{printf "%02d", $0}')
    nodeid="chod${machine}${fmt_seq_id}"
    serfid="serf${machine}${fmt_seq_id}"
    cfg=$wdir/config_${nodeid}.ini
    cp $src/match/datanode/config.ini.template $cfg

    sed -i "s/^hostname.*/hostname = ${nodeid}/g" $cfg
    sed -i "s/^node_name.*/node_name = ${serfid}/g" $cfg

    serf_bind_port=$(port_shift 7700 $seq_id)
    serf_rpc_port=$(port_shift 7500 $seq_id)
    bind_port=$(port_shift 5000 $seq_id)
    rpc_port=$(port_shift 5500 $seq_id)
    sed -i "s/^bind_port = 7946$/bind_port = ${serf_bind_port}/g" $cfg
    sed -i "s/7373/${serf_rpc_port}/g" $cfg
    sed -i "s/^bind_port = 5000$/bind_port = ${bind_port}/g" $cfg
    sed -i "s/^rpc_port = 5500$/rpc_port = ${rpc_port}/g" $cfg

    parsed_wdir=$(echo $wdir|sed 's/\//\\\//g')
    sed -i "s/^workdir.*/workdir = ${parsed_wdir}/g" $cfg
    sed -i "s/^log_file = \.\/datanode\.log/log_file = .\/datanode_${nodeid}.log/g" $cfg
    sed -i "s/^log_file = \.\/serf\.log/log_file = .\/datanode_${serfid}.log/g" $cfg
    sed -i "s/^pid_file.*/pid_file = .\/datanode_${nodeid}.pid/g" $cfg

    if [ "$entrypoint" == "" ]; then
        $wdir/datanode -c $cfg -s=$starthash
    else
        $wdir/datanode -c $cfg -s=$starthash -e=$entrypoint
    fi

}

port_wait() {
    ip=$1
    port=$2

    while true; do
        port_test=$(nc -vz $ip $port 2>&1|grep "Connection refused")
        if [ "$port_test" != "" ]; then
            echo "$(date) - waiting for serf bind on $ip:$port start up..."
            sleep 1
        else
            echo "Connection with $ip:$port successfully..."
            break
        fi
    done
}

start() {
    prepare
    start_datanode_single 1 0000000000000000000000000000000000000000 &
    port_wait $ip $(port_shift 7700 1)
    start_datanode_single 2 0800000000000000000000000000000000000000 $ip:$(port_shift 7700 1) &
}

stop() {
    echo "killing datanode..."
    pids=$(cat $wdir/datanode_*.pid)
    for pid in $pids; do
        echo "killing datanode with pid=$pid"
        kill $pid
    done
}

show_help() {
    echo "Usage:"
    echo ${sep}

    echo -e "${red}start${NC} two datanodes"
    echo "$0 start"
    echo ""

    echo -e "${red}stop${NC} all datanodes"
    echo "$0 stop"
    echo ""

    echo ${sep}
    exit 1
}

case "$1" in
    start)
        shift 1
        start $*
        ;;
    stop)
        shift 1
        stop $*
        ;;
    *)
        show_help
        ;;
esac
