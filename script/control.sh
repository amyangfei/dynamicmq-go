#!/bin/bash

cuser=$(whoami)
# directory of script itself
cur=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
wdir=$cur/../build
src=$cur/..
etcd_bin="etcd"
redis_bin="redis-server"
machine="01"
sep="----------------------------------------------------"
red='\e[0;31m'
NC='\e[0m' # No Color

start_etcd() {
    if [ ! -d "$wdir/etcd_meta" ]; then
        mkdir -p $wdir/etcd_meta
    fi

    cd $wdir/etcd_meta
    echo "starting meta etcd..."
    $etcd_bin --listen-client-urls 'http://localhost:2379,http://localhost:4001' \
        --listen-peer-urls 'http://localhost:2380,http://localhost:7001' \
        >>../etcd_meta.log 2>&1 &
    echo $! > ../etcd_meta.pid

    if [ ! -d "$wdir/etcd_attr" ]; then
        mkdir -p $wdir/etcd_attr
    fi
    cd $wdir/etcd_attr
    echo "starting attr etcd..."
    $etcd_bin --listen-client-urls 'http://localhost:2479,http://localhost:4101' \
        --listen-peer-urls 'http://localhost:2480,http://localhost:7101' \
        >>../etcd_attr.log 2>&1 &
    echo $! > ../etcd_attr.pid
}

start_redis() {
    cd $wdir
    echo "starting redis..."
    $redis_bin >>redis.log 2>&1 &
    echo $! > redis.pid
}

start_authsrv() {
    cd $wdir
    cp $src/authsrv/authsrv $wdir
    cp $src/authsrv/config.ini.template $wdir/authsrv_config.ini

    echo "starting authsrv..."
    $wdir/authsrv -c $wdir/authsrv_config.ini &
}

port_shift() {
    port=$1
    sft=$2
    echo $((10#${port}+10#${sft}))
}

start_connector_single() {
    seq_id=$1
    fmt_seq_id=$(echo $seq_id|awk '{printf "%02d", $0}')
    nodeid="conn${machine}${fmt_seq_id}"
    cfg=$wdir/config_${nodeid}.ini
    cp $src/connector/config.ini.template $cfg

    sed -i "s/^node_id.*/node_id = ${nodeid}/g" $cfg

    sub_port=$(port_shift 7253 $seq_id)
    sed -i "s/^sub_tcp_bind.*/sub_tcp_bind = 0.0.0.0:${sub_port}/g" $cfg

    route_port=$(port_shift 7353 $seq_id)
    sed -i "s/^router_tcp_bind.*/router_tcp_bind = 0.0.0.0:${route_port}/g" $cfg

    parsed_wdir=$(echo $wdir|sed 's/\//\\\//g')
    sed -i "s/^working_dir.*/working_dir = ${parsed_wdir}/g" $cfg
    sed -i "s/^log_file.*/log_file = .\/connector_${nodeid}.log/g" $cfg
    sed -i "s/^pid_file.*/pid_file = .\/connector_${nodeid}.pid/g" $cfg

    echo "starting connector $nodeid"
    $wdir/connector -c $cfg &
}

start_connector() {
    echo "starting connector..."
    cp $src/connector/connector $wdir
    for i in $(seq $1); do
        start_connector_single $i
    done
}

start_dispatcher_single() {
    seq_id=$1
    fmt_seq_id=$(echo $seq_id|awk '{printf "%02d", $0}')
    nodeid="disp${machine}${fmt_seq_id}"
    cfg=$wdir/config_${nodeid}.ini
    cp $src/dispatcher/config.ini.template $cfg

    sed -i "s/^node_id.*/node_id = ${nodeid}/g" $cfg

    match_port=$(port_shift 6000 $seq_id)
    sed -i "s/^match_tcp_port.*/match_tcp_port = ${match_port}/g" $cfg

    parsed_wdir=$(echo $wdir|sed 's/\//\\\//g')
    sed -i "s/^working_dir.*/working_dir = ${parsed_wdir}/g" $cfg
    sed -i "s/^log_file.*/log_file = .\/dispatcher_${nodeid}.log/g" $cfg
    sed -i "s/^pid_file.*/pid_file = .\/dispatcher_${nodeid}.pid/g" $cfg

    echo "starting dispatcher $nodeid"
    $wdir/dispatcher -c $cfg &
}

start_dispatcher() {
    echo "starting diaptcher..."
    cp $src/dispatcher/dispatcher $wdir
    for i in $(seq $1); do
        start_dispatcher_single $i
    done
}

start_indexnode() {
    cd $wdir
    cp $src/match/indexnode/indexnode $wdir
    cp $src/match/indexnode/config.ini.template $wdir/indexnode.ini

    echo "starting indexnode..."
    $wdir/indexnode -c $wdir/indexnode.ini &
}

start_datanode_single() {
    echo "not implement"
}

start_datanode() {
    echo "starting datanode..."
    cp $src/match/datanode/datanode $wdir
    for i in $(seq $1); do
        start_datanode_single $i
    done
}

stop_etcd() {
    echo "stopping Etcd..."
    etcd_pids=$(cat $wdir/etcd_*.pid)
    for pid in $etcd_pids; do
        echo "killing etcd with pid=$pid"
        kill $pid
    done
}

stop_redis() {
    echo "stopping Redis..."
    redis_pid=$(cat "${wdir}/redis.pid")
    echo "killing redis-server with pid ${redis_pid}"
    kill $redis_pid
}

stop_authsrv() {
    echo "killing authsrv..."
    pids=$(cat $wdir/authsrv.pid)
    for pid in $pids; do
        echo "killing authsrv with pid=$pid"
        kill $pid
    done
}

stop_connector() {
    echo "killing connector..."
    pids=$(cat $wdir/connector_*.pid)
    for pid in $pids; do
        echo "killing connector with pid=$pid"
        kill $pid
    done
}

stop_dispatcher() {
    echo "killing dispatcher..."
    pids=$(cat $wdir/dispatcher_*.pid)
    for pid in $pids; do
        echo "killing dispatcher with pid=$pid"
        kill $pid
    done
}

stop_indexnode() {
    echo "killing indexnode..."
    pids=$(cat $wdir/indexnode.pid)
    for pid in $pids; do
        echo "killing indexnode with pid=$pid"
        kill $pid
    done
}

stop_datanode() {
    echo "killing datanode..."
    pids=$(cat $wdir/datanode_*.pid)
    for pid in $pids; do
        echo "killing datanode with pid=$pid"
        kill $pid
    done
}

build_x() {
    x=$1
    if [ "$x" == "indexnode" ] || [ "$x" == "datanode" ]; then
        cd $cur/../match/$x
    else
        cd $cur/../$x
    fi
    echo "building $x..."
    go build
}

build() {
    if [ "$1" == "all" ]; then
        build_x "connector"
        build_x "dispatcher"
        build_x "authsrv"
        build_x "indexnode"
        build_x "datanode"
    else
        for x in "$@"; do
            build_x $x
        done
    fi
}

singlestart() {
    for x in "$@"; do
        if [ "$x" == "redis" ]; then
            start_$x
        elif [ "$x" == "etcd" ]; then
            start_$x
        else
            echo "not support $x in single start"
        fi
    done
}

start() {
    cd $cur
    mkdir -p $wdir
    re='^[0-9]+$'
    if ! [[ $1 =~ $re ]] ; then
        echo "error: parameter '$1' not a number" >&2; exit 1
    fi
    # start_etcd
    # start_redis
    start_authsrv $*
    start_connector $*
    start_dispatcher $*
    start_indexnode $*
    # start_datanode $(($1 + 1))
}

stop() {
    if [ "$1" == "all" ]; then
        # stop_redis
        # stop_etcd
        stop_dispatcher
        stop_connector
        stop_authsrv
        # stop_datanode
        stop_indexnode
    else
        for x in "$@"; do
            stop_$x
        done
    fi
}

status() {
    ps aux|grep '[e]tcd'
    ps aux|grep '[r]edis-server'
    ps aux|grep '[/]authsrv'
    ps aux|grep '[/]connector'
    ps aux|grep '[/]dispatcher'
    ps aux|grep '[/]indexnode'
    ps aux|grep '[/]datanode'
}

show_help() {
    echo "Usage:"
    echo ${sep}

    echo -e "${red}start${NC} N nodes:"
    echo "$0 start N"
    echo ""

    echo -e "${red}stop${NC} all nodes or specific kind of nodes:"
    echo "$0 stop all"
    echo "$0 stop connector dispatcher"
    echo ""

    echo -e "${red}singlestart${NC} redis or etcd:"
    echo "$0 singlestart redis"
    echo "$0 singlestart etcd redis"
    echo ""

    echo -e "${red}build${NC} all or specific sub project:"
    echo "$0 build all"
    echo "$0 build authsrv connector dispatcher"
    echo ""

    echo ${sep}
    exit 1
}

if [ $# -lt 1 ]; then
    show_help $*
fi

if [ $1 == "-h" ]; then
    show_help $*
fi

if [ $# -lt 2 ] && [ "$1" != "status" ]; then
    show_help $*
fi

if [ ! -d "$wdir" ]; then
    mkdir -p $wdir
fi

case "$1" in
    build)
        shift 1
        build $*
        ;;
    singlestart)
        shift 1
        singlestart $*
        ;;
    start)
        shift 1
        start $*
        ;;
    stop)
        shift 1
        stop $*
        ;;
    restart)
        shift 1
        restart $*
        ;;
    status)
        shift 1
        status $*
        ;;
    *)
        show_help $*
        ;;
esac
echo done.
exit 0
