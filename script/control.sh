#!/bin/bash

cuser=$(whoami)
# directory of script itself
cur=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
wdir=$cur/build
src=$cur/..
etcd=/home/$cuser/work/opensrc/etcd-base/etcd/etcd
redis=/usr/local/bin/redis-server
machine="01"
sep="----------------------------------------------------"
red='\e[0;31m'
NC='\e[0m' # No Color

start_etcd() {
    cd $wdir
    echo "starting etcd..."
    $etcd &
}

start_redis() {
    cd $wdir
    echo "starting redis..."
    $redis &
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
    sed -i "s/^match_tcp_bind.*/match_tcp_bind = 0.0.0.0:${match_port}/g" $cfg

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

stop_etcd() {
    echo "stopping etcd..."
    etcd_pid=$(ps aux|grep '[/]etcd'|awk '{print $2}')
    for pid in $etcd_pid; do
        echo "killing etcd with pid ${pid}"
        kill $pid
    done
}

stop_redis() {
    echo "stopping redis..."
    redis_pid=$(ps aux|grep '[r]edis-server'|awk '{print $2}')
    for pid in $redis_pid; do
        echo "killing redis-server with pid ${pid}"
        kill $pid
    done
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

build_x() {
    x=$1
    cd $cur/../$x
    echo "building $x..."
    go build
}

build() {
    if [ "$1" == "all" ]; then
        build_x "connector"
        build_x "dispatcher"
        build_x "authsrv"
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
    # start_etcd
    # start_redis
    start_authsrv $*
    start_connector $*
    start_dispatcher $*
}

stop() {
    if [ "$1" == "all" ]; then
        # stop_redis
        # stop_etcd
        stop_dispatcher
        stop_connector
        stop_authsrv
    else
        for x in "$@"; do
            stop_$x
        done
    fi
}

status() {
    ps aux|grep '[/]etcd'
    ps aux|grep '[r]edis-server'
    ps aux|grep '[/]authsrv'
    ps aux|grep '[/]connector'
    ps aux|grep '[/]dispatcher'
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

    echo ${sep}
    exit 1
}

if [ $# -lt 1 ]; then
    show_help $*
fi

if [ $1 == "-h" ]; then
    show_help $*
fi

if [ $# -lt 2 ]; then
    show_help $*
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
