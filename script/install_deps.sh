#!/bin/bash

check_install_local() {
    read -r -p "Install dynamicmq-go local dependency from local? [y/N] " response
    case $response in
        [yY][eE][sS]|[yY])
            use_local=true
            ;;
        *)
            use_local=false
            ;;
    esac
}

install_remote_dep() {
    go get -u -v github.com/coreos/go-etcd/etcd
    go get -u -v github.com/op/go-logging
    go get -u -v github.com/rakyll/globalconf
    go get -u -v github.com/gorilla/mux
    go get -u -v github.com/garyburd/redigo/redis
    go get -u -v gopkg.in/mgo.v2
    go get -u -v github.com/amyangfei/sherlock-go
}

install_local_dep() {
    if [ "$use_local" = true ]; then
        cur=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
        bash +x $cur/dev_deps_update.sh
    else
        go get -u -v github.com/amyangfei/dynamicmq-go/dynamicmq
        go get -u -v github.com/amyangfei/dynamicmq-go/sdk
        go get -u -v github.com/amyangfei/dynamicmq-go/chord
    fi
}

check_install_local $*
install_remote_dep $*
install_local_dep $*
