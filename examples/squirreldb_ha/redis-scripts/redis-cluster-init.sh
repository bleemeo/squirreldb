#!/bin/sh

set -eu

get_ip() {
    local host="$1"

    getent hosts "$host" | awk '{print $1}'
}

while true; do
    node_count=$(redis-cli -h redis1 cluster nodes|wc -l)
    if [ "${node_count}" -gt 1 ]; then
        echo "Cluster already initialized"
    else
        if redis-cli --cluster info redis1:6379 | grep -q "0 keys in 1 masters"; then
            echo "Initialized cluster"
            yes yes | redis-cli --cluster create \
                $(get_ip redis1):6379 $(get_ip redis2):6379 \
                $(get_ip redis2):6379 $(get_ip redis3):6379 \
                $(get_ip redis4):6379 $(get_ip redis5):6379 \
                --cluster-replicas 1
        fi
    fi

    sleep 60
done
