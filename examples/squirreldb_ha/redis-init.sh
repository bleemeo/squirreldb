#!/bin/sh

set -e

while true; do
    node_count=$(redis-cli -h redis1 cluster nodes|wc -l)
    if [ "${node_count}" -gt 1 ]; then
        echo "Cluster already initialized"
    else
        if redis-cli --cluster info redis1:6379 | grep -q "0 keys in 1 masters"; then
            echo "Initialized cluster"
            yes yes | redis-cli --cluster create 172.28.0.21:6379 172.28.0.22:6379 172.28.0.23:6379 172.28.0.24:6379 172.28.0.25:6379 172.28.0.26:6379 --cluster-replicas 1
        fi
    fi

    sleep 60
done