#!/bin/sh

set -eu

CLUSTER_CONFIG="/data/nodes.conf"
POD_IP=$(getent host $(hostname) | awk '{ print $1}')

if [ -f ${CLUSTER_CONFIG} ]; then
    if [ -z "${POD_IP}" ]; then 
        echo "Unable to determine Pod IP address!"
        exit 1
    fi
    echo "Updating my IP to ${POD_IP} in ${CLUSTER_CONFIG}"
    sed -i.bak -e "/myself/ s/[0-9]\{1,3\}\.[0-9]\{1,3\}\.[0-9]\{1,3\}\.[0-9]\{1,3\}/${POD_IP}/" ${CLUSTER_CONFIG}
fi

cat /srv/redis-files/redis.conf > /tmp/redis.conf
echo "cluster-announce-hostname $(hostname)" >> /tmp/redis.conf

exec redis-server /tmp/redis.conf
