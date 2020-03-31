#!/bin/sh

# This is required if Redis can change their IP
# Note that if (too many) other Redis also changed their IP, the cluster may
# not restart.

CLUSTER_CONFIG="/data/nodes.conf"
if [ -f ${CLUSTER_CONFIG} ]; then
    MY_IP=$(hostname -i)
    if [ -z "${MY_IP}" ]; then
        echo "Unable to determine my IP address!"
        exit 1
    fi
    echo "Updating my IP to ${MY_IP} in ${CLUSTER_CONFIG}"
    sed -i.bak -e '/myself/ s/[0-9]\{1,3\}\.[0-9]\{1,3\}\.[0-9]\{1,3\}\.[0-9]\{1,3\}/'${MY_IP}'/' ${CLUSTER_CONFIG}
fi
exec redis-server /etc/redis.conf
