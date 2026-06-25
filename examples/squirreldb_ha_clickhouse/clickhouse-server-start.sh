#!/bin/sh

set -eu

POD_INDEX=${HOSTNAME##*-}

case "${POD_INDEX}" in
    0)
        export CLICKHOUSE_DISPLAY_NAME=clickhouse-server-${POD_INDEX}
        export CLICKHOUSE_MACROS_SHARD=shard01
        export CLICKHOUSE_MACROS_REPLICA=replica01
        ;;
    1)
        export CLICKHOUSE_DISPLAY_NAME=clickhouse-server-${POD_INDEX}
        export CLICKHOUSE_MACROS_SHARD=shard01
        export CLICKHOUSE_MACROS_REPLICA=replica02
        ;;
    *)
        echo "Unknown POD index ${POD_INDEX}"
        exit 1
        ;;
esac

exec /entrypoint.sh
