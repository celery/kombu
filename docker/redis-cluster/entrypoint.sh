#!/bin/sh
# Start a single-node Redis Cluster for the test suite.
#
# One node that owns every hash slot is enough: redis-py's RedisCluster
# client and cluster mode itself enforce cluster behaviour (MOVED, no
# SELECT, etc.) regardless of node count.
#
# A fresh node is a cluster with no slots, so once the server accepts
# connections, claim all 16384 slots in the background.
set -eu

# Default away from macOS Control Center's claim on 7000.
port=${REDIS_CLUSTER_PORT:-6370}

(
  until redis-cli -p "$port" ping 2>/dev/null | grep -q PONG; do
    sleep 0.2
  done
  if ! redis-cli -p "$port" cluster info | grep -q '^cluster_slots_assigned:16384'; then
    redis-cli -p "$port" cluster addslotsrange 0 16383
  fi
) &

# Announce the host-mapped address so clients outside the container
# (tox / GitHub Actions) do not get MOVED to an unreachable Docker IP.
announce_ip=${REDIS_CLUSTER_ANNOUNCE_IP:-127.0.0.1}

exec redis-server \
  --port "$port" \
  --cluster-enabled yes \
  --cluster-node-timeout 5000 \
  --cluster-announce-ip "$announce_ip" \
  --cluster-announce-port "$port" \
  --appendonly no \
  --save '' \
  "$@"
