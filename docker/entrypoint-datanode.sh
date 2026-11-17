#!/usr/bin/env bash
# DataNode + NodeManager entrypoint for a worker container.
set -euo pipefail

echo "[worker] starting DataNode on $(hostname)"
hdfs --daemon start datanode
echo "[worker] starting NodeManager on $(hostname)"
yarn --daemon start nodemanager

echo "[worker] ready"
tail -f /opt/hadoop/logs/*datanode*.log /opt/hadoop/logs/*nodemanager*.log 2>/dev/null || sleep infinity
