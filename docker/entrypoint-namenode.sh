#!/usr/bin/env bash
# NameNode + ResourceManager entrypoint.
#
# Formatting is idempotent: the marker file lives on the named volume, so a restart
# keeps the cluster's data instead of reformatting underneath it (which would silently
# destroy every uploaded corpus).
set -euo pipefail

MARKER=/hadoop/name/.formatted

if [ ! -f "${MARKER}" ]; then
  echo "[namenode] formatting the filesystem"
  hdfs namenode -format -nonInteractive -force
  touch "${MARKER}"
else
  echo "[namenode] existing filesystem detected, skipping format"
fi

echo "[namenode] starting NameNode"
hdfs --daemon start namenode
echo "[namenode] starting ResourceManager"
yarn --daemon start resourcemanager

# Wait until HDFS answers before advertising readiness, then create the well-known
# directories once (idempotent -p).
for attempt in $(seq 1 60); do
  if hdfs dfsadmin -report >/dev/null 2>&1; then
    echo "[namenode] HDFS is up after ${attempt} attempt(s)"
    hdfs dfs -mkdir -p /data/raw /data/stopwords /data/lexicon /out || true
    break
  fi
  sleep 2
done

echo "[namenode] cluster ready"
tail -f /opt/hadoop/logs/*namenode*.log /opt/hadoop/logs/*resourcemanager*.log 2>/dev/null || sleep infinity
