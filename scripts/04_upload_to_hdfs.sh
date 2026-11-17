#!/usr/bin/env bash
# Copies a generated corpus and the auxiliary resources into HDFS.
#
# Usage: HDFS_URI=hdfs://namenode:9000 scripts/04_upload_to_hdfs.sh 10m
# Requires a running cluster with `hdfs` on PATH (the Docker cluster provides both;
# in local mode this script is not needed at all).
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/lib/common.sh"

DATASET="${1:-10m}"
HDFS_URI="${HDFS_URI:-hdfs://namenode:9000}"
LOCAL_CORPUS="${PROJECT_ROOT}/data/generated/${DATASET}"

[ -d "${LOCAL_CORPUS}" ] || die "corpus not generated: ${LOCAL_CORPUS} (run scripts/02_fetch_data.sh)"
command -v hdfs >/dev/null 2>&1 || die "hdfs client not on PATH - run this inside the cluster container"

log "uploading ${DATASET} to ${HDFS_URI}/data/raw/${DATASET}"
hdfs dfs -mkdir -p /data/raw/"${DATASET}" /data/stopwords /data/lexicon
hdfs dfs -put -f "${LOCAL_CORPUS}"/doc-*.txt "/data/raw/${DATASET}/"
hdfs dfs -put -f "${PROJECT_ROOT}/data/stopwords/en-stopwords.txt" /data/stopwords/
hdfs dfs -put -f "${PROJECT_ROOT}/data/lexicon/sentiment-lexicon.tsv" /data/lexicon/
log "HDFS now holds:"
hdfs dfs -du -h "/data/raw/${DATASET}" | tail -3
