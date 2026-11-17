#!/usr/bin/env bash
# Runs the whole pipeline for one dataset.
#
# Usage: scripts/05_run_all.sh [10m|100m|500m|1g|tiny10k]
# Framework selection: FS=local (default, LocalJobRunner) or FS=yarn (cluster).
#
# FS=local submits from the build image against the local filesystem.
# FS=yarn submits from inside the NameNode container, so the corpus and the
# auxiliary resources are addressed by their HDFS/container paths (/data is
# mounted read-only into the cluster image).
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/lib/common.sh"

DATASET="${1:-10m}"
FS_MODE="${FS:-local}"
REDUCERS="${REDUCERS:-2}"
CORPUS_LOCAL="${PROJECT_ROOT}/data/generated/${DATASET}"
LOG="${PROJECT_ROOT}/results/logs/pipeline-${DATASET}-${FS_MODE}.log"

[ -d "${CORPUS_LOCAL}" ] || die "corpus not generated: ${CORPUS_LOCAL} (run scripts/02_fetch_data.sh)"

if [ "${FS_MODE}" = "local" ]; then
  CORPUS="data/generated/${DATASET}"
  OUTPUT_REL="out/${DATASET}-local"
  TA_ARGS=(-Dmapreduce.framework.name=local)
  STOPWORDS=data/stopwords/en-stopwords.txt
  LEXICON=data/lexicon/sentiment-lexicon.tsv
  RUN=ta_mr
else
  CORPUS="hdfs://namenode:9000/data/raw/${DATASET}"
  OUTPUT_REL="/out/${DATASET}-yarn"
  TA_ARGS=(-Dmapreduce.framework.name=yarn)
  STOPWORDS=/data/stopwords/en-stopwords.txt
  LEXICON=/data/lexicon/sentiment-lexicon.tsv
  RUN=ta_yarn
fi

mkdir -p "${PROJECT_ROOT}/results/logs"
log "running pipeline: dataset=${DATASET} fs=${FS_MODE} reducers=${REDUCERS}"

"${RUN}" pipeline \
  "${TA_ARGS[@]}" \
  -Dta.reducers="${REDUCERS}" \
  -Dta.overwrite=true \
  -Dta.stopwords.path="${STOPWORDS}" \
  -Dta.lexicon.path="${LEXICON}" \
  "${CORPUS}" "${OUTPUT_REL}" 2>&1 | tee "${LOG}" | grep -E "finished|ERROR" || true

grep -q "pipeline finished" "${LOG}" || die "pipeline failed - see ${LOG}"
log "results under ${OUTPUT_REL}"

if [ "${FS_MODE}" = "yarn" ]; then
  compose exec -T namenode bash -lc "hdfs dfs -ls ${OUTPUT_REL}"
else
  ls "${PROJECT_ROOT}/${OUTPUT_REL}"
fi
