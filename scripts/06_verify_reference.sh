#!/usr/bin/env bash
# Verifies pipeline output against the single-machine Python reference.
#
# Usage: scripts/06_verify_reference.sh [10m|100m|5000m|1g] [output-root]
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/lib/common.sh"

DATASET="${1:-10m}"
OUTPUT_REL="${2:-out/${DATASET}-local}"
CORPUS="data/generated/${DATASET}"

[ -d "${PROJECT_ROOT}/${CORPUS}" ] || die "corpus not generated: ${CORPUS}"

# 01-wordcount is a SequenceFile (it chains into J2), so render it as text first:
# the verifier is pure Python and must not need a JVM.
log "rendering 01-wordcount as text for the verifier"
ta_mr dump -Dmapreduce.framework.name=local -Dta.overwrite=true \
  "${OUTPUT_REL}/01-wordcount" "${OUTPUT_REL}/_verify-wordcount" \
  | grep -E "ERROR|dumped" || true

log "comparing against the Python reference"
python "${PROJECT_ROOT}/python/verify_outputs.py" \
  --corpus "${PROJECT_ROOT}/${CORPUS}" \
  --output-root "${PROJECT_ROOT}/${OUTPUT_REL}" \
  --wordcount-text "${PROJECT_ROOT}/${OUTPUT_REL}/_verify-wordcount" \
  --stopwords "${PROJECT_ROOT}/data/stopwords/en-stopwords.txt" \
  --lexicon "${PROJECT_ROOT}/data/lexicon/sentiment-lexicon.tsv"
