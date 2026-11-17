#!/usr/bin/env bash
# Removes build output, run output and generated corpora.
#
# Never deletes data/stopwords or data/lexicon: those are hand-maintained resources,
# not artefacts, and losing them silently would break every later run.
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/lib/common.sh"

WHAT="${1:-all}"

clean_outputs() { rm -rf "${PROJECT_ROOT}/out" && log "removed out/"; }
clean_target()  { rm -rf "${PROJECT_ROOT}/target" && log "removed target/"; }
clean_data()    { rm -rf "${PROJECT_ROOT}/data/generated" && log "removed data/generated/"; }
clean_results() { rm -rf "${PROJECT_ROOT}/results/logs" "${PROJECT_ROOT}/results/plots" \
                    "${PROJECT_ROOT}/results/csv/bench-raw.csv" \
                    "${PROJECT_ROOT}/results/csv/bench-metrics.csv" \
                    "${PROJECT_ROOT}/results/csv/bench-summary.csv" \
                    && log "removed generated results"; }

case "${WHAT}" in
  outputs) clean_outputs ;;
  build)   clean_target ;;
  data)    clean_data ;;
  results) clean_results ;;
  all)     clean_outputs; clean_target; clean_results ;;
  *)       die "usage: scripts/99_clean.sh [outputs|build|data|results|all]" ;;
esac
log "clean done (${WHAT})"
