#!/usr/bin/env bash
# Runs the measurement harness and the single-machine Python baseline, then plots.
#
# Usage: scripts/07_benchmark.sh
# Outputs: results/csv/bench-raw.csv, results/csv/bench-metrics.csv,
#          results/csv/baseline-python.csv, results/plots/*.png
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/lib/common.sh"

require_jar
mkdir -p "${PROJECT_ROOT}/results/csv" "${PROJECT_ROOT}/results/logs" \
         "${PROJECT_ROOT}/results/plots" "${PROJECT_ROOT}/out/bench"

rm -f "${PROJECT_ROOT}/results/csv/bench-raw.csv" "${PROJECT_ROOT}/results/csv/bench-metrics.csv"

log "Hadoop experiments (inside the build image)"
ta_run bash scripts/bench_inside.sh

log "single-machine Python baseline"
python "${PROJECT_ROOT}/python/baselines/bench_python_baseline.py" \
  --datasets 10m 100m \
  --out "${PROJECT_ROOT}/results/csv/baseline-python.csv"

log "aggregating"
python "${PROJECT_ROOT}/python/bench_aggregate.py" \
  --raw "${PROJECT_ROOT}/results/csv/bench-raw.csv" \
  --metrics "${PROJECT_ROOT}/results/csv/bench-metrics.csv" \
  --out "${PROJECT_ROOT}/results/csv/bench-summary.csv"

log "plotting"
python "${PROJECT_ROOT}/python/bench_plot.py" \
  --summary "${PROJECT_ROOT}/results/csv/bench-summary.csv" \
  --baseline "${PROJECT_ROOT}/results/csv/baseline-python.csv" \
  --metrics "${PROJECT_ROOT}/results/csv/bench-metrics.csv" \
  --out "${PROJECT_ROOT}/results/plots"

log "done - see results/csv and results/plots"
