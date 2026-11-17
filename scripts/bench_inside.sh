#!/usr/bin/env bash
# Benchmark loop. Runs INSIDE the build container (java + bash, no python).
# Emits one CSV row per experiment to results/csv/bench-raw.csv.
#
# Every number written here is measured in this run. Nothing is estimated: an
# experiment that fails is recorded with status=failed instead of being dropped,
# because a missing row is how benchmark tables start lying.
set -uo pipefail

CSV="results/csv/bench-raw.csv"
METRICS="results/csv/bench-metrics.csv"
mkdir -p results/csv results/logs
[ -f "${CSV}" ] || echo "dataset,files,bytes,experiment,variant,reducers,wall_ms,status,output_bytes" > "${CSV}"
[ -f "${METRICS}" ] || echo "dataset,experiment,counter,value" > "${METRICS}"

mvn -B -q dependency:build-classpath -Dmdep.outputFile=/tmp/cp.txt -Dmdep.includeScope=test >/dev/null 2>&1
CP="target/classes:$(cat /tmp/cp.txt)"

java_run() {  # java_run <args...>
  java -Dfile.encoding=UTF-8 -cp "${CP}" edu.bdav.ta.Main "$@"
}

dataset_stats() {  # echoes files,bytes
  local dir="data/generated/$1"
  local files bytes
  files=$(find "${dir}" -name 'doc-*.txt' | wc -l | tr -d ' ')
  bytes=$(find "${dir}" -name 'doc-*.txt' -printf '%s\n' | awk '{s+=$1} END {print s+0}')
  echo "${files} ${bytes}"
}

record_counters() {  # record_counters <dataset> <experiment> <output-dir>
  local dataset="$1" experiment="$2" dir="$3"
  local f="${dir}/_counters.tsv"
  [ -f "${f}" ] || return 0
  awk -F'\t' -v ds="${dataset}" -v ex="${experiment}" '
    NR>1 { print ds "," ex "," $2 "," $3 }
  ' "${f}" >> "${METRICS}"
}

output_bytes() {
  if [ -d "$1" ]; then
    find "$1" -name 'part-*' -printf '%s\n' | awk '{s+=$1} END {print s+0}'
  else
    echo 0
  fi
}

run_wordcount() {  # run_wordcount <dataset> <experiment> <variant> <reducers> <extra args...>
  local dataset="$1" experiment="$2" variant="$3" reducers="$4"; shift 4
  local out="out/bench/${dataset}-${experiment}"
  rm -rf "${out}"
  local read files bytes start end status
  read -r files bytes <<< "$(dataset_stats "${dataset}")"
  start=$(date +%s%3N)
  if java_run wordcount -Dmapreduce.framework.name=local -Dta.reducers="${reducers}" "$@" \
        "data/generated/${dataset}" "${out}" > "results/logs/bench-${dataset}-${experiment}.log" 2>&1; then
    status=ok
  else
    status=failed
  fi
  end=$(date +%s%3N)
  echo "${dataset},${files},${bytes},${experiment},${variant},${reducers},$((end-start)),${status},$(output_bytes "${out}")" >> "${CSV}"
  record_counters "${dataset}" "${experiment}" "${out}"
  echo "  ${dataset}/${experiment} (${variant}, reducers=${reducers}): $((end-start)) ms ${status}"
}

run_other() {  # run_other <dataset> <stage> <experiment> <variant> <reducers> <input> <extra...>
  local dataset="$1" stage="$2" experiment="$3" variant="$4" reducers="$5" input="$6"; shift 6
  local out="out/bench/${dataset}-${experiment}"
  rm -rf "${out}"
  local read files bytes start end status
  read -r files bytes <<< "$(dataset_stats "${dataset}")"
  start=$(date +%s%3N)
  if java_run "${stage}" -Dmapreduce.framework.name=local -Dta.reducers="${reducers}" "$@" \
        "${input}" "${out}" > "results/logs/bench-${dataset}-${experiment}.log" 2>&1; then
    status=ok
  else
    status=failed
  fi
  end=$(date +%s%3N)
  echo "${dataset},${files},${bytes},${experiment},${variant},${reducers},$((end-start)),${status},$(output_bytes "${out}")" >> "${CSV}"
  record_counters "${dataset}" "${experiment}" "${out}"
  echo "  ${dataset}/${experiment} (${variant}): $((end-start)) ms ${status}"
}

echo "=== scalability in input size (word count, 2 reducers, combiner on) ==="
for dataset in 10m 100m; do
  run_wordcount "${dataset}" "size-wc" "default" 2
done

echo "=== scalability in reducer count (10m and 100m) ==="
for dataset in 10m 100m; do
  for reducers in 1 2 4; do
    run_wordcount "${dataset}" "reducers-${reducers}" "default" "${reducers}"
  done
done

echo "=== combiner / in-mapper ablations (10m) ==="
run_wordcount 10m "abl-nocombiner" "combine=none" 2 -Dta.combine.mode=none
run_wordcount 10m "abl-combiner" "combine=combiner" 2 -Dta.combine.mode=combiner
run_wordcount 10m "abl-inmapper" "combine=inmapper" 2 -Dta.combine.mode=inmapper
run_wordcount 10m "abl-both" "combine=both" 2 -Dta.combine.mode=both

echo "=== map-output compression ablation (10m) ==="
# bzip2 is included because it is the only codec in this set with a pure-Java
# implementation; gzip/snappy/lz4 need native libraries that the build image does not
# ship, so the harness records their fallback rather than pretending they compressed.
for codec in none bzip2 gzip snappy lz4; do
  run_wordcount 10m "abl-codec-${codec}" "codec=${codec}" 2 -Dta.codec="${codec}"
done

echo "=== small-files ablation: TextInputFormat vs CombineTextInputFormat ==="
# threshold=1 byte forces the plain TextInputFormat (one split per file);
# the default threshold auto-selects CombineTextInputFormat for this corpus.
run_wordcount tiny10k "abl-smallfiles-text" "TextInputFormat" 2 -Dta.smallfile.threshold=1
run_wordcount tiny10k "abl-smallfiles-combine" "CombineTextInputFormat" 2

echo "=== pairs vs stripes (10m) ==="
input_seq="out/bench/10m-size-wc"
run_other 10m pairs "pattern-pairs" "pairs" 2 "data/generated/10m"
run_other 10m stripes "pattern-stripes" "stripes" 2 "data/generated/10m"
run_other 10m inverted "pattern-inverted" "whole-file" 2 "data/generated/10m"

echo "=== done: ${CSV} and ${METRICS} written ==="
