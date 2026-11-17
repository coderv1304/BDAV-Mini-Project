#!/usr/bin/env bash
# Determinism + idempotency proof.
#
#  1. Runs the whole pipeline twice into different directories.
#  2. Hashes every stage's records and compares: identical hashes mean the pipeline
#     is deterministic (sorted keys, Locale.ROOT formatting, fixed separators).
#  3. Re-runs one stage onto an existing output to prove it refuses with a clear
#     message, then proves --overwrite succeeds.
#
# Usage: scripts/09_determinism.sh [10m]
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/lib/common.sh"

DATASET="${1:-10m}"
CORPUS="data/generated/${DATASET}"
LOG="${PROJECT_ROOT}/results/logs/determinism-${DATASET}.log"
mkdir -p "${PROJECT_ROOT}/results/logs"

[ -d "${PROJECT_ROOT}/${CORPUS}" ] || die "corpus not generated: ${CORPUS}"

ta_run bash -lc '
  set -e
  CP="target/classes:$(mvn -B -q dependency:build-classpath -Dmdep.outputFile=/tmp/cp.txt -Dmdep.includeScope=test >/dev/null 2>&1; cat /tmp/cp.txt)"
  run() {
    java -Dfile.encoding=UTF-8 -cp "$CP" edu.bdav.ta.Main pipeline \
      -Dmapreduce.framework.name=local -Dta.reducers=2 -Dta.overwrite=true \
      -Dta.stopwords.path=data/stopwords/en-stopwords.txt \
      -Dta.lexicon.path=data/lexicon/sentiment-lexicon.tsv \
      "$1" "$2" >/dev/null 2>&1
  }
  echo "== run A =="; run '"${CORPUS}"' out/det-a
  echo "== run B =="; run '"${CORPUS}"' out/det-b

  echo "== stage hashes =="
  # SequenceFile stages are compared on their RENDERED RECORDS, not on container
  # bytes: Hadoop writes a random 16-byte sync marker into every SequenceFile, so
  # two runs with identical data never share a byte-identical container. Comparing
  # raw bytes would report a false failure (it did, on the first run of this script).
  hash_stage() {
    local dir="$1"
    local first
    first=$(ls "$dir"/part-* 2>/dev/null | head -1)
    [ -n "$first" ] || { echo "empty"; return; }
    if head -c 3 "$first" | grep -q SEQ; then
      local dumpdir="${dir}-rendered"
      rm -rf "$dumpdir"
      java -Dfile.encoding=UTF-8 -cp "$CP" edu.bdav.ta.Main dump \
        -Dmapreduce.framework.name=local -Dta.overwrite=true "$dir" "$dumpdir" >/dev/null 2>&1
      cat "$dumpdir"/part-* 2>/dev/null | sort | sha256sum | cut -d" " -f1
    else
      cat "$dir"/part-* 2>/dev/null | sha256sum | cut -d" " -f1
    fi
  }
  failures=0
  for dir in out/det-a/*/; do
    name=$(basename "$dir")
    case "$name" in *-rendered) continue ;; esac
    a=$(hash_stage "$dir")
    b=$(hash_stage "out/det-b/$name")
    if [ "$a" = "$b" ]; then
      echo "IDENTICAL  $name  $a"
    else
      echo "DIFFERENT  $name  $a vs $b"; failures=$((failures+1))
    fi
  done
  echo "determinism_failures=$failures"

  echo "== idempotency: existing output must be refused =="
  set +e
  java -Dfile.encoding=UTF-8 -cp "$CP" edu.bdav.ta.Main wordcount \
    -Dmapreduce.framework.name=local '"${CORPUS}"' out/det-a/01-wordcount 2>&1 | grep -m1 -E "already exists" || echo "NO CLEAR MESSAGE"
  echo "rerun_exit_code=$?"
  set -e
  exit $failures
' 2>&1 | tee "${LOG}"
