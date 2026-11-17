#!/usr/bin/env bash
# Fault-tolerance experiment: kill a worker mid-job and prove the output is identical.
#
# Requires the Docker cluster (docker/docker-compose.yml). In local mode there is no
# DataNode to kill, so the script refuses rather than pretending to have tested it.
#
# What it does:
#   1. runs the word count to a reference directory, renders it as text, hashes it
#   2. starts the same job again and kills a DataNode+NodeManager container mid-run
#   3. waits for the job to finish (the AM retries the lost tasks), restarts the worker
#   4. renders and hashes the fault output and fails if the two hashes differ
#
# The text rendering matters: J1 writes a SequenceFile, and every SequenceFile file
# carries a random 16-byte sync marker, so the container bytes are never byte-equal
# even when the records are. `Main dump` compares the records, not the layout.
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/lib/common.sh"

DATASET="${1:-10m}"
VICTIM="${VICTIM:-datanode2}"
JAR="/opt/app/text-analytics.jar"

compose ps --status running 2>/dev/null | grep -q namenode \
  || die "cluster is not running - start it with: docker compose -f $(compose_file) up -d"

in_cluster() {  # in_cluster <shell command>
  compose exec -T namenode bash -lc "$1"
}

run_wordcount() {  # run_wordcount <hdfs-output>
  in_cluster "hadoop jar ${JAR} wordcount \
    -Dmapreduce.framework.name=yarn -Dta.reducers=2 -Dta.overwrite=true \
    /data/raw/${DATASET} $1"
}

render_and_hash() {  # render_and_hash <hdfs-dir> -> sha256 of the rendered records
  in_cluster "hadoop jar ${JAR} dump -Dmapreduce.framework.name=local -Dta.overwrite=true \
      $1 ${1}-dump >/dev/null 2>&1
    hdfs dfs -cat ${1}-dump/part-* | sha256sum | awk '{print \$1}'" | tail -1
}

log "control run"
run_wordcount "/out/ft-control" >/dev/null
control_hash="$(render_and_hash "/out/ft-control")"
log "control hash: ${control_hash}"

log "fault run: killing ${VICTIM} while the job is running"
run_wordcount "/out/ft-fault" >/dev/null 2>&1 &
JOB_PID=$!
sleep 15
compose kill "${VICTIM}" >/dev/null
log "${VICTIM} killed"
wait "${JOB_PID}" || log "job reported failure after the kill (expected: the AM retries)"

fault_hash="$(render_and_hash "/out/ft-fault")"
log "fault hash:   ${fault_hash}"

compose start "${VICTIM}" >/dev/null
log "${VICTIM} restarted"

if [ "${control_hash}" = "${fault_hash}" ]; then
  log "PASS: identical output despite the killed worker"
  mkdir -p "${PROJECT_ROOT}/results/csv"
  printf 'fault_tolerance,dataset=%s,victim=%s,identical=true,hash=%s\n' \
    "${DATASET}" "${VICTIM}" "${control_hash}" >> "${PROJECT_ROOT}/results/csv/fault-tolerance.csv"
else
  log "FAIL: output differs after the fault injection"
  exit 1
fi
