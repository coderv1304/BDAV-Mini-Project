#!/usr/bin/env bash
# Builds the jar and (optionally) runs the full test suite with the coverage gate.
#
# Everything runs inside the JDK-11 build image, because Hadoop 3.3.x is not
# supported on the JDK 17+ that modern hosts ship and a real Maven is required.
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/lib/common.sh"

WITH_TESTS="${WITH_TESTS:-1}"

if [ "${WITH_TESTS}" = "1" ]; then
  log "build + unit tests + integration tests + coverage gate"
  ta_run mvn -B -Pcoverage-gate verify | tee "${PROJECT_ROOT}/results/logs/build-and-test.log" | grep -E "Tests run:|BUILD|Rule violated" || true
  grep -q "BUILD SUCCESS" "${PROJECT_ROOT}/results/logs/build-and-test.log" \
    || die "maven build failed - see results/logs/build-and-test.log"
else
  log "compile + package only (WITH_TESTS=0)"
  ta_run mvn -B -DskipTests package > "${PROJECT_ROOT}/results/logs/build.log" 2>&1 \
    || die "maven build failed - see results/logs/build.log"
fi

[ -f "${PROJECT_ROOT}/target/text-analytics.jar" ] || die "jar was not produced"
log "jar: target/text-analytics.jar ($(wc -c < "${PROJECT_ROOT}/target/text-analytics.jar") bytes)"
if [ -d "${PROJECT_ROOT}/target/site/jacoco" ]; then
  log "coverage report: target/site/jacoco/index.html"
fi
