#!/usr/bin/env bash
# Shared helpers for the run scripts: logging, root discovery, container plumbing.
# Sourced by every script so path handling and error style stay identical.
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
readonly PROJECT_ROOT

# Image used for every Java/MapReduce command. Override with TA_IMAGE.
readonly TA_IMAGE="${TA_IMAGE:-maven:3.9-eclipse-temurin-11}"
readonly TA_M2_VOLUME="${TA_M2_VOLUME:-bdav-m2}"

log()  { printf '[%s] %s\n' "$(date +%H:%M:%S)" "$*" >&2; }
die()  { printf '[%s] ERROR: %s\n' "$(date +%H:%M:%S)" "$*" >&2; exit 1; }

# Runs a command inside the build image with the project mounted at /work.
# Usage: ta_run <command...>
ta_run() {
  local win
  win="$(cygpath -m "${PROJECT_ROOT}" 2>/dev/null || printf '%s' "${PROJECT_ROOT}")"
  MSYS_NO_PATHCONV=1 docker run --rm \
    -v "${win}:/work" -w /work -v "${TA_M2_VOLUME}:/root/.m2" \
    "${TA_IMAGE}" "$@"
}

# Ensures dependencies are installed and the jar is built.
require_jar() {
  [ -f "${PROJECT_ROOT}/target/text-analytics.jar" ] \
    || die "target/text-analytics.jar is missing - run scripts/01_build.sh first"
}

# Builds the runtime classpath once and caches it.
classpath_file() {
  local file="${PROJECT_ROOT}/target/runtime-classpath.txt"
  if [ ! -s "${file}" ]; then
    log "resolving runtime classpath"
    ta_run mvn -B -q dependency:build-classpath \
      -Dmdep.outputFile=/work/target/runtime-classpath.txt \
      -Dmdep.includeScope=test >/dev/null
  fi
  printf '%s' "${file}"
}

# Path to the compose file as the Docker CLI needs to see it. On Git Bash the
# repository path is /c/..., which the native Windows docker.exe cannot open.
compose_file() {
  local file="${PROJECT_ROOT}/docker/docker-compose.yml"
  if command -v cygpath >/dev/null 2>&1; then cygpath -m "${file}"; else printf '%s' "${file}"; fi
}

# Runs `docker compose` against the project's cluster definition.
compose() {
  command -v docker >/dev/null 2>&1 || die "docker is required for the cluster commands"
  MSYS_NO_PATHCONV=1 docker compose -f "$(compose_file)" "$@"
}

# Runs a MapReduce command on the Docker cluster, inside the NameNode container.
# The jar manifest carries Main-Class, so `hadoop jar <jar>` must NOT be given a
# class name - doing so makes Hadoop treat it as the sub-command argument.
# Usage: ta_yarn <Main arguments...>
ta_yarn() {
  require_jar
  compose exec -T namenode bash -lc "hadoop jar /opt/app/text-analytics.jar $*"
}

# Runs a MapReduce command locally (LocalJobRunner, file:///).
# Usage: ta_mr <Main arguments...>
ta_mr() {
  require_jar
  local cp
  cp="$(classpath_file)"
  ta_run bash -lc "java -Dfile.encoding=UTF-8 -cp 'target/classes':\"\$(cat ${cp#${PROJECT_ROOT}/})\" \
    edu.bdav.ta.Main $*"
}
