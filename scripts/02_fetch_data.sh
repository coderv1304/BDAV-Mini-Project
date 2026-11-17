#!/usr/bin/env bash
# Generates (or downloads) the corpus at every required size.
#
# Sizes: 10m, 100m, 500m, 1g  plus a tiny10k variant that exists purely to isolate the
# small-files effect: 10 000 files of ~1.4 KB each (~14.4 MB, not 10 MB). The generator
# writes whole paragraphs, so a per-file cap of 1200 bytes lands at ~1444 bytes mean;
# the exact figure comes from the corpus itself, not from the flag.
#
# Optional real data: set TA_DOWNLOAD=1 to fetch Project Gutenberg books instead
# of synthesising. The download is deliberately opt-in so the build stays offline
# capable and reproducible.
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/lib/common.sh"

DATA_DIR="${PROJECT_ROOT}/data/generated"
mkdir -p "${DATA_DIR}"

generate() {
  local name="$1" bytes="$2" files="$3" maxfile="$4"
  if [ -f "${DATA_DIR}/${name}/_manifest.json" ]; then
    log "${name}: already present, skipping (delete ${DATA_DIR}/${name} to regenerate)"
    return 0
  fi
  log "${name}: generating ~${bytes} bytes"
  python "${PROJECT_ROOT}/python/generate_corpus.py" \
    --out "${DATA_DIR}/${name}" --target-bytes "${bytes}" \
    ${files:+--files "${files}"} ${maxfile:+--max-file-bytes "${maxfile}"}
}

generate 10m     10000000  0 0
generate 100m    100000000 0 0
generate tiny10k 10000000  10000 1200
if [ "${TA_BIG:-1}" = "1" ]; then
  generate 500m 500000000  0 0
  generate 1g   1000000000 0 0
fi

log "corpora under ${DATA_DIR}"
du -sh "${DATA_DIR}"/* 2>/dev/null || true
