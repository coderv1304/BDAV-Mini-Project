#!/usr/bin/env bash
# Validates the stop-word and sentiment-lexicon resources before a run.
#
# Both files are read by the jobs verbatim, so a stray blank line, comment or
# duplicate silently changes the analysis. The checks count *entries* (non-empty,
# non-comment lines) rather than raw lines, reject duplicates, and verify the
# lexicon's TAB-separated shape.
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/lib/common.sh"

STOPWORDS="${PROJECT_ROOT}/data/stopwords/en-stopwords.txt"
LEXICON="${PROJECT_ROOT}/data/lexicon/sentiment-lexicon.tsv"

[ -s "${STOPWORDS}" ] || die "missing stop-word list: ${STOPWORDS}"
[ -s "${LEXICON}" ] || die "missing sentiment lexicon: ${LEXICON}"

# Non-empty, non-comment lines: the entries each job will actually load.
entries() { awk 'NF && $1 !~ /^#/' "$1"; }

words=$(entries "${STOPWORDS}" | wc -l)
lex_entries=$(entries "${LEXICON}" | wc -l)
log "stop words: ${words} entries"
log "lexicon:    ${lex_entries} entries"

if [ "${words}" -lt 20 ]; then
  die "stop-word list looks too small (${words}); check the format (one word per line)"
fi
if [ "${lex_entries}" -lt 20 ]; then
  die "lexicon looks too small (${lex_entries}); check the format (word<TAB>score)"
fi

duplicate_words=$(entries "${STOPWORDS}" | tr 'A-Z' 'a-z' | sort | uniq -d | tr '\n' ' ')
[ -z "${duplicate_words}" ] \
  || die "duplicate stop words (they would be counted once, not twice): ${duplicate_words}"

# Every lexicon line must be exactly word<TAB>number; malformed rows would make the
# polarity score of a whole document silently wrong.
bad_rows=$(entries "${LEXICON}" | awk -F'\t' 'NF != 2 || $1 == "" || $2 !~ /^-?[0-9]+(\.[0-9]+)?$/' | wc -l)
[ "${bad_rows}" -eq 0 ] \
  || die "${bad_rows} lexicon row(s) are not word<TAB>score - check the file format"

log "resources OK"
