package edu.bdav.ta.counters;

/**
 * Named MapReduce counters used for instrumentation and self-checks.
 *
 * <p>Counters double as assertions: {@link #TOKENS_EMITTED} must equal the sum
 * of the word counts produced by job J1, which is the invariant the verifier
 * checks instead of trusting a comment.</p>
 */
public enum AnalysisCounters {

    /** Logical input records (lines or documents) consumed by a mapper. */
    RECORDS_IN,
    /** Tokens that survived normalisation and were emitted. */
    TOKENS_EMITTED,
    /** Distinct stop-word tokens removed after aggregation. */
    STOPWORDS_DROPPED,
    /** Occurrences (sum of counts) that belonged to stop words. */
    STOPWORD_OCCURRENCES_DROPPED,
    /** Blank or whitespace-only records. */
    EMPTY_LINES,
    /** Records whose bytes were not valid UTF-8 (replaced with U+FFFD). */
    MALFORMED_LINES,
    /** Symbol/emoji code points treated as separators (categories S*). */
    SYMBOL_CHARS_DROPPED,
    /** Numeric tokens replaced by the {@code <num>} mask. */
    NUMBERS_MASKED,
    /** Numeric tokens discarded in {@code drop} mode. */
    NUMBERS_DROPPED,
    /** Tokens rejected by the minimum-length rule. */
    TOKENS_TOO_SHORT,
    /** Longest accepted token length in code points (max over records). */
    LONGEST_TOKEN_LEN,
    /** Documents seen by a whole-file mapper. */
    DOCUMENTS,
    /** n-grams emitted by the mapper. */
    NGRAMS_EMITTED,
    /** n-grams pruned in the reducer by the minimum-frequency rule. */
    NGRAMS_PRUNED,
    /** Co-occurrence pairs emitted (pairs pattern, after windowing). */
    PAIRS_EMITTED,
    /** Co-occurrence stripes emitted (stripes pattern). */
    STRIPES_EMITTED,
    /** Neighbour entries carried inside stripes (shuffle-volume proxy). */
    NEIGHBOURS_PER_WORD,
    /** Postings (word, document) records emitted by the index mapper. */
    POSTINGS_EMITTED,
    /** Documents dropped by the similarity candidate filter. */
    DOCS_DROPPED_BY_FILTER,
    /** Tokens absent from the sentiment lexicon. */
    UNKNOWN_LEXICON_WORDS
}
