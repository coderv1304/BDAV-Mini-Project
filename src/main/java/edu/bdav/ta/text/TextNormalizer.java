package edu.bdav.ta.text;

import java.text.Normalizer;
import java.util.Locale;
import org.apache.hadoop.conf.Configuration;
import edu.bdav.ta.config.ConfKeys;
import edu.bdav.ta.config.ConfigSupport;

/**
 * The platform's single tokenisation contract, mirrored 1:1 by
 * {@code python/reference/tokenizer.py}.
 *
 * <p>Order of operations (changing it silently breaks the reference diff):</p>
 * <ol>
 *   <li>Unicode NFC normalisation;</li>
 *   <li>lowercasing with {@link Locale#ROOT};</li>
 *   <li>deletion of intra-word apostrophes (U+0027, U+2019, U+02BC, U+FF07);</li>
 *   <li>splitting on every code point whose general category is not
 *       {@code L*}, {@code M*} or {@code N*};</li>
 *   <li>number policy ({@code keep}/{@code drop}/{@code mask});</li>
 *   <li>minimum-length filtering.</li>
 * </ol>
 *
 * <p>Object reuse: the normaliser owns one {@link StringBuilder} and hands out
 * transient {@link CharSequence} views over it, so no {@code String} is created
 * per token. Consumers must copy what they need before the next call.</p>
 *
 * <p>Not thread-safe by design: one instance per mapper task.</p>
 */
public final class TextNormalizer {

    /** Replacement token used by {@link NumberMode#MASK}. */
    public static final String NUMBER_TOKEN = "<num>";

    /** Digits-only tokens are handled according to this policy. */
    public enum NumberMode {
        /** Keep numeric tokens unchanged. */
        KEEP,
        /** Discard numeric tokens entirely. */
        DROP,
        /** Replace numeric tokens with {@link #NUMBER_TOKEN}. */
        MASK
    }

    /**
     * Receives one accepted token; the sequence is valid only during the call.
     *
     * <p>Checked exceptions are declared so a mapper can write straight to
     * {@code Context.write} without wrapping failures in {@code RuntimeException}.</p>
     */
    public interface TokenConsumer {
        /**
         * @param token transient token view (copy it before returning)
         * @param length token length in code points
         * @param position ordinal of the accepted token within the record
         * @throws java.io.IOException when the sink fails
         * @throws InterruptedException when the task is cancelled
         */
        void accept(CharSequence token, int length, int position) throws java.io.IOException, InterruptedException;
    }

    /** Per-record measurements, aggregated by mappers into counters. */
    public static final class Stats {
        /** Tokens accepted. */
        public long tokens;
        /** Numeric tokens masked. */
        public long numbersMasked;
        /** Numeric tokens dropped. */
        public long numbersDropped;
        /** Tokens rejected as too short. */
        public long tokensTooShort;
        /** Symbol/emoji code points treated as separators. */
        public long symbolChars;
        /** Longest accepted token in code points. */
        public long longestToken;

        /** Adds another record's measurements into this accumulator. */
        public void add(Stats other) {
            tokens += other.tokens;
            numbersMasked += other.numbersMasked;
            numbersDropped += other.numbersDropped;
            tokensTooShort += other.tokensTooShort;
            symbolChars += other.symbolChars;
            longestToken = Math.max(longestToken, other.longestToken);
        }

        /** Resets all measurements to zero. */
        public void reset() {
            tokens = 0L;
            numbersMasked = 0L;
            numbersDropped = 0L;
            tokensTooShort = 0L;
            symbolChars = 0L;
            longestToken = 0L;
        }
    }

    private static final boolean[] APOSTROPHE = new boolean[Character.MAX_VALUE + 1];

    static {
        APOSTROPHE['\''] = true;
        APOSTROPHE['\u2019'] = true;
        APOSTROPHE['\u02BC'] = true;
        APOSTROPHE['\uFF07'] = true;
    }

    private final NumberMode numberMode;
    private final int minTokenLength;
    private final boolean lowercase;
    private final StringBuilder buffer = new StringBuilder(512);

    /**
     * @param numberMode numeric-token policy
     * @param minTokenLength tokens shorter than this are discarded
     * @param lowercase whether to lowercase (always true in the pipeline; a knob for tests)
     */
    public TextNormalizer(NumberMode numberMode, int minTokenLength, boolean lowercase) {
        if (minTokenLength < 1) {
            throw new IllegalArgumentException("minTokenLength must be >= 1");
        }
        this.numberMode = numberMode;
        this.minTokenLength = minTokenLength;
        this.lowercase = lowercase;
    }

    /** Builds the normaliser described by the job configuration. */
    public static TextNormalizer from(Configuration conf) {
        NumberMode mode = ConfigSupport.getEnum(conf, ConfKeys.NUMBER_MODE, NumberMode.class, NumberMode.MASK);
        int minLength = ConfigSupport.getInt(conf, ConfKeys.MIN_TOKEN_LENGTH, 1, 512);
        return new TextNormalizer(mode, minLength, true);
    }

    /** @return configured numeric-token policy */
    public NumberMode numberMode() {
        return numberMode;
    }

    /** @return configured minimum accepted token length */
    public int minTokenLength() {
        return minTokenLength;
    }

    /** Tokenises one record and returns the number of accepted tokens. */
    public int tokenize(CharSequence input, TokenConsumer consumer)
            throws java.io.IOException, InterruptedException {
        return tokenize(input, 0, input.length(), consumer, 0, new Stats());
    }

    /** Tokenises one record, accumulating measurements. */
    public int tokenize(CharSequence input, TokenConsumer consumer, Stats stats)
            throws java.io.IOException, InterruptedException {
        return tokenize(input, 0, input.length(), consumer, 0, stats);
    }

    /**
     * Overload for Hadoop's {@link org.apache.hadoop.io.Text}.
     *
     * <p>{@code Text} implements {@code Writable} and {@code Comparable}, not
     * {@code CharSequence}, so mappers need this bridge. It costs one String per
     * <em>record</em> (not per token), which is the accepted allocation in the hot
     * path.</p>
     */
    public int tokenize(org.apache.hadoop.io.Text input, TokenConsumer consumer)
            throws java.io.IOException, InterruptedException {
        return tokenize(input.toString(), consumer, new Stats());
    }

    /** @param stats measurement accumulator @return number of accepted tokens */
    public int tokenize(org.apache.hadoop.io.Text input, TokenConsumer consumer, Stats stats)
            throws java.io.IOException, InterruptedException {
        return tokenize(input.toString(), consumer, stats);
    }

    /**
     * Tokenises a slice of a record.
     *
     * @param input source text
     * @param start inclusive start offset in {@code input}
     * @param end exclusive end offset in {@code input}
     * @param consumer token sink
     * @param startPosition ordinal assigned to the first accepted token
     * @param stats measurement accumulator (may be shared across records)
     * @return number of accepted tokens
     */
    public int tokenize(CharSequence input, int start, int end, TokenConsumer consumer,
                        int startPosition, Stats stats)
            throws java.io.IOException, InterruptedException {
        prepare(input, start, end);
        final int length = buffer.length();
        int index = 0;
        int position = startPosition;
        int accepted = 0;
        while (index < length) {
            int codePoint = Character.codePointAt(buffer, index);
            if (!isWordCodePoint(codePoint)) {
                if (isSymbolCodePoint(codePoint)) {
                    stats.symbolChars++;
                }
                index += Character.charCount(codePoint);
                continue;
            }
            int tokenStart = index;
            while (index < length) {
                int current = Character.codePointAt(buffer, index);
                if (!isWordCodePoint(current)) {
                    break;
                }
                index += Character.charCount(current);
            }
            int codePoints = Character.codePointCount(buffer, tokenStart, index);
            if (isAllDigits(tokenStart, index)) {
                if (numberMode == NumberMode.DROP) {
                    stats.numbersDropped++;
                    continue;
                }
                if (numberMode == NumberMode.MASK) {
                    stats.numbersMasked++;
                    codePoints = NUMBER_TOKEN.length();
                    if (codePoints < minTokenLength) {
                        stats.tokensTooShort++;
                        continue;
                    }
                    consumer.accept(NUMBER_TOKEN, codePoints, position++);
                    accepted++;
                    stats.tokens++;
                    stats.longestToken = Math.max(stats.longestToken, codePoints);
                    continue;
                }
            }
            if (codePoints < minTokenLength) {
                stats.tokensTooShort++;
                continue;
            }
            consumer.accept(buffer.subSequence(tokenStart, index), codePoints, position++);
            accepted++;
            stats.tokens++;
            stats.longestToken = Math.max(stats.longestToken, codePoints);
        }
        return accepted;
    }

    /** Fills the internal buffer with the normalised form of the slice. */
    private void prepare(CharSequence input, int start, int end) {
        buffer.setLength(0);
        String slice = input.subSequence(start, end).toString();
        String normalised = Normalizer.isNormalized(slice, Normalizer.Form.NFC)
                ? slice
                : Normalizer.normalize(slice, Normalizer.Form.NFC);
        if (lowercase) {
            normalised = normalised.toLowerCase(Locale.ROOT);
        }
        final int length = normalised.length();
        for (int i = 0; i < length; i++) {
            char c = normalised.charAt(i);
            if (c > Character.MAX_VALUE || !APOSTROPHE[c]) {
                buffer.append(c);
            }
        }
    }

    /** @return true when the code point belongs to a letter, mark or number category */
    public static boolean isWordCodePoint(int codePoint) {
        switch (Character.getType(codePoint)) {
            case Character.UPPERCASE_LETTER:
            case Character.LOWERCASE_LETTER:
            case Character.TITLECASE_LETTER:
            case Character.MODIFIER_LETTER:
            case Character.OTHER_LETTER:
            case Character.NON_SPACING_MARK:
            case Character.ENCLOSING_MARK:
            case Character.COMBINING_SPACING_MARK:
            case Character.DECIMAL_DIGIT_NUMBER:
            case Character.LETTER_NUMBER:
            case Character.OTHER_NUMBER:
                return true;
            default:
                return false;
        }
    }

    /** @return true for S* categories (math, currency, modifier, other symbols) */
    public static boolean isSymbolCodePoint(int codePoint) {
        switch (Character.getType(codePoint)) {
            case Character.MATH_SYMBOL:
            case Character.CURRENCY_SYMBOL:
            case Character.MODIFIER_SYMBOL:
            case Character.OTHER_SYMBOL:
                return true;
            default:
                return false;
        }
    }

    private boolean isAllDigits(int start, int end) {
        for (int i = start; i < end; i++) {
            if (Character.getType(buffer.charAt(i)) != Character.DECIMAL_DIGIT_NUMBER) {
                return false;
            }
        }
        return end > start;
    }
}
