package edu.bdav.ta.text;

import org.apache.hadoop.io.Text;

/**
 * Sliding-window n-gram builder with optional sentence padding.
 *
 * <p>Emitted n-grams join components with {@link #SEPARATOR} (U+0001, "start of
 * heading"), so the field can never collide with a token that itself contains a
 * space after normalisation. Padding uses the familiar {@code <s>} / {@code </s>}
 * markers so that bigram counts of sentence-initial words are comparable.</p>
 *
 * <p>Object reuse: the caller passes the target {@link Text}; only the window
 * slots allocate, and only when a token is first copied in.</p>
 */
public final class NGramBuilder {

    /** N-gram component separator (never produced by the tokeniser). */
    public static final char SEPARATOR = '\u0001';

    /** Sentence-start marker used when padding is enabled. */
    public static final String START = "<s>";

    /** Sentence-end marker used when padding is enabled. */
    public static final String END = "</s>";

    private final int n;
    private final boolean padding;
    private final StringBuilder joined = new StringBuilder(64);
    private String[] window;
    private int filled;
    private int cursor;
    private boolean finished;

    /**
     * @param n n-gram order (>= 2 in practice; 1 degenerates to a word count)
     * @param padding whether to emit boundary n-grams with {@link #START}/{@link #END}
     */
    public NGramBuilder(int n, boolean padding) {
        if (n < 1 || n > 5) {
            throw new IllegalArgumentException("n must be in [1, 5] but was " + n);
        }
        this.n = n;
        this.padding = padding;
        this.window = new String[n];
        reset();
    }

    /** Empties the window; call at the start of every record. */
    public void reset() {
        filled = 0;
        cursor = 0;
        finished = false;
        if (padding) {
            for (int i = 0; i < n - 1; i++) {
                window[cursor] = START;
                cursor = (cursor + 1) % n;
                filled++;
            }
        }
    }

    /** @return the configured n-gram order */
    public int order() {
        return n;
    }

    /**
     * Pushes one token and writes the resulting n-gram, if one is complete.
     *
     * @param token token to add (copied; the view may be transient)
     * @param out destination for a completed n-gram
     * @return {@code true} when {@code out} was written
     */
    public boolean add(CharSequence token, Text out) {
        if (finished) {
            return false;
        }
        window[cursor] = token.toString();
        cursor = (cursor + 1) % n;
        if (filled < n) {
            filled++;
        }
        if (filled < n) {
            return false;
        }
        join(out);
        return true;
    }

    /**
     * Emits the trailing boundary n-grams when padding is enabled.
     *
     * @param out destination
     * @return {@code true} when {@code out} was written
     */
    public boolean finish(Text out) {
        if (!padding || finished) {
            finished = true;
            return false;
        }
        window[cursor] = END;
        cursor = (cursor + 1) % n;
        finished = true;
        join(out);
        return true;
    }

    /** Writes the current window into {@code out}, oldest component first. */
    private void join(Text out) {
        joined.setLength(0);
        int start = filled == n ? cursor : 0;
        for (int i = 0; i < n; i++) {
            String component = window[(start + i) % n];
            if (i > 0) {
                joined.append(SEPARATOR);
            }
            joined.append(component == null ? "" : component);
        }
        out.set(joined.toString());
    }

    /** @param ngramKey stored n-gram key @return the readable rendering */
    public static String render(org.apache.hadoop.io.Text ngramKey) {
        return render((CharSequence) ngramKey.toString());
    }

    /** Human-readable rendering of a stored n-gram key (for text outputs). */
    public static String render(CharSequence ngramKey) {
        StringBuilder sb = new StringBuilder(ngramKey.length());
        for (int i = 0; i < ngramKey.length(); i++) {
            char c = ngramKey.charAt(i);
            sb.append(c == SEPARATOR ? ' ' : c);
        }
        return sb.toString();
    }
}
