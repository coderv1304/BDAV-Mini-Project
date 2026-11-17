package edu.bdav.ta.text;

/**
 * Code-point ordering for term keys.
 *
 * <p>{@link String#compareTo} compares UTF-16 code <em>units</em>, which disagrees
 * with code-point order whenever an astral-plane character is compared with a
 * character in U+E000..U+FFFF. Python compares code points, Hadoop's
 * {@link org.apache.hadoop.io.Text#compareTo} compares UTF-8 bytes (which is also
 * code-point order), so anything Java-sorting a {@code String} must use this
 * helper to stay consistent with both.</p>
 */
public final class TextOrder {

    private TextOrder() {
    }

    /**
     * Compares two strings by Unicode code point.
     *
     * @return negative, zero or positive, exactly like {@link String#compareTo}
     *         but with code-point semantics
     */
    public static int compare(CharSequence left, CharSequence right) {
        int i = 0;
        int j = 0;
        final int leftLength = left.length();
        final int rightLength = right.length();
        while (i < leftLength && j < rightLength) {
            int leftCodePoint = Character.codePointAt(left, i);
            int rightCodePoint = Character.codePointAt(right, j);
            if (leftCodePoint != rightCodePoint) {
                return Integer.compare(leftCodePoint, rightCodePoint);
            }
            i += Character.charCount(leftCodePoint);
            j += Character.charCount(rightCodePoint);
        }
        return Integer.compare(leftLength - i, rightLength - j);
    }
}
