package edu.bdav.ta.text;

/**
 * Minimal UTF-8 encoder for the mapper hot path.
 *
 * <p>Why not {@code String.getBytes(UTF_8)}? Because that allocates a
 * {@code String} plus a {@code byte[]} for every token. Mappers here write into
 * a reusable buffer and hand it to {@link org.apache.hadoop.io.Text#set(byte[], int, int)},
 * which is how the "efficient object reuse" requirement is actually met.</p>
 *
 * <p>Correctness: surrogate pairs are combined into one four-byte sequence and
 * unpaired surrogates become {@code '?'}, so the produced bytes are identical to
 * {@code String.getBytes(UTF_8)} for every input, including astral-plane letters.
 * That equivalence is asserted by {@code NGramAndUtf8Test}.</p>
 */
public final class Utf8 {

    private Utf8() {
    }

    /** @return an upper bound on the encoded size of {@code chars} in bytes */
    public static int maxBytes(CharSequence chars) {
        return chars.length() * 3 + 3;
    }

    /**
     * Encodes {@code source} into {@code target} starting at offset 0.
     *
     * @param source characters to encode
     * @param target destination buffer, must hold at least {@link #maxBytes(CharSequence)} bytes
     * @return number of bytes written
     */
    public static int encode(CharSequence source, byte[] target) {
        int out = 0;
        final int length = source.length();
        int i = 0;
        while (i < length) {
            char c = source.charAt(i);
            int codePoint;
            if (Character.isHighSurrogate(c) && i + 1 < length
                    && Character.isLowSurrogate(source.charAt(i + 1))) {
                codePoint = Character.toCodePoint(c, source.charAt(i + 1));
                i += 2;
            } else if (Character.isSurrogate(c)) {
                // Lone surrogate: the JDK's UTF-8 encoder emits '?' (0x3F) for an
                // unpaired surrogate, and this encoder must agree with it byte for
                // byte, otherwise a token's bytes would depend on the code path.
                codePoint = '?';
                i++;
            } else {
                codePoint = c;
                i++;
            }
            if (codePoint < 0x80) {
                target[out++] = (byte) codePoint;
            } else if (codePoint < 0x800) {
                target[out++] = (byte) (0xC0 | (codePoint >> 6));
                target[out++] = (byte) (0x80 | (codePoint & 0x3F));
            } else if (codePoint < 0x10000) {
                target[out++] = (byte) (0xE0 | (codePoint >> 12));
                target[out++] = (byte) (0x80 | ((codePoint >> 6) & 0x3F));
                target[out++] = (byte) (0x80 | (codePoint & 0x3F));
            } else {
                target[out++] = (byte) (0xF0 | (codePoint >> 18));
                target[out++] = (byte) (0x80 | ((codePoint >> 12) & 0x3F));
                target[out++] = (byte) (0x80 | ((codePoint >> 6) & 0x3F));
                target[out++] = (byte) (0x80 | (codePoint & 0x3F));
            }
        }
        return out;
    }
}
