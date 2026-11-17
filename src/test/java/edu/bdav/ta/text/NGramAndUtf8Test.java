package edu.bdav.ta.text;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.io.Text;
import org.junit.Test;

/** Tests for {@link NGramBuilder} and {@link Utf8}. */
public class NGramAndUtf8Test {

    private static List<String> ngrams(int n, boolean padding, String... tokens) {
        NGramBuilder builder = new NGramBuilder(n, padding);
        builder.reset();
        Text out = new Text();
        List<String> result = new ArrayList<>();
        for (String token : tokens) {
            if (builder.add(token, out)) {
                result.add(NGramBuilder.render(out));
            }
        }
        while (builder.finish(out)) {
            result.add(NGramBuilder.render(out));
        }
        return result;
    }

    @Test
    public void bigramsWithPaddingIncludeBoundaries() {
        assertEquals(List.of("<s> a", "a b", "b </s>"), ngrams(2, true, "a", "b"));
    }

    @Test
    public void bigramsWithoutPaddingSkipBoundaries() {
        assertEquals(List.of("a b"), ngrams(2, false, "a", "b"));
    }

    @Test
    public void trigramsOverlapCorrectly() {
        assertEquals(List.of("<s> <s> a", "<s> a b", "a b c", "b c </s>"),
                ngrams(3, true, "a", "b", "c"));
    }

    @Test
    public void ngramKeysUseTheUnitSeparator() {
        NGramBuilder builder = new NGramBuilder(2, false);
        builder.reset();
        Text out = new Text();
        assertFalse(builder.add("a", out));
        assertTrue(builder.add("b", out));
        assertEquals('a', (char) out.getBytes()[0]);
        assertEquals(NGramBuilder.SEPARATOR, (char) out.getBytes()[1]);
    }

    @Test
    public void resetStartsANewRecord() {
        NGramBuilder builder = new NGramBuilder(2, false);
        builder.reset();
        Text out = new Text();
        builder.add("a", out);
        assertTrue(builder.add("b", out));
        builder.reset();
        assertFalse("a fresh record must not see the previous token", builder.add("c", out));
    }

    @Test(expected = IllegalArgumentException.class)
    public void rejectsOutOfRangeOrder() {
        new NGramBuilder(0, true);
    }

    @Test
    public void encodesAsciiAndMultibyteIdenticallyToTheJdk() {
        for (String sample : new String[] {"ascii", "café", "naïve", "zürich", "日本語", "\uD835\uDC00x"}) {
            byte[] expected = sample.getBytes(StandardCharsets.UTF_8);
            byte[] actual = new byte[Utf8.maxBytes(sample)];
            int written = Utf8.encode(sample, actual);
            assertEquals("length mismatch for " + sample, expected.length, written);
            byte[] trimmed = new byte[written];
            System.arraycopy(actual, 0, trimmed, 0, written);
            assertArrayEquals("byte mismatch for " + sample, expected, trimmed);
        }
    }

    @Test
    public void replacesLoneSurrogatesLikeTheJdkEncoder() {
        String lone = "a\uD800b";
        byte[] expected = lone.getBytes(StandardCharsets.UTF_8);
        byte[] buffer = new byte[Utf8.maxBytes(lone)];
        int written = Utf8.encode(lone, buffer);
        byte[] actual = new byte[written];
        System.arraycopy(buffer, 0, actual, 0, written);
        assertArrayEquals(expected, actual);
    }
}
