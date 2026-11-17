package edu.bdav.ta.text;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import org.junit.Test;
import edu.bdav.ta.TestSupport;

/**
 * Pins the normalisation contract that the Python reference mirrors.
 *
 * <p>Each test name states the rule being protected; a change here means the
 * reference implementation must change too.</p>
 */
public class TextNormalizerTest {

    private static List<String> tokens(String text, TextNormalizer.NumberMode mode) throws Exception {
        TextNormalizer normalizer = new TextNormalizer(mode, 1, true);
        final List<String> collected = new ArrayList<>();
        normalizer.tokenize(text, (token, length, position) -> collected.add(token.toString()));
        return collected;
    }

    private static List<Integer> lengths(String text, TextNormalizer.NumberMode mode) throws Exception {
        TextNormalizer normalizer = new TextNormalizer(mode, 1, true);
        final List<Integer> collected = new ArrayList<>();
        normalizer.tokenize(text, (token, length, position) -> collected.add(length));
        return collected;
    }

    @Test
    public void lowercasesAndStripsPunctuation() throws Exception {
        assertEquals(List.of("hello", "world"), tokens("Hello, WORLD!", TextNormalizer.NumberMode.MASK));
    }

    @Test
    public void splitsHyphenatedWords() throws Exception {
        assertEquals(List.of("state", "of", "the", "art"),
                tokens("state-of-the-art", TextNormalizer.NumberMode.MASK));
    }

    @Test
    public void keepsContractionsAsOneToken() throws Exception {
        assertEquals(List.of("dont", "its"), tokens("don't it's", TextNormalizer.NumberMode.MASK));
        assertEquals(List.of("dont"), tokens("don\u2019t", TextNormalizer.NumberMode.MASK));
    }

    @Test
    public void handlesUnicodeLettersAndMarks() throws Exception {
        assertEquals(List.of("café", "naïve", "zürich"),
                tokens("Café naïve ZÜRICH", TextNormalizer.NumberMode.MASK));
        // Decomposed form must normalise to the composed token, otherwise NFC and
        // precomposed input would produce different vocabularies.
        assertEquals(List.of("café"), tokens("cafe\u0301", TextNormalizer.NumberMode.MASK));
    }

    @Test
    public void treatsEmojiAndSymbolsAsSeparatorsButCountsThem() throws Exception {
        TextNormalizer normalizer = new TextNormalizer(TextNormalizer.NumberMode.MASK, 1, true);
        final List<String> collected = new ArrayList<>();
        TextNormalizer.Stats stats = new TextNormalizer.Stats();
        normalizer.tokenize("rocket 🚀 launch €5", (token, length, position) -> collected.add(token.toString()), stats);
        // '5' is a numeric token and is masked by the default policy; the emoji and the
        // currency sign are separators.
        assertEquals(List.of("rocket", "launch", "<num>"), collected);
        assertTrue("symbols must be counted, not ignored silently", stats.symbolChars >= 2);
    }

    @Test
    public void appliesNumberModes() throws Exception {
        assertEquals(List.of("chapter", "<num>"), tokens("chapter 1984", TextNormalizer.NumberMode.MASK));
        assertEquals(List.of("chapter"), tokens("chapter 1984", TextNormalizer.NumberMode.DROP));
        assertEquals(List.of("chapter", "1984"), tokens("chapter 1984", TextNormalizer.NumberMode.KEEP));
    }

    @Test
    public void decimalNumbersSplitIntoTwoNumericTokens() throws Exception {
        assertEquals(List.of("<num>", "<num>"), tokens("3.14", TextNormalizer.NumberMode.MASK));
    }

    @Test
    public void enforcesMinimumTokenLength() throws Exception {
        TextNormalizer normalizer = new TextNormalizer(TextNormalizer.NumberMode.MASK, 3, true);
        final List<String> collected = new ArrayList<>();
        normalizer.tokenize("a an the word", (token, length, position) -> collected.add(token.toString()));
        assertEquals(List.of("the", "word"), collected);
    }

    @Test
    public void reportsLengthInCodePointsNotUtf16Units() throws Exception {
        // U+1D400 MATHEMATICAL BOLD CAPITAL A is one code point but two UTF-16 units.
        assertEquals(List.of(1), lengths("\uD835\uDC00", TextNormalizer.NumberMode.MASK));
    }

    @Test
    public void assignsConsecutivePositionsToAcceptedTokens() throws Exception {
        TextNormalizer normalizer = new TextNormalizer(TextNormalizer.NumberMode.MASK, 1, true);
        final List<Integer> positions = new ArrayList<>();
        normalizer.tokenize("a b 42 c", (token, length, position) -> positions.add(position));
        assertEquals(List.of(0, 1, 2, 3), positions);
    }

    @Test
    public void emptyAndWhitespaceInputYieldsNothing() throws Exception {
        assertTrue(tokens("", TextNormalizer.NumberMode.MASK).isEmpty());
        assertTrue(tokens("   \t  ", TextNormalizer.NumberMode.MASK).isEmpty());
    }

    @Test
    public void longestTokenIsTracked() throws Exception {
        TextNormalizer normalizer = TextNormalizer.from(TestSupport.conf());
        TextNormalizer.Stats stats = new TextNormalizer.Stats();
        normalizer.tokenize("pneumonoultramicroscopicsilicovolcanoconiosis a",
                (token, length, position) -> { }, stats);
        assertEquals(45L, stats.longestToken);
    }
}
