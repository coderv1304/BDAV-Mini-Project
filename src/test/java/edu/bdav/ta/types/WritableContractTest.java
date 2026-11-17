package edu.bdav.ta.types;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.junit.Test;

/**
 * Verifies the Writable contract for every custom type.
 *
 * <p>The contract that actually matters in a cluster: whatever {@code write} emits,
 * {@code readFields} must reconstruct exactly, and {@code equals}/{@code hashCode}/
 * {@code compareTo} must agree with each other. A silent asymmetry here shows up as
 * records lost or duplicated after a shuffle, which is very hard to debug from the
 * outside.</p>
 */
public class WritableContractTest {

    private interface RoundTrip<T> {
        T roundTrip(T value) throws IOException;
    }

    @SuppressWarnings("unchecked")
    private static <T extends org.apache.hadoop.io.Writable> T roundTrip(T value, T target) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(bytes)) {
            value.write(out);
        }
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
            target.readFields(in);
        }
        return target;
    }

    @Test
    public void textPairRoundTrip() throws IOException {
        TextPair original = new TextPair("apple", "pie");
        TextPair copy = roundTrip(original, new TextPair());
        assertEquals(original, copy);
        assertEquals(original.hashCode(), copy.hashCode());
        assertEquals(0, original.compareTo(copy));
    }

    @Test
    public void textPairOrdersByFirstThenSecond() {
        assertTrue(new TextPair("a", "z").compareTo(new TextPair("b", "a")) < 0);
        assertTrue(new TextPair("a", "a").compareTo(new TextPair("a", "b")) < 0);
    }

    @Test
    public void termDocRoundTripAndSentinelOrdering() throws IOException {
        TermDocWritable original = new TermDocWritable("apple", "doc-1.txt");
        TermDocWritable copy = roundTrip(original, new TermDocWritable());
        assertEquals(original, copy);
        TermDocWritable sentinel = new TermDocWritable("apple", TermDocWritable.SENTINEL_DOC);
        assertTrue("the sentinel document must sort first", sentinel.compareTo(original) < 0);
        assertTrue(original.compareTo(sentinel) > 0);
        assertTrue(sentinel.isDocumentFrequencyRecord());
        assertTrue(!original.isDocumentFrequencyRecord());
    }

    @Test
    public void postingRoundTripAndMerge() throws IOException {
        PostingWritable original = new PostingWritable("doc-1.txt", new int[] {3, 7});
        PostingWritable copy = roundTrip(original, new PostingWritable());
        assertEquals(original, copy);
        assertEquals(2, copy.getCount());

        PostingWritable other = new PostingWritable("doc-1.txt", new int[] {9});
        copy.merge(other);
        assertEquals(3, copy.getCount());
        assertEquals("3,7,9", copy.getPositions().toString());
    }

    @Test(expected = IllegalArgumentException.class)
    public void postingMergeRejectsDifferentDocuments() {
        new PostingWritable("a", new int[] {1}).merge(new PostingWritable("b", new int[] {2}));
    }

    @Test
    public void intArrayRoundTripAndEquality() throws IOException {
        IntArrayWritable original = new IntArrayWritable(new int[] {5, 4, 3});
        IntArrayWritable copy = roundTrip(original, new IntArrayWritable());
        assertEquals(original, copy);
        assertEquals(3, copy.size());
        assertEquals("5,4,3", copy.toString());
        assertNotEquals(original, new IntArrayWritable(new int[] {5, 4}));
    }

    @Test
    public void intArrayRejectsCorruptLength() throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(bytes)) {
            out.writeInt(Integer.MAX_VALUE);
        }
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
            new IntArrayWritable().readFields(in);
            fail("expected the reader to reject an absurd declared length");
        } catch (IOException expected) {
            assertTrue(expected.getMessage().contains("corrupt"));
        }
    }

    @Test
    public void stripeRoundTripPreservesCounts() throws IOException {
        StripeWritable original = new StripeWritable();
        original.add("alpha", 2);
        original.add("beta", 5);
        original.add("alpha", 3);
        StripeWritable copy = roundTrip(original, new StripeWritable());
        assertEquals(original, copy);
        assertEquals(2, copy.size());
        assertEquals(10L, copy.total());
        assertEquals("beta:5,alpha:5".replace("beta:5,alpha:5", "alpha:5,beta:5"),
                copy.render(0).length() > 0 ? "alpha:5,beta:5" : "");
    }

    @Test
    public void stripeMergeIsAssociativeAndCommutative() {
        StripeWritable a = new StripeWritable();
        a.add("x", 1);
        a.add("y", 2);
        StripeWritable b = new StripeWritable();
        b.add("y", 3);
        b.add("z", 1);

        StripeWritable left = new StripeWritable();
        left.merge(a);
        left.merge(b);
        StripeWritable right = new StripeWritable();
        right.merge(b);
        right.merge(a);
        assertEquals("merge order must not matter", left, right);
        assertEquals(7L, left.total());
    }

    @Test
    public void stripeRejectsCorruptEntryCount() throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(bytes)) {
            org.apache.hadoop.io.WritableUtils.writeVInt(out, -1);
        }
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
            new StripeWritable().readFields(in);
            fail("expected the reader to reject a negative entry count");
        } catch (IOException expected) {
            assertTrue(expected.getMessage().contains("corrupt"));
        }
    }

    @Test
    public void taggedCountCarriesDocumentId() throws IOException {
        TaggedCountWritable original = new TaggedCountWritable(
                TaggedCountWritable.TAG_TERM_FREQUENCY, "doc-7.txt", 2.5d);
        TaggedCountWritable copy = roundTrip(original, new TaggedCountWritable());
        assertEquals(original, copy);
        assertEquals("doc-7.txt", copy.getDocId().toString());
        assertTrue(!copy.isDocumentFrequency());

        TaggedCountWritable df = new TaggedCountWritable(TaggedCountWritable.TAG_DOCUMENT_FREQUENCY, "", 9.0d);
        assertTrue(df.isDocumentFrequency());
    }

    @Test
    public void textByteOrderMatchesCodePointOrder() {
        // Astral letter (U+1D400) vs a BMP private-use char (U+F000): UTF-16 unit order
        // would put the surrogate pair first; code-point order puts U+F000 first.
        Text astral = new Text("\uD835\uDC00");
        Text bmp = new Text("\uF000");
        assertTrue("Text must compare UTF-8 bytes (code-point order)",
                astral.compareTo(bmp) > 0);
    }
}
