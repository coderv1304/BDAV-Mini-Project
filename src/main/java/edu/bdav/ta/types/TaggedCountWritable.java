package edu.bdav.ta.types;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 * A tagged numeric value, used to fuse the two TF-IDF join inputs into one
 * shuffle: tag {@code 'T'} carries a term frequency, tag {@code 'D'} a
 * document frequency.
 *
 * <p>A single-byte tag is cheaper than two distinct value classes plus a
 * {@code MultipleOutputs}, and it makes the join contract visible at the type
 * level instead of hiding it in the reducer's assumptions.</p>
 *
 * <p><b>Why the document id travels in the value.</b> With a grouping comparator the
 * reducer receives the <em>first</em> key of each group, and here that first key is
 * the sentinel {@code (term, "*")} record. The per-record document id is therefore
 * not recoverable from the key and must travel with the value. This is the classic
 * secondary-sort join trap and it is the reason this class exists at all.</p>
 */
public final class TaggedCountWritable implements Writable {

    /** Marks a term-frequency record. */
    public static final char TAG_TERM_FREQUENCY = 'T';

    /** Marks a document-frequency record. */
    public static final char TAG_DOCUMENT_FREQUENCY = 'D';

    private char tag;
    private final Text docId = new Text();
    private double value;

    /** Required by the Writable framework. */
    public TaggedCountWritable() {
    }

    /** @param tag one of the TAG_* constants @param documentId document id (empty for DF records) @param value numeric payload */
    public TaggedCountWritable(char tag, String documentId, double value) {
        this.tag = tag;
        this.docId.set(documentId);
        this.value = value;
    }

    /** @return the document id attached to a term-frequency record (empty for DF records) */
    public Text getDocId() {
        return docId;
    }

    /** @return the tag */
    public char getTag() {
        return tag;
    }

    /** @return the numeric payload */
    public double getValue() {
        return value;
    }

    /** @return true when this record carries a document frequency */
    public boolean isDocumentFrequency() {
        return tag == TAG_DOCUMENT_FREQUENCY;
    }

    /** Sets tag, document id and payload, for mapper-side reuse. */
    public void set(char tag, String documentId, double value) {
        this.tag = tag;
        this.docId.set(documentId);
        this.value = value;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeChar(tag);
        docId.write(out);
        out.writeDouble(value);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        tag = in.readChar();
        docId.readFields(in);
        value = in.readDouble();
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof TaggedCountWritable)) {
            return false;
        }
        TaggedCountWritable that = (TaggedCountWritable) other;
        return tag == that.tag && docId.equals(that.docId) && Double.compare(value, that.value) == 0;
    }

    @Override
    public int hashCode() {
        long bits = Double.doubleToLongBits(value);
        return 31 * (31 * (int) tag + docId.hashCode()) + (int) (bits ^ (bits >>> 32));
    }

    @Override
    public String toString() {
        return tag + ":" + docId + ":" + value;
    }
}
