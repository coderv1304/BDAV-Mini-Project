package edu.bdav.ta.types;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

/**
 * Composite key {@code (term, docId)} for the TF-IDF chain.
 *
 * <p>The document-frequency record uses the sentinel document id
 * {@link #SENTINEL_DOC} ({@code "*"}). Because {@code '*'} (0x2A) precedes every
 * alphanumeric byte, the sentinel naturally sorts first within a term group; the
 * dedicated {@code TermDocSortComparator} makes that explicit rather than
 * accidental.</p>
 */
public final class TermDocWritable implements WritableComparable<TermDocWritable> {

    /** Document id reserved for "this record carries the document frequency". */
    public static final String SENTINEL_DOC = "*";

    private final Text term = new Text();
    private final Text docId = new Text();

    /** Required by the Writable framework. */
    public TermDocWritable() {
    }

    /** @param term term @param docId document id or {@link #SENTINEL_DOC} */
    public TermDocWritable(String term, String docId) {
        this.term.set(term);
        this.docId.set(docId);
    }

    /** @return the term (live, mutable) */
    public Text getTerm() {
        return term;
    }

    /** @return the document id (live, mutable) */
    public Text getDocId() {
        return docId;
    }

    /** @return true when this key carries a document frequency */
    public boolean isDocumentFrequencyRecord() {
        return SENTINEL_DOC.equals(docId.toString());
    }

    /** Sets both components, for reuse from a mapper. */
    public void set(String term, String docId) {
        this.term.set(term);
        this.docId.set(docId);
    }

    /** Sets both components from {@link CharSequence} values. */
    public void set(CharSequence term, CharSequence docId) {
        this.term.set(term.toString());
        this.docId.set(docId.toString());
    }

    /** Sets the term from a Hadoop {@link Text} (not a {@code CharSequence}) and a document id. */
    public void set(Text term, String docId) {
        this.term.set(term);
        this.docId.set(docId);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        term.write(out);
        docId.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        term.readFields(in);
        docId.readFields(in);
    }

    @Override
    public int compareTo(TermDocWritable other) {
        int comparison = term.compareTo(other.term);
        return comparison != 0 ? comparison : docId.compareTo(other.docId);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof TermDocWritable)) {
            return false;
        }
        TermDocWritable that = (TermDocWritable) other;
        return term.equals(that.term) && docId.equals(that.docId);
    }

    @Override
    public int hashCode() {
        return 31 * term.hashCode() + docId.hashCode();
    }

    @Override
    public String toString() {
        return term.toString() + "\t" + docId.toString();
    }
}
