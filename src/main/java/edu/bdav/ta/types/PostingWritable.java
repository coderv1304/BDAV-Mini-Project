package edu.bdav.ta.types;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 * One index entry: which document, how often, and where.
 *
 * <p>Used both as the mapper's value (one posting per word per document, built by
 * in-mapper combining) and as the combiner's unit of work.</p>
 */
public final class PostingWritable implements Writable {

    private final Text docId = new Text();
    private int count;
    private final IntArrayWritable positions = new IntArrayWritable();

    /** Creates an empty posting. */
    public PostingWritable() {
    }

    /** @param docId document id @param positions term positions (ownership transferred) */
    public PostingWritable(String docId, int[] positions) {
        this.docId.set(docId);
        this.positions.set(positions);
        this.count = positions == null ? 0 : positions.length;
    }

    /** @return the document id (live, mutable) */
    public Text getDocId() {
        return docId;
    }

    /** @return total occurrences of the term in the document */
    public int getCount() {
        return count;
    }

    /** @return the recorded positions (live, mutable) */
    public IntArrayWritable getPositions() {
        return positions;
    }

    /** Sets the document and its positions, recomputing the count. */
    public void set(String documentId, int[] termPositions) {
        docId.set(documentId);
        positions.set(termPositions);
        count = termPositions == null ? 0 : termPositions.length;
    }

    /** Merges another posting for the same document (associative, combiner-safe). */
    public void merge(PostingWritable other) {
        if (!docId.equals(other.docId)) {
            throw new IllegalArgumentException("cannot merge postings from different documents: "
                    + docId + " vs " + other.docId);
        }
        int[] mine = positions.get();
        int[] theirs = other.positions.get();
        int[] merged = new int[mine.length + theirs.length];
        System.arraycopy(mine, 0, merged, 0, mine.length);
        System.arraycopy(theirs, 0, merged, mine.length, theirs.length);
        positions.set(merged);
        count = merged.length;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        docId.write(out);
        out.writeInt(count);
        positions.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        docId.readFields(in);
        count = in.readInt();
        if (count < 0) {
            throw new IOException("corrupt PostingWritable: negative count " + count);
        }
        positions.readFields(in);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof PostingWritable)) {
            return false;
        }
        PostingWritable that = (PostingWritable) other;
        return count == that.count && docId.equals(that.docId) && positions.equals(that.positions);
    }

    @Override
    public int hashCode() {
        return 31 * (31 * docId.hashCode() + count) + positions.hashCode();
    }

    @Override
    public String toString() {
        return docId + ":" + count + ":" + positions;
    }
}
