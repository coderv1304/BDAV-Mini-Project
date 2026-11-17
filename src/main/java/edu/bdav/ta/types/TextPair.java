package edu.bdav.ta.types;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

/**
 * A (word, neighbour) pair, the key of the pairs-pattern co-occurrence job.
 *
 * <p>Comparison is first component then second, both through {@link Text#compareTo}
 * (unsigned UTF-8 byte order), which keeps Java ordering identical to Python's
 * code-point ordering - the property the reference diff relies on.</p>
 */
public final class TextPair implements WritableComparable<TextPair> {

    private final Text first = new Text();
    private final Text second = new Text();

    /** Required by the Writable framework. */
    public TextPair() {
    }

    /** @param first left component @param second right component */
    public TextPair(String first, String second) {
        this.first.set(first);
        this.second.set(second);
    }

    /** @return the left component (live, mutable) */
    public Text getFirst() {
        return first;
    }

    /** @return the right component (live, mutable) */
    public Text getSecond() {
        return second;
    }

    /** Sets both components. */
    public void set(String first, String second) {
        this.first.set(first);
        this.second.set(second);
    }

    /** Copies both components from another pair. */
    public void set(TextPair other) {
        this.first.set(other.first);
        this.second.set(other.second);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        first.write(out);
        second.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        first.readFields(in);
        second.readFields(in);
    }

    @Override
    public int compareTo(TextPair other) {
        int comparison = first.compareTo(other.first);
        return comparison != 0 ? comparison : second.compareTo(other.second);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof TextPair)) {
            return false;
        }
        TextPair that = (TextPair) other;
        return first.equals(that.first) && second.equals(that.second);
    }

    @Override
    public int hashCode() {
        return 31 * first.hashCode() + second.hashCode();
    }

    @Override
    public String toString() {
        return first.toString() + "\t" + second.toString();
    }
}
