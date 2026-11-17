package edu.bdav.ta.types;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import org.apache.hadoop.io.Writable;

/**
 * Variable-length {@code int[]}, used for term positions in the inverted index.
 *
 * <p>Deliberately not {@code ArrayWritable}: that class boxes every element and
 * writes a class name per record, which is both slower and non-deterministic
 * across Hadoop versions.</p>
 *
 * <p>{@link #readFields} validates the declared length, so a corrupted or
 * maliciously crafted record raises a clear {@link IOException} instead of
 * attempting a multi-gigabyte allocation.</p>
 */
public final class IntArrayWritable implements Writable {

    private static final int[] EMPTY = new int[0];

    /** Upper bound on a single positions payload; guards against corrupt lengths. */
    public static final int MAX_ENTRIES = 1 << 24;

    private int[] values = EMPTY;

    /** Creates an empty payload. */
    public IntArrayWritable() {
    }

    /** @param values the array is copied */
    public IntArrayWritable(int[] values) {
        set(values);
    }

    /** Takes ownership of {@code values} (no copy) - mapper-side fast path. */
    public void setOwned(int[] values) {
        this.values = values == null ? EMPTY : values;
    }

    /** Copies {@code values}. */
    public void set(int[] values) {
        this.values = values == null ? EMPTY : Arrays.copyOf(values, values.length);
    }

    /** @return the live array (no copy) */
    public int[] get() {
        return values;
    }

    /** @return number of entries */
    public int size() {
        return values.length;
    }

    /** @param index 0-based index @return the entry at {@code index} */
    public int get(int index) {
        return values[index];
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(values.length);
        for (int value : values) {
            out.writeInt(value);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        int length = in.readInt();
        if (length < 0 || length > MAX_ENTRIES) {
            throw new IOException("corrupt IntArrayWritable: declared length " + length
                    + " outside [0, " + MAX_ENTRIES + "]");
        }
        if (values.length != length) {
            values = new int[length];
        }
        for (int i = 0; i < length; i++) {
            values[i] = in.readInt();
        }
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof IntArrayWritable)) {
            return false;
        }
        return Arrays.equals(values, ((IntArrayWritable) other).values);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(values);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder(values.length * 4);
        for (int i = 0; i < values.length; i++) {
            if (i > 0) {
                sb.append(',');
            }
            sb.append(values[i]);
        }
        return sb.toString();
    }
}
