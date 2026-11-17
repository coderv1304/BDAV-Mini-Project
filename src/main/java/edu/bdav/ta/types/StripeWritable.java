package edu.bdav.ta.types;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

/**
 * Sparse "stripe": a map from neighbour term to co-occurrence count.
 *
 * <p>Bytes are written as a vint count followed by (vint length, UTF-8 bytes,
 * vlong count) triples. That is compact (no class names, no per-entry object
 * overhead) and, critically, <strong>deterministic in size</strong>, so the
 * benchmark's "shuffle bytes" comparison between the pairs and stripes patterns
 * measures the pattern and not a serialisation accident.</p>
 *
 * <p>{@link #merge(StripeWritable)} makes the type combinable: the merge is
 * associative and commutative, which is exactly the property that licenses the
 * combiner (and in-mapper combining) for the stripes job.</p>
 */
public final class StripeWritable implements Writable {

    /** Payload guard: refuse absurd entry counts from corrupt input. */
    public static final int MAX_ENTRIES = 1 << 24;

    private final Map<String, Long> counts = new HashMap<>();

    /** Creates an empty stripe. */
    public StripeWritable() {
    }

    /** @param neighbour neighbour term @param count contribution to add */
    public void add(CharSequence neighbour, long count) {
        counts.merge(neighbour.toString(), count, Long::sum);
    }

    /** Adds all entries of another stripe into this one (associative merge). */
    public void merge(StripeWritable other) {
        for (Map.Entry<String, Long> entry : other.counts.entrySet()) {
            counts.merge(entry.getKey(), entry.getValue(), Long::sum);
        }
    }

    /** @return number of distinct neighbours */
    public int size() {
        return counts.size();
    }

    /** @return total of all counts in the stripe */
    public long total() {
        long sum = 0L;
        for (Long value : counts.values()) {
            sum += value;
        }
        return sum;
    }

    /** Removes the stripe's contents. */
    public void clear() {
        counts.clear();
    }

    /** @return read-only view of the entries */
    public Map<String, Long> asMap() {
        return Collections.unmodifiableMap(counts);
    }

    /**
     * Returns the highest-scoring entries, ordered by count descending then by
     * neighbour ascending (the tie-break that makes output deterministic).
     *
     * @param k maximum entries; {@code <= 0} means "all"
     */
    public List<Map.Entry<String, Long>> topK(int k) {
        List<Map.Entry<String, Long>> entries = new ArrayList<>(counts.entrySet());
        entries.sort(new Comparator<Map.Entry<String, Long>>() {
            @Override
            public int compare(Map.Entry<String, Long> a, Map.Entry<String, Long> b) {
                int byCount = Long.compare(b.getValue(), a.getValue());
                return byCount != 0 ? byCount : a.getKey().compareTo(b.getKey());
            }
        });
        if (k > 0 && entries.size() > k) {
            return entries.subList(0, k);
        }
        return entries;
    }

    /** Deterministic rendering: {@code neighbour:count,...} with the {@link #topK} order. */
    public String render(int k) {
        StringBuilder sb = new StringBuilder(counts.size() * 8);
        for (Map.Entry<String, Long> entry : topK(k)) {
            if (sb.length() > 0) {
                sb.append(',');
            }
            sb.append(entry.getKey()).append(':').append(entry.getValue());
        }
        return sb.toString();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        WritableUtils.writeVInt(out, counts.size());
        for (Map.Entry<String, Long> entry : counts.entrySet()) {
            byte[] key = entry.getKey().getBytes(StandardCharsets.UTF_8);
            WritableUtils.writeVInt(out, key.length);
            out.write(key);
            WritableUtils.writeVLong(out, entry.getValue());
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        counts.clear();
        int entries = WritableUtils.readVInt(in);
        if (entries < 0 || entries > MAX_ENTRIES) {
            throw new IOException("corrupt StripeWritable: declared entries " + entries
                    + " outside [0, " + MAX_ENTRIES + "]");
        }
        for (int i = 0; i < entries; i++) {
            int length = WritableUtils.readVInt(in);
            if (length < 0 || length > 1 << 16) {
                throw new IOException("corrupt StripeWritable: key length " + length);
            }
            byte[] key = new byte[length];
            in.readFully(key);
            counts.merge(new String(key, StandardCharsets.UTF_8), WritableUtils.readVLong(in), Long::sum);
        }
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof StripeWritable)) {
            return false;
        }
        return counts.equals(((StripeWritable) other).counts);
    }

    @Override
    public int hashCode() {
        return counts.hashCode();
    }

    @Override
    public String toString() {
        return render(0);
    }
}
