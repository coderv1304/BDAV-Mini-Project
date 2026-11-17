package edu.bdav.ta.reduce;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Generic 64-bit sum combiner for any key type {@code K}.
 *
 * <p>Used where the map output value is already a {@code long} (word counts
 * carried between stages, co-occurrence counts), so no narrowing can occur and
 * the associative/commutative argument holds trivially.</p>
 *
 * @param <K> map-output key type
 */
public class LongSumCombiner<K> extends Reducer<K, LongWritable, K, LongWritable> {

    private final LongWritable outValue = new LongWritable();

    @Override
    protected void reduce(K key, Iterable<LongWritable> values, Context context)
            throws IOException, InterruptedException {
        long sum = 0L;
        for (LongWritable value : values) {
            sum += value.get();
        }
        outValue.set(sum);
        context.write(key, outValue);
    }
}
