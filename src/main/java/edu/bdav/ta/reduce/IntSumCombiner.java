package edu.bdav.ta.reduce;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Generic integer-sum combiner, usable for any key type {@code K}.
 *
 * <p><b>Safety proof.</b> Integer addition is associative and commutative, so
 * {@code f(f(a,b),c) = f(a,f(b,c))} and {@code f(a,b) = f(b,a)}; partial
 * aggregation therefore cannot change a sum. The overflow check turns a silent
 * wrap-around into a task failure, because a wrong count is worse than a failed
 * job.</p>
 *
 * <p>It is <em>not</em> safe for mean, median or top-N: a mean needs (sum, count),
 * and a bounded heap is not associative at all.</p>
 *
 * @param <K> map-output key type
 */
public class IntSumCombiner<K> extends Reducer<K, IntWritable, K, IntWritable> {

    private final IntWritable outValue = new IntWritable();

    @Override
    protected void reduce(K key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        long sum = 0L;
        for (IntWritable value : values) {
            sum += value.get();
        }
        if (sum > Integer.MAX_VALUE) {
            throw new IOException("combiner overflow for key '" + key
                    + "': switch this stage to long-valued map output");
        }
        outValue.set((int) sum);
        context.write(key, outValue);
    }
}
