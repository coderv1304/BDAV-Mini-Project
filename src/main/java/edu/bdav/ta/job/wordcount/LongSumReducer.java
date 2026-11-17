package edu.bdav.ta.job.wordcount;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Final aggregation to {@code (token, total)} using a 64-bit accumulator.
 *
 * <p>The combiner works in {@code int} (4-byte shuffle values) while the reducer
 * accumulates in {@code long}: on a 1 GB corpus a single token can exceed
 * {@code Integer.MAX_VALUE} occurrences only in pathological inputs, but the
 * reducer is the right place to be exact because it sees the global sum.</p>
 */
public class LongSumReducer extends Reducer<Text, IntWritable, Text, LongWritable> {

    private final LongWritable outValue = new LongWritable();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        long sum = 0L;
        for (IntWritable value : values) {
            sum += value.get();
        }
        outValue.set(sum);
        context.write(key, outValue);
    }
}
