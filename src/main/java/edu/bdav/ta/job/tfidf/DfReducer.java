package edu.bdav.ta.job.tfidf;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * J7b reducer: sums the per-document contributions into a document frequency.
 */
public class DfReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

    private final LongWritable outValue = new LongWritable();

    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context)
            throws IOException, InterruptedException {
        long documentFrequency = 0L;
        for (LongWritable value : values) {
            documentFrequency += value.get();
        }
        outValue.set(documentFrequency);
        context.write(key, outValue);
    }
}
