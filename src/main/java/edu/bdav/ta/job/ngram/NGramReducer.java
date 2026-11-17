package edu.bdav.ta.job.ngram;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import edu.bdav.ta.config.ConfKeys;
import edu.bdav.ta.config.ConfigSupport;
import edu.bdav.ta.counters.AnalysisCounters;
import edu.bdav.ta.text.NGramBuilder;

/**
 * J5 reducer: sums n-gram counts and prunes below {@code ta.ngram.minfreq}.
 *
 * <p>Pruning in the reducer (not the mapper) is deliberate: a mapper-local
 * threshold would be applied to a partial count and would therefore discard
 * n-grams that are frequent globally - the classic "filter before aggregation"
 * mistake.</p>
 */
public class NGramReducer extends Reducer<Text, IntWritable, Text, LongWritable> {

    private final LongWritable outValue = new LongWritable();
    private final Text outKey = new Text();
    private int minFrequency;
    private long pruned;

    @Override
    protected void setup(Context context) {
        minFrequency = ConfigSupport.getInt(context.getConfiguration(), ConfKeys.NGRAM_MIN_FREQ, 1, Integer.MAX_VALUE);
    }

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        long sum = 0L;
        for (IntWritable value : values) {
            sum += value.get();
        }
        if (sum < minFrequency) {
            pruned++;
            return;
        }
        outKey.set(NGramBuilder.render(key));
        outValue.set(sum);
        context.write(outKey, outValue);
    }

    @Override
    protected void cleanup(Context context) {
        context.getCounter(AnalysisCounters.NGRAMS_PRUNED).increment(pruned);
    }
}
