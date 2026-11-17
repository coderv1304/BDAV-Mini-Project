package edu.bdav.ta.job.length;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import edu.bdav.ta.counters.AnalysisCounters;

/**
 * J3 reducer: accumulates a length histogram and emits {@code length<TAB>count}.
 *
 * <p>Counts only - no percentages. Percentages would be floating point, and a
 * floating-point field is where "byte-identical to the Python reference" quietly
 * dies (Java formats {@code %f} half-up, Python rounds the binary double
 * half-even). The verifier and the plotting script derive ratios themselves from
 * exact integers.</p>
 *
 * <p>One reducer is correct here and cheap: the key cardinality is bounded by the
 * longest token (a few hundred), so a single reducer is O(1) work regardless of
 * corpus size.</p>
 */
public class WordLengthReducer extends Reducer<IntWritable, LongWritable, IntWritable, LongWritable> {

    private final TreeMap<Integer, Long> histogram = new TreeMap<>();
    private final LongWritable outValue = new LongWritable();

    @Override
    protected void reduce(IntWritable key, Iterable<LongWritable> values, Context context) {
        long sum = 0L;
        for (LongWritable value : values) {
            sum += value.get();
        }
        histogram.merge(key.get(), sum, Long::sum);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for (Map.Entry<Integer, Long> entry : histogram.entrySet()) {
            context.write(new IntWritable(entry.getKey()), outValue(entry.getValue()));
        }
        context.getCounter(AnalysisCounters.RECORDS_IN).increment(histogram.size());
    }

    private LongWritable outValue(long value) {
        outValue.set(value);
        return outValue;
    }
}
