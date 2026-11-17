package edu.bdav.ta.job.cooccur;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import edu.bdav.ta.counters.AnalysisCounters;
import edu.bdav.ta.types.StripeWritable;

/**
 * J8s combiner: merges stripes for the same word into one.
 *
 * <p><b>Safety.</b> Stripe merge is entry-wise integer addition on a map: both
 * associative and commutative ({@code (A ∪ B) + C = A + (B ∪ C)}, order irrelevant),
 * so partial aggregation cannot change any count. This is the property the pairs
 * pattern also enjoys, but here it is far more effective: one combiner call replaces
 * thousands of pair records with a single stripe.</p>
 *
 * <p>This combiner is where the stripes pattern usually wins, and the benchmark
 * quantifies it as a reduction in shuffle bytes.</p>
 */
public class StripesCombiner extends Reducer<Text, StripeWritable, Text, StripeWritable> {

    private final StripeWritable merged = new StripeWritable();

    @Override
    protected void reduce(Text key, Iterable<StripeWritable> values, Context context)
            throws IOException, InterruptedException {
        merged.clear();
        for (StripeWritable value : values) {
            merged.merge(value);
        }
        context.getCounter(AnalysisCounters.STRIPES_EMITTED).increment(1);
        context.write(key, merged);
    }
}
