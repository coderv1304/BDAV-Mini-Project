package edu.bdav.ta.job.cooccur;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import edu.bdav.ta.config.ConfKeys;
import edu.bdav.ta.config.ConfigSupport;
import edu.bdav.ta.counters.AnalysisCounters;
import edu.bdav.ta.types.StripeWritable;

/**
 * J8s reducer: merges stripes, prunes noise and renders the top-K neighbours.
 *
 * <p>Output: {@code word<TAB>neighbour:count,...} with neighbours ordered by count
 * descending then neighbour ascending, truncated to {@code ta.cooccur.topk}. Rendering
 * a bounded top-K is not cosmetic: an unbounded stripe for a head term on a 1 GB
 * corpus is a multi-megabyte line that no downstream tool can consume, and the bound
 * is what makes the output comparable to the pairs matrix.</p>
 */
public class StripesReducer extends Reducer<Text, StripeWritable, Text, Text> {

    private final StripeWritable merged = new StripeWritable();
    private final StripeWritable pruned = new StripeWritable();
    private final Text outValue = new Text();
    private int minCount;
    private int topK;

    @Override
    protected void setup(Context context) {
        minCount = ConfigSupport.getInt(context.getConfiguration(), ConfKeys.COOCCUR_MIN_COUNT, 1, Integer.MAX_VALUE);
        topK = ConfigSupport.getInt(context.getConfiguration(), ConfKeys.COOCCUR_TOP_K, 0, 1_000_000);
    }

    @Override
    protected void reduce(Text key, Iterable<StripeWritable> values, Context context)
            throws IOException, InterruptedException {
        merged.clear();
        for (StripeWritable value : values) {
            merged.merge(value);
        }
        pruned.clear();
        long kept = 0L;
        for (Map.Entry<String, Long> entry : merged.asMap().entrySet()) {
            if (entry.getValue() >= minCount) {
                pruned.add(entry.getKey(), entry.getValue());
                kept++;
            }
        }
        if (kept == 0L) {
            return;
        }
        outValue.set(pruned.render(topK));
        context.write(key, outValue);
        context.getCounter(AnalysisCounters.NEIGHBOURS_PER_WORD).increment(kept);
    }

    /** @return the top-K entries of the rendered stripe, for tests */
    List<String> preview() {
        List<String> rendered = new ArrayList<>();
        for (Map.Entry<String, Long> entry : pruned.topK(topK)) {
            rendered.add(entry.getKey() + ":" + entry.getValue());
        }
        return rendered;
    }
}
