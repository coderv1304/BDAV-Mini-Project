package edu.bdav.ta.job.tfidf;

import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import edu.bdav.ta.config.ConfKeys;
import edu.bdav.ta.config.ConfigSupport;
import edu.bdav.ta.types.TermDocWritable;

/**
 * J7a reducer: converts a raw frequency into a weighted term frequency.
 *
 * <p>Weights are computed once, here, and carried as doubles through the chain, so
 * that the TF-IDF join and the cosine-similarity job consume the same numbers.</p>
 *
 * <p>Three modes, selected by {@code ta.tf.mode} and mirrored exactly by the Python
 * reference:</p>
 * <ul>
 *   <li>{@code raw}: {@code tf = n(t,d)}</li>
 *   <li>{@code log} (default): {@code tf = 1 + log10(n(t,d))}</li>
 *   <li>{@code augmented}: {@code tf = 0.5 + 0.5 * n(t,d) / max_t n(t,d)}</li>
 * </ul>
 */
public class TfReducer extends Reducer<TermDocWritable, IntWritable, TermDocWritable, DoubleWritable> {

    private final DoubleWritable outValue = new DoubleWritable();
    private String mode;

    @Override
    protected void setup(Context context) {
        mode = ConfigSupport.getTrimmed(context.getConfiguration(), ConfKeys.TF_MODE, "log");
        if (!"raw".equals(mode) && !"log".equals(mode) && !"augmented".equals(mode)) {
            throw new IllegalArgumentException(
                    "key " + ConfKeys.TF_MODE + " expects raw|log|augmented but was '" + mode + "'");
        }
    }

    @Override
    protected void reduce(TermDocWritable key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        long raw = 0L;
        for (IntWritable value : values) {
            raw += value.get();
        }
        outValue.set(weight(raw, 1L));
        context.write(key, outValue);
    }

    /**
     * Applies the configured weighting.
     *
     * @param rawFrequency occurrences of the term in the document
     * @param documentMax maximum term frequency in the document (used by {@code augmented})
     */
    double weight(long rawFrequency, long documentMax) {
        if (rawFrequency <= 0L) {
            return 0.0d;
        }
        switch (mode) {
            case "raw":
                return rawFrequency;
            case "augmented":
                return 0.5d + 0.5d * ((double) rawFrequency / Math.max(1L, documentMax));
            case "log":
            default:
                return 1.0d + Math.log10(rawFrequency);
        }
    }

    /** @return the configured weighting mode (tests) */
    String mode() {
        return mode;
    }
}
