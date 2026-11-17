package edu.bdav.ta.job.cooccur;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import edu.bdav.ta.config.ConfKeys;
import edu.bdav.ta.config.ConfigSupport;
import edu.bdav.ta.types.TextPair;

/**
 * J8p reducer: sums pair counts and emits {@code w1<TAB>w2<TAB>count}.
 *
 * <p>Pairs below {@code ta.cooccur.mincount} are dropped: on real text the tail of
 * the the pair distribution is overwhelmingly noise, and a co-occurrence matrix is
 * only useful when it is sparse in the right way. The threshold is applied after
 * aggregation, never inside the mapper, because a mapper sees a partial count.</p>
 */
public class PairsReducer extends Reducer<TextPair, IntWritable, Text, Text> {

    private final Text outValue = new Text();
    private int minCount;

    @Override
    protected void setup(Context context) {
        minCount = ConfigSupport.getInt(context.getConfiguration(), ConfKeys.COOCCUR_MIN_COUNT, 1, Integer.MAX_VALUE);
    }

    @Override
    protected void reduce(TextPair key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        long total = 0L;
        for (IntWritable value : values) {
            total += value.get();
        }
        if (total < minCount) {
            return;
        }
        outValue.set(key.getSecond().toString() + "\t" + total);
        context.write(key.getFirst(), outValue);
    }
}
