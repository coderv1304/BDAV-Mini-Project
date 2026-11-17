package edu.bdav.ta.job.cooccur;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import edu.bdav.ta.config.ConfKeys;
import edu.bdav.ta.config.ConfigSupport;
import edu.bdav.ta.counters.AnalysisCounters;
import edu.bdav.ta.text.TextNormalizer;
import edu.bdav.ta.types.TextPair;

/**
 * J8p mapper: the <b>pairs</b> pattern - {@code (w, w') -> 1} for every word pair
 * inside a co-occurrence window.
 *
 * <p>Cost model, which is the whole point of the comparison: the number of emitted
 * records is {@code O(tokens x window)} and every one of them is a full
 * {@code TextPair} plus an {@code IntWritable} through the shuffle. Distinct pairs
 * are only collapsed later, in the reducer. This is the pattern's defining
 * weakness: it is correct and simple, but its shuffle volume grows with occurrences
 * rather than with the vocabulary.</p>
 *
 * <p>With {@code ta.cooccur.ordered=true} only pairs {@code (w_i, w_j), i < j} are
 * emitted (half the records); with {@code false} both directions are emitted. The
 * comparison experiment runs the ordered variant because it is the one that keeps
 * the matrix's meaning while halving the shuffle.</p>
 */
public class PairsMapper extends Mapper<Text, Text, TextPair, IntWritable> {

    private final List<String> tokens = new ArrayList<>();
    private final TextPair outKey = new TextPair();
    private final IntWritable one = new IntWritable(1);
    private final TextNormalizer.Stats recordStats = new TextNormalizer.Stats();
    private final TextNormalizer.Stats totalStats = new TextNormalizer.Stats();
    private TextNormalizer normalizer;
    private Context context;
    private int window = 5;
    private boolean ordered = true;
    private long pairsEmitted;

    private final TextNormalizer.TokenConsumer consumer = new TextNormalizer.TokenConsumer() {
        @Override
        public void accept(CharSequence token, int length, int position) {
            tokens.add(token.toString());
        }
    };

    @Override
    protected void setup(Context context) {
        this.context = context;
        Configuration conf = context.getConfiguration();
        this.normalizer = TextNormalizer.from(conf);
        this.window = ConfigSupport.getInt(conf, ConfKeys.COOCCUR_WINDOW, 1, 1000);
        this.ordered = ConfigSupport.getBoolean(conf, ConfKeys.COOCCUR_ORDERED);
    }

    @Override
    protected void map(Text documentId, Text content, Context context)
            throws IOException, InterruptedException {
        tokens.clear();
        recordStats.reset();
        normalizer.tokenize(content, consumer, recordStats);
        totalStats.add(recordStats);
        int size = tokens.size();
        for (int i = 0; i < size; i++) {
            int limit = Math.min(size, i + window + 1);
            for (int j = i + 1; j < limit; j++) {
                outKey.set(tokens.get(i), tokens.get(j));
                context.write(outKey, one);
                pairsEmitted++;
                if (!ordered) {
                    outKey.set(tokens.get(j), tokens.get(i));
                    context.write(outKey, one);
                    pairsEmitted++;
                }
            }
        }
    }

    @Override
    protected void cleanup(Context context) {
        context.getCounter(AnalysisCounters.PAIRS_EMITTED).increment(pairsEmitted);
        context.getCounter(AnalysisCounters.TOKENS_EMITTED).increment(totalStats.tokens);
        context.getCounter(AnalysisCounters.DOCUMENTS).increment(1);
    }
}
