package edu.bdav.ta.job.cooccur;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import edu.bdav.ta.config.ConfKeys;
import edu.bdav.ta.config.ConfigSupport;
import edu.bdav.ta.counters.AnalysisCounters;
import edu.bdav.ta.text.TextNormalizer;
import edu.bdav.ta.types.StripeWritable;

/**
 * J8s mapper: the <b>stripes</b> pattern - {@code w -> {w' : count}}.
 *
 * <p>Cost model: one record per distinct word per document instead of one record per
 * pair occurrence. The shuffle therefore scales with the vocabulary and the number of
 * documents, not with the number of pairs, and the combiner merges whole stripes in
 * one operation.</p>
 *
 * <p>The price is memory and serialisation work on the map side: a stripe for a head
 * term in a large document can hold many neighbours, and Hadoop serialises the whole
 * stripe on every emit. The benchmark reports shuffle bytes and elapsed time for both
 * patterns rather than declaring a winner in advance.</p>
 */
public class StripesMapper extends Mapper<Text, Text, Text, StripeWritable> {

    private final List<String> tokens = new ArrayList<>();
    private final Map<String, StripeWritable> stripes = new LinkedHashMap<>();
    private final Text outKey = new Text();
    private final TextNormalizer.Stats recordStats = new TextNormalizer.Stats();
    private final TextNormalizer.Stats totalStats = new TextNormalizer.Stats();
    private TextNormalizer normalizer;
    private Context context;
    private int window = 5;
    private boolean ordered = true;
    private long stripesEmitted;
    private long neighbours;

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
        stripes.clear();
        recordStats.reset();
        normalizer.tokenize(content, consumer, recordStats);
        totalStats.add(recordStats);
        int size = tokens.size();
        for (int i = 0; i < size; i++) {
            String word = tokens.get(i);
            StripeWritable stripe = stripes.get(word);
            if (stripe == null) {
                stripe = new StripeWritable();
                stripes.put(word, stripe);
            }
            int limit = Math.min(size, i + window + 1);
            for (int j = i + 1; j < limit; j++) {
                stripe.add(tokens.get(j), 1L);
                neighbours++;
                if (!ordered) {
                    stripe(wordOf(tokens.get(j)), tokens.get(i));
                    neighbours++;
                }
            }
        }
        for (Map.Entry<String, StripeWritable> entry : stripes.entrySet()) {
            outKey.set(entry.getKey());
            context.write(outKey, entry.getValue());
            stripesEmitted++;
        }
    }

    private StripeWritable wordOf(String word) {
        StripeWritable stripe = stripes.get(word);
        if (stripe == null) {
            stripe = new StripeWritable();
            stripes.put(word, stripe);
        }
        return stripe;
    }

    /** Adds {@code neighbour -> 1} to the stripe of {@code word}. */
    private void stripe(StripeWritable target, String neighbour) {
        target.add(neighbour, 1L);
    }

    @Override
    protected void cleanup(Context context) {
        context.getCounter(AnalysisCounters.STRIPES_EMITTED).increment(stripesEmitted);
        context.getCounter(AnalysisCounters.NEIGHBOURS_PER_WORD).increment(neighbours);
        context.getCounter(AnalysisCounters.TOKENS_EMITTED).increment(totalStats.tokens);
    }
}
