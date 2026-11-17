package edu.bdav.ta.job.ngram;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import edu.bdav.ta.config.ConfKeys;
import edu.bdav.ta.config.ConfigSupport;
import edu.bdav.ta.counters.AnalysisCounters;
import edu.bdav.ta.text.NGramBuilder;
import edu.bdav.ta.text.TextNormalizer;

/**
 * J5 mapper: {@code (line) -> (ngram, 1)} for a configurable n.
 *
 * <p>Window state resets per line, so n-grams never straddle a line break - the
 * alternative would join the last word of one chapter to the first of the next.
 * With {@code ta.ngram.padding=true} the boundary n-grams are emitted with
 * {@code <s>} / {@code </s>} markers, which is what makes sentence-initial
 * bigrams comparable instead of invisible.</p>
 *
 * <p>Components are joined with U+0001 so an n-gram key can never be confused
 * with a token containing a space.</p>
 */
public class NGramMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private final Text outKey = new Text();
    private final IntWritable one = new IntWritable(1);
    private final TextNormalizer.Stats recordStats = new TextNormalizer.Stats();
    private final TextNormalizer.Stats totalStats = new TextNormalizer.Stats();
    private TextNormalizer normalizer;
    private NGramBuilder builder;
    private Context context;
    private long emitted;

    private final TextNormalizer.TokenConsumer consumer = new TextNormalizer.TokenConsumer() {
        @Override
        public void accept(CharSequence token, int length, int position)
                throws IOException, InterruptedException {
            if (builder.add(token, outKey)) {
                context.write(outKey, one);
                emitted++;
            }
        }
    };

    @Override
    protected void setup(Context context) {
        this.context = context;
        Configuration conf = context.getConfiguration();
        this.normalizer = TextNormalizer.from(conf);
        int order = ConfigSupport.getInt(conf, ConfKeys.NGRAM_N, 1, 5);
        boolean padding = ConfigSupport.getBoolean(conf, ConfKeys.NGRAM_PADDING);
        this.builder = new NGramBuilder(order, padding);
    }

    @Override
    protected void map(LongWritable offset, Text line, Context context)
            throws IOException, InterruptedException {
        builder.reset();
        recordStats.reset();
        normalizer.tokenize(line, consumer, recordStats);
        while (builder.finish(outKey)) {
            context.write(outKey, one);
            emitted++;
        }
        totalStats.add(recordStats);
    }

    @Override
    protected void cleanup(Context context) {
        context.getCounter(AnalysisCounters.NGRAMS_EMITTED).increment(emitted);
        context.getCounter(AnalysisCounters.TOKENS_EMITTED).increment(totalStats.tokens);
        context.getCounter(AnalysisCounters.NUMBERS_MASKED).increment(totalStats.numbersMasked);
    }
}
