package edu.bdav.ta.job.sentiment;

import java.io.IOException;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import edu.bdav.ta.config.ConfKeys;
import edu.bdav.ta.counters.AnalysisCounters;
import edu.bdav.ta.text.SentimentLexicon;
import edu.bdav.ta.text.TextNormalizer;

/**
 * J9 mapper: scores each document against the lexicon.
 *
 * <p>Score definition, fixed so the Python reference can reproduce it exactly:
 * {@code polarity = (positive - negative) / tokens} where {@code positive} is the
 * sum of positive lexicon scores, {@code negative} the absolute sum of negative
 * ones, and {@code tokens} the number of accepted tokens in the document. Dividing
 * by length is what makes documents of different sizes comparable; without it,
 * longer documents always look more extreme.</p>
 *
 * <p>Tokens absent from the lexicon are counted
 * ({@code UNKNOWN_LEXICON_WORDS}) rather than ignored silently: coverage is a
 * first-order limitation of lexicon-based sentiment and should be visible in the
 * job's own metrics.</p>
 */
public class SentimentMapper extends Mapper<Text, Text, Text, Text> {

    private final Text outValue = new Text();
    private final TextNormalizer.Stats recordStats = new TextNormalizer.Stats();
    private final TextNormalizer.Stats totalStats = new TextNormalizer.Stats();
    private TextNormalizer normalizer;
    private SentimentLexicon lexicon;
    private double positive;
    private double negative;
    private long tokens;
    private long unknown;

    private final TextNormalizer.TokenConsumer consumer = new TextNormalizer.TokenConsumer() {
        @Override
        public void accept(CharSequence token, int length, int position) {
            Double score = lexicon.score(token);
            if (score == null) {
                unknown++;
                return;
            }
            if (score > 0.0d) {
                positive += score;
            } else if (score < 0.0d) {
                negative += -score;
            }
        }
    };

    @Override
    protected void setup(Context context) throws IOException {
        Configuration conf = context.getConfiguration();
        this.normalizer = TextNormalizer.from(conf);
        URI[] cacheFiles = context.getCacheFiles();
        this.lexicon = SentimentLexicon.load(conf, cacheFiles, conf.getTrimmed(ConfKeys.LEXICON_PATH, null));
    }

    @Override
    protected void map(Text documentId, Text content, Context context)
            throws IOException, InterruptedException {
        positive = 0.0d;
        negative = 0.0d;
        tokens = 0L;
        recordStats.reset();
        normalizer.tokenize(content, consumer, recordStats);
        totalStats.add(recordStats);
        tokens = recordStats.tokens;
        double polarity = tokens == 0L ? 0.0d : (positive - negative) / tokens;
        outValue.set(String.format(java.util.Locale.ROOT, "%.6f\t%d\t%.6f\t%.6f",
                polarity, tokens, positive, negative));
        context.write(documentId, outValue);
    }

    @Override
    protected void cleanup(Context context) {
        context.getCounter(AnalysisCounters.UNKNOWN_LEXICON_WORDS).increment(unknown);
        context.getCounter(AnalysisCounters.TOKENS_EMITTED).increment(totalStats.tokens);
        context.getCounter(AnalysisCounters.DOCUMENTS).increment(1);
    }
}
