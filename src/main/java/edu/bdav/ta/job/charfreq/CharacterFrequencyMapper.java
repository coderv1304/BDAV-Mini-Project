package edu.bdav.ta.job.charfreq;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import edu.bdav.ta.counters.AnalysisCounters;
import edu.bdav.ta.text.TextNormalizer;

/**
 * J4 mapper: {@code (line) -> (codePoint, 1)} for every letter/mark/digit code point.
 *
 * <p>Emits over the <em>normalised</em> text, so the histogram matches the token
 * stream the other jobs analyse (lowercasing has already folded case; symbols and
 * emoji are separators and are reported by {@code SYMBOL_CHARS_DROPPED} instead of
 * appearing here).</p>
 *
 * <p>{@link Text} keys are interned per code point in a small map: the alphabet is
 * a few thousand symbols, and re-using one key object per code point makes the
 * per-occurrence cost a hash lookup instead of an allocation.</p>
 */
public class CharacterFrequencyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private final IntWritable one = new IntWritable(1);
    private final Map<Integer, Text> keyCache = new HashMap<>();
    private final TextNormalizer.Stats recordStats = new TextNormalizer.Stats();
    private final TextNormalizer.Stats totalStats = new TextNormalizer.Stats();
    private TextNormalizer normalizer;
    private Context context;

    private final TextNormalizer.TokenConsumer consumer = new TextNormalizer.TokenConsumer() {
        @Override
        public void accept(CharSequence token, int length, int position)
                throws IOException, InterruptedException {
            int index = 0;
            while (index < token.length()) {
                int codePoint = Character.codePointAt(token, index);
                index += Character.charCount(codePoint);
                context.write(keyFor(codePoint), one);
            }
        }
    };

    @Override
    protected void setup(Context context) {
        this.context = context;
        Configuration conf = context.getConfiguration();
        this.normalizer = TextNormalizer.from(conf);
    }

    @Override
    protected void map(LongWritable offset, Text line, Context context)
            throws IOException, InterruptedException {
        recordStats.reset();
        normalizer.tokenize(line, consumer, recordStats);
        totalStats.add(recordStats);
    }

    @Override
    protected void cleanup(Context context) {
        context.getCounter(AnalysisCounters.TOKENS_EMITTED).increment(totalStats.tokens);
        context.getCounter(AnalysisCounters.SYMBOL_CHARS_DROPPED).increment(totalStats.symbolChars);
    }

    /** Returns a cached, reusable key for a code point. */
    private Text keyFor(int codePoint) {
        Text cached = keyCache.get(codePoint);
        if (cached == null) {
            cached = new Text(new String(Character.toChars(codePoint)));
            keyCache.put(codePoint, cached);
        }
        return cached;
    }
}
