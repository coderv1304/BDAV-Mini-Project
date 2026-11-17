package edu.bdav.ta.job.length;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import edu.bdav.ta.counters.AnalysisCounters;
import edu.bdav.ta.text.TextNormalizer;

/**
 * J3 mapper: {@code (line) -> (tokenLength, 1)}.
 *
 * <p>Length is measured in Unicode code points, not {@code String.length()}:
 * a UTF-16 length would report 2 for an emoji or an astral letter and would
 * disagree with both the Python reference ({@code len(token)}) and the tokeniser
 * contract.</p>
 */
public class WordLengthMapper extends Mapper<LongWritable, Text, IntWritable, LongWritable> {

    private final IntWritable outKey = new IntWritable();
    private final LongWritable one = new LongWritable(1L);
    private final TextNormalizer.Stats recordStats = new TextNormalizer.Stats();
    private final TextNormalizer.Stats totalStats = new TextNormalizer.Stats();
    private TextNormalizer normalizer;
    private Context context;

    private final TextNormalizer.TokenConsumer consumer = new TextNormalizer.TokenConsumer() {
        @Override
        public void accept(CharSequence token, int length, int position)
                throws IOException, InterruptedException {
            outKey.set(length);
            context.write(outKey, one);
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
        context.getCounter(AnalysisCounters.LONGEST_TOKEN_LEN).increment(totalStats.longestToken);
        context.getCounter(AnalysisCounters.SYMBOL_CHARS_DROPPED).increment(totalStats.symbolChars);
        context.getCounter(AnalysisCounters.NUMBERS_MASKED).increment(totalStats.numbersMasked);
    }
}
