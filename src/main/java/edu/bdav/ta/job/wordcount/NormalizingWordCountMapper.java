package edu.bdav.ta.job.wordcount;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import edu.bdav.ta.config.ConfKeys;
import edu.bdav.ta.config.ConfigSupport;
import edu.bdav.ta.counters.AnalysisCounters;
import edu.bdav.ta.text.TextNormalizer;
import edu.bdav.ta.text.Utf8;

/**
 * Normalising tokeniser mapper: {@code (line) -> (token, 1)}.
 *
 * <p>Three things are worth defending in a viva:</p>
 * <ol>
 *   <li><b>Object reuse.</b> One {@link Text} key, one {@link IntWritable} value and
 *       one reusable {@code byte[]}; tokens are encoded straight from the
 *       normaliser's buffer, so the hot loop allocates nothing per token.</li>
 *   <li><b>Three combining modes</b> ({@code none}, {@code combiner}, {@code inmapper},
 *       {@code both}) selected by {@code ta.combine.mode}. In-mapper combining is
 *       bounded by {@code ta.inmapper.max.entries} so that a pathological
 *       vocabulary flushes early instead of exhausting the container heap.</li>
 *   <li><b>Counters are flushed once in {@code cleanup()}</b>, not per record:
 *       counter updates are cheap but not free, and batching them keeps the
 *       measurement overhead out of the benchmark.</li>
 * </ol>
 */
public class NormalizingWordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    /** Configuration key bounding the in-mapper aggregation map. */
    public static final String INMAPPER_MAX_ENTRIES = "ta.inmapper.max.entries";

    private final Text outKey = new Text();
    private final IntWritable one = new IntWritable(1);
    private final IntWritable reusableCount = new IntWritable();
    private final TextNormalizer.Stats recordStats = new TextNormalizer.Stats();
    private final TextNormalizer.Stats totalStats = new TextNormalizer.Stats();

    private byte[] keyBuffer = new byte[256];
    private TextNormalizer normalizer;
    private Context context;
    private Map<String, Integer> inMapperCounts;
    private int inMapperMaxEntries = 50_000;
    private long records;
    private long emptyLines;
    private long malformedLines;

    private final TextNormalizer.TokenConsumer consumer = new TextNormalizer.TokenConsumer() {
        @Override
        public void accept(CharSequence token, int length, int position) throws IOException, InterruptedException {
            if (inMapperCounts == null) {
                emit(token);
            } else {
                String key = token.toString();
                Integer previous = inMapperCounts.get(key);
                inMapperCounts.put(key, previous == null ? 1 : previous + 1);
                if (inMapperCounts.size() >= inMapperMaxEntries) {
                    flushInMapperCounts();
                }
            }
        }
    };

    @Override
    protected void setup(Context context) {
        this.context = context;
        Configuration conf = context.getConfiguration();
        this.normalizer = TextNormalizer.from(conf);
        this.inMapperMaxEntries = conf.getInt(INMAPPER_MAX_ENTRIES, 50_000);
        String mode = ConfigSupport.getTrimmed(conf, ConfKeys.COMBINE_MODE, "combiner");
        if ("inmapper".equalsIgnoreCase(mode) || "both".equalsIgnoreCase(mode)) {
            this.inMapperCounts = new HashMap<>();
        }
    }

    @Override
    protected void map(LongWritable offset, Text line, Context context)
            throws IOException, InterruptedException {
        records++;
        if (line.getLength() == 0 || isBlank(line)) {
            emptyLines++;
            return;
        }
        if (containsReplacementChar(line)) {
            malformedLines++;
        }
        recordStats.reset();
        normalizer.tokenize(line, consumer, recordStats);
        totalStats.add(recordStats);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        flushInMapperCounts();
        context.getCounter(AnalysisCounters.RECORDS_IN).increment(records);
        context.getCounter(AnalysisCounters.EMPTY_LINES).increment(emptyLines);
        context.getCounter(AnalysisCounters.MALFORMED_LINES).increment(malformedLines);
        context.getCounter(AnalysisCounters.TOKENS_EMITTED).increment(totalStats.tokens);
        context.getCounter(AnalysisCounters.NUMBERS_MASKED).increment(totalStats.numbersMasked);
        context.getCounter(AnalysisCounters.NUMBERS_DROPPED).increment(totalStats.numbersDropped);
        context.getCounter(AnalysisCounters.TOKENS_TOO_SHORT).increment(totalStats.tokensTooShort);
        context.getCounter(AnalysisCounters.SYMBOL_CHARS_DROPPED).increment(totalStats.symbolChars);
        context.getCounter(AnalysisCounters.LONGEST_TOKEN_LEN).increment(totalStats.longestToken);
    }

    /** Encodes a token straight into {@link Text} without an intermediate String. */
    private void emit(CharSequence token) throws IOException, InterruptedException {
        int needed = Utf8.maxBytes(token);
        if (needed > keyBuffer.length) {
            keyBuffer = new byte[needed];
        }
        int bytes = Utf8.encode(token, keyBuffer);
        outKey.set(keyBuffer, 0, bytes);
        context.write(outKey, one);
    }

    /** Flushes and clears the in-mapper aggregation table. */
    private void flushInMapperCounts() throws IOException, InterruptedException {
        if (inMapperCounts == null || inMapperCounts.isEmpty()) {
            return;
        }
        Iterator<Map.Entry<String, Integer>> iterator = inMapperCounts.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, Integer> entry = iterator.next();
            outKey.set(entry.getKey());
            reusableCount.set(entry.getValue());
            context.write(outKey, reusableCount);
            iterator.remove();
        }
    }

    /**
     * @return true when the record holds only ASCII whitespace.
     *
     * <p>Cheap byte scan: every byte {@code <= 0x20} is whitespace in UTF-8, so a line
     * with any printable or multibyte byte is immediately non-blank. Non-ASCII
     * whitespace (U+00A0) is not treated as blank, which is consistent with the
     * tokeniser, where it is a separator and therefore produces no tokens anyway.</p>
     */
    static boolean isBlank(Text line) {
        byte[] bytes = line.getBytes();
        for (int i = 0; i < line.getLength(); i++) {
            if ((bytes[i] & 0xFF) > 0x20) {
                return false;
            }
        }
        return true;
    }

    /** @return true when the line contains U+FFFD, i.e. the decoder replaced bytes */
    static boolean containsReplacementChar(Text line) {
        byte[] bytes = line.getBytes();
        for (int i = 0; i < line.getLength() - 2; i++) {
            if ((bytes[i] & 0xFF) == 0xEF && (bytes[i + 1] & 0xFF) == 0xBF && (bytes[i + 2] & 0xFF) == 0xBD) {
                return true;
            }
        }
        return false;
    }
}
