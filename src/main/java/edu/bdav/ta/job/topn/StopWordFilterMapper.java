package edu.bdav.ta.job.topn;

import java.io.IOException;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import edu.bdav.ta.config.ConfKeys;
import edu.bdav.ta.counters.AnalysisCounters;
import edu.bdav.ta.text.StopWordDictionary;

/**
 * J2 mapper: filters aggregated counts against the {@code DistributedCache} stop-word list.
 *
 * <p>Filtering <em>after</em> aggregation is a deliberate design choice: J1 emits one
 * record per distinct token, so this stage reads the vocabulary (orders of
 * magnitude smaller than the corpus) instead of re-scanning every token of the
 * raw text. Stop-word totals are preserved as counters, so dropping them stays
 * measurable rather than invisible.</p>
 */
public class StopWordFilterMapper extends Mapper<Text, LongWritable, Text, LongWritable> {

    private final StopWordDictionary dictionary;
    private final Text outKey = new Text();
    private final LongWritable outValue = new LongWritable();
    private long droppedTokens;
    private long droppedOccurrences;

    /** No-arg constructor for the framework; the dictionary is loaded in {@link #setup}. */
    public StopWordFilterMapper() {
        this.dictionary = null;
    }

    /** Test constructor: injects a dictionary so the mapper needs no cache files. */
    StopWordFilterMapper(StopWordDictionary dictionary) {
        this.dictionary = dictionary;
    }

    private StopWordDictionary activeDictionary;

    @Override
    protected void setup(Context context) throws IOException {
        if (dictionary != null) {
            activeDictionary = dictionary;
            return;
        }
        Configuration conf = context.getConfiguration();
        URI[] cacheFiles = context.getCacheFiles();
        activeDictionary = StopWordDictionary.load(conf, cacheFiles,
                conf.getTrimmed(ConfKeys.STOPWORDS_PATH, null));
    }

    @Override
    protected void map(Text key, LongWritable value, Context context)
            throws IOException, InterruptedException {
        if (activeDictionary.isStopWord(key)) {
            droppedTokens++;
            droppedOccurrences += value.get();
            return;
        }
        outKey.set(key);
        outValue.set(value.get());
        context.write(outKey, outValue);
    }

    @Override
    protected void cleanup(Context context) {
        context.getCounter(AnalysisCounters.STOPWORDS_DROPPED).increment(droppedTokens);
        context.getCounter(AnalysisCounters.STOPWORD_OCCURRENCES_DROPPED).increment(droppedOccurrences);
    }
}
