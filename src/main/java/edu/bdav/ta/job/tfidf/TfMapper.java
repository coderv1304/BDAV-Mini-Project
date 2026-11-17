package edu.bdav.ta.job.tfidf;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import edu.bdav.ta.counters.AnalysisCounters;
import edu.bdav.ta.text.TextNormalizer;
import edu.bdav.ta.types.TermDocWritable;

/**
 * J7a mapper: {@code (docId, document) -> ((term, docId), tf)}.
 *
 * <p>One record per (term, document) is produced by in-mapper combining: a word
 * occurring 300 times emits {@code tf = 300}, not 300 records. That is both the
 * smallest possible shuffle for this job and the value the reducer needs.</p>
 */
public class TfMapper extends Mapper<Text, Text, TermDocWritable, IntWritable> {

    /** Per-document distinct-term guard, same rationale as the inverted index. */
    public static final String MAX_TERMS_PER_DOC = "ta.tf.maxterms";

    private final Map<String, Integer> counts = new HashMap<>();
    private final TermDocWritable outKey = new TermDocWritable();
    private final IntWritable outValue = new IntWritable();
    private final TextNormalizer.Stats recordStats = new TextNormalizer.Stats();
    private final TextNormalizer.Stats totalStats = new TextNormalizer.Stats();
    private TextNormalizer normalizer;
    private int maxTermsPerDocument = 2_000_000;
    private long documents;
    private long emitted;

    private final TextNormalizer.TokenConsumer consumer = new TextNormalizer.TokenConsumer() {
        @Override
        public void accept(CharSequence token, int length, int position) {
            counts.merge(token.toString(), 1, Integer::sum);
        }
    };

    @Override
    protected void setup(Context context) {
        Configuration conf = context.getConfiguration();
        this.normalizer = TextNormalizer.from(conf);
        this.maxTermsPerDocument = conf.getInt(MAX_TERMS_PER_DOC, 2_000_000);
    }

    @Override
    protected void map(Text documentId, Text content, Context context)
            throws IOException, InterruptedException {
        documents++;
        counts.clear();
        recordStats.reset();
        normalizer.tokenize(content, consumer, recordStats);
        totalStats.add(recordStats);
        if (counts.size() > maxTermsPerDocument) {
            throw new IOException("document '" + documentId + "' exceeds " + maxTermsPerDocument
                    + " distinct terms; raise -D" + MAX_TERMS_PER_DOC);
        }
        String document = documentId.toString();
        for (Map.Entry<String, Integer> entry : counts.entrySet()) {
            outKey.set(entry.getKey(), document);
            outValue.set(entry.getValue());
            context.write(outKey, outValue);
            emitted++;
        }
    }

    @Override
    protected void cleanup(Context context) {
        context.getCounter(AnalysisCounters.DOCUMENTS).increment(documents);
        context.getCounter(AnalysisCounters.TOKENS_EMITTED).increment(totalStats.tokens);
        context.getCounter(AnalysisCounters.RECORDS_IN).increment(emitted);
    }
}
