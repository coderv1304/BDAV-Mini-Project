package edu.bdav.ta.job.inverted;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import edu.bdav.ta.counters.AnalysisCounters;
import edu.bdav.ta.text.TextNormalizer;
import edu.bdav.ta.types.PostingWritable;

/**
 * J6 mapper: {@code (docId, document) -> (word, posting)}.
 *
 * <p>Reads whole documents ({@code WholeFileInputFormat}), so document boundaries
 * are real boundaries rather than split artefacts. Positions are the ordinal of
 * each accepted token inside the document, which is what makes the index usable
 * for phrase queries.</p>
 *
 * <p><b>In-mapper combining is mandatory here, not optional.</b> A word occurring
 * 400 times in a document would otherwise emit 400 postings through the shuffle;
 * aggregating per document first emits one posting per (word, document) pair, and
 * the combiner then merges across documents. Memory is bounded by the document's
 * vocabulary - accepted because a document, not the corpus, is the unit of work.</p>
 */
public class InvertedIndexMapper extends Mapper<Text, Text, Text, PostingWritable> {

    /** Configuration key documenting the per-document memory bound. */
    public static final String MAX_TERMS_PER_DOC = "ta.inverted.maxterms";

    private final Map<String, List<Integer>> postings = new LinkedHashMap<>();
    private final Text outKey = new Text();
    private final PostingWritable outValue = new PostingWritable();
    private final TextNormalizer.Stats recordStats = new TextNormalizer.Stats();
    private final TextNormalizer.Stats totalStats = new TextNormalizer.Stats();
    private TextNormalizer normalizer;
    private Context context;
    private int maxTermsPerDocument = 2_000_000;
    private long documents;
    private long emitted;

    private final TextNormalizer.TokenConsumer consumer = new TextNormalizer.TokenConsumer() {
        @Override
        public void accept(CharSequence token, int length, int position) {
            postings.computeIfAbsent(token.toString(), key -> new ArrayList<>(4)).add(position);
        }
    };

    @Override
    protected void setup(Context context) {
        this.context = context;
        Configuration conf = context.getConfiguration();
        this.normalizer = TextNormalizer.from(conf);
        this.maxTermsPerDocument = conf.getInt(MAX_TERMS_PER_DOC, 2_000_000);
    }

    @Override
    protected void map(Text documentId, Text content, Context context)
            throws IOException, InterruptedException {
        documents++;
        postings.clear();
        recordStats.reset();
        normalizer.tokenize(content, consumer, recordStats);
        totalStats.add(recordStats);
        if (postings.size() > maxTermsPerDocument) {
            throw new IOException("document '" + documentId + "' exceeds " + maxTermsPerDocument
                    + " distinct terms; raise -D" + MAX_TERMS_PER_DOC + " or split the document");
        }
        for (Map.Entry<String, List<Integer>> entry : postings.entrySet()) {
            List<Integer> positions = entry.getValue();
            int[] array = new int[positions.size()];
            for (int i = 0; i < array.length; i++) {
                array[i] = positions.get(i);
            }
            outValue.set(documentId.toString(), array);
            outKey.set(entry.getKey());
            context.write(outKey, outValue);
            emitted++;
        }
    }

    @Override
    protected void cleanup(Context context) {
        context.getCounter(AnalysisCounters.DOCUMENTS).increment(documents);
        context.getCounter(AnalysisCounters.POSTINGS_EMITTED).increment(emitted);
        context.getCounter(AnalysisCounters.TOKENS_EMITTED).increment(totalStats.tokens);
        context.getCounter(AnalysisCounters.MALFORMED_LINES).increment(0);
    }
}
