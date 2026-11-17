package edu.bdav.ta.job.tfidf;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import edu.bdav.ta.config.ConfKeys;
import edu.bdav.ta.config.ConfigSupport;
import edu.bdav.ta.types.TaggedCountWritable;
import edu.bdav.ta.types.TermDocWritable;

/**
 * J7c reducer: the TF-IDF join itself.
 *
 * <p><b>Join contract.</b> The grouping comparator groups by term only, the sort
 * comparator orders the sentinel document first, so the first value of every group
 * is the document frequency. Term frequencies follow, one per document. The reducer
 * therefore computes {@code idf = log10(N / df)} once per term and multiplies it
 * into each {@code tf} - a single pass with bounded memory (only the documents that
 * contain the term are buffered).</p>
 *
 * <p>Term frequencies are buffered rather than streamed so the reducer still works
 * if the document-frequency record is missing (a partially written DF stage). In
 * that case the term is skipped and counted, instead of producing silently wrong
 * weights - a join that cannot fail loudly is worse than one that fails.</p>
 *
 * <p>Output: text {@code term<TAB>docId<TAB>weight}, {@code weight} formatted with
 * {@code ta.numeric.format} (default {@code %.6f}) under {@code Locale.ROOT}.</p>
 */
public class TfIdfJoinReducer extends Reducer<TermDocWritable, TaggedCountWritable, Text, Text> {

    /** Counter name for groups without a document-frequency record. */
    public static final String MISSING_DF_COUNTER = "TFIDF_MISSING_DF";

    private static final Logger LOG = LoggerFactory.getLogger(TfIdfJoinReducer.class);

    private final List<String> documents = new ArrayList<>();
    private final List<Double> weights = new ArrayList<>();
    private final Text outValue = new Text();
    private long missingDocumentFrequency;
    private long documentsInCorpus;
    private String numericFormat;

    @Override
    protected void setup(Context context) {
        documentsInCorpus = ConfigSupport.getLong(context.getConfiguration(), ConfKeys.CORPUS_DOCS, 1L, Long.MAX_VALUE);
        numericFormat = ConfigSupport.getTrimmed(context.getConfiguration(), ConfKeys.NUMERIC_FORMAT, "%.6f");
    }

    @Override
    protected void reduce(TermDocWritable key, Iterable<TaggedCountWritable> values, Context context)
            throws IOException, InterruptedException {
        documents.clear();
        weights.clear();
        double documentFrequency = -1.0d;
        for (TaggedCountWritable value : values) {
            if (value.isDocumentFrequency()) {
                documentFrequency = value.getValue();
            } else {
                // The document id must come from the value: the reduce key is the group's
                // FIRST key, i.e. the sentinel (term, "*") record, not this record's key.
                documents.add(value.getDocId().toString());
                weights.add(value.getValue());
            }
        }
        if (documentFrequency <= 0.0d) {
            missingDocumentFrequency++;
            return;
        }
        double idf = Math.log10((double) documentsInCorpus / documentFrequency);
        String term = key.getTerm().toString();
        for (int i = 0; i < documents.size(); i++) {
            double weight = weights.get(i) * idf;
            outValue.set(documents.get(i) + "\t"
                    + String.format(java.util.Locale.ROOT, numericFormat, weight));
            context.write(new Text(term), outValue);
        }
    }

    @Override
    protected void cleanup(Context context) {
        if (missingDocumentFrequency > 0L) {
            LOG.warn("{} terms had no document-frequency record; rerun the DF stage", missingDocumentFrequency);
        }
        context.getCounter("TA", MISSING_DF_COUNTER).increment(missingDocumentFrequency);
    }
}
