package edu.bdav.ta.job.inverted;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import edu.bdav.ta.types.PostingWritable;

/**
 * J6 combiner: merges postings of the same word that come from the same document.
 *
 * <p><b>Safety.</b> Position-list concatenation per {@code (word, document)} is
 * associative and commutative <em>as a multiset operation</em>: the merged list
 * contains exactly the union of the contributions, and the reducer sorts it before
 * emitting, so the final order does not depend on how the shuffle grouped partial
 * results. Without that sort the combiner would be safe but the output would not be
 * deterministic - which is why the reducer sorts.</p>
 *
 * <p>Values for different documents are held in a small map bounded by the number
 * of distinct documents in one combine group; each is flushed immediately.</p>
 */
public class InvertedIndexCombiner extends Reducer<Text, PostingWritable, Text, PostingWritable> {

    private final Map<String, PostingWritable> byDocument = new LinkedHashMap<>();

    @Override
    protected void reduce(Text key, Iterable<PostingWritable> values, Context context)
            throws IOException, InterruptedException {
        byDocument.clear();
        for (PostingWritable value : values) {
            String document = value.getDocId().toString();
            PostingWritable existing = byDocument.get(document);
            if (existing == null) {
                PostingWritable copy = new PostingWritable();
                copy.set(document, value.getPositions().get().clone());
                byDocument.put(document, copy);
            } else {
                existing.merge(value);
            }
        }
        for (PostingWritable posting : byDocument.values()) {
            context.write(key, posting);
        }
    }
}
