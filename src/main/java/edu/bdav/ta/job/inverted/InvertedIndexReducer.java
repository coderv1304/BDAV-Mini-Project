package edu.bdav.ta.job.inverted;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.TreeMap;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import edu.bdav.ta.types.PostingWritable;

/**
 * J6 reducer: emits {@code word<TAB>doc:count:p1,p2,...<TAB>doc:count:...}.
 *
 * <p>Postings are collected into a {@link TreeMap} keyed by document id, so the
 * emitted list is ordered by document in UTF-8 byte order regardless of shuffle
 * order - the determinism requirement. Positions inside each posting are sorted
 * for the same reason, which also makes the combiner's merge order irrelevant.</p>
 *
 * <p>Memory is bounded by the postings of one word, the natural unit of work for an
 * index. A Zipfian head term can still be large; that is a property of the data, and
 * it is measured rather than hidden.</p>
 */
public class InvertedIndexReducer extends Reducer<Text, PostingWritable, Text, Text> {

    private final TreeMap<String, PostingWritable> postings = new TreeMap<>();
    private final Text outValue = new Text();

    @Override
    protected void reduce(Text key, Iterable<PostingWritable> values, Context context)
            throws IOException, InterruptedException {
        postings.clear();
        for (PostingWritable value : values) {
            String document = value.getDocId().toString();
            PostingWritable existing = postings.get(document);
            if (existing == null) {
                postings.put(document, copyOf(value));
            } else {
                existing.merge(value);
            }
        }
        StringBuilder rendered = new StringBuilder(postings.size() * 24);
        for (Map.Entry<String, PostingWritable> entry : postings.entrySet()) {
            int[] positions = entry.getValue().getPositions().get().clone();
            Arrays.sort(positions);
            if (rendered.length() > 0) {
                rendered.append('\t');
            }
            rendered.append(entry.getKey()).append(':').append(positions.length).append(':');
            appendPositions(rendered, positions);
        }
        outValue.set(rendered.toString());
        context.write(key, outValue);
    }

    private static void appendPositions(StringBuilder target, int[] positions) {
        for (int i = 0; i < positions.length; i++) {
            if (i > 0) {
                target.append(',');
            }
            target.append(positions[i]);
        }
    }

    private static PostingWritable copyOf(PostingWritable source) {
        PostingWritable copy = new PostingWritable();
        copy.set(source.getDocId().toString(), source.getPositions().get().clone());
        return copy;
    }
}
