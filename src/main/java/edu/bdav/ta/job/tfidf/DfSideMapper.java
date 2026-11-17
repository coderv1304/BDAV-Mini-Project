package edu.bdav.ta.job.tfidf;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import edu.bdav.ta.types.TaggedCountWritable;
import edu.bdav.ta.types.TermDocWritable;

/**
 * J7c mapper for the document-frequency side of the join.
 *
 * <p>Rewrites {@code (term, df)} into {@code ((term, "*"), df)} so that both sides of
 * the join share one key space and one shuffle. The sentinel document id sorts
 * before every real document id in UTF-8 byte order, which is what lets the reducer
 * read the IDF before any term frequency arrives.</p>
 */
public class DfSideMapper extends Mapper<Text, LongWritable, TermDocWritable, TaggedCountWritable> {

    private final TermDocWritable outKey = new TermDocWritable();
    private final TaggedCountWritable outValue = new TaggedCountWritable();

    @Override
    protected void map(Text key, LongWritable value, Context context)
            throws IOException, InterruptedException {
        outKey.set(key, TermDocWritable.SENTINEL_DOC);
        outValue.set(TaggedCountWritable.TAG_DOCUMENT_FREQUENCY, "", value.get());
        context.write(outKey, outValue);
    }
}
