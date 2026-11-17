package edu.bdav.ta.job.tfidf;

import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Mapper;
import edu.bdav.ta.types.TaggedCountWritable;
import edu.bdav.ta.types.TermDocWritable;

/**
 * J7c mapper for the term-frequency side of the join.
 *
 * <p>Emits the untouched composite key {@code (term, docId)} with a {@code 'T'}
 * tag. The document-frequency side is handled by {@link DfSideMapper}; the two are
 * wired together with {@code MultipleInputs}.</p>
 */
public class TfIdfJoinMapper extends Mapper<TermDocWritable, DoubleWritable, TermDocWritable, TaggedCountWritable> {

    private final TaggedCountWritable outValue = new TaggedCountWritable();

    @Override
    protected void map(TermDocWritable key, DoubleWritable value, Context context)
            throws IOException, InterruptedException {
        outValue.set(TaggedCountWritable.TAG_TERM_FREQUENCY, key.getDocId().toString(), value.get());
        context.write(key, outValue);
    }
}
