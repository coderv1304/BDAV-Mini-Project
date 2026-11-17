package edu.bdav.ta.job.tfidf;

import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import edu.bdav.ta.types.TermDocWritable;

/**
 * J7b mapper: {@code ((term, docId), tf) -> (term, 1)}.
 *
 * <p>Because J7a emits exactly one record per (term, document), the number of
 * records for a term <em>is</em> its document frequency. Counting these records is
 * therefore the DF - no set, no deduplication, no extra pass.</p>
 */
public class DfMapper extends Mapper<TermDocWritable, DoubleWritable, Text, LongWritable> {

    private final Text outKey = new Text();
    private final LongWritable one = new LongWritable(1L);

    @Override
    protected void map(TermDocWritable key, DoubleWritable value, Context context)
            throws IOException, InterruptedException {
        outKey.set(key.getTerm());
        context.write(outKey, one);
    }
}
