package edu.bdav.ta.partition;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;
import edu.bdav.ta.types.TermDocWritable;

/**
 * Partitions composite {@code (term, docId)} keys by the <strong>term only</strong>.
 *
 * <p>This is the partitioner that makes a secondary-sort join possible: every
 * record for a term - the document-frequency record with the sentinel document
 * and all its term-frequency records - must reach the same reducer, otherwise no
 * reducer ever sees a complete join group and the join silently produces
 * nothing. {@code HashPartitioner} on the whole composite key does exactly that
 * wrong thing, which is the classic TF-IDF bug this class exists to prevent.</p>
 *
 * <p>Uses {@link Text#hashCode()}, a deterministic byte-level hash, so partition
 * assignment is stable across JVMs and runs (a prerequisite for byte-identical
 * output).</p>
 */
public class TermPartitioner extends Partitioner<TermDocWritable, Object> {

    @Override
    public int getPartition(TermDocWritable key, Object value, int numPartitions) {
        return Math.floorMod(key.getTerm().hashCode(), numPartitions);
    }
}
