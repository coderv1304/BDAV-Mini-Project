package edu.bdav.ta.sort;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import edu.bdav.ta.types.TermDocWritable;

/**
 * Grouping comparator for the TF-IDF join: groups by <strong>term only</strong>.
 *
 * <p>The partitioner decides which reducer sees a term; the grouping comparator
 * decides how many {@code reduce()} calls that reducer makes for it. Grouping by
 * the full composite key would produce one call per {@code (term, doc)} pair and
 * make the join impossible, which is why partitioner and grouping comparator must
 * be designed together.</p>
 */
public class TermGroupingComparator extends WritableComparator {

    /** Registers the comparator for {@link TermDocWritable}. */
    public TermGroupingComparator() {
        super(TermDocWritable.class, true);
    }

    @Override
    @SuppressWarnings("rawtypes")
    public int compare(WritableComparable a, WritableComparable b) {
        return ((TermDocWritable) a).getTerm().compareTo(((TermDocWritable) b).getTerm());
    }
}
