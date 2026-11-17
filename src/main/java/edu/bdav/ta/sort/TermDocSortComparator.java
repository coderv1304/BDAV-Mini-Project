package edu.bdav.ta.sort;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import edu.bdav.ta.types.TermDocWritable;

/**
 * Sort order for the TF-IDF join: term ascending, then document id ascending.
 *
 * <p>The sentinel document {@code "*"} sorts before every alphanumeric document
 * name in UTF-8 byte order, so the document-frequency record is the <em>first</em>
 * value the reducer sees for a term. That ordering is the entire join mechanism:
 * no buffering, no side list, bounded memory per group.</p>
 *
 * <p>Implemented as a full-object comparator ({@link Comparable#compareTo}) because
 * the key is already a compact {@code WritableComparable}; a byte-level comparator
 * would duplicate the vint framing logic for no measurable gain at these record
 * sizes.</p>
 */
public class TermDocSortComparator extends WritableComparator {

    /** Registers the comparator for {@link TermDocWritable}. */
    public TermDocSortComparator() {
        super(TermDocWritable.class, true);
    }

    @Override
    @SuppressWarnings("rawtypes")
    public int compare(WritableComparable a, WritableComparable b) {
        return ((TermDocWritable) a).compareTo((TermDocWritable) b);
    }
}
