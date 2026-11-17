package edu.bdav.ta.partition;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.HashSet;
import java.util.Set;
import org.apache.hadoop.io.Text;
import org.junit.Test;
import edu.bdav.ta.sort.TermDocSortComparator;
import edu.bdav.ta.sort.TermGroupingComparator;
import edu.bdav.ta.types.TermDocWritable;

/** Tests for the partitioner/grouping pair that make the secondary-sort join work. */
public class PartitionerAndComparatorTest {

    @Test
    public void termPartitionerIgnoresTheDocumentId() {
        TermPartitioner partitioner = new TermPartitioner();
        int sentinel = partitioner.getPartition(new TermDocWritable("apple", "*"), null, 7);
        for (String document : new String[] {"doc-1.txt", "doc-2.txt", "zzz"}) {
            assertEquals("all records of a term must reach the same reducer",
                    sentinel, partitioner.getPartition(new TermDocWritable("apple", document), null, 7));
        }
    }

    @Test
    public void termPartitionerStaysInRangeAndSpreadsTerms() {
        TermPartitioner partitioner = new TermPartitioner();
        Set<Integer> used = new HashSet<>();
        for (int i = 0; i < 200; i++) {
            int partition = partitioner.getPartition(new TermDocWritable("term" + i, "d"), null, 8);
            assertTrue(partition >= 0 && partition < 8);
            used.add(partition);
        }
        assertTrue("a hash partitioner should use most reducers", used.size() >= 6);
    }

    @Test
    public void groupingComparatorGroupsByTermOnly() {
        TermGroupingComparator grouping = new TermGroupingComparator();
        TermDocWritable sentinel = new TermDocWritable("apple", "*");
        TermDocWritable first = new TermDocWritable("apple", "doc-1.txt");
        TermDocWritable otherTerm = new TermDocWritable("banana", "doc-1.txt");
        assertEquals(0, grouping.compare(sentinel, first));
        assertTrue(grouping.compare(sentinel, otherTerm) < 0);
    }

    @Test
    public void sortComparatorPutsTheSentinelFirst() {
        TermDocSortComparator sort = new TermDocSortComparator();
        TermDocWritable sentinel = new TermDocWritable("apple", "*");
        TermDocWritable first = new TermDocWritable("apple", "doc-1.txt");
        assertTrue("the DF record must arrive before any TF record",
                sort.compare(sentinel, first) < 0);
        assertTrue(sort.compare(first, sentinel) > 0);
    }

    @Test
    public void saltedPartitionerSplitsHotTermsAcrossReducers() {
        SaltedTextPartitioner partitioner = new SaltedTextPartitioner();
        partitioner.addHotTerm("the");
        partitioner.setSaltPool(4);
        Set<Integer> partitions = new HashSet<>();
        for (int i = 0; i < 200; i++) {
            partitions.add(partitioner.getPartition(new Text("the"), null, 8));
        }
        assertTrue("a hot term must be spread, not pinned to one reducer", partitions.size() > 1);
    }

    @Test
    public void saltedPartitionerKeepsColdTermsStable() {
        SaltedTextPartitioner partitioner = new SaltedTextPartitioner();
        partitioner.addHotTerm("the");
        partitioner.setSaltPool(4);
        int first = partitioner.getPartition(new Text("zeppelin"), null, 8);
        for (int i = 0; i < 20; i++) {
            assertEquals(first, partitioner.getPartition(new Text("zeppelin"), null, 8));
        }
        assertTrue(first < 8);
    }
}
