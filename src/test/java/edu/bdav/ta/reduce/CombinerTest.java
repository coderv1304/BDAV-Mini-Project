package edu.bdav.ta.reduce;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Reducer;
import org.junit.Test;
import edu.bdav.ta.TestSupport;

/**
 * Tests for the generic sum combiners.
 *
 * <p>The property under test is not "it adds numbers" but "partial aggregation is
 * safe": grouping the same multiset of values differently must produce the same
 * total. That is the associativity/commutativity argument that licenses a combiner
 * at all, so it is asserted directly.</p>
 */
public class CombinerTest {

    @Test
    public void intSumCombinerIsAssociativeAndCommutative() throws Exception {
        long groupedOnce = sumInts(new int[] {1, 2, 3, 4, 5});
        long groupedDifferently = sumInts(new int[] {5, 1, 4, 2, 3});
        assertEquals(15L, groupedOnce);
        assertEquals("combining must not depend on value order", groupedOnce, groupedDifferently);
    }

    @Test
    public void intSumCombinerHandlesNegativeAndZero() throws Exception {
        assertEquals(0L, sumInts(new int[] {0, 0, 0}));
        assertEquals(-2L, sumInts(new int[] {-5, 3}));
    }

    @Test
    public void longSumCombinerAccumulatesBeyondIntRange() throws Exception {
        LongSumCombiner<Text> combiner = new LongSumCombiner<>();
        List<TestSupport.Emitted> sink = TestSupport.sink();
        Reducer.Context context = TestSupport.reducerContext(sink, new Counters(), TestSupport.conf());
        combiner.reduce(new Text("x"), Arrays.asList(new LongWritable(3_000_000_000L),
                new LongWritable(3_000_000_000L)), context);
        assertEquals(Arrays.asList("x=6000000000"), TestSupport.render(sink));
    }

    private static long sumInts(int[] values) throws Exception {
        IntSumCombiner<Text> combiner = new IntSumCombiner<>();
        List<TestSupport.Emitted> sink = TestSupport.sink();
        Reducer.Context context = TestSupport.reducerContext(sink, new Counters(), TestSupport.conf());
        IntWritable[] writables = new IntWritable[values.length];
        for (int i = 0; i < values.length; i++) {
            writables[i] = new IntWritable(values[i]);
        }
        combiner.reduce(new Text("k"), Arrays.asList(writables), context);
        return ((IntWritable) sink.get(0).value()).get();
    }
}
