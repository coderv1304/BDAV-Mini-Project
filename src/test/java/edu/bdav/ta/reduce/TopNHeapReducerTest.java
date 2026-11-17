package edu.bdav.ta.reduce;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.junit.Test;
import edu.bdav.ta.TestSupport;

/** Behavioural tests for the bounded min-heap top-N reducer. */
public class TopNHeapReducerTest {

    private static List<String> run(int topN, String[][] input) throws Exception {
        TopNHeapReducer reducer = new TopNHeapReducer();
        List<TestSupport.Emitted> sink = TestSupport.sink();
        Counters counters = new Counters();
        org.apache.hadoop.mapreduce.Reducer.Context context =
                TestSupport.reducerContext(sink, counters, TestSupport.conf("ta.topn.n", String.valueOf(topN)));
        reducer.setup(context);
        for (String[] row : input) {
            LongWritable value = new LongWritable(Long.parseLong(row[1]));
            reducer.reduce(new Text(row[0]), Arrays.asList(value), context);
        }
        reducer.cleanup(context);
        return TestSupport.render(sink);
    }

    @Test
    public void keepsTheHighestCountsInDescendingOrder() throws Exception {
        List<String> result = run(2, new String[][] {{"a", "5"}, {"b", "9"}, {"c", "7"}});
        assertEquals(Arrays.asList("b=9", "c=7"), result);
    }

    @Test
    public void breaksTiesByTermAscending() throws Exception {
        List<String> result = run(2, new String[][] {{"zebra", "4"}, {"apple", "4"}, {"mango", "4"}});
        assertEquals("ties must be stable and match the Python reference",
                Arrays.asList("apple=4", "mango=4"), result);
    }

    @Test
    public void sumsValuesWithinAGroup() throws Exception {
        TopNHeapReducer reducer = new TopNHeapReducer();
        List<TestSupport.Emitted> sink = TestSupport.sink();
        org.apache.hadoop.mapreduce.Reducer.Context context =
                TestSupport.reducerContext(sink, new Counters(), TestSupport.conf("ta.topn.n", "1"));
        reducer.setup(context);
        reducer.reduce(new Text("word"),
                Arrays.asList(new LongWritable(2), new LongWritable(3)), context);
        reducer.cleanup(context);
        assertEquals(Arrays.asList("word=5"), TestSupport.render(sink));
    }

    @Test
    public void emitsAtMostNEntries() throws Exception {
        String[][] input = new String[50][];
        for (int i = 0; i < 50; i++) {
            input[i] = new String[] {"term" + i, String.valueOf(i)};
        }
        assertEquals(10, run(10, input).size());
    }

    @Test
    public void handlesFewerCandidatesThanN() throws Exception {
        assertEquals(Arrays.asList("a=1"), run(100, new String[][] {{"a", "1"}}));
    }
}
