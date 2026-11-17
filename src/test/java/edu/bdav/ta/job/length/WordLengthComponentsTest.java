package edu.bdav.ta.job.length;

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
import edu.bdav.ta.counters.AnalysisCounters;

/** Tests for the word-length distribution stage. */
public class WordLengthComponentsTest {

    @Test
    public void emitsLengthPerToken() throws Exception {
        WordLengthMapper mapper = new WordLengthMapper();
        List<TestSupport.Emitted> sink = TestSupport.sink();
        Counters counters = new Counters();
        org.apache.hadoop.mapreduce.Mapper.Context context =
                TestSupport.mapperContext(sink, counters, TestSupport.conf());
        mapper.setup(context);
        mapper.map(new LongWritable(0), new Text("a bb ccc"), context);
        mapper.cleanup(context);
        assertEquals(Arrays.asList("1=1", "2=1", "3=1"), TestSupport.render(sink));
        assertEquals(3L, TestSupport.counter(counters, AnalysisCounters.TOKENS_EMITTED));
    }

    @Test
    public void reducerBuildsASortedHistogram() throws Exception {
        WordLengthReducer reducer = new WordLengthReducer();
        List<TestSupport.Emitted> sink = TestSupport.sink();
        Reducer.Context context = TestSupport.reducerContext(sink, new Counters(), TestSupport.conf());
        reducer.reduce(new IntWritable(3), Arrays.asList(new LongWritable(2), new LongWritable(1)), context);
        reducer.reduce(new IntWritable(1), Arrays.asList(new LongWritable(5)), context);
        reducer.cleanup(context);
        assertEquals(Arrays.asList("1=5", "3=3"), TestSupport.render(sink));
    }
}
