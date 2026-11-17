package edu.bdav.ta.job.topn;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.junit.Test;
import edu.bdav.ta.TestSupport;
import edu.bdav.ta.counters.AnalysisCounters;
import edu.bdav.ta.text.StopWordDictionary;

/** Tests the DistributedCache-backed stop-word filter and its accounting. */
public class StopWordTopNComponentsTest {

    @Test
    public void dropsStopWordsButKeepsTheirTotalsInCounters() throws Exception {
        StopWordFilterMapper mapper = new StopWordFilterMapper(
                StopWordDictionary.of(Arrays.asList("the", "and")));
        List<TestSupport.Emitted> sink = TestSupport.sink();
        Counters counters = new Counters();
        org.apache.hadoop.mapreduce.Mapper.Context context =
                TestSupport.mapperContext(sink, counters, TestSupport.conf());
        mapper.setup(context);
        mapper.map(new Text("the"), new LongWritable(100), context);
        mapper.map(new Text("and"), new LongWritable(50), context);
        mapper.map(new Text("whale"), new LongWritable(7), context);
        mapper.cleanup(context);

        assertEquals(Arrays.asList("whale=7"), TestSupport.render(sink));
        assertEquals(2L, TestSupport.counter(counters, AnalysisCounters.STOPWORDS_DROPPED));
        assertEquals(150L, TestSupport.counter(counters, AnalysisCounters.STOPWORD_OCCURRENCES_DROPPED));
    }

    @Test
    public void emptyDictionaryKeepsEverything() throws Exception {
        StopWordFilterMapper mapper = new StopWordFilterMapper(StopWordDictionary.empty());
        List<TestSupport.Emitted> sink = TestSupport.sink();
        org.apache.hadoop.mapreduce.Mapper.Context context =
                TestSupport.mapperContext(sink, new Counters(), TestSupport.conf());
        mapper.setup(context);
        mapper.map(new Text("the"), new LongWritable(1), context);
        assertEquals(1, sink.size());
    }
}
