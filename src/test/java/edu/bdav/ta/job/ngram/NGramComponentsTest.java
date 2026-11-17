package edu.bdav.ta.job.ngram;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

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

/** Tests for the n-gram frequency stage. */
public class NGramComponentsTest {

    @Test
    public void emitsPaddedBigramsPerLine() throws Exception {
        NGramMapper mapper = new NGramMapper();
        List<TestSupport.Emitted> sink = TestSupport.sink();
        Counters counters = new Counters();
        org.apache.hadoop.mapreduce.Mapper.Context context =
                TestSupport.mapperContext(sink, counters,
                        TestSupport.conf("ta.ngram.n", "2", "ta.ngram.padding", "true"));
        mapper.setup(context);
        mapper.map(new LongWritable(0), new Text("a b"), context);
        mapper.cleanup(context);
        List<String> rendered = TestSupport.render(sink).stream()
                .map(line -> line.replace('\u0001', ' '))
                .collect(java.util.stream.Collectors.toList());
        assertEquals(Arrays.asList("<s> a=1", "a b=1", "b </s>=1"), rendered);
        assertEquals(3L, TestSupport.counter(counters, AnalysisCounters.NGRAMS_EMITTED));
    }

    @Test
    public void doesNotStraddleLineBoundaries() throws Exception {
        NGramMapper mapper = new NGramMapper();
        List<TestSupport.Emitted> sink = TestSupport.sink();
        org.apache.hadoop.mapreduce.Mapper.Context context =
                TestSupport.mapperContext(sink, new Counters(),
                        TestSupport.conf("ta.ngram.n", "2", "ta.ngram.padding", "false"));
        mapper.setup(context);
        mapper.map(new LongWritable(0), new Text("a"), context);
        mapper.map(new LongWritable(0), new Text("b"), context);
        assertTrue("a single token per line cannot form a bigram", sink.isEmpty());
    }

    @Test
    public void reducerPrunesBelowMinimumFrequency() throws Exception {
        NGramReducer reducer = new NGramReducer();
        List<TestSupport.Emitted> sink = TestSupport.sink();
        Counters counters = new Counters();
        Reducer.Context context = TestSupport.reducerContext(sink, counters,
                TestSupport.conf("ta.ngram.minfreq", "3"));
        reducer.setup(context);
        reducer.reduce(new Text("a\u0001b"), Arrays.asList(new IntWritable(2)), context);
        reducer.reduce(new Text("c\u0001d"), Arrays.asList(new IntWritable(2), new IntWritable(2)), context);
        reducer.cleanup(context);
        assertEquals(Arrays.asList("c d=4"), TestSupport.render(sink));
        assertEquals(1L, TestSupport.counter(counters, AnalysisCounters.NGRAMS_PRUNED));
    }
}
