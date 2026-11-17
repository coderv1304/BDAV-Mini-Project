package edu.bdav.ta.job.wordcount;

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
import edu.bdav.ta.reduce.IntSumCombiner;

/** Unit tests for the J1 components, including the counter contract. */
public class WordCountComponentsTest {

    private static List<String> runMapper(String[] lines, Counters counters) throws Exception {
        NormalizingWordCountMapper mapper = new NormalizingWordCountMapper();
        List<TestSupport.Emitted> sink = TestSupport.sink();
        org.apache.hadoop.mapreduce.Mapper.Context context =
                TestSupport.mapperContext(sink, counters, TestSupport.conf());
        mapper.setup(context);
        for (String line : lines) {
            mapper.map(new LongWritable(0), new Text(line), context);
        }
        mapper.cleanup(context);
        return TestSupport.render(sink);
    }

    @Test
    public void emitsOnePerToken() throws Exception {
        assertEquals(Arrays.asList("hello=1", "world=1", "hello=1"), runMapper(new String[] {"Hello world hello"}, new Counters()));
    }

    @Test
    public void countsEmptyLinesAndTokens() throws Exception {
        Counters counters = new Counters();
        runMapper(new String[] {"", "  ", "a b"}, counters);
        assertEquals(2L, TestSupport.counter(counters, AnalysisCounters.EMPTY_LINES));
        assertEquals(2L, TestSupport.counter(counters, AnalysisCounters.TOKENS_EMITTED));
        assertEquals(3L, TestSupport.counter(counters, AnalysisCounters.RECORDS_IN));
    }

    @Test
    public void flagsLinesWithReplacementCharactersAsMalformed() throws Exception {
        Counters counters = new Counters();
        runMapper(new String[] {"broken \uFFFD bytes", "clean line"}, counters);
        assertEquals(1L, TestSupport.counter(counters, AnalysisCounters.MALFORMED_LINES));
    }

    @Test
    public void reducerAccumulatesIntoLong() throws Exception {
        LongSumReducer reducer = new LongSumReducer();
        List<TestSupport.Emitted> sink = TestSupport.sink();
        Reducer.Context context = TestSupport.reducerContext(sink, new Counters(), TestSupport.conf());
        reducer.reduce(new Text("x"),
                Arrays.asList(new IntWritable(Integer.MAX_VALUE), new IntWritable(Integer.MAX_VALUE)), context);
        assertEquals("counts must widen to 64 bits in the reducer",
                Arrays.asList("x=" + (2L * Integer.MAX_VALUE)), TestSupport.render(sink));
    }

    @Test
    public void inMapperModeAggregatesBeforeEmitting() throws Exception {
        NormalizingWordCountMapper mapper = new NormalizingWordCountMapper();
        List<TestSupport.Emitted> sink = TestSupport.sink();
        org.apache.hadoop.mapreduce.Mapper.Context context = TestSupport.mapperContext(sink, new Counters(),
                TestSupport.conf("ta.combine.mode", "inmapper"));
        mapper.setup(context);
        mapper.map(new LongWritable(0), new Text("a a a b"), context);
        mapper.cleanup(context);
        List<String> rendered = TestSupport.render(sink);
        assertEquals(2, rendered.size());
        assertTrue(rendered.contains("a=3"));
        assertTrue(rendered.contains("b=1"));
    }

    @Test
    public void detectsReplacementCharacters() {
        assertTrue(NormalizingWordCountMapper.containsReplacementChar(new Text("\uFFFD")));
        assertTrue(!NormalizingWordCountMapper.containsReplacementChar(new Text("clean")));
    }
}
