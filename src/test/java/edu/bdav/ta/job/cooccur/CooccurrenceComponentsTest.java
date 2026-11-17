package edu.bdav.ta.job.cooccur;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Reducer;
import org.junit.Test;
import edu.bdav.ta.TestSupport;
import edu.bdav.ta.counters.AnalysisCounters;
import edu.bdav.ta.types.StripeWritable;
import edu.bdav.ta.types.TextPair;

/** Side-by-side tests for the pairs and stripes patterns. */
public class CooccurrenceComponentsTest {

    @Test
    public void pairsEmitsEveryWindowPairOnce() throws Exception {
        PairsMapper mapper = new PairsMapper();
        List<TestSupport.Emitted> sink = TestSupport.sink();
        Counters counters = new Counters();
        org.apache.hadoop.mapreduce.Mapper.Context context =
                TestSupport.mapperContext(sink, counters,
                        TestSupport.conf("ta.cooccur.window", "2", "ta.cooccur.ordered", "true"));
        mapper.setup(context);
        mapper.map(new Text("d1"), new Text("a b c d"), context);
        mapper.cleanup(context);
        List<String> rendered = TestSupport.render(sink);
        assertTrue(rendered.contains("a\tb=1"));
        assertTrue(rendered.contains("a\tc=1"));
        assertEquals("window=2 means at most two neighbours per token", 5, rendered.size());
        assertEquals(5L, TestSupport.counter(counters, AnalysisCounters.PAIRS_EMITTED));
    }

    @Test
    public void unorderedPairsEmitBothDirections() throws Exception {
        PairsMapper mapper = new PairsMapper();
        List<TestSupport.Emitted> sink = TestSupport.sink();
        org.apache.hadoop.mapreduce.Mapper.Context context =
                TestSupport.mapperContext(sink, new Counters(),
                        TestSupport.conf("ta.cooccur.window", "1", "ta.cooccur.ordered", "false"));
        mapper.setup(context);
        mapper.map(new Text("d1"), new Text("a b"), context);
        assertEquals(2, sink.size());
    }

    @Test
    public void pairsReducerAppliesTheMinimumCount() throws Exception {
        PairsReducer reducer = new PairsReducer();
        List<TestSupport.Emitted> sink = TestSupport.sink();
        Reducer.Context context = TestSupport.reducerContext(sink, new Counters(),
                TestSupport.conf("ta.cooccur.mincount", "3"));
        reducer.setup(context);
        reducer.reduce(new TextPair("a", "b"), Arrays.asList(new IntWritable(1), new IntWritable(1)), context);
        reducer.reduce(new TextPair("c", "d"), Arrays.asList(new IntWritable(2), new IntWritable(2)), context);
        assertEquals(Arrays.asList("c=d\t4"), TestSupport.render(sink));
    }

    @Test
    public void stripesAggregateNeighboursPerWord() throws Exception {
        StripesMapper mapper = new StripesMapper();
        List<TestSupport.Emitted> sink = TestSupport.sink();
        Counters counters = new Counters();
        org.apache.hadoop.mapreduce.Mapper.Context context =
                TestSupport.mapperContext(sink, counters,
                        TestSupport.conf("ta.cooccur.window", "2", "ta.cooccur.ordered", "true"));
        mapper.setup(context);
        mapper.map(new Text("d1"), new Text("a b a c"), context);
        mapper.cleanup(context);
        assertEquals("stripes emit one record per distinct word in the document", 3, sink.size());
        assertTrue(TestSupport.counter(counters, AnalysisCounters.NEIGHBOURS_PER_WORD) > 0);
    }

    @Test
    public void stripesCombinerMergesAssociatively() throws Exception {
        StripesCombiner combiner = new StripesCombiner();
        List<TestSupport.Emitted> sink = TestSupport.sink();
        Reducer.Context context = TestSupport.reducerContext(sink, new Counters(), TestSupport.conf());
        StripeWritable first = new StripeWritable();
        first.add("x", 2);
        StripeWritable second = new StripeWritable();
        second.add("x", 1);
        second.add("y", 4);
        combiner.reduce(new Text("word"), Arrays.asList(first, second), context);
        assertEquals(1, sink.size());
        assertEquals("stripes render by count descending, then neighbour ascending",
                "y:4,x:3", ((StripeWritable) sink.get(0).value()).render(0));
    }

    @Test
    public void stripesReducerPrunesAndTruncates() throws Exception {
        StripesReducer reducer = new StripesReducer();
        List<TestSupport.Emitted> sink = TestSupport.sink();
        Reducer.Context context = TestSupport.reducerContext(sink, new Counters(),
                TestSupport.conf("ta.cooccur.mincount", "2", "ta.cooccur.topk", "2"));
        reducer.setup(context);
        StripeWritable stripe = new StripeWritable();
        stripe.add("a", 5);
        stripe.add("b", 4);
        stripe.add("c", 3);
        stripe.add("rare", 1);
        reducer.reduce(new Text("word"), Arrays.asList(stripe), context);
        assertEquals(Arrays.asList("word=a:5,b:4"), TestSupport.render(sink));
    }
}
