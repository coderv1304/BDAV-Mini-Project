package edu.bdav.ta.job.inverted;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Reducer;
import org.junit.Test;
import edu.bdav.ta.TestSupport;
import edu.bdav.ta.counters.AnalysisCounters;
import edu.bdav.ta.types.PostingWritable;

/** Tests for the index mapper, combiner and reducer. */
public class InvertedIndexComponentsTest {

    private static List<TestSupport.Emitted> map(String documentId, String content, Counters counters) throws Exception {
        InvertedIndexMapper mapper = new InvertedIndexMapper();
        List<TestSupport.Emitted> sink = TestSupport.sink();
        org.apache.hadoop.mapreduce.Mapper.Context context =
                TestSupport.mapperContext(sink, counters, TestSupport.conf());
        mapper.setup(context);
        mapper.map(new Text(documentId), new Text(content), context);
        mapper.cleanup(context);
        return sink;
    }

    @Test
    public void emitsOnePostingPerTermPerDocument() throws Exception {
        Counters counters = new Counters();
        List<TestSupport.Emitted> emissions = map("doc-1.txt", "a b a a", counters);
        assertEquals("in-mapper combining must collapse repeated terms", 2, emissions.size());
        assertEquals(1L, TestSupport.counter(counters, AnalysisCounters.DOCUMENTS));
        assertEquals(2L, TestSupport.counter(counters, AnalysisCounters.POSTINGS_EMITTED));
    }

    @Test
    public void recordsPositionsOfEachOccurrence() throws Exception {
        List<TestSupport.Emitted> emissions = map("doc-1.txt", "a b a a", new Counters());
        PostingWritable posting = (PostingWritable) emissions.get(0).value();
        if ("a".equals(emissions.get(0).key().toString())) {
            assertEquals(3, posting.getCount());
            assertEquals("0,2,3", posting.getPositions().toString());
        } else {
            assertEquals("b", emissions.get(0).key().toString());
        }
    }

    @Test
    public void combinerMergesPostingsOfTheSameDocument() throws Exception {
        InvertedIndexCombiner combiner = new InvertedIndexCombiner();
        List<TestSupport.Emitted> sink = TestSupport.sink();
        Reducer.Context context = TestSupport.reducerContext(sink, new Counters(), TestSupport.conf());
        combiner.reduce(new Text("word"),
                Arrays.asList(new PostingWritable("doc-1.txt", new int[] {1}),
                        new PostingWritable("doc-1.txt", new int[] {4}),
                        new PostingWritable("doc-2.txt", new int[] {2})),
                context);
        assertEquals("one posting per (word, document) after combining", 2, sink.size());
    }

    @Test
    public void reducerRendersSortedPostings() throws Exception {
        InvertedIndexReducer reducer = new InvertedIndexReducer();
        List<TestSupport.Emitted> sink = TestSupport.sink();
        Reducer.Context context = TestSupport.reducerContext(sink, new Counters(), TestSupport.conf());
        reducer.reduce(new Text("word"),
                Arrays.asList(new PostingWritable("doc-2.txt", new int[] {5, 1}),
                        new PostingWritable("doc-1.txt", new int[] {0})),
                context);
        assertEquals(Arrays.asList("word=doc-1.txt:1:0\tdoc-2.txt:2:1,5"), TestSupport.render(sink));
    }

    @Test
    public void reducerSortsPositionsEvenWhenCombinerOrderVaries() throws Exception {
        InvertedIndexReducer reducer = new InvertedIndexReducer();
        List<TestSupport.Emitted> sink = TestSupport.sink();
        Reducer.Context context = TestSupport.reducerContext(sink, new Counters(), TestSupport.conf());
        reducer.reduce(new Text("w"), Arrays.asList(new PostingWritable("d", new int[] {9, 2, 4})), context);
        assertTrue(((Text) sink.get(0).value()).toString().endsWith(":3:2,4,9"));
    }
}
