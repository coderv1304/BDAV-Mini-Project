package edu.bdav.ta.job.tfidf;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Reducer;
import org.junit.Test;
import edu.bdav.ta.TestSupport;
import edu.bdav.ta.types.TaggedCountWritable;
import edu.bdav.ta.types.TermDocWritable;

/**
 * Tests the join contract: the reducers must receive the document-frequency record
 * first and each term-frequency record must carry its own document id (the reduce key
 * is the group's first key, not the record's key).
 */
public class TfIdfJoinReducerTest {

    private static List<String> run(String term, double documentFrequency, double[] tfs,
                                    String[] documentIds, long documents) throws Exception {
        TfIdfJoinReducer reducer = new TfIdfJoinReducer();
        List<TestSupport.Emitted> sink = TestSupport.sink();
        Counters counters = new Counters();
        Reducer.Context context = TestSupport.reducerContext(sink, counters,
                TestSupport.conf("ta.corpus.ndocs", String.valueOf(documents)));
        reducer.setup(context);

        TaggedCountWritable[] values = new TaggedCountWritable[tfs.length + 1];
        values[0] = new TaggedCountWritable(TaggedCountWritable.TAG_DOCUMENT_FREQUENCY, "", documentFrequency);
        for (int i = 0; i < tfs.length; i++) {
            values[i + 1] = new TaggedCountWritable(TaggedCountWritable.TAG_TERM_FREQUENCY, documentIds[i], tfs[i]);
        }
        reducer.reduce(new TermDocWritable(term, TermDocWritable.SENTINEL_DOC), Arrays.asList(values), context);
        reducer.cleanup(context);
        return TestSupport.render(sink);
    }

    @Test
    public void multipliesTfByIdfUsingTheValueDocumentId() throws Exception {
        // N = 10 documents, df = 5 -> idf = log10(2) = 0.30103
        List<String> result = run("apple", 5.0d, new double[] {2.0d, 0.5d},
                new String[] {"doc-1.txt", "doc-2.txt"}, 10L);
        assertEquals(2, result.size());
        assertTrue(result.get(0).startsWith("apple=doc-1.txt\t0."));
        assertTrue(result.get(1).startsWith("apple=doc-2.txt\t0."));
    }

    @Test
    public void computesTheExpectedWeight() throws Exception {
        List<String> result = run("apple", 5.0d, new double[] {2.0d}, new String[] {"doc-1.txt"}, 10L);
        double expected = 2.0d * Math.log10(10.0d / 5.0d);
        String actual = result.get(0).split("\t")[1];
        assertEquals(String.format(java.util.Locale.ROOT, "%.6f", expected), actual);
    }

    @Test
    public void skipsTermsWithoutADocumentFrequencyRecord() throws Exception {
        TfIdfJoinReducer reducer = new TfIdfJoinReducer();
        List<TestSupport.Emitted> sink = TestSupport.sink();
        Counters counters = new Counters();
        Reducer.Context context = TestSupport.reducerContext(sink, counters,
                TestSupport.conf("ta.corpus.ndocs", "10"));
        reducer.setup(context);
        reducer.reduce(new TermDocWritable("orphan", "doc-1.txt"),
                Arrays.asList(new TaggedCountWritable(TaggedCountWritable.TAG_TERM_FREQUENCY, "doc-1.txt", 3.0d)),
                context);
        reducer.cleanup(context);
        assertTrue("a term with no DF must be dropped, not guessed", sink.isEmpty());
        assertEquals(1L, counters.findCounter("TA", TfIdfJoinReducer.MISSING_DF_COUNTER).getValue());
    }

    @Test
    public void weightsAreScaledByIdfMonotonically() throws Exception {
        List<String> rare = run("rare", 1.0d, new double[] {1.0d}, new String[] {"doc-1.txt"}, 100L);
        List<String> common = run("common", 100.0d, new double[] {1.0d}, new String[] {"doc-1.txt"}, 100L);
        double rareWeight = Double.parseDouble(rare.get(0).split("\t")[1]);
        double commonWeight = Double.parseDouble(common.get(0).split("\t")[1]);
        assertTrue("a term in every document must weigh ~0", Math.abs(commonWeight) < 1e-9);
        assertTrue("a rare term must weigh more", rareWeight > commonWeight);
    }
}
