package edu.bdav.ta.job.charfreq;

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

/** Tests for the character-level frequency stage. */
public class CharacterFrequencyComponentsTest {

    @Test
    public void countsCodePointsOfNormalisedTokens() throws Exception {
        CharacterFrequencyMapper mapper = new CharacterFrequencyMapper();
        List<TestSupport.Emitted> sink = TestSupport.sink();
        org.apache.hadoop.mapreduce.Mapper.Context context =
                TestSupport.mapperContext(sink, new Counters(), TestSupport.conf());
        mapper.setup(context);
        mapper.map(new LongWritable(0), new Text("Aa b"), context);
        List<String> rendered = TestSupport.render(sink);
        assertEquals("one record per code-point occurrence (the combiner sums them)", 3, rendered.size());
        assertEquals("a=1", rendered.get(0));
        assertEquals("a=1", rendered.get(1));
        assertEquals("b=1", rendered.get(2));
    }

    @Test
    public void reducerLabelsCodePointsAndSortsThem() throws Exception {
        CharacterFrequencyReducer reducer = new CharacterFrequencyReducer();
        List<TestSupport.Emitted> sink = TestSupport.sink();
        Reducer.Context context = TestSupport.reducerContext(sink, new Counters(), TestSupport.conf());
        reducer.reduce(new Text("b"), Arrays.asList(new IntWritable(2)), context);
        reducer.reduce(new Text("a"), Arrays.asList(new IntWritable(3)), context);
        reducer.cleanup(context);
        assertEquals(Arrays.asList("U+0061=a\t3", "U+0062=b\t2"), TestSupport.render(sink));
    }
}
