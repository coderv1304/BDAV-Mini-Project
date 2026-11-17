package edu.bdav.ta.job.charfreq;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * J4 reducer: emits {@code U+XXXX<TAB>char<TAB>count} ordered by code point.
 *
 * <p>The code-point label is what makes the output readable and diffable: a
 * bare tab or zero-width character in the first column would be invisible in a
 * text file and would make the reference comparison meaningless. Ordering by
 * numeric code point matches Python's {@code sorted(counts)} over code points.</p>
 */
public class CharacterFrequencyReducer extends Reducer<Text, IntWritable, Text, Text> {

    private final TreeMap<Integer, Long> counts = new TreeMap<>();
    private final Text outKey = new Text();
    private final Text outValue = new Text();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) {
        int codePoint = key.toString().codePointAt(0);
        long sum = 0L;
        for (IntWritable value : values) {
            sum += value.get();
        }
        counts.merge(codePoint, sum, Long::sum);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for (Map.Entry<Integer, Long> entry : counts.entrySet()) {
            int codePoint = entry.getKey();
            outKey.set(String.format(java.util.Locale.ROOT, "U+%04X", codePoint));
            outValue.set(new String(Character.toChars(codePoint)) + "\t" + entry.getValue());
            context.write(outKey, outValue);
        }
    }
}
