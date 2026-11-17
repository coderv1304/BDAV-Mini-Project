package edu.bdav.ta.reduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import edu.bdav.ta.config.ConfKeys;
import edu.bdav.ta.config.ConfigSupport;
import edu.bdav.ta.text.TextOrder;

/**
 * Bounded min-heap top-N reducer: emits the N highest-count keys.
 *
 * <p>Why a heap and not a total-order sort? The heap needs O(N) memory and one
 * pass over the (already combined) counts, whereas a total order would shuffle
 * the same data through a sampling partitioner and then sort it in full to
 * discard all but N records. With a Zipfian distribution the head is a handful
 * of terms, so the full sort is pure overhead.</p>
 *
 * <p>Ties are broken by term ascending in code-point order, which makes the
 * output deterministic and identical to the Python reference's
 * {@code sorted(items, key=lambda kv: (-kv[1], kv[0]))[:n]}.</p>
 */
public class TopNHeapReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

    /** One candidate entry. */
    private static final class Entry {
        private final String term;
        private final long count;

        Entry(String term, long count) {
            this.term = term;
            this.count = count;
        }
    }

    /** Worst entry first: lowest count, and on ties the code-point-largest term. */
    private static final Comparator<Entry> WORST_FIRST = new Comparator<Entry>() {
        @Override
        public int compare(Entry a, Entry b) {
            int byCount = Long.compare(a.count, b.count);
            return byCount != 0 ? byCount : TextOrder.compare(b.term, a.term);
        }
    };

    /** Best entry first: highest count, and on ties the code-point-smallest term. */
    private static final Comparator<Entry> BEST_FIRST = new Comparator<Entry>() {
        @Override
        public int compare(Entry a, Entry b) {
            int byCount = Long.compare(b.count, a.count);
            return byCount != 0 ? byCount : TextOrder.compare(a.term, b.term);
        }
    };

    private int topN;
    private PriorityQueue<Entry> heap;
    private final Text outKey = new Text();
    private final LongWritable outValue = new LongWritable();

    @Override
    protected void setup(Context context) {
        topN = ConfigSupport.getInt(context.getConfiguration(), ConfKeys.TOPN_N, 1, 1_000_000);
        heap = new PriorityQueue<>(Math.min(topN, 4096), WORST_FIRST);
    }

    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) {
        long total = 0L;
        for (LongWritable value : values) {
            total += value.get();
        }
        if (heap.size() < topN) {
            heap.add(new Entry(key.toString(), total));
            return;
        }
        Entry candidate = new Entry(key.toString(), total);
        if (WORST_FIRST.compare(candidate, heap.peek()) > 0) {
            heap.poll();
            heap.add(candidate);
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        List<Entry> entries = new ArrayList<>(heap);
        entries.sort(BEST_FIRST);
        for (Entry entry : entries) {
            outKey.set(entry.term);
            outValue.set(entry.count);
            context.write(outKey, outValue);
        }
    }

    /** Exposed for tests: the current heap contents in output order. */
    List<String> snapshot() {
        List<Entry> entries = new ArrayList<>(heap);
        entries.sort(BEST_FIRST);
        List<String> rendered = new ArrayList<>(entries.size());
        for (Entry entry : entries) {
            rendered.add(entry.term + "\t" + entry.count);
        }
        return rendered;
    }
}
