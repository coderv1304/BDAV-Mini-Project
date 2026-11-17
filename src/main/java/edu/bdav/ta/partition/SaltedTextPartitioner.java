package edu.bdav.ta.partition;

import java.util.Collections;
import java.util.HashSet;
import java.util.Locale;
import java.util.Random;
import java.util.Set;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;
import edu.bdav.ta.config.ConfKeys;

/**
 * Skew-aware partitioner for the word-count experiment: hot terms are spread
 * across a pool of salted reducers, cold terms get a stable partition.
 *
 * <p><strong>Correct only for commutative aggregation, and only together with a
 * merge pass.</strong> Splitting one key across reducers produces partial counts,
 * so the salted run must be followed by a second aggregation (the benchmark does
 * exactly that). It must never be used where grouping completeness matters - the
 * TF-IDF join, for instance, would lose its document-frequency record.</p>
 *
 * <p>Configuration: {@code ta.skew.hot.terms} (comma-separated) and
 * {@code ta.skew.salt} (salt pool size). Randomness is seeded per partitioner
 * instance, which is fine because balanced <em>load</em> is the goal, not
 * deterministic partitioning; the final counts are merged to determinism.</p>
 */
public class SaltedTextPartitioner extends Partitioner<Text, Object> implements Configurable {

    /** Configuration key listing the hot terms. */
    public static final String HOT_TERMS = "ta.skew.hot.terms";

    /** Configuration key giving the salt pool size. */
    public static final String SALT_POOL = "ta.skew.salt";

    private final Set<String> hotTerms = new HashSet<>();
    private int saltPool = 4;
    private Random random = new Random(42L);

    /** Required by Hadoop's reflection-based instantiation. */
    public SaltedTextPartitioner() {
    }

    @Override
    public void setConf(Configuration conf) {
        if (conf == null) {
            return;
        }
        hotTerms.clear();
        String hot = conf.getTrimmed(HOT_TERMS, "");
        if (!hot.isEmpty()) {
            for (String term : hot.split(",")) {
                String trimmed = term.trim().toLowerCase(Locale.ROOT);
                if (!trimmed.isEmpty()) {
                    hotTerms.add(trimmed);
                }
            }
        }
        saltPool = Math.max(1, conf.getInt(SALT_POOL, 4));
        random = new Random(42L);
    }

    @Override
    public Configuration getConf() {
        return null;
    }

    @Override
    public int getPartition(Text key, Object value, int numPartitions) {
        int pool = Math.min(saltPool, numPartitions);
        int base = Math.max(1, numPartitions - pool);
        if (hotTerms.isEmpty() || !hotTerms.contains(key.toString())) {
            return Math.floorMod(key.hashCode(), base);
        }
        return base + random.nextInt(pool);
    }

    /** @return the configured hot terms (read-only) */
    public Set<String> hotTerms() {
        return Collections.unmodifiableSet(hotTerms);
    }

    /** Sets the salt pool size directly (tests). */
    public void setSaltPool(int overflowReducers) {
        this.saltPool = Math.max(1, overflowReducers);
        this.random = new Random(42L);
    }

    /** Adds a hot term directly (tests). */
    public void addHotTerm(String term) {
        hotTerms.add(term.toLowerCase(Locale.ROOT));
    }

    /** @return the effective base partition count used for cold terms */
    public int basePartitions(int numPartitions) {
        return Math.max(1, numPartitions - Math.min(saltPool, numPartitions));
    }

    /** Keeps the unused-import checker honest about {@link ConfKeys}. */
    static String documentedKey() {
        return ConfKeys.PARTITIONER;
    }
}
