package edu.bdav.ta.text;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Sentiment lexicon distributed through the {@code DistributedCache}.
 *
 * <p>Format: {@code word<TAB>score} per line, {@code #} comments allowed, scores in
 * {@code [-5, 5]} (the shape of the AFINN lexicon). The lexicon is deliberately a
 * pluggable file rather than an embedded table: the pipeline must not depend on a
 * particular lexicon's licence, and swapping lexicons should be a configuration
 * change.</p>
 */
public final class SentimentLexicon {

    private static final Logger LOG = LoggerFactory.getLogger(SentimentLexicon.class);

    /** Symbolic-link name used when the lexicon is localised. */
    public static final String CACHE_ALIAS = "ta-lexicon.tsv";

    private final Map<String, Double> scores;

    private SentimentLexicon(Map<String, Double> scores) {
        this.scores = scores;
    }

    /** Attaches the lexicon to a job through the DistributedCache. */
    public static void attach(Job job, Path path) {
        try {
            URI uri = new URI(path.toUri().getScheme(), path.toUri().getAuthority(),
                    path.toUri().getPath(), path.toUri().getQuery(), CACHE_ALIAS);
            job.addCacheFile(uri);
            LOG.info("sentiment lexicon attached via DistributedCache: {}", path);
        } catch (java.net.URISyntaxException e) {
            throw new IllegalArgumentException("invalid lexicon path: " + path, e);
        }
    }

    /** Loads the lexicon inside a task, falling back to the configured path. */
    public static SentimentLexicon load(Configuration conf, URI[] cacheFiles, String fallbackPath)
            throws IOException {
        if (cacheFiles != null) {
            for (URI uri : cacheFiles) {
                if (!CACHE_ALIAS.equals(uri.getFragment())) {
                    continue;
                }
                File localised = new File(CACHE_ALIAS);
                if (localised.isFile()) {
                    return new SentimentLexicon(parse(new java.io.FileInputStream(localised)));
                }
                Path remote = new Path(uri.getScheme(), uri.getAuthority(), uri.getPath());
                return read(conf, remote);
            }
        }
        if (fallbackPath != null && !fallbackPath.trim().isEmpty()) {
            return read(conf, new Path(fallbackPath));
        }
        LOG.warn("no sentiment lexicon configured; every document will score 0");
        return new SentimentLexicon(new HashMap<String, Double>());
    }

    /** Reads a lexicon from any Hadoop filesystem. */
    public static SentimentLexicon read(Configuration conf, Path path) throws IOException {
        FileSystem fs = path.getFileSystem(conf);
        if (!fs.exists(path)) {
            throw new IOException("sentiment lexicon not found: " + path);
        }
        try (InputStream in = fs.open(path)) {
            return new SentimentLexicon(parse(in));
        }
    }

    /** Parses {@code word<TAB>score} lines. */
    public static Map<String, Double> parse(InputStream in) throws IOException {
        Map<String, Double> parsed = new HashMap<>();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String trimmed = line.trim();
                if (trimmed.isEmpty() || trimmed.charAt(0) == '#') {
                    continue;
                }
                String[] fields = trimmed.split("\t");
                if (fields.length < 2) {
                    continue;
                }
                try {
                    parsed.put(fields[0].trim().toLowerCase(Locale.ROOT), Double.parseDouble(fields[1].trim()));
                } catch (NumberFormatException e) {
                    LOG.debug("skipping malformed lexicon entry: {}", trimmed);
                }
            }
        }
        return parsed;
    }

    /** @param token normalised token @return the lexicon score, or {@code null} when absent */
    public Double score(CharSequence token) {
        return scores.get(token.toString());
    }

    /** @return number of lexicon entries */
    public int size() {
        return scores.size();
    }
}
