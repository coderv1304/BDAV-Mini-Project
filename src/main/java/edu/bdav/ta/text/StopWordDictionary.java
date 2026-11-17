package edu.bdav.ta.text;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Locale;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Stop-word dictionary distributed through the {@code DistributedCache}.
 *
 * <p>The file is attached with a symbolic-link fragment ({@link #CACHE_ALIAS}),
 * so the mapper opens it by name from its working directory and never needs to
 * know the cluster's filesystem. A filesystem fallback keeps unit tests and
 * local-mode runs working when no localisation happens.</p>
 */
public final class StopWordDictionary {

    private static final Logger LOG = LoggerFactory.getLogger(StopWordDictionary.class);

    /** Symbolic-link name under which the stop-word file is localised. */
    public static final String CACHE_ALIAS = "ta-stopwords.txt";

    private final Set<String> words;

    private StopWordDictionary(Set<String> words) {
        this.words = words;
    }

    /** Wraps an explicit word collection (tests, small runs). */
    public static StopWordDictionary of(Collection<String> words) {
        Set<String> copy = new LinkedHashSet<>();
        for (String word : words) {
            String normalised = word.trim().toLowerCase(Locale.ROOT);
            if (!normalised.isEmpty()) {
                copy.add(normalised);
            }
        }
        return new StopWordDictionary(Collections.unmodifiableSet(copy));
    }

    /** An empty dictionary that matches nothing. */
    public static StopWordDictionary empty() {
        return new StopWordDictionary(Collections.<String>emptySet());
    }

    /**
     * Attaches the stop-word file to a job through the DistributedCache.
     *
     * @param job the job receiving the cache file
     * @param path filesystem path of the stop-word list (HDFS or local)
     */
    public static void attach(Job job, Path path) {
        try {
            URI uri = new URI(path.toUri().getScheme(), path.toUri().getAuthority(),
                    path.toUri().getPath(), path.toUri().getQuery(), CACHE_ALIAS);
            job.addCacheFile(uri);
            LOG.info("stop words attached via DistributedCache: {}#{}", path, CACHE_ALIAS);
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException("invalid stop-word path: " + path, e);
        }
    }

    /**
     * Loads the dictionary inside a task from the localised cache files.
     *
     * @param conf task configuration
     * @param cacheFiles {@code context.getCacheFiles()} (may be {@code null})
     * @param fallbackPath configured path used when no cache entry is present
     */
    public static StopWordDictionary load(Configuration conf, URI[] cacheFiles, String fallbackPath)
            throws IOException {
        if (cacheFiles != null) {
            for (URI uri : cacheFiles) {
                String fragment = uri.getFragment();
                if (fragment == null || !CACHE_ALIAS.equals(fragment)) {
                    continue;
                }
                File localised = new File(CACHE_ALIAS);
                if (localised.isFile()) {
                    LOG.info("stop words loaded from localised cache file {}", localised.getAbsolutePath());
                    return new StopWordDictionary(parse(new java.io.FileInputStream(localised)));
                }
                Path remote = new Path(uri.getScheme(), uri.getAuthority(), uri.getPath());
                LOG.info("stop words read from filesystem {} (no localisation)", remote);
                return read(conf, remote);
            }
        }
        if (fallbackPath != null && !fallbackPath.trim().isEmpty()) {
            LOG.warn("no cache entry found; falling back to {}", fallbackPath);
            return read(conf, new Path(fallbackPath));
        }
        LOG.warn("no stop-word list configured; no tokens will be filtered");
        return empty();
    }

    /** Reads a stop-word list from any Hadoop filesystem. */
    public static StopWordDictionary read(Configuration conf, Path path) throws IOException {
        FileSystem fs = path.getFileSystem(conf);
        if (!fs.exists(path)) {
            throw new IOException("stop-word file not found: " + path);
        }
        try (InputStream in = fs.open(path)) {
            return new StopWordDictionary(parse(in));
        }
    }

    /**
     * Parses one-word-per-line input; blank lines and {@code #} comments are
     * ignored and words are lowercased so they match normalised tokens.
     */
    public static Set<String> parse(InputStream in) throws IOException {
        Set<String> parsed = new LinkedHashSet<>();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String word = line.trim();
                if (word.isEmpty() || word.charAt(0) == '#') {
                    continue;
                }
                parsed.add(word.toLowerCase(Locale.ROOT));
            }
        }
        return parsed;
    }

    /** @param token normalised token @return true when the token is a stop word */
    public boolean isStopWord(CharSequence token) {
        return words.contains(token.toString());
    }

    /** @param token Hadoop {@link org.apache.hadoop.io.Text} token @return true when it is a stop word */
    public boolean isStopWord(org.apache.hadoop.io.Text token) {
        return isStopWord((CharSequence) token.toString());
    }

    /** @return number of loaded stop words */
    public int size() {
        return words.size();
    }

    /** @return immutable view of the loaded words */
    public Set<String> words() {
        return words;
    }
}
