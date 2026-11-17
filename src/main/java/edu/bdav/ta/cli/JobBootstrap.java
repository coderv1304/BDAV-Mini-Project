package edu.bdav.ta.cli;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.io.compress.Lz4Codec;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import edu.bdav.ta.config.ConfKeys;
import edu.bdav.ta.config.ConfigSupport;

/**
 * Shared driver plumbing: argument normalisation, idempotent output handling,
 * compression setup, counter export and error-to-exit-code mapping.
 *
 * <p>Keeping this in one place is what makes every {@code *Job} class a thin,
 * readable job definition and gives the platform uniform failure behaviour.</p>
 */
public final class JobBootstrap {

    private static final Logger LOG = LoggerFactory.getLogger(JobBootstrap.class);

    /** Machine-readable counter dump written next to each stage's output. */
    public static final String COUNTERS_FILE = "_counters.tsv";

    private JobBootstrap() {
    }

    /**
     * Runs a Tool and exits the JVM with a meaningful code.
     *
     * <p>No stack trace reaches the user: messages are logged at ERROR, details
     * at DEBUG. Interrupts are re-asserted after restoration.</p>
     */
    public static void launch(Tool tool, String[] args) {
        int code;
        try {
            code = ToolRunner.run(ConfigSupport.withDefaults(), tool, normaliseArgs(args));
        } catch (UsageException e) {
            LOG.error(e.getMessage());
            code = ExitCodes.USAGE;
        } catch (IllegalArgumentException e) {
            LOG.error("configuration error: {}", e.getMessage());
            LOG.debug("configuration error detail", e);
            code = ExitCodes.CONFIG;
        } catch (IOException e) {
            LOG.error("I/O error: {}", e.getMessage());
            LOG.debug("I/O error detail", e);
            code = ExitCodes.IO;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.error("interrupted: {}", e.getMessage());
            code = ExitCodes.JOB_FAILED;
        } catch (Exception e) {
            LOG.error("job failed: {}", describe(e));
            LOG.debug("job failure detail", e);
            code = ExitCodes.JOB_FAILED;
        }
        System.exit(code);
    }

    /** Rewrites convenience flags into {@code -D} form understood by GenericOptionsParser. */
    public static String[] normaliseArgs(String[] args) {
        if (args == null) {
            return new String[0];
        }
        List<String> out = new ArrayList<>(args.length + 1);
        for (String arg : args) {
            if ("--overwrite".equals(arg)) {
                out.add("-D" + ConfKeys.OVERWRITE + "=true");
            } else if ("--no-overwrite".equals(arg)) {
                out.add("-D" + ConfKeys.OVERWRITE + "=false");
            } else {
                out.add(arg);
            }
        }
        return out.toArray(new String[0]);
    }

    /** Returns the root cause message, or the exception's own message if unrooted. */
    public static String describe(Throwable t) {
        Throwable root = t;
        while (root.getCause() != null && root.getCause() != root) {
            root = root.getCause();
        }
        String message = root.getMessage();
        return message == null ? root.getClass().getSimpleName() : message;
    }

    /**
     * Enforces idempotency: an existing output directory is either replaced
     * (when {@code ta.overwrite=true}) or rejected with actionable guidance.
     */
    public static void prepareOutput(Configuration conf, Path output) throws IOException {
        FileSystem fs = output.getFileSystem(conf);
        if (!fs.exists(output)) {
            return;
        }
        if (!ConfigSupport.getBoolean(conf, ConfKeys.OVERWRITE)) {
            throw new UsageException("output path already exists: " + output
                    + " - rerun with -D" + ConfKeys.OVERWRITE + "=true (or --overwrite) to replace it");
        }
        LOG.warn("replacing existing output directory {}", output);
        if (!fs.delete(output, true)) {
            throw new IOException("could not delete existing output " + output);
        }
    }

    /** Fails early when a mandatory input does not exist, with the path in the message. */
    public static void requireInput(Configuration conf, Path input, String what) throws IOException {
        FileSystem fs = input.getFileSystem(conf);
        if (!fs.exists(fs.makeQualified(input))) {
            throw new UsageException(what + " not found: " + input);
        }
    }

    /**
     * Enables map-output compression when the requested codec is actually
     * usable on this node; otherwise logs a warning and continues uncompressed.
     *
     * <p>This is a deliberate engineering choice: failing a 1 GB job because a
     * native Snappy library is missing would be worse than losing a factor of
     * two in shuffle volume, and the benchmark records which branch was taken.</p>
     */
    public static void applyIntermediateCompression(Configuration conf) {
        Class<? extends CompressionCodec> codec = resolveUsableCodec(conf);
        if (codec == null) {
            conf.setBoolean(ConfKeys.MAP_OUTPUT_COMPRESS, false);
            return;
        }
        conf.setBoolean(ConfKeys.MAP_OUTPUT_COMPRESS, true);
        conf.set(ConfKeys.MAP_OUTPUT_CODEC, codec.getName());
        LOG.info("map-output compression enabled: {}", codec.getSimpleName());
    }

    /**
     * Resolves {@code ta.codec} to a codec that can actually produce a compressor on
     * this node, caching the verdict in {@link ConfKeys#CODEC_EFFECTIVE}.
     *
     * <p>The probe must not merely instantiate the class: several Hadoop codecs
     * (gzip, snappy, lz4) depend on native libraries and their
     * {@code createCompressor()} silently returns {@code null} when the library is
     * absent. Trusting the class alone made a gzip run report "enabled" and then die
     * inside {@code IFile} with "Could not obtain compressor from CodecPool". A
     * {@code null} compressor or decompressor is therefore treated as unusable.</p>
     *
     * @return the codec class, or {@code null} when nothing usable was requested
     */
    public static Class<? extends CompressionCodec> resolveUsableCodec(Configuration conf) {
        String requested = ConfigSupport.getTrimmed(conf, ConfKeys.CODEC, "none").toLowerCase(Locale.ROOT);
        if (requested.isEmpty() || "none".equals(requested)) {
            conf.set(ConfKeys.CODEC_EFFECTIVE, "");
            return null;
        }
        // An invalid name is a usage error, not something to paper over.
        Class<? extends CompressionCodec> codec = codecClass(requested);
        String cached = conf.get(ConfKeys.CODEC_EFFECTIVE);
        if (cached != null) {
            return cached.isEmpty() ? null : codec;
        }
        try {
            CompressionCodec instance = ReflectionUtils.newInstance(codec, conf);
            Compressor compressor = instance.createCompressor();
            Decompressor decompressor = instance.createDecompressor();
            try {
                if (compressor == null || decompressor == null) {
                    throw new IllegalStateException("no native implementation available");
                }
            } finally {
                if (compressor != null) {
                    CodecPool.returnCompressor(compressor);
                }
                if (decompressor != null) {
                    CodecPool.returnDecompressor(decompressor);
                }
            }
            conf.set(ConfKeys.CODEC_EFFECTIVE, codec.getName());
            return codec;
        } catch (RuntimeException | LinkageError failure) {
            conf.set(ConfKeys.CODEC_EFFECTIVE, "");
            LOG.warn("codec '{}' unusable here ({}); continuing uncompressed",
                    requested, failure.toString());
            return null;
        }
    }

    /** Maps a codec name to its Hadoop implementation class. */
    public static Class<? extends CompressionCodec> codecClass(String name) {
        switch (name) {
            case "snappy":
                return SnappyCodec.class;
            case "lz4":
                return Lz4Codec.class;
            case "gzip":
                return GzipCodec.class;
            case "bzip2":
                return BZip2Codec.class;
            default:
                throw new IllegalArgumentException(
                        "unknown codec '" + name + "' (accepted: none|snappy|lz4|gzip|bzip2)");
        }
    }

    /**
     * Logs every counter and exports them to {@code <output>/_counters.tsv}.
     *
     * <p>The TSV is what {@code scripts/07_benchmark.sh} aggregates: counters are
     * the only trustworthy source for shuffle bytes and spilled records, and
     * scraping them from driver logs would be fragile.</p>
     */
    public static void recordMetrics(Job job, Path output) throws IOException {
        Counters counters = job.getCounters();
        StringBuilder sb = new StringBuilder("group\tcounter\tvalue\n");
        for (CounterGroup group : counters) {
            for (Counter counter : group) {
                sb.append(group.getName()).append('\t')
                  .append(counter.getName()).append('\t')
                  .append(counter.getValue()).append('\n');
                LOG.info("counter {}.{}={}", group.getName(), counter.getName(), counter.getValue());
            }
        }
        if (output == null) {
            return;
        }
        Path target = new Path(output, COUNTERS_FILE);
        FileSystem fs = target.getFileSystem(job.getConfiguration());
        try (FSDataOutputStream out = fs.create(target, true)) {
            out.write(sb.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8));
        }
    }

    /** Reads one counter value from a finished job, or 0 when absent. */
    public static long counterValue(Job job, Enum<?> counter) throws IOException {
        return job.getCounters().findCounter(counter).getValue();
    }
}
