package edu.bdav.ta.io;

import java.io.IOException;
import java.util.Locale;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import edu.bdav.ta.config.ConfKeys;
import edu.bdav.ta.config.ConfigSupport;

/**
 * Input/output format decisions, in one place so that every job answers the
 * small-files question identically.
 *
 * <p>Contract enforced here:</p>
 * <ul>
 *   <li><em>Intermediate</em> stages write {@link SequenceFileOutputFormat} -
 *       type-safe, splittable, dependency-free.</li>
 *   <li><em>Terminal</em> stages write text with a TAB separator, because the
 *       Python oracle cannot read a SequenceFile without a JVM.</li>
 *   <li>Line-oriented jobs switch to {@link org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat}
 *       when the corpus is made of small files.</li>
 * </ul>
 */
public final class InputFormats {

    private static final Logger LOG = LoggerFactory.getLogger(InputFormats.class);

    /** Files inspected when estimating average file size (bounds driver cost). */
    private static final int SIZE_SAMPLE_LIMIT = 1000;

    private InputFormats() {
    }

    /**
     * Configures a line-oriented input: plain {@link TextInputFormat} for normal
     * corpora, {@code CombineTextInputFormat} for small-file corpora.
     *
     * @param job job to configure
     * @param input corpus root
     * @return {@code true} when the combine format was selected
     */
    public static boolean configureLineOriented(Job job, Path input) throws IOException {
        Configuration conf = job.getConfiguration();
        FileInputFormat.addInputPath(job, input);
        FileInputFormat.setInputDirRecursive(job, true);
        if (prefersCombine(conf, input)) {
            long maxSplit = ConfigSupport.getLong(conf, ConfKeys.SMALLFILE_THRESHOLD, 1L, Long.MAX_VALUE);
            org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat.setMaxInputSplitSize(job, maxSplit);
            job.setInputFormatClass(org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat.class);
            LOG.info("input format: CombineTextInputFormat (max split {} bytes) - corpus is small-file heavy", maxSplit);
            return true;
        }
        job.setInputFormatClass(TextInputFormat.class);
        LOG.info("input format: TextInputFormat");
        return false;
    }

    /** Configures a whole-document input (one mapper invocation per file). */
    public static void configureWholeFile(Job job, Path input) throws IOException {
        FileInputFormat.addInputPath(job, input);
        FileInputFormat.setInputDirRecursive(job, true);
        job.setInputFormatClass(WholeFileInputFormat.class);
        LOG.info("input format: WholeFileInputFormat (one record per document)");
    }

    /** Configures a SequenceFile input produced by an earlier stage. */
    public static void configureSequence(Job job, Path input) throws IOException {
        FileInputFormat.addInputPath(job, input);
        FileInputFormat.setInputDirRecursive(job, true);
        job.setInputFormatClass(SequenceFileInputFormat.class);
    }

    /**
     * Decides whether the small-files problem applies.
     *
     * <p>Average size is computed over at most {@value #SIZE_SAMPLE_LIMIT} files:
     * a full {@code listStatus} on 10 000 files is cheap, but on millions it is
     * not, and the decision only needs the order of magnitude.</p>
     */
    public static boolean prefersCombine(Configuration conf, Path input) throws IOException {
        FileSystem fs = input.getFileSystem(conf);
        long threshold = ConfigSupport.getLong(conf, ConfKeys.SMALLFILE_THRESHOLD, 1L, Long.MAX_VALUE);
        long total = 0L;
        int files = 0;
        for (FileStatus status : fs.listStatus(input)) {
            if (status.isDirectory()) {
                continue;
            }
            String name = status.getPath().getName();
            if (name.startsWith("_") || name.startsWith(".")) {
                continue;
            }
            total += status.getLen();
            if (++files >= SIZE_SAMPLE_LIMIT) {
                break;
            }
        }
        if (files == 0) {
            return false;
        }
        double average = (double) total / files;
        return average < threshold;
    }

    /** Text output with deterministic TAB separation and no output compression. */
    public static void configureTextOutput(Job job, Path output) {
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, output);
        Configuration conf = job.getConfiguration();
        conf.set(ConfKeys.TEXT_OUTPUT_SEPARATOR, "\t");
        conf.setBoolean("mapreduce.output.fileoutputformat.compress", false);
    }

    /** SequenceFile output for chained stages, optionally compressed with the same codec. */
    public static void configureSequenceOutput(Job job, Path output) {
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        SequenceFileOutputFormat.setOutputPath(job, output);
        Configuration conf = job.getConfiguration();
        // Ask the same resolver the shuffle uses, so an unusable codec (missing native
        // library) disables BOTH paths instead of only one - a mismatch here made an
        // lz4 run fail while writing its reducer output after the shuffle had already
        // been switched off.
        Class<? extends CompressionCodec> codec = edu.bdav.ta.cli.JobBootstrap.resolveUsableCodec(conf);
        if (codec == null) {
            return;
        }
        SequenceFileOutputFormat.setCompressOutput(job, true);
        SequenceFileOutputFormat.setOutputCompressionType(job, SequenceFile.CompressionType.BLOCK);
        SequenceFileOutputFormat.setOutputCompressorClass(job, codec);
    }

    /**
     * Chooses the output format from {@code ta.output.format} ({@code seq} or
     * {@code text}), defaulting to whatever the stage needs for chaining.
     */
    public static void configureOutput(Job job, Path output, String defaultFormat) {
        String format = ConfigSupport.getTrimmed(job.getConfiguration(), ConfKeys.OUTPUT_FORMAT, defaultFormat);
        if ("text".equalsIgnoreCase(format)) {
            configureTextOutput(job, output);
        } else if ("seq".equalsIgnoreCase(format)) {
            configureSequenceOutput(job, output);
        } else {
            throw new IllegalArgumentException(
                    "key " + ConfKeys.OUTPUT_FORMAT + " expects seq|text but was '" + format + "'");
        }
    }
}
