package edu.bdav.ta.job.topn;

import java.io.IOException;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.util.Tool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import edu.bdav.ta.cli.ExitCodes;
import edu.bdav.ta.cli.JobBootstrap;
import edu.bdav.ta.cli.UsageException;
import edu.bdav.ta.config.ConfKeys;
import edu.bdav.ta.config.ConfigSupport;
import edu.bdav.ta.io.InputFormats;
import edu.bdav.ta.reduce.TopNHeapReducer;
import edu.bdav.ta.text.StopWordDictionary;

/**
 * J2 - stop-word removal plus global top-N.
 *
 * <p>Two strategies, both implemented, selected by {@code ta.topn.strategy}:</p>
 * <ul>
 *   <li><b>heap</b> (default): one reducer holding a bounded min-heap of size N.
 *       Exact, O(N) memory, single pass. This is the right answer because the
 *       top of a Zipfian distribution is a handful of terms; sorting the whole
 *       vocabulary to keep 100 records is waste.</li>
 *   <li><b>partitioned</b>: R reducers each compute their partition's top-N, then
 *       a second job merges the R*N partial results with one reducer. Uses the
 *       cluster for the expensive part and keeps the merge tiny - the standard
 *       answer when the vocabulary itself does not fit comfortably in one
 *       reducer's memory.</li>
 * </ul>
 *
 * <p>Both produce byte-identical output for the same input; the benchmark reports
 * the cost difference.</p>
 */
public class StopWordTopNJob extends Configured implements Tool {

    /** Stage identifier used in job names and metrics. */
    public static final String STAGE = "J2-stopword-topn";

    private static final Logger LOG = LoggerFactory.getLogger(StopWordTopNJob.class);

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            throw new UsageException("usage: " + STAGE + " [-D key=value] <wordcount-seq> <output-path>");
        }
        Path input = new Path(args[0]);
        Path output = new Path(args[1]);
        JobBootstrap.requireInput(getConf(), input, "word-count input");
        JobBootstrap.prepareOutput(getConf(), output);

        String strategy = ConfigSupport.getTrimmed(getConf(), ConfKeys.TOPN_STRATEGY, "heap");
        if ("heap".equalsIgnoreCase(strategy)) {
            return runSinglePass(input, output);
        }
        if ("partitioned".equalsIgnoreCase(strategy)) {
            return runPartitioned(input, output);
        }
        throw new IllegalArgumentException(
                "key " + ConfKeys.TOPN_STRATEGY + " expects heap|partitioned but was '" + strategy + "'");
    }

    /** Single-reducer bounded heap: exact, cheapest for a small N. */
    private int runSinglePass(Path input, Path output) throws Exception {
        Job job = newJob(input, output);
        job.setNumReduceTasks(1);
        if (!job.waitForCompletion(true)) {
            return ExitCodes.JOB_FAILED;
        }
        JobBootstrap.recordMetrics(job, output);
        return ExitCodes.OK;
    }

    /** Per-partition top-N followed by a merge job. */
    private int runPartitioned(Path input, Path output) throws Exception {
        Path intermediate = new Path(output.getParent(), output.getName() + "_partitions");
        FileSystem fs = intermediate.getFileSystem(getConf());
        if (fs.exists(intermediate)) {
            fs.delete(intermediate, true);
        }
        Job first = newJob(input, intermediate);
        int reducers = ConfigSupport.getInt(getConf(), ConfKeys.REDUCERS, 1, 4096);
        first.setNumReduceTasks(reducers);
        first.setOutputFormatClass(org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat.class);
        if (!first.waitForCompletion(true)) {
            return ExitCodes.JOB_FAILED;
        }

        Job merge = Job.getInstance(getConf(), STAGE + " merge");
        merge.setJarByClass(StopWordTopNJob.class);
        merge.setMapperClass(org.apache.hadoop.mapreduce.Mapper.class);
        merge.setReducerClass(TopNHeapReducer.class);
        merge.setMapOutputKeyClass(Text.class);
        merge.setMapOutputValueClass(LongWritable.class);
        merge.setOutputKeyClass(Text.class);
        merge.setOutputValueClass(LongWritable.class);
        merge.setInputFormatClass(SequenceFileInputFormat.class);
        merge.setNumReduceTasks(1);
        org.apache.hadoop.mapreduce.lib.input.FileInputFormat.addInputPath(merge, intermediate);
        InputFormats.configureTextOutput(merge, output);
        if (!merge.waitForCompletion(true)) {
            return ExitCodes.JOB_FAILED;
        }
        JobBootstrap.recordMetrics(merge, output);
        if (!fs.delete(intermediate, true)) {
            LOG.warn("could not delete intermediate directory {}", intermediate);
        }
        return ExitCodes.OK;
    }

    /** Builds the shared first-stage job. */
    private Job newJob(Path input, Path output) throws IOException {
        Job job = Job.getInstance(getConf(), STAGE + " " + input.getName());
        job.setJarByClass(StopWordTopNJob.class);
        job.setMapperClass(StopWordFilterMapper.class);
        job.setReducerClass(TopNHeapReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        InputFormats.configureSequence(job, input);
        InputFormats.configureTextOutput(job, output);
        String stopWords = getConf().getTrimmed(ConfKeys.STOPWORDS_PATH, null);
        if (stopWords != null) {
            StopWordDictionary.attach(job, new Path(stopWords));
        } else {
            LOG.warn("no {} configured: no tokens will be filtered", ConfKeys.STOPWORDS_PATH);
        }
        return job;
    }

    /** CLI entry point. */
    public static void main(String[] args) {
        JobBootstrap.launch(new StopWordTopNJob(), args);
    }
}
