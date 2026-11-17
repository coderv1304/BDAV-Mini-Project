package edu.bdav.ta.job.wordcount;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import edu.bdav.ta.cli.ExitCodes;
import edu.bdav.ta.cli.JobBootstrap;
import edu.bdav.ta.cli.UsageException;
import edu.bdav.ta.config.ConfKeys;
import edu.bdav.ta.config.ConfigSupport;
import edu.bdav.ta.io.InputFormats;
import edu.bdav.ta.partition.SaltedTextPartitioner;

/**
 * J1 - normalised word count: the pipeline's foundation stage.
 *
 * <p>Input: raw text lines. Output: {@code (token, count)} as a SequenceFile
 * (default) so J2/J3 can chain without re-parsing text, or as text via
 * {@code -Dta.output.format=text} for eyeballing.</p>
 *
 * <p>Usage: {@code NormalizedWordCountJob [-D key=value] <input> <output>}</p>
 */
public class NormalizedWordCountJob extends Configured implements Tool {

    /** Stage identifier used in job names and metrics. */
    public static final String STAGE = "J1-normalized-word-count";

    private static final Logger LOG = LoggerFactory.getLogger(NormalizedWordCountJob.class);

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            throw new UsageException("usage: " + STAGE + " [-D key=value] <input-path> <output-path>");
        }
        Path input = new Path(args[0]);
        Path output = new Path(args[1]);
        JobBootstrap.requireInput(getConf(), input, "input path");
        JobBootstrap.prepareOutput(getConf(), output);
        JobBootstrap.applyIntermediateCompression(getConf());

        Job job = Job.getInstance(getConf(), STAGE + " " + input.getName());
        job.setJarByClass(NormalizedWordCountJob.class);
        job.setMapperClass(NormalizingWordCountMapper.class);

        String combineMode = ConfigSupport.getTrimmed(getConf(), ConfKeys.COMBINE_MODE, "combiner");
        if ("combiner".equalsIgnoreCase(combineMode) || "both".equalsIgnoreCase(combineMode)) {
            job.setCombinerClass(edu.bdav.ta.reduce.IntSumCombiner.class);
            LOG.info("combiner enabled ({})", combineMode);
        } else {
            LOG.info("combiner disabled ({}), in-mapper={}", combineMode,
                    "inmapper".equalsIgnoreCase(combineMode));
        }

        job.setReducerClass(LongSumReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        job.setNumReduceTasks(ConfigSupport.getInt(getConf(), ConfKeys.REDUCERS, 1, 4096));

        String partitioner = ConfigSupport.getTrimmed(getConf(), ConfKeys.PARTITIONER, "hash");
        if ("salted".equalsIgnoreCase(partitioner)) {
            job.setPartitionerClass(SaltedTextPartitioner.class);
            LOG.warn("salted partitioner selected: counts are partial and must be re-aggregated "
                    + "(this mode exists only for the skew experiment)");
        }

        InputFormats.configureLineOriented(job, input);
        InputFormats.configureOutput(job, output, "seq");

        if (!job.waitForCompletion(true)) {
            LOG.error("{} failed; see the task logs above", STAGE);
            return ExitCodes.JOB_FAILED;
        }
        JobBootstrap.recordMetrics(job, output);
        LOG.info("{} finished", STAGE);
        return ExitCodes.OK;
    }

    /** CLI entry point. */
    public static void main(String[] args) {
        JobBootstrap.launch(new NormalizedWordCountJob(), args);
    }
}
