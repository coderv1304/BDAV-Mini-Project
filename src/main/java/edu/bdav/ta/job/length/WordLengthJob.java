package edu.bdav.ta.job.length;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
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
import edu.bdav.ta.reduce.LongSumCombiner;

/**
 * J3 - word-length distribution.
 *
 * <p>Usage: {@code WordLengthJob [-D key=value] <corpus> <output>}.
 * Output lines: {@code length<TAB>count}, sorted by length.</p>
 */
public class WordLengthJob extends Configured implements Tool {

    /** Stage identifier used in job names and metrics. */
    public static final String STAGE = "J3-word-length";

    private static final Logger LOG = LoggerFactory.getLogger(WordLengthJob.class);

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            throw new UsageException("usage: " + STAGE + " [-D key=value] <input-path> <output-path>");
        }
        Path input = new Path(args[0]);
        Path output = new Path(args[1]);
        JobBootstrap.requireInput(getConf(), input, "input path");
        JobBootstrap.prepareOutput(getConf(), output);

        Job job = Job.getInstance(getConf(), STAGE + " " + input.getName());
        job.setJarByClass(WordLengthJob.class);
        job.setMapperClass(WordLengthMapper.class);
        job.setCombinerClass(LongSumCombiner.class);
        job.setReducerClass(WordLengthReducer.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(LongWritable.class);
        job.setNumReduceTasks(1);
        InputFormats.configureLineOriented(job, input);
        InputFormats.configureTextOutput(job, output);

        if (!job.waitForCompletion(true)) {
            LOG.error("{} failed", STAGE);
            return ExitCodes.JOB_FAILED;
        }
        JobBootstrap.recordMetrics(job, output);
        LOG.info("{} finished with {} reducers", STAGE,
                ConfigSupport.getInt(getConf(), ConfKeys.REDUCERS, 1, 4096));
        return ExitCodes.OK;
    }

    /** CLI entry point. */
    public static void main(String[] args) {
        JobBootstrap.launch(new WordLengthJob(), args);
    }
}
