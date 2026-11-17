package edu.bdav.ta.job.tfidf;

import java.io.IOException;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
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
import edu.bdav.ta.reduce.LongSumCombiner;

/**
 * J7b - document frequency per term.
 *
 * <p>Usage: {@code DfJob [-D key=value] <tf-output-seq> <output>}.
 * Output: SequenceFile {@code (term, df)}.</p>
 */
public class DfJob extends Configured implements Tool {

    /** Stage identifier used in job names and metrics. */
    public static final String STAGE = "J7b-document-frequency";

    private static final Logger LOG = LoggerFactory.getLogger(DfJob.class);

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            throw new UsageException("usage: " + STAGE + " [-D key=value] <tf-seq> <output-path>");
        }
        Path input = new Path(args[0]);
        Path output = new Path(args[1]);
        JobBootstrap.requireInput(getConf(), input, "term-frequency input");
        JobBootstrap.prepareOutput(getConf(), output);
        JobBootstrap.applyIntermediateCompression(getConf());

        Job job = Job.getInstance(getConf(), STAGE + " " + input.getName());
        job.setJarByClass(DfJob.class);
        job.setMapperClass(DfMapper.class);
        job.setCombinerClass(LongSumCombiner.class);
        job.setReducerClass(DfReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        job.setNumReduceTasks(ConfigSupport.getInt(getConf(), ConfKeys.REDUCERS, 1, 4096));
        InputFormats.configureSequence(job, input);
        InputFormats.configureSequenceOutput(job, output);

        if (!job.waitForCompletion(true)) {
            LOG.error("{} failed", STAGE);
            return ExitCodes.JOB_FAILED;
        }
        JobBootstrap.recordMetrics(job, output);
        LOG.info("{} finished", STAGE);
        return ExitCodes.OK;
    }

    /** CLI entry point. */
    public static void main(String[] args) {
        JobBootstrap.launch(new DfJob(), args);
    }
}
