package edu.bdav.ta.job.cooccur;

import java.io.IOException;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
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
import edu.bdav.ta.types.StripeWritable;

/**
 * J8s - co-occurrence matrix, <b>stripes</b> pattern.
 *
 * <p>Usage: {@code StripesCooccurrenceJob [-D key=value] <corpus> <output>}.</p>
 */
public class StripesCooccurrenceJob extends Configured implements Tool {

    /** Stage identifier used in job names and metrics. */
    public static final String STAGE = "J8s-cooccurrence-stripes";

    private static final Logger LOG = LoggerFactory.getLogger(StripesCooccurrenceJob.class);

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            throw new UsageException("usage: " + STAGE + " [-D key=value] <input-path> <output-path>");
        }
        Path input = new Path(args[0]);
        Path output = new Path(args[1]);
        JobBootstrap.requireInput(getConf(), input, "input path");
        JobBootstrap.prepareOutput(getConf(), output);
        Job job = createJob(input, output);
        if (!job.waitForCompletion(true)) {
            LOG.error("{} failed", STAGE);
            return ExitCodes.JOB_FAILED;
        }
        JobBootstrap.recordMetrics(job, output);
        LOG.info("{} finished", STAGE);
        return ExitCodes.OK;
    }

    /**
     * Builds the job without running it, so a {@code JobControl} pipeline can
     * schedule it alongside the pairs variant.
     *
     * @param input corpus
     * @param output destination
     * @return a fully configured job
     */
    public Job createJob(Path input, Path output) throws IOException {
        JobBootstrap.applyIntermediateCompression(getConf());
        getConf().set(ConfKeys.INPUT_ROOT, input.toString());
        Job job = Job.getInstance(getConf(), STAGE + " " + input.getName());
        job.setJarByClass(StripesCooccurrenceJob.class);
        job.setMapperClass(StripesMapper.class);
        job.setCombinerClass(StripesCombiner.class);
        job.setReducerClass(StripesReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(StripeWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(ConfigSupport.getInt(getConf(), ConfKeys.REDUCERS, 1, 4096));
        InputFormats.configureWholeFile(job, input);
        InputFormats.configureTextOutput(job, output);
        return job;
    }

    /** CLI entry point. */
    public static void main(String[] args) {
        JobBootstrap.launch(new StripesCooccurrenceJob(), args);
    }
}
