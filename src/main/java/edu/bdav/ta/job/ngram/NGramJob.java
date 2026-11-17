package edu.bdav.ta.job.ngram;

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
import edu.bdav.ta.reduce.IntSumCombiner;

/**
 * J5 - bigram (n=2) and trigram (n=3) frequency; n is {@code ta.ngram.n}.
 *
 * <p>Usage: {@code NGramJob [-D ta.ngram.n=3] <corpus> <output>}.
 * Output lines: {@code ngram<TAB>count} with components space-separated.</p>
 */
public class NGramJob extends Configured implements Tool {

    /** Stage identifier used in job names and metrics. */
    public static final String STAGE = "J5-ngram-frequency";

    private static final Logger LOG = LoggerFactory.getLogger(NGramJob.class);

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            throw new UsageException("usage: " + STAGE + " [-D " + ConfKeys.NGRAM_N + "=2|3] "
                    + "<input-path> <output-path>");
        }
        Path input = new Path(args[0]);
        Path output = new Path(args[1]);
        JobBootstrap.requireInput(getConf(), input, "input path");
        JobBootstrap.prepareOutput(getConf(), output);
        JobBootstrap.applyIntermediateCompression(getConf());

        int order = ConfigSupport.getInt(getConf(), ConfKeys.NGRAM_N, 1, 5);
        Job job = Job.getInstance(getConf(), STAGE + " n=" + order + " " + input.getName());
        job.setJarByClass(NGramJob.class);
        job.setMapperClass(NGramMapper.class);
        job.setCombinerClass(IntSumCombiner.class);
        job.setReducerClass(NGramReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        job.setNumReduceTasks(ConfigSupport.getInt(getConf(), ConfKeys.REDUCERS, 1, 4096));
        InputFormats.configureLineOriented(job, input);
        InputFormats.configureTextOutput(job, output);

        if (!job.waitForCompletion(true)) {
            LOG.error("{} failed", STAGE);
            return ExitCodes.JOB_FAILED;
        }
        JobBootstrap.recordMetrics(job, output);
        LOG.info("{} finished (n={})", STAGE, order);
        return ExitCodes.OK;
    }

    /** CLI entry point. */
    public static void main(String[] args) {
        JobBootstrap.launch(new NGramJob(), args);
    }
}
