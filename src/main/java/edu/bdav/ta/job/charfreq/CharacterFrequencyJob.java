package edu.bdav.ta.job.charfreq;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import edu.bdav.ta.cli.ExitCodes;
import edu.bdav.ta.cli.JobBootstrap;
import edu.bdav.ta.cli.UsageException;
import edu.bdav.ta.io.InputFormats;
import edu.bdav.ta.reduce.IntSumCombiner;

/**
 * J4 - character-level frequency.
 *
 * <p>Usage: {@code CharacterFrequencyJob [-D key=value] <corpus> <output>}.
 * Output lines: {@code U+XXXX<TAB>char<TAB>count}, ordered by code point.</p>
 */
public class CharacterFrequencyJob extends Configured implements Tool {

    /** Stage identifier used in job names and metrics. */
    public static final String STAGE = "J4-character-frequency";

    private static final Logger LOG = LoggerFactory.getLogger(CharacterFrequencyJob.class);

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
        job.setJarByClass(CharacterFrequencyJob.class);
        job.setMapperClass(CharacterFrequencyMapper.class);
        job.setCombinerClass(IntSumCombiner.class);
        job.setReducerClass(CharacterFrequencyReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(1);
        InputFormats.configureLineOriented(job, input);
        InputFormats.configureTextOutput(job, output);

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
        JobBootstrap.launch(new CharacterFrequencyJob(), args);
    }
}
