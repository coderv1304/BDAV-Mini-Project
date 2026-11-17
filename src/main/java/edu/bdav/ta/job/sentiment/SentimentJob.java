package edu.bdav.ta.job.sentiment;

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
import edu.bdav.ta.io.InputFormats;
import edu.bdav.ta.text.SentimentLexicon;

/**
 * J9 (bonus) - lexicon-based document sentiment, implemented as a <b>map-only</b> job.
 *
 * <p>Zero reducers is the right structure here: scoring a document needs only that
 * document, no aggregation crosses documents, and a shuffle would add cost without
 * changing a single number. Setting {@code setNumReduceTasks(0)} is therefore a
 * deliberate optimisation, not a shortcut - and it is the one job in the pipeline
 * where "why not a reducer?" has a clean answer.</p>
 *
 * <p>Usage: {@code SentimentJob [-D ta.lexicon.path=...] <corpus> <output>}.</p>
 */
public class SentimentJob extends Configured implements Tool {

    /** Stage identifier used in job names and metrics. */
    public static final String STAGE = "J9-sentiment";

    private static final Logger LOG = LoggerFactory.getLogger(SentimentJob.class);

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            throw new UsageException("usage: " + STAGE + " [-D " + ConfKeys.LEXICON_PATH
                    + "=<path>] <input-path> <output-path>");
        }
        Path input = new Path(args[0]);
        Path output = new Path(args[1]);
        JobBootstrap.requireInput(getConf(), input, "input path");
        JobBootstrap.prepareOutput(getConf(), output);

        getConf().set(ConfKeys.INPUT_ROOT, input.toString());
        Job job = Job.getInstance(getConf(), STAGE + " " + input.getName());
        job.setJarByClass(SentimentJob.class);
        job.setMapperClass(SentimentMapper.class);
        job.setNumReduceTasks(0);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        String lexicon = getConf().getTrimmed(ConfKeys.LEXICON_PATH, null);
        if (lexicon != null) {
            SentimentLexicon.attach(job, new Path(lexicon));
        } else {
            LOG.warn("no {} configured: every polarity will be 0", ConfKeys.LEXICON_PATH);
        }
        InputFormats.configureWholeFile(job, input);
        InputFormats.configureTextOutput(job, output);

        if (!job.waitForCompletion(true)) {
            LOG.error("{} failed", STAGE);
            return ExitCodes.JOB_FAILED;
        }
        JobBootstrap.recordMetrics(job, output);
        LOG.info("{} finished (map-only)", STAGE);
        return ExitCodes.OK;
    }

    /** CLI entry point. */
    public static void main(String[] args) {
        JobBootstrap.launch(new SentimentJob(), args);
    }
}
