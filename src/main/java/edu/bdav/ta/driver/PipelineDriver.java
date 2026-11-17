package edu.bdav.ta.driver;

import java.io.IOException;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import edu.bdav.ta.cli.ExitCodes;
import edu.bdav.ta.cli.JobBootstrap;
import edu.bdav.ta.cli.UsageException;
import edu.bdav.ta.config.ConfKeys;
import edu.bdav.ta.config.ConfigSupport;
import edu.bdav.ta.job.charfreq.CharacterFrequencyJob;
import edu.bdav.ta.job.cooccur.PairsCooccurrenceJob;
import edu.bdav.ta.job.cooccur.StripesCooccurrenceJob;
import edu.bdav.ta.job.inverted.InvertedIndexJob;
import edu.bdav.ta.job.length.WordLengthJob;
import edu.bdav.ta.job.ngram.NGramJob;
import edu.bdav.ta.job.sentiment.SentimentJob;
import edu.bdav.ta.job.tfidf.DfJob;
import edu.bdav.ta.job.tfidf.TfIdfJoinJob;
import edu.bdav.ta.job.tfidf.TfJob;
import edu.bdav.ta.job.topn.StopWordTopNJob;
import edu.bdav.ta.job.wordcount.NormalizedWordCountJob;

/**
 * Orchestrating driver: runs the whole pipeline with explicit dependencies.
 *
 * <p><b>Chaining strategy, and why it is mixed.</b></p>
 * <ul>
 *   <li>Stages with a data dependency (J1 → J2, J7a → J7b → J7c) run
 *       <em>sequentially</em>: a {@code JobControl} graph would also encode the
 *       dependency, but the next stage needs the previous stage's <em>output</em> to
 *       exist, so the simplest correct model is "run, check, then continue". Failure
 *       propagation is explicit: the first non-zero exit code aborts the pipeline and
 *       is returned to the shell.</li>
 *   <li>Independent stages (the pairs and stripes co-occurrence jobs) run
 *       <em>in parallel</em> through {@link JobControl}, which is what that class is
 *       actually good at: scheduling, polling and collecting per-job failures.</li>
 * </ul>
 *
 * <p>Intermediate directories are removed after the dependent stages have consumed
 * them when {@code ta.cleanup.intermediate=true}, which keeps a 1 GB run's output
 * tree from filling the disk with data nobody reads.</p>
 *
 * <p>Usage: {@code PipelineDriver [-D key=value] <corpus> <output-root>}.</p>
 */
public class PipelineDriver extends Configured implements Tool {

    /** Stage identifier used in job names and metrics. */
    public static final String STAGE = "pipeline";

    /** Configuration key selecting which stages to run. */
    public static final String STAGES = "ta.pipeline.stages";

    /** Configuration key enabling intermediate cleanup. */
    public static final String CLEANUP_INTERMEDIATE = "ta.cleanup.intermediate";

    /** Configuration key bounding how long the parallel co-occurrence branch may run. */
    public static final String JOB_CONTROL_BUDGET = "ta.pipeline.jobcontrol.budget.seconds";

    private static final Logger LOG = LoggerFactory.getLogger(PipelineDriver.class);

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            throw new UsageException("usage: " + STAGE + " [-D " + STAGES + "=core|all] "
                    + "<corpus> <output-root>");
        }
        Path corpus = new Path(args[0]);
        Path root = new Path(args[1]);
        JobBootstrap.requireInput(getConf(), corpus, "corpus");
        String stages = ConfigSupport.getTrimmed(getConf(), STAGES, "core");
        LOG.info("pipeline starting: corpus={} root={} stages={}", corpus, root, stages);

        Path wordCount = child(root, "01-wordcount");
        int code = runStage(new NormalizedWordCountJob(), corpus.toString(), wordCount.toString());
        if (code != ExitCodes.OK) {
            return code;
        }

        code = runStage(new StopWordTopNJob(), wordCount.toString(), child(root, "02-topn").toString());
        if (code != ExitCodes.OK) {
            return code;
        }

        code = runStage(new WordLengthJob(), corpus.toString(), child(root, "03-word-length").toString());
        if (code != ExitCodes.OK) {
            return code;
        }

        code = runStage(new CharacterFrequencyJob(), corpus.toString(), child(root, "04-char-freq").toString());
        if (code != ExitCodes.OK) {
            return code;
        }

        code = runStage(new NGramJob(), "-D" + ConfKeys.NGRAM_N + "=2",
                corpus.toString(), child(root, "05a-bigrams").toString());
        if (code != ExitCodes.OK) {
            return code;
        }
        code = runStage(new NGramJob(), "-D" + ConfKeys.NGRAM_N + "=3",
                corpus.toString(), child(root, "05b-trigrams").toString());
        if (code != ExitCodes.OK) {
            return code;
        }

        code = runStage(new InvertedIndexJob(), corpus.toString(), child(root, "06-inverted-index").toString());
        if (code != ExitCodes.OK) {
            return code;
        }

        Path tf = child(root, "07a-tf");
        code = runStage(new TfJob(), corpus.toString(), tf.toString());
        if (code != ExitCodes.OK) {
            return code;
        }
        Path df = child(root, "07b-df");
        code = runStage(new DfJob(), tf.toString(), df.toString());
        if (code != ExitCodes.OK) {
            return code;
        }
        code = runStage(new TfIdfJoinJob(), tf.toString(), df.toString(),
                child(root, "07c-tfidf").toString());
        if (code != ExitCodes.OK) {
            return code;
        }

        code = runCooccurrenceInParallel(corpus, root);
        if (code != ExitCodes.OK) {
            return code;
        }

        if ("all".equalsIgnoreCase(stages)
                || getConf().getTrimmed(ConfKeys.LEXICON_PATH, null) != null) {
            code = runStage(new SentimentJob(), corpus.toString(), child(root, "09-sentiment").toString());
            if (code != ExitCodes.OK) {
                return code;
            }
        }

        if (ConfigSupport.getBoolean(getConf(), CLEANUP_INTERMEDIATE)) {
            deleteQuietly(tf);
            deleteQuietly(df);
        }
        LOG.info("pipeline finished: results under {}", root);
        return ExitCodes.OK;
    }

    /**
     * Runs the pairs and stripes jobs concurrently through {@link JobControl}.
     *
     * @param corpus input corpus
     * @param root pipeline output root
     * @return {@link ExitCodes#OK} when both jobs succeeded
     */
    int runCooccurrenceInParallel(Path corpus, Path root) throws Exception {
        PairsCooccurrenceJob pairs = new PairsCooccurrenceJob();
        pairs.setConf(getConf());
        StripesCooccurrenceJob stripes = new StripesCooccurrenceJob();
        stripes.setConf(getConf());

        Path pairsOut = child(root, "08a-pairs");
        Path stripesOut = child(root, "08b-stripes");
        JobBootstrap.prepareOutput(getConf(), pairsOut);
        JobBootstrap.prepareOutput(getConf(), stripesOut);

        Job pairsJob = pairs.createJob(corpus, pairsOut);
        Job stripesJob = stripes.createJob(corpus, stripesOut);

        JobControl control = new JobControl("cooccurrence");
        control.addJob(new ControlledJob(pairsJob, null));
        control.addJob(new ControlledJob(stripesJob, null));
        Thread runner = new Thread(control, "jobcontrol-cooccurrence");
        runner.setDaemon(true);
        runner.start();
        long budgetSeconds = ConfigSupport.getLong(getConf(), JOB_CONTROL_BUDGET, 60L, 86_400L);
        long deadline = System.currentTimeMillis() + budgetSeconds * 1000L;
        while (!control.allFinished()) {
            if (System.currentTimeMillis() > deadline) {
                control.stop();
                LOG.error("co-occurrence jobs exceeded the {}s budget", budgetSeconds);
                return ExitCodes.JOB_FAILED;
            }
            Thread.sleep(500L);
        }
        control.stop();
        List<ControlledJob> failed = control.getFailedJobList();
        if (!failed.isEmpty()) {
            for (ControlledJob job : failed) {
                LOG.error("co-occurrence job failed: {} ({})", job.getJobName(), job.getMessage());
            }
            return ExitCodes.JOB_FAILED;
        }
        JobBootstrap.recordMetrics(pairsJob, pairsOut);
        JobBootstrap.recordMetrics(stripesJob, stripesOut);
        return ExitCodes.OK;
    }

    /**
     * Runs one stage Tool with the driver's configuration.
     *
     * <p>Goes through {@link ToolRunner} rather than calling {@code tool.run} directly:
     * that is what strips {@code -D} options out of the argument array and merges them
     * into the configuration. Calling {@code run} directly would hand {@code -D} flags
     * to the stage as positional arguments (a real bug this pipeline hit once).</p>
     */
    private int runStage(Tool tool, String... args) throws Exception {
        int code = ToolRunner.run(getConf(), tool, JobBootstrap.normaliseArgs(args));
        if (code != ExitCodes.OK) {
            LOG.error("stage {} exited with code {}", tool.getClass().getSimpleName(), code);
        }
        return code;
    }

    private Path child(Path root, String name) {
        return new Path(root, name);
    }

    private void deleteQuietly(Path path) throws IOException {
        FileSystem fs = path.getFileSystem(getConf());
        if (fs.exists(path) && !fs.delete(path, true)) {
            LOG.warn("could not delete intermediate {}", path);
        }
    }

    /** CLI entry point. */
    public static void main(String[] args) {
        JobBootstrap.launch(new PipelineDriver(), args);
    }
}
