package edu.bdav.ta.job.tfidf;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
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
import edu.bdav.ta.partition.TermPartitioner;
import edu.bdav.ta.sort.TermDocSortComparator;
import edu.bdav.ta.sort.TermGroupingComparator;
import edu.bdav.ta.types.TaggedCountWritable;
import edu.bdav.ta.types.TermDocWritable;

/**
 * J7c - TF-IDF join.
 *
 * <p>Wiring that matters, in the order an examiner asks about it:</p>
 * <ol>
 *   <li>{@link MultipleInputs} feeds the two SequenceFiles to two different mappers
 *       (TF tagged {@code 'T'}, DF tagged {@code 'D'}).</li>
 *   <li>{@link TermPartitioner} hashes the term only, so a term's DF record and all
 *       its TF records land in the same reducer.</li>
 *   <li>{@link TermDocSortComparator} orders each group {@code (term, "*")} first.</li>
 *   <li>{@link TermGroupingComparator} makes one {@code reduce()} call per term.</li>
 * </ol>
 *
 * <p>Usage: {@code TfIdfJoinJob [-D key=value] <tf-seq> <df-seq> <output>}.</p>
 */
public class TfIdfJoinJob extends Configured implements Tool {

    /** Stage identifier used in job names and metrics. */
    public static final String STAGE = "J7c-tfidf-join";

    private static final Logger LOG = LoggerFactory.getLogger(TfIdfJoinJob.class);

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 3) {
            throw new UsageException("usage: " + STAGE + " [-D key=value] <tf-seq> <df-seq> <output-path>");
        }
        Path tfInput = new Path(args[0]);
        Path dfInput = new Path(args[1]);
        Path output = new Path(args[2]);
        JobBootstrap.requireInput(getConf(), tfInput, "term-frequency input");
        JobBootstrap.requireInput(getConf(), dfInput, "document-frequency input");
        JobBootstrap.prepareOutput(getConf(), output);
        JobBootstrap.applyIntermediateCompression(getConf());

        Job job = Job.getInstance(getConf(), STAGE);
        job.setJarByClass(TfIdfJoinJob.class);
        MultipleInputs.addInputPath(job, tfInput, SequenceFileInputFormat.class, TfIdfJoinMapper.class);
        MultipleInputs.addInputPath(job, dfInput, SequenceFileInputFormat.class, DfSideMapper.class);
        job.setMapOutputKeyClass(TermDocWritable.class);
        job.setMapOutputValueClass(TaggedCountWritable.class);
        job.setPartitionerClass(TermPartitioner.class);
        job.setSortComparatorClass(TermDocSortComparator.class);
        job.setGroupingComparatorClass(TermGroupingComparator.class);
        job.setReducerClass(TfIdfJoinReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(ConfigSupport.getInt(getConf(), ConfKeys.REDUCERS, 1, 4096));
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
        JobBootstrap.launch(new TfIdfJoinJob(), args);
    }
}
