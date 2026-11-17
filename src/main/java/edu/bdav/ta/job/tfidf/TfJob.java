package edu.bdav.ta.job.tfidf;

import java.io.IOException;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
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
import edu.bdav.ta.types.TermDocWritable;

/**
 * J7a - term frequency per (term, document), written as a SequenceFile.
 *
 * <p>The driver counts the corpus documents once with {@code listStatus} and stores
 * the count in {@code ta.corpus.ndocs}, because the IDF in the join stage needs
 * {@code N}. Counting documents is a driver-side metadata operation; doing it as a
 * MapReduce pass would cost a full scan of the corpus to learn a single integer.</p>
 *
 * <p>Usage: {@code TfJob [-D key=value] <corpus> <output>}.</p>
 */
public class TfJob extends Configured implements Tool {

    /** Stage identifier used in job names and metrics. */
    public static final String STAGE = "J7a-term-frequency";

    private static final Logger LOG = LoggerFactory.getLogger(TfJob.class);

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

        long documents = countDocuments(input);
        getConf().setLong(ConfKeys.CORPUS_DOCS, documents);
        LOG.info("corpus documents (N) = {}", documents);

        getConf().set(ConfKeys.INPUT_ROOT, input.toString());
        Job job = Job.getInstance(getConf(), STAGE + " " + input.getName());
        job.setJarByClass(TfJob.class);
        job.setMapperClass(TfMapper.class);
        job.setCombinerClass(IntSumCombiner.class);
        job.setReducerClass(TfReducer.class);
        job.setMapOutputKeyClass(TermDocWritable.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(TermDocWritable.class);
        job.setOutputValueClass(DoubleWritable.class);
        job.setNumReduceTasks(ConfigSupport.getInt(getConf(), ConfKeys.REDUCERS, 1, 4096));
        InputFormats.configureWholeFile(job, input);
        InputFormats.configureSequenceOutput(job, output);

        if (!job.waitForCompletion(true)) {
            LOG.error("{} failed", STAGE);
            return ExitCodes.JOB_FAILED;
        }
        JobBootstrap.recordMetrics(job, output);
        LOG.info("{} finished", STAGE);
        return ExitCodes.OK;
    }

    /** Counts non-hidden files under {@code input}, recursively. */
    long countDocuments(Path input) throws IOException {
        FileSystem fs = input.getFileSystem(getConf());
        long documents = 0L;
        for (FileStatus status : fs.listStatus(input)) {
            if (status.isDirectory()) {
                documents += countDocuments(status.getPath());
            } else if (!status.getPath().getName().startsWith("_")
                    && !status.getPath().getName().startsWith(".")) {
                documents++;
            }
        }
        return documents;
    }

    /** CLI entry point. */
    public static void main(String[] args) {
        JobBootstrap.launch(new TfJob(), args);
    }
}
