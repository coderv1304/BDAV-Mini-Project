package edu.bdav.ta.cli;

import java.io.IOException;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.util.Tool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import edu.bdav.ta.io.InputFormats;

/**
 * Renders a SequenceFile stage output as text.
 *
 * <p>Intermediate stages are SequenceFiles because that is the right format for
 * chaining, but two consumers need text: a human reading an intermediate result, and
 * the Python verifier, which has no JVM. Rather than duplicating every stage's
 * output (extra IO on every run), this tool converts on demand.</p>
 *
 * <p>Usage: {@code dump [-D key=value] <sequence-input> <text-output>}.</p>
 */
public class SequenceDump extends Configured implements Tool {

    private static final Logger LOG = LoggerFactory.getLogger(SequenceDump.class);

    /** Converts arbitrary {@code Writable}s to their text rendering. */
    public static class DumpMapper extends Mapper<Object, Object, Text, Text> {

        private final Text outKey = new Text();
        private final Text outValue = new Text();

        @Override
        protected void map(Object key, Object value, Context context)
                throws IOException, InterruptedException {
            outKey.set(key.toString());
            outValue.set(value.toString());
            context.write(outKey, outValue);
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            throw new UsageException("usage: dump [-D key=value] <sequence-input> <text-output>");
        }
        Path input = new Path(args[0]);
        Path output = new Path(args[1]);
        JobBootstrap.requireInput(getConf(), input, "sequence input");
        JobBootstrap.prepareOutput(getConf(), output);

        Job job = Job.getInstance(getConf(), "dump " + input.getName());
        job.setJarByClass(SequenceDump.class);
        job.setMapperClass(DumpMapper.class);
        job.setNumReduceTasks(0);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        FileInputFormat.addInputPath(job, input);
        FileInputFormat.setInputDirRecursive(job, true);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        InputFormats.configureTextOutput(job, output);
        if (!job.waitForCompletion(true)) {
            LOG.error("dump failed for {}", input);
            return ExitCodes.JOB_FAILED;
        }
        LOG.info("dumped {} to {}", input, output);
        return ExitCodes.OK;
    }

    /** CLI entry point. */
    public static void main(String[] args) {
        JobBootstrap.launch(new SequenceDump(), args);
    }
}
