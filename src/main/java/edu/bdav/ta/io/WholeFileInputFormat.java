package edu.bdav.ta.io;

import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

/**
 * Input format delivering one record per document: key = document id, value = file content.
 *
 * <p>Required wherever the analysis is document-scoped (inverted index, TF, TF-IDF,
 * sentiment, cosine): a line-oriented split would hide document boundaries, and
 * the whole point of these jobs is "which document does this term come from".</p>
 *
 * <p>Cost, stated openly: one mapper per file (no splitting) and the whole file in
 * memory per record. Acceptable because documents are books/articles; it would be
 * the wrong choice for a corpus of multi-gigabyte single files, where a
 * boundary-aware format (e.g. {@code NLineInputFormat} with header accounting)
 * would be needed.</p>
 */
public class WholeFileInputFormat extends FileInputFormat<Text, Text> {

    @Override
    protected boolean isSplitable(JobContext context, Path filename) {
        return false;
    }

    @Override
    public RecordReader<Text, Text> createRecordReader(InputSplit split, TaskAttemptContext context)
            throws IOException, InterruptedException {
        WholeFileRecordReader reader = new WholeFileRecordReader();
        reader.initialize(split, context);
        return reader;
    }
}
