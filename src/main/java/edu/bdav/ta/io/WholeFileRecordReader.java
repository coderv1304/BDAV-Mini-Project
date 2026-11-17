package edu.bdav.ta.io;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import edu.bdav.ta.config.ConfKeys;

/**
 * Reads one file per {@code nextKeyValue()} call.
 *
 * <p>The key is the document id: the file's path <em>relative to the configured
 * input root</em> ({@code ta.input.root}). Relative ids are what make results
 * reproducible across local mode, pseudo-distributed mode and the multi-node
 * cluster - an absolute {@code hdfs://namenode:9000/...} id would differ per mode.</p>
 *
 * <p>Bytes are decoded as UTF-8 with replacement, and the reader reports whether
 * invalid sequences were present so the mapper can increment
 * {@code MALFORMED_LINES}.</p>
 */
public class WholeFileRecordReader extends RecordReader<Text, Text> {

    /** Extra configuration key carrying the "malformed" flag for the current record. */
    public static final String MALFORMED_FLAG = "ta.record.malformed";

    private FileSplit split;
    private Configuration conf;
    private boolean processed;
    private final Text key = new Text();
    private final Text value = new Text();

    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext context) {
        this.split = (FileSplit) inputSplit;
        this.conf = context.getConfiguration();
    }

    @Override
    public boolean nextKeyValue() throws IOException {
        if (processed) {
            return false;
        }
        processed = true;
        Path path = split.getPath();
        FileSystem fs = path.getFileSystem(conf);
        long length = fs.getFileStatus(path).getLen();
        if (length > Integer.MAX_VALUE - 8) {
            throw new IOException("document too large for WholeFileInputFormat (" + length
                    + " bytes): " + path + " - use a split-aware format for such a corpus");
        }
        byte[] buffer = new byte[(int) length];
        try (FSDataInputStream in = fs.open(path)) {
            IOUtils.readFully(in, buffer, 0, buffer.length);
        }
        key.set(documentId(conf, path));
        Decoded decoded = decode(buffer);
        value.set(decoded.text);
        // Stash the flag where the mapper can read it without another decode pass.
        conf.setBoolean(MALFORMED_FLAG, decoded.malformed);
        return true;
    }

    /** @return always 1.0: a document is either fully read or not read at all */
    @Override
    public float getProgress() {
        return processed ? 1.0f : 0.0f;
    }

    @Override
    public Text getCurrentKey() {
        return key;
    }

    @Override
    public Text getCurrentValue() {
        return value;
    }

    @Override
    public void close() {
        // Nothing to release: the stream is closed per record.
    }

    /** Result of a decode attempt. */
    private static final class Decoded {
        private final String text;
        private final boolean malformed;

        Decoded(String text, boolean malformed) {
            this.text = text;
            this.malformed = malformed;
        }
    }

    /** Decodes UTF-8, replacing invalid bytes but reporting that it happened. */
    static Decoded decode(byte[] bytes) {
        CharsetDecoder strict = StandardCharsets.UTF_8.newDecoder()
                .onMalformedInput(CodingErrorAction.REPORT)
                .onUnmappableCharacter(CodingErrorAction.REPORT);
        CharsetDecoder lenient = StandardCharsets.UTF_8.newDecoder()
                .onMalformedInput(CodingErrorAction.REPLACE)
                .onUnmappableCharacter(CodingErrorAction.REPLACE);
        try {
            CharBuffer decoded = strict.decode(ByteBuffer.wrap(bytes));
            return new Decoded(decoded.toString(), false);
        } catch (CharacterCodingException e) {
            try {
                CharBuffer decoded = lenient.decode(ByteBuffer.wrap(bytes));
                return new Decoded(decoded.toString(), true);
            } catch (CharacterCodingException impossible) {
                // REPLACE cannot fail; kept for completeness.
                return new Decoded(new String(bytes, StandardCharsets.UTF_8), true);
            }
        }
    }

    /** Derives the document id as a path relative to the configured input root. */
    static String documentId(Configuration conf, Path path) {
        String root = conf.getTrimmed(ConfKeys.INPUT_ROOT, "");
        String uri = path.toUri().getPath();
        if (root.isEmpty()) {
            return uri;
        }
        Path rootPath = new Path(root);
        String rootUri = rootPath.toUri().getPath();
        if (!rootUri.endsWith("/")) {
            rootUri = rootUri + "/";
        }
        if (uri.startsWith(rootUri)) {
            return uri.substring(rootUri.length());
        }
        return path.getName();
    }
}
