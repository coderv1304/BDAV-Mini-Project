package edu.bdav.ta.io;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.nio.charset.StandardCharsets;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.Test;
import edu.bdav.ta.TestSupport;
import edu.bdav.ta.config.ConfKeys;

/** Tests for document-id derivation and tolerant decoding. */
public class WholeFileRecordReaderTest {

    @Test
    public void documentIdIsRelativeToTheInputRoot() {
        Configuration conf = TestSupport.conf();
        conf.set(ConfKeys.INPUT_ROOT, "hdfs://namenode:9000/data/raw");
        assertEquals("book-1.txt",
                WholeFileRecordReader.documentId(conf, new Path("hdfs://namenode:9000/data/raw/book-1.txt")));
        assertEquals("sub/book-2.txt",
                WholeFileRecordReader.documentId(conf, new Path("hdfs://namenode:9000/data/raw/sub/book-2.txt")));
    }

    @Test
    public void documentIdFallsBackToTheFileNameOutsideTheRoot() {
        Configuration conf = TestSupport.conf();
        conf.set(ConfKeys.INPUT_ROOT, "/data/raw");
        assertEquals("other.txt", WholeFileRecordReader.documentId(conf, new Path("/elsewhere/other.txt")));
    }

    @Test
    public void decodesValidUtf8WithoutFlaggingIt() {
        byte[] bytes = "hello café".getBytes(StandardCharsets.UTF_8);
        assertEquals("hello café", decodeText(bytes));
        assertTrue(!decodeMalformed(bytes));
    }

    @Test
    public void replacesInvalidBytesAndFlagsTheDocument() {
        byte[] bytes = new byte[] {'a', (byte) 0xC3, 'b'};
        assertTrue("invalid UTF-8 must be reported, not silently accepted", decodeMalformed(bytes));
        assertTrue(decodeText(bytes).startsWith("a"));
    }

    private static String decodeText(byte[] bytes) {
        return readDecoded(bytes)[0];
    }

    private static boolean decodeMalformed(byte[] bytes) {
        return Boolean.parseBoolean(readDecoded(bytes)[1]);
    }

    private static String[] readDecoded(byte[] bytes) {
        try {
            java.lang.reflect.Method method = WholeFileRecordReader.class
                    .getDeclaredMethod("decode", byte[].class);
            method.setAccessible(true);
            Object decoded = method.invoke(null, (Object) bytes);
            java.lang.reflect.Field text = decoded.getClass().getDeclaredField("text");
            java.lang.reflect.Field malformed = decoded.getClass().getDeclaredField("malformed");
            text.setAccessible(true);
            malformed.setAccessible(true);
            return new String[] {(String) text.get(decoded), String.valueOf(malformed.getBoolean(decoded))};
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException(e);
        }
    }
}
