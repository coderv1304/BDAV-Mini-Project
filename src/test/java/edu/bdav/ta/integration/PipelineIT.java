package edu.bdav.ta.integration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import edu.bdav.ta.cli.ExitCodes;
import edu.bdav.ta.cli.UsageException;
import edu.bdav.ta.config.ConfKeys;
import edu.bdav.ta.config.ConfigSupport;
import edu.bdav.ta.job.topn.StopWordTopNJob;
import edu.bdav.ta.job.wordcount.NormalizedWordCountJob;

/**
 * End-to-end tests through {@code LocalJobRunner}.
 *
 * <p>These are the tests that catch the failures unit tests cannot see: a job that
 * compiles and has correct mappers but is wired with the wrong input/output format,
 * a missing output path, or a combiner whose types do not match the map output. They
 * run the real MapReduce machinery in local mode, so they exercise splits,
 * serialisation, the shuffle and the commit protocol.</p>
 *
 * <p>Named {@code *IT} so Maven Failsafe runs them in the {@code verify} phase rather
 * than treating them as unit tests.</p>
 */
public class PipelineIT {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private Configuration localConf() {
        Configuration conf = ConfigSupport.withDefaults();
        conf.set("mapreduce.framework.name", "local");
        conf.set(ConfKeys.REDUCERS, "1");
        conf.set(ConfKeys.OUTPUT_FORMAT, "text");
        return conf;
    }

    private Path writeFile(String name, String content) throws IOException {
        File file = folder.newFile(name);
        Files.write(file.toPath(), content.getBytes(StandardCharsets.UTF_8));
        return new Path(file.toURI());
    }

    private Path writeRawFile(String name, byte[] content) throws IOException {
        File file = folder.newFile(name);
        Files.write(file.toPath(), content);
        return new Path(file.toURI());
    }

    private Path corpusDirectory() throws IOException {
        File directory = folder.newFolder("corpus");
        return new Path(directory.toURI());
    }

    /** Reads all part files of a stage output, sorted, ignoring _SUCCESS and .crc. */
    private List<String> readOutput(Path output) throws IOException {
        FileSystem fs = output.getFileSystem(new Configuration());
        List<String> records = new ArrayList<>();
        for (org.apache.hadoop.fs.FileStatus status : fs.listStatus(output)) {
            String name = status.getPath().getName();
            if (!name.startsWith("part-")) {
                continue;
            }
            try (java.util.Scanner scanner = new java.util.Scanner(
                    fs.open(status.getPath()), StandardCharsets.UTF_8.name())) {
                while (scanner.hasNextLine()) {
                    records.add(scanner.nextLine());
                }
            }
        }
        java.util.Collections.sort(records);
        return records;
    }

    @Test
    public void wordCountRunsEndToEndOnTwoDocuments() throws Exception {
        Path corpus = corpusDirectory();
        copyInto(corpus, writeFile("a.txt", "the whale and the sea\n"));
        copyInto(corpus, writeFile("b.txt", "the sea was calm\n"));
        Path output = new Path(folder.getRoot().toURI().toString() + "/out-wc");

        NormalizedWordCountJob job = new NormalizedWordCountJob();
        job.setConf(localConf());
        int code = job.run(new String[] {corpus.toString(), output.toString()});

        assertEquals(ExitCodes.OK, code);
        assertEquals(List.of("and\t1", "calm\t1", "sea\t2", "the\t3", "was\t1", "whale\t1"), readOutput(output));
    }

    @Test
    public void existingOutputIsRejectedUnlessOverwriteIsSet() throws Exception {
        Path corpus = corpusDirectory();
        copyInto(corpus, writeFile("a.txt", "alpha beta\n"));
        Path output = new Path(folder.getRoot().toURI().toString() + "/out-out");

        NormalizedWordCountJob first = new NormalizedWordCountJob();
        first.setConf(localConf());
        assertEquals(ExitCodes.OK, first.run(new String[] {corpus.toString(), output.toString()}));

        NormalizedWordCountJob second = new NormalizedWordCountJob();
        second.setConf(localConf());
        try {
            second.run(new String[] {corpus.toString(), output.toString()});
            fail("re-running onto an existing output must fail loudly");
        } catch (UsageException expected) {
            assertTrue(expected.getMessage().contains("already exists"));
            assertTrue("the message must name the fix", expected.getMessage().contains(ConfKeys.OVERWRITE));
        }

        NormalizedWordCountJob third = new NormalizedWordCountJob();
        Configuration overwrite = localConf();
        overwrite.set(ConfKeys.OVERWRITE, "true");
        third.setConf(overwrite);
        assertEquals(ExitCodes.OK, third.run(new String[] {corpus.toString(), output.toString()}));
    }

    @Test
    public void emptyDocumentSetStillProducesAValidOutput() throws Exception {
        Path corpus = corpusDirectory();
        copyInto(corpus, writeRawFile("empty.txt", new byte[0]));
        Path output = new Path(folder.getRoot().toURI().toString() + "/out-empty");

        NormalizedWordCountJob job = new NormalizedWordCountJob();
        job.setConf(localConf());
        assertEquals(ExitCodes.OK, job.run(new String[] {corpus.toString(), output.toString()}));
        assertTrue("an empty corpus must yield zero records, not a failure", readOutput(output).isEmpty());
    }

    @Test
    public void nonUtf8BytesAndUnicodeAreBothSurvivable() throws Exception {
        Path corpus = corpusDirectory();
        byte[] mixed = new byte[] {'c', 'a', 'f', (byte) 0xC3, (byte) 0xA9, ' ',
                (byte) 0xFF, ' ', 'x', '\n'};
        copyInto(corpus, writeRawFile("mixed.txt", mixed));
        Path output = new Path(folder.getRoot().toURI().toString() + "/out-utf8");

        NormalizedWordCountJob job = new NormalizedWordCountJob();
        job.setConf(localConf());
        assertEquals(ExitCodes.OK, job.run(new String[] {corpus.toString(), output.toString()}));
        List<String> records = readOutput(output);
        assertTrue("the valid token must survive", records.contains("x\t1"));
        assertTrue("the accented token must survive", records.stream().anyMatch(r -> r.startsWith("caf")));
    }

    @Test
    public void stopWordFilteringCanEmptyTheTopN() throws Exception {
        Path corpus = corpusDirectory();
        copyInto(corpus, writeFile("stopwords-only.txt", "the and of the and\n"));
        Path seqOutput = new Path(folder.getRoot().toURI().toString() + "/out-seq");
        Path topnOutput = new Path(folder.getRoot().toURI().toString() + "/out-topn");

        Configuration sequenceConf = localConf();
        sequenceConf.set(ConfKeys.OUTPUT_FORMAT, "seq");
        copyInto(corpus, writeFile("stopwords-list.txt", "the\nand\nof\n"));
        Path stopWords = new Path(corpus.toUri().toString() + "/stopwords-list.txt");
        sequenceConf.set(ConfKeys.STOPWORDS_PATH, stopWords.toString());

        NormalizedWordCountJob wordCount = new NormalizedWordCountJob();
        wordCount.setConf(sequenceConf);
        assertEquals(ExitCodes.OK, wordCount.run(new String[] {corpus.toString(), seqOutput.toString()}));

        StopWordTopNJob topN = new StopWordTopNJob();
        Configuration topNConf = localConf();
        topNConf.set(ConfKeys.STOPWORDS_PATH, stopWords.toString());
        topN.setConf(topNConf);
        assertEquals(ExitCodes.OK, topN.run(new String[] {seqOutput.toString(), topnOutput.toString()}));

        List<String> records = readOutput(topnOutput);
        assertTrue("an all-stop-word corpus must produce no ranked entries", records.isEmpty());
    }

    /** Copies a single file into the corpus directory so the mapper sees one input root. */
    private void copyInto(Path corpusDirectory, Path file) throws IOException {
        FileSystem fs = corpusDirectory.getFileSystem(new Configuration());
        Path target = new Path(corpusDirectory, file.getName());
        try (OutputStream out = fs.create(target, true);
             java.io.InputStream in = fs.open(file)) {
            org.apache.hadoop.io.IOUtils.copyBytes(in, out, new Configuration(), false);
        }
    }
}
