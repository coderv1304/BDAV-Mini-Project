package edu.bdav.ta.integration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Arrays;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import edu.bdav.ta.cli.ExitCodes;
import edu.bdav.ta.cli.SequenceDump;
import edu.bdav.ta.config.ConfKeys;
import edu.bdav.ta.config.ConfigSupport;
import edu.bdav.ta.driver.PipelineDriver;
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
 * Runs every stage through {@code LocalJobRunner} on a three-document corpus.
 *
 * <p>This is where wiring defects surface - wrong output path, mismatched map-output
 * types for a combiner, a stage reading its input with the wrong format. Each of
 * those bugs existed at least once during development and none of them is visible to
 * a unit test of a mapper.</p>
 */
public class FullPipelineIT {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private Configuration conf;
    private Path corpus;
    private Path stopWords;
    private Path lexicon;
    private String root;

    @Before
    public void setUp() throws IOException {
        conf = ConfigSupport.withDefaults();
        conf.set("mapreduce.framework.name", "local");
        conf.set(ConfKeys.REDUCERS, "1");
        conf.set(ConfKeys.OVERWRITE, "true");
        root = folder.getRoot().toURI().toString();

        corpus = new Path(folder.newFolder("corpus").toURI());
        write(corpus, "doc-1.txt", "The whale swam. A good and brave whale, 1984.\n");
        write(corpus, "doc-2.txt", "The sea was calm; the whale slept well.\n");
        write(corpus, "doc-3.txt", "Café owners don't like a bad, terrible storm.\n");

        stopWords = writeText("stopwords.txt", "the\nand\na\nof\nwas\n");
        lexicon = writeText("lexicon.tsv", "good\t2\nbrave\t2\nbad\t-2\nterrible\t-3\ncalm\t1\n");
        conf.set(ConfKeys.STOPWORDS_PATH, stopWords.toString());
        conf.set(ConfKeys.LEXICON_PATH, lexicon.toString());
    }

    @Test
    public void everyStageRunsEndToEnd() throws Exception {
        Path wordCount = out("01-wordcount");
        assertOk(new NormalizedWordCountJob(), corpus.toString(), wordCount.toString());

        assertOk(new StopWordTopNJob(), wordCount.toString(), out("02-topn").toString());
        assertOk(new WordLengthJob(), corpus.toString(), out("03-length").toString());
        assertOk(new CharacterFrequencyJob(), corpus.toString(), out("04-charfreq").toString());
        assertOk(new NGramJob(), corpus.toString(), out("05-bigram").toString());
        assertOk(new InvertedIndexJob(), corpus.toString(), out("06-inverted").toString());

        Path tf = out("07a-tf");
        Path df = out("07b-df");
        assertOk(new TfJob(), corpus.toString(), tf.toString());
        assertOk(new DfJob(), tf.toString(), df.toString());
        assertOk(new TfIdfJoinJob(), tf.toString(), df.toString(), out("07c-tfidf").toString());

        assertOk(new PairsCooccurrenceJob(), corpus.toString(), out("08a-pairs").toString());
        assertOk(new StripesCooccurrenceJob(), corpus.toString(), out("08b-stripes").toString());
        assertOk(new SentimentJob(), corpus.toString(), out("09-sentiment").toString());
        assertOk(new SequenceDump(), wordCount.toString(), out("dump").toString());

        assertTrue("the dump must be readable text",
                readText(out("dump")).contains("\t"));
        assertTrue("sentiment must score documents",
                readText(out("09-sentiment")).contains("doc-1.txt"));
    }

    @Test
    public void orchestratorRunsTheWholePipeline() throws Exception {
        Path outputRoot = new Path(root + "pipeline-out");
        PipelineDriver driver = new PipelineDriver();
        driver.setConf(conf);
        int code = ToolRunner.run(conf, driver, new String[] {corpus.toString(), outputRoot.toString()});
        assertEquals(ExitCodes.OK, code);
        for (String stage : Arrays.asList("01-wordcount", "02-topn", "07c-tfidf",
                "08a-pairs", "08b-stripes", "09-sentiment")) {
            assertTrue("missing stage output " + stage,
                    outputRoot.getFileSystem(conf).exists(new Path(outputRoot, stage)));
        }
    }

    @Test
    public void partitionedTopNMatchesTheSingleReducerResult() throws Exception {
        Path wordCount = out("01-wordcount");
        assertOk(new NormalizedWordCountJob(), corpus.toString(), wordCount.toString());

        Path heapOutput = out("02-topn-heap");
        StopWordTopNJob heapJob = new StopWordTopNJob();
        heapJob.setConf(conf);
        assertEquals(ExitCodes.OK, ToolRunner.run(conf, heapJob,
                new String[] {wordCount.toString(), heapOutput.toString()}));

        Configuration partitioned = new Configuration(conf);
        partitioned.set(ConfKeys.TOPN_STRATEGY, "partitioned");
        partitioned.set(ConfKeys.REDUCERS, "2");
        Path partitionedOutput = out("02-topn-partitioned");
        StopWordTopNJob job = new StopWordTopNJob();
        job.setConf(partitioned);
        assertEquals(ExitCodes.OK, ToolRunner.run(partitioned, job,
                new String[] {wordCount.toString(), partitionedOutput.toString()}));

        assertEquals("both top-N strategies must agree",
                readText(heapOutput), readText(partitionedOutput));
    }

    private void assertOk(Tool tool, String... args) throws Exception {
        if (tool instanceof org.apache.hadoop.conf.Configured) {
            ((org.apache.hadoop.conf.Configured) tool).setConf(conf);
        }
        int code = ToolRunner.run(conf, tool, args);
        assertEquals("stage " + tool.getClass().getSimpleName() + " failed", ExitCodes.OK, code);
    }

    private Path out(String name) {
        return new Path(root + "out/" + name);
    }

    private Path writeText(String name, String content) throws IOException {
        File file = folder.newFile(name);
        Files.write(file.toPath(), content.getBytes(StandardCharsets.UTF_8));
        return new Path(file.toURI());
    }

    private void write(Path directory, String name, String content) throws IOException {
        FileSystem fs = directory.getFileSystem(new Configuration());
        Path target = new Path(directory, name);
        try (java.io.OutputStream out = fs.create(target, true)) {
            out.write(content.getBytes(StandardCharsets.UTF_8));
        }
    }

    private String readText(Path directory) throws IOException {
        FileSystem fs = directory.getFileSystem(new Configuration());
        StringBuilder sb = new StringBuilder();
        for (org.apache.hadoop.fs.FileStatus status : fs.listStatus(directory)) {
            if (!status.getPath().getName().startsWith("part-")) {
                continue;
            }
            try (java.util.Scanner scanner = new java.util.Scanner(
                    fs.open(status.getPath()), StandardCharsets.UTF_8.name())) {
                while (scanner.hasNextLine()) {
                    sb.append(scanner.nextLine()).append('\n');
                }
            }
        }
        return sb.toString();
    }
}
