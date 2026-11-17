package edu.bdav.ta;

import java.util.Arrays;
import edu.bdav.ta.cli.ExitCodes;
import edu.bdav.ta.cli.JobBootstrap;
import edu.bdav.ta.cli.SequenceDump;
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
 * Single entry point: {@code java -cp ... edu.bdav.ta.Main <command> [args]}.
 *
 * <p>Having one dispatcher means the jar advertises one main class while every stage
 * remains individually runnable - useful for debugging a single stage and for the
 * benchmark harness, which runs stages in isolation to attribute cost.</p>
 *
 * <p>All arguments after the command are passed through unchanged, including
 * {@code -D} options, which {@code GenericOptionsParser} handles inside each Tool.</p>
 */
public final class Main {

    private static final String USAGE = String.join(System.lineSeparator(),
            "usage: Main <command> [options] [paths]",
            "  pipeline    whole pipeline:  <corpus> <output-root>",
            "  wordcount   J1 normalised word count",
            "  topn        J2 stop-word filter + global top-N",
            "  length      J3 word-length distribution",
            "  charfreq    J4 character frequency",
            "  ngram       J5 n-gram frequency (-D ta.ngram.n=2|3)",
            "  inverted    J6 inverted index",
            "  tf          J7a term frequency",
            "  df          J7b document frequency",
            "  tfidf       J7c TF-IDF join: <tf-seq> <df-seq> <output>",
            "  pairs       J8p co-occurrence (pairs)",
            "  stripes     J8s co-occurrence (stripes)",
            "  sentiment   J9 lexicon sentiment (-D ta.lexicon.path=...)",
            "  dump        render a SequenceFile stage output as text",
            "",
            "Every command accepts -D key=value options before its positional paths.");

    private Main() {
    }

    /** @param args command name followed by that command's arguments */
    public static void main(String[] args) {
        if (args == null || args.length == 0) {
            System.err.println(USAGE);
            System.exit(ExitCodes.USAGE);
            return;
        }
        String command = args[0];
        String[] rest = Arrays.copyOfRange(args, 1, args.length);
        switch (command) {
            case "pipeline":
                JobBootstrap.launch(new PipelineDriver(), rest);
                break;
            case "wordcount":
                JobBootstrap.launch(new NormalizedWordCountJob(), rest);
                break;
            case "topn":
                JobBootstrap.launch(new StopWordTopNJob(), rest);
                break;
            case "length":
                JobBootstrap.launch(new WordLengthJob(), rest);
                break;
            case "charfreq":
                JobBootstrap.launch(new CharacterFrequencyJob(), rest);
                break;
            case "ngram":
                JobBootstrap.launch(new NGramJob(), rest);
                break;
            case "inverted":
                JobBootstrap.launch(new InvertedIndexJob(), rest);
                break;
            case "tf":
                JobBootstrap.launch(new TfJob(), rest);
                break;
            case "df":
                JobBootstrap.launch(new DfJob(), rest);
                break;
            case "tfidf":
                JobBootstrap.launch(new TfIdfJoinJob(), rest);
                break;
            case "pairs":
                JobBootstrap.launch(new PairsCooccurrenceJob(), rest);
                break;
            case "stripes":
                JobBootstrap.launch(new StripesCooccurrenceJob(), rest);
                break;
            case "sentiment":
                JobBootstrap.launch(new SentimentJob(), rest);
                break;
            case "dump":
                JobBootstrap.launch(new SequenceDump(), rest);
                break;
            default:
                System.err.println("unknown command: " + command);
                System.err.println(USAGE);
                System.exit(ExitCodes.USAGE);
        }
    }
}
