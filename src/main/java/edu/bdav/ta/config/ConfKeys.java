package edu.bdav.ta.config;

/**
 * Single registry of every configuration key the platform reads.
 *
 * <p>Rationale (viva): a driver, a mapper and a benchmark script must agree on
 * key spelling; scattering string literals is how "-Dtopn.n" vs "-Dta.topn.n"
 * bugs survive. Every project key carries the {@code ta.} prefix, so a job's
 * full configuration is greppable in one pass over the submission log.</p>
 */
public final class ConfKeys {

    private ConfKeys() {
    }

    /** Namespace for all project keys. */
    public static final String PREFIX = "ta.";

    // ---- inputs -----------------------------------------------------------
    public static final String STOPWORDS_PATH = PREFIX + "stopwords.path";
    public static final String LEXICON_PATH = PREFIX + "lexicon.path";

    // ---- analysis parameters ---------------------------------------------
    public static final String TOPN_N = PREFIX + "topn.n";
    public static final String TOPN_STRATEGY = PREFIX + "topn.strategy";
    public static final String NGRAM_N = PREFIX + "ngram.n";
    public static final String NGRAM_MIN_FREQ = PREFIX + "ngram.minfreq";
    public static final String NGRAM_PADDING = PREFIX + "ngram.padding";
    public static final String MIN_TOKEN_LENGTH = PREFIX + "min.token.length";
    public static final String NUMBER_MODE = PREFIX + "number.mode";
    public static final String COOCCUR_WINDOW = PREFIX + "cooccur.window";
    public static final String COOCCUR_ORDERED = PREFIX + "cooccur.ordered";
    public static final String COOCCUR_MIN_COUNT = PREFIX + "cooccur.mincount";
    public static final String COOCCUR_TOP_K = PREFIX + "cooccur.topk";
    public static final String TF_MODE = PREFIX + "tf.mode";
    public static final String CORPUS_DOCS = PREFIX + "corpus.ndocs";
    public static final String SIM_MIN_SHARED = PREFIX + "sim.min.shared";
    public static final String SIM_TOP_TERMS = PREFIX + "sim.top.terms";

    // ---- engineering knobs (the ablation switches) ------------------------
    public static final String COMBINE_MODE = PREFIX + "combine.mode";
    public static final String CODEC = PREFIX + "codec";
    public static final String OUTPUT_FORMAT = PREFIX + "output.format";
    public static final String OVERWRITE = PREFIX + "overwrite";
    public static final String REDUCERS = PREFIX + "reducers";
    public static final String SMALLFILE_THRESHOLD = PREFIX + "smallfile.threshold";
    /** Input root used to derive document ids as root-relative paths. */
    public static final String INPUT_ROOT = PREFIX + "input.root";
    public static final String PARTITIONER = PREFIX + "partitioner";
    public static final String NUMERIC_FORMAT = PREFIX + "numeric.format";

    /**
     * Cached outcome of probing the requested codec on this node: the compressor
     * class name when it is usable, or the empty string when it is not. Written by
     * {@code JobBootstrap.resolveUsableCodec} and read by the output-format setup, so
     * the shuffle and the SequenceFile writers can never disagree about whether
     * compression is on.
     */
    public static final String CODEC_EFFECTIVE = PREFIX + "codec.effective";

    // ---- Hadoop keys referenced by name (kept here so they are greppable) --
    public static final String MAP_OUTPUT_COMPRESS = "mapreduce.map.output.compress";
    public static final String MAP_OUTPUT_CODEC = "mapreduce.map.output.compress.codec";
    public static final String JOB_REDUCES = "mapreduce.job.reduces";
    public static final String TEXT_OUTPUT_SEPARATOR = "mapreduce.output.textoutputformat.separator";
    public static final String IO_SORT_MB = "mapreduce.task.io.sort.mb";
}
