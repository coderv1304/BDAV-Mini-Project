# 02 — Academic Report Outline (IEEE conference style)

Structure, section content, and what to insert where. Numbers in `⟨angle brackets⟩` are
deliberately empty: fill them from `results/csv/bench-summary.csv` after a real run, never
from memory. Everything not in brackets is already true of this implementation and can be
stated as fact.

Length target: 8–10 pages in IEEE two-column (`\documentclass[conference]{IEEEtran}`).

---

## Abstract (150–200 words)

Template:

> MapReduce remains the reference model for batch text analytics on commodity clusters,
> yet most student implementations stop at word count. This paper presents a
> ⟨n⟩-stage text-analytics pipeline on Hadoop 3.3.⟨x⟩ covering normalised word counting,
> stop-word filtering, global top-N, length and character distributions, n-gram
> frequencies, a positional inverted index, TF-IDF via a secondary-sort join, and a
> co-occurrence matrix implemented twice (pairs and stripes). Correctness is established
> against a single-machine Python oracle rather than assumed: ⟨11⟩ stages are compared,
> including ⟨506 215⟩ co-occurrence pairs and ⟨92 078⟩ TF-IDF weights, with all
> non-floating-point fields byte-identical. We report measured runtimes and shuffle
> volumes across four corpus sizes, ⟨1–4⟩ reducers and five configuration ablations,
> quantify the shuffle reduction from combining at ⟨x⟩ %, and show that the pairs and
> stripes patterns differ by ⟨y⟩ in shuffle bytes and ⟨z⟩ % in runtime. We conclude with
> the crossover point at which distribution begins to pay and a discussion of Zipfian
> skew.

## 1. Introduction

1. Motivation: corpora exceed single-machine memory; MapReduce's cost model (sequential
   scan, shuffle, sort) matches counting and indexing workloads.
2. Gap: textbook examples stop at word count; the interesting engineering is the join,
   the partitioner/grouping contract, and the shuffle economics.
3. Contributions (list them as bullets):
   * a complete ⟨9⟩-stage pipeline, all against the `mapreduce` API;
   * a normative tokenisation contract, mirrored in a Python oracle, giving **diff = 0**
     verification;
   * a secondary-sort TF-IDF join with a sentinel-key ordering and its correctness
     argument;
   * a pairs-vs-stripes comparison with measured shuffle volumes;
   * a reproducibility harness (one `make`) and an honest small-data baseline.
4. Paper organisation.

## 2. Related Work

Cite only work you are certain exists (M. Zaharia et al, "Resilient Distributed
Datasets"; J. Dean & S. Ghemawat, "MapReduce: Simplified Data Processing on Large
Clusters", OSDI 2004 / CACM 2008; J. Lin & C. Dyer, *Data-Intensive Text Processing with
MapReduce*, Morgan & Claypool, 2010).

Points to make:
* Dean & Ghemawat define the model and the combiner/partitioning extension this paper
  relies on.
* Lin & Dyer is the canonical treatment of the pairs vs stripes patterns and of
  secondary sorting for joins — this project implements both patterns and measures them.
* Spark/RDD work motivates the discussion in §8 (in-memory lineage vs disk-based shuffle);
  cite it as the alternative, not as a strawman.
* Any additional citation must be verified before submission: **do not paste references
  from memory or from this outline without checking the venue, year and page numbers.**

## 3. System Design

1. Architecture figure: one jar, three deployment modes (`docs/01-architecture.md` §3).
2. Pipeline DAG (§6 of the architecture document) with SequenceFile edges.
3. Key/value contract table for every stage (the table in §7 — reproduce the important
   rows: J1, J6, J7c, J8p, J8s).
4. Design decisions with one-line justifications (§8 of the architecture document):
   combiner safety, single-reducer bounded heap vs partitioned top-N, sentinel join,
   term-only partitioner, sequence-vs-text formats, determinism rules.
5. Tokenisation contract (§5) — this belongs in the paper because the diff = 0 claim
   depends on it; include the numbered rule list and the astral-character ordering note.

## 4. Implementation

1. Cluster/standalone configuration (Hadoop version, Java, container layout).
2. Custom types: `TextPair`, `TermDocWritable`, `PostingWritable`, `StripeWritable`,
   `TaggedCountWritable` — what each encodes and why the Writable contract matters.
3. Partitioner/grouping/sort triple for the join, with the record layout figure.
4. Combiners: safety argument per job, and the two jobs where a combiner is *not* used.
5. In-mapper combining with its memory bound (`ta.inmapper.max.entries`).
6. Counters as instrumentation *and* self-checks (`TOKENS_EMITTED` = Σ J1 counts).
7. Object reuse: one `Text`, one `IntWritable`, one reusable byte buffer; token encoding
   without intermediate `String`.
8. Chaining: sequential driver for dependent stages, `JobControl` for the two independent
   co-occurrence jobs; idempotency via explicit overwrite.

## 5. Experimental Setup

1. Hardware: ⟨CPU model, cores, RAM, disk⟩; Docker Desktop ⟨version⟩; container limits.
2. Software: Hadoop 3.3.6, Java 11 (Temurin), Maven 3.9.
3. Corpora: synthetic Zipfian text (seed 42, vocab 5 000, exponent 1.07) at 10 MB /
   100 MB / 500 MB / 1 GB, plus a 10 000-file variant. State clearly that synthetic
   corpora control the distribution and avoid licensing questions; note how to substitute
   Gutenberg or a Wikipedia subset.
4. Metrics and where they come from: wall clock per stage, and Hadoop counters
   (`MAP_OUTPUT_BYTES`, `REDUCE_SHUFFLE_BYTES`, `MAP_OUTPUT_RECORDS`,
   `COMBINE_INPUT_RECORDS`, `GC_TIME_MILLIS`) exported to `_counters.tsv` by the driver.
5. Statistics: ⟨repeats per configuration⟩; report median and spread, not a single run.
6. Baseline: the Python reference on the same host, timed with `time.perf_counter`.

## 6. Results

Figures to reproduce (all generated by `python/bench_plot.py`):

* Fig. 1 runtime vs input size, Hadoop vs Python baseline (`runtime-vs-size.png`).
* Fig. 2 runtime and measured speedup vs reducer count (`runtime-vs-reducers.png`).
* Fig. 3 combiner ablation: shuffle bytes for none / combiner / in-mapper / both
  (`shuffle-ablations.png`).
* Fig. 4 pairs vs stripes: shuffle bytes, map output records, runtime
  (`pairs-vs-stripes.png`).
* Fig. 5 small-files problem: `TextInputFormat` vs `CombineTextInputFormat`
  (`small-files.png`).
* Fig. 6 compression ablation (`compression.png`).
* Table I: per-configuration wall time, MB/s, shuffle bytes, GC time (`bench-summary.csv`).
* Table II: verification results per stage (counts compared, max relative delta).

Report speedup `S(p) = T(1)/T(p)` and efficiency `E(p) = S(p)/p`. State plainly whether
`E` falls with `p` and attribute it (fixed per-job cost at small sizes; shuffle contention;
Zipfian skew).

## 7. Discussion

1. **Skew.** Zipf's law means the head terms dominate; explain why the combiner helps most
   exactly there, why more reducers do not split a single hot key, and what salting costs
   (a second pass) — cite the measured `NEIGHBOURS_PER_WORD` / shuffle numbers.
2. **Crossover.** Where the fixed cost of a job (JVM start, planning, shuffle setup,
   commit) stops dominating. Be explicit that local-mode numbers understate cluster
   speedup and that the cluster point is ⟨measured / to be measured⟩.
3. **Pairs vs stripes.** Stripes shrink the shuffle but concentrate memory in one record
   per word; pairs keep records small but multiply their number. Quote both numbers.
4. **Threats to validity.** Synthetic corpus ⇒ no linguistic noise; single host ⇒ no
   network contention; speculative execution disabled ⇒ no straggler mitigation; the
   Python baseline is single-threaded.

## 8. Limitations and Future Work

Summarise `docs/01-architecture.md` §12: Spark port (RDD/DataFrame equivalents of each
stage), HBase-backed inverted index for head terms, streaming ingestion (Kafka + Spark
Structured Streaming or Flink) for continually arriving text, cosine similarity on TF-IDF
vectors, and phrase/proximity queries on the positional index.

## 9. Conclusion

Answer the question the abstract posed: which stages are MapReduce-shaped, what the
measured cost of each engineering decision was, and what the verification actually
establishes (11 stages, byte-exact except floats — a claim you can defend line by line).

## References

Only verified entries. Starting set (check venue/year before submission):

1. J. Dean and S. Ghemawat, "MapReduce: Simplified Data Processing on Large Clusters,"
   *Communications of the ACM*, vol. 51, no. 1, pp. 107–113, 2008.
2. J. Lin and C. Dyer, *Data-Intensive Text Processing with MapReduce*. Morgan & Claypool
   Publishers, 2010.
3. M. Zaharia et al., "Resilient Distributed Datasets: A Fault-Tolerant Abstraction for
   In-Memory Cluster Computing," NSDI 2012.
4. Apache Hadoop 3.3.6 documentation (MapReduce tutorial, `DistributedCache`,
   `TotalOrderPartitioner`).
5. G. K. Zipf, *Human Behavior and the Principle of Least Effort*, 1949 (for the
   frequency-rank law).
6. AFINN (F. Årup Nielsen) if you swap in a real sentiment lexicon — verify the version.

## Appendix A — Reproduction commands

```bash
make data && make build && make run && make verify     # correctness
make bench                                             # measurements + plots
make cluster-up && make cluster-run DATASET=100m       # cluster numbers
```

## Appendix B — Verification transcript

Paste the actual `verify_outputs.py` output (the 11 `PASS` lines). It is the single most
persuasive page in the report.
