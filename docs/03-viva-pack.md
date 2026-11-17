# 03 — Viva Pack

Two artefacts: a 10-slide presentation outline and 15 likely questions with answers you
can defend. Answers are short on purpose — an examiner wants the reason, not a lecture.

---

## Part A — 10-slide presentation outline

| # | Slide | Content (one line per bullet) | Visual |
|---|-------|-------------------------------|--------|
| 1 | Title | Project name, your name, course, supervisor, date | — |
| 2 | Problem | Text corpora outgrow one machine's memory and one core's time; naïve Python hits a wall | one chart: runtime vs size, reference implementation |
| 3 | Objectives | 9 mandatory analytics stages, 3 deployment modes, reference-verified correctness, measured performance | requirement checklist |
| 4 | Architecture | One jar, three modes; job DAG; SequenceFile between stages, text at the edges | the DAG from `docs/01-architecture.md` §6 |
| 5 | MapReduce engineering | Custom `WritableComparable`, `TermPartitioner` + grouping comparator, combiners, in-mapper combining, counters | the secondary-sort join diagram |
| 6 | Correctness strategy | Normative tokenisation contract mirrored in Python; 11 stages diffed; 506 215 pairs matched | verifier output screenshot (`PASS` lines) |
| 7 | Testing | 97 tests, 85.2 % line coverage, LocalJobRunner integration tests, edge cases | JaCoCo summary |
| 8 | Results | runtime vs size; runtime vs reducers; combiner/shuffle reduction; pairs vs stripes; small files | 3 plots from `results/plots` |
| 9 | Discussion | Where Hadoop loses (small data, JVM startup, no network to amortise), Zipfian skew, why not Spark | crossover table |
| 10 | Conclusions & future work | What was proven, what was not, Spark port / HBase index / streaming | — |

Speaking tips: put the verifier output on screen (it is the strongest slide), quote wall
times **with the measurement conditions**, and never claim a speedup that a plot does not
show.

---

## Part B — 15 likely questions with defensible answers

**1. Why is a combiner safe in your word count?**
Integer addition is associative and commutative, so aggregating `(w,1)` partially cannot
change the final sum. I also guard against `int` overflow in the combiner and accumulate
in `long` in the reducer — a wrong count is worse than a failed task. It would *not* be
safe for mean, median, or top-N: a mean needs (sum,count) as a pair, and a bounded heap
is not associative at all, which is why J2 uses no combiner.

**2. Walk me through the shuffle.**
Map writes go to a circular buffer (`mapreduce.task.io.sort.mb`), spill to disk when full
after sorting by partition then key; spills are merged into one file per output partition
(`mapreduce.task.io.sort.factor`). Reducers fetch their partitions over HTTP from the
`ShuffleHandler`, merge them (again on disk), and group by key using the grouping
comparator before calling `reduce`. Compression applies to the map output, so it reduces
network and disk, not CPU-free.

**3. What happens if a reducer dies mid-job?**
The AM sees the task attempt fail, releases its container, and reschedules the attempt on
another node; the reducer re-fetches its map outputs from the still-running map tasks.
If the *node* dies, its completed map outputs are gone, so the AM marks those map tasks
as failed and re-executes them. The job's output is committed only when all reducers
succeed, so a partially written output is never visible.

**4. Your top-N uses one reducer. Isn't that a bottleneck?**
It is O(distinct terms) work with O(N) memory and one shuffle — and the head of a Zipf
distribution is a handful of terms, so a total-order sort would move far more data to
throw nearly all of it away. I implemented the alternative too
(`ta.topn.strategy=partitioned`: R reducers keep their partition's top-N, then a merge job
combines R×N records) and the integration test asserts both produce identical output; the
benchmark reports the cost difference.

**5. Why does J7c need a custom partitioner *and* a grouping comparator?**
The partitioner decides which reducer sees a term, the grouping comparator decides how
many `reduce()` calls that reducer makes. Partitioning on the whole `(term,doc)` key would
scatter a term's DF record from its TF records, so no reducer would ever see a complete
join group; grouping on the whole key would give one call per pair. Both must be
consistent, and the sort comparator additionally puts the sentinel document first so the
reducer reads IDF before any TF.

**6. How do you know the reducer gets the right document id in the join?**
Because I got it wrong first: with a grouping comparator the reduce key is the group's
*first* key, i.e. the sentinel `(term,"*")` record, so the per-record document id is not
recoverable from the key. `TaggedCountWritable` therefore carries the document id in the
value. `TfIdfJoinReducerTest` pins this, and the 92 078 verified TF-IDF weights confirm it.

**7. Why not Spark?**
Spark would likely be faster here and would express TF-IDF in a few lines. This project's
objective is MapReduce engineering: explicit partitioners, grouping comparators, combiners
with proofs, and the shuffle economics behind them — all of which Spark hides. For
production text analytics I would choose Spark; §12 of the architecture document says so
and sketches the port.

**8. How does data locality work, and does your pipeline exploit it?**
The AM schedules a map task on a node holding a replica of its split (node-local, then
rack-local, then any). Line-oriented jobs read 128 MB blocks, so locality holds. The
whole-file jobs deliberately break it: one file per record means one mapper per document
and no splitting, which is the right trade for document-scoped analysis but costs
locality on many-small-file corpora — which is why the small-files ablation exists.

**9. What is the small-files problem and what did you measure?**
One mapper per file means per-task JVM and bookkeeping overhead dominating the actual
read. I generated a 10 000-file corpus and ran the same word count with plain
`TextInputFormat` (one split per file) versus `CombineTextInputFormat` (splits packed to
`ta.smallfile.threshold`); `results/plots/small-files.png` shows the measured difference.
The job chooses the format automatically from the average file size.

**10. Prove your output is deterministic.**
Sorted keys, `Locale.ROOT` formatting, fixed `\n` record separators, one reducer for the
percentage-free histograms, and `Text`-based (UTF-8 byte) comparison everywhere — including
a deliberate rejection of `String.compareTo`, which differs from code-point order for
astral characters. `scripts/09_determinism.sh` runs the pipeline twice and compares
`sha256` per stage.

**11. Why mask numbers as `<num>` instead of dropping them?**
Dropping deletes signal (years, quantities) and keeps document-frequency shape wrong;
keeping pollutes the top-N with thousands of distinct numeric ids. Masking compresses the
numeric vocabulary to one token, keeps the counts sane, and is visible in the output — you
can see `<num>` at rank 1 in the 10 MB run, which is the honest result for that corpus.

**12. What is Zipf's law doing to your reducers?**
Word frequencies follow roughly 1/rank, so a hash partitioner sends the head terms to a few
reducers and those straggle while the rest idle. Mitigations: a combiner (cuts the volume
before the shuffle), more reducers (finer split of the *vocabulary*, not of the hot key),
and salting (`SaltedTextPartitioner`) which spreads a hot key across a pool of reducers —
legitimate only for commutative aggregation, and only with a second merge pass, because a
salted run's counts are partial.

**13. Why SequenceFile between stages but text at the edges?**
SequenceFile is typed, splittable and dependency-free, so a downstream mapper needs no
parsing; text is what a human and the Python verifier can read. Mixing the two silently
breaks chaining — a stage that writes text cannot be read by `SequenceFileInputFormat` —
and that exact bug happened during development, which is why `ta.output.format` is
intentionally unset in `defaults.properties` and each stage declares its own default.

**14. Where does Hadoop actually lose, and where would it win?**
It loses on small inputs: JVM start, split planning, shuffle setup and the commit protocol
are fixed costs of tens of seconds, while the Python reference finishes 10 MB in a few
seconds. It wins when a single machine can no longer hold the data or when the work is
CPU-bound across many cores — the crossover is a property of the cluster, so I report the
measured local-mode curve and label the cluster point as not yet measured rather than
inventing a number.

**15. What would you do differently with more time?**
Three things, in order: run the full size/reducer study on the 4-container cluster so the
speedup numbers are real; implement cosine similarity on the TF-IDF vectors (optional in
the brief, documented as future work); and replace the inverted index's in-reducer posting
list with an HBase-backed index so the head terms stop being memory-bound.
