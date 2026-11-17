# Benchmark report

Every number below was produced by running the code in this repository, and every one of
them can be traced to a row in `results/csv/` or to a log in `results/logs/`. Where a
measurement does not exist, the section says so instead of offering a plausible figure.

---

## 0. What is measured, and on what

| Item | Value |
|---|---|
| Host | Windows workstation, Docker Desktop; JVM and Hadoop run in containers |
| Java | Temurin 11 (inside `maven:3.9-eclipse-temurin-11` and `bdav/hadoop:3.3.6`) |
| Hadoop | 3.3.6 |
| Corpora | `10m` = 10 004 228 B in 20 files; `100m` = 100 048 656 B in 200 files; `tiny10k` = 14 443 296 B in 10 000 files |
| Reducers | 2 unless a section says otherwise |
| Repetitions | one per configuration, declared everywhere it matters |

One run per configuration shows that a stage works and roughly what it costs. It is not
enough for confidence intervals, and no section pretends otherwise.

Raw records: `results/csv/bench-raw.csv` (one row per run), `results/csv/bench-summary.csv`
(timings and counted bytes), `results/csv/bench-metrics.csv` (every Hadoop counter, per
experiment), `results/csv/baseline-python.csv` (the Python reference).

---

## 1. Local-mode cost and reducer scaling

`LocalJobRunner`, one JVM running every task in sequence, word count
(`bench-summary.csv`, experiment `size-wc`):

| Corpus | Wall time | Throughput | Map output bytes | Shuffle bytes |
|---|---|---|---|---|
| 10 MB | 15 931 ms | 0.628 MB/s | 16 997 534 | 76 322 |
| 100 MB | 66 620 ms | 1.502 MB/s | 170 041 230 | 228 966 |

Reducer scaling (`reducers-1/2/4`), wall time in ms:

| Corpus | 1 reducer | 2 reducers | 4 reducers |
|---|---|---|---|
| 10 MB | 15 314 | 14 831 | 15 848 |
| 100 MB | 66 920 | 64 055 | 65 106 |

Rendered as speedup vs one reducer this looks like 1.03× at two reducers — which is noise,
not scaling. `LocalJobRunner` runs tasks one at a time, so a second reducer cannot help;
the recorded `reducer_efficiency` column (0.52 at two reducers, 0.24–0.26 at four) makes
that explicit. These numbers bound the pipeline's cost. They say nothing about a cluster.

## 2. Combiner and in-mapper combining

10 MB, word count, single run each (`abl-nocombiner`, `abl-combiner`, `abl-inmapper`,
`abl-both`):

| Configuration | Wall time | Map output bytes | Reduce shuffle bytes | Map output records |
|---|---|---|---|---|
| no combiner | 16 883 ms | 16 997 534 | 20 589 542 | 1 795 998 |
| combiner | 14 744 ms | 16 997 534 | 76 322 | 1 795 998 |
| in-mapper | 12 706 ms | 66 278 | 76 322 | 5 016 |
| both | 12 653 ms | 66 278 | 76 322 | 5 016 |

The combiner alone cuts shuffle bytes from 20 589 542 to 76 322 — a 99.6 % reduction —
and in-mapper combining cuts them one stage earlier, at the map output. In local mode the
time saving is modest (16.9 s → 12.7 s) because there is no network shuffle to avoid; the
byte counts are what would matter on a cluster.

## 3. Compression ablation

10 MB, single run each (`abl-codec-*`):

| Codec | Wall time | Reduce shuffle bytes | Verdict |
|---|---|---|---|
| none | 14 678 ms | 76 322 | baseline |
| bzip2 | 14 710 ms | 22 423 | real compressor (−71 %) |
| snappy | 14 679 ms | 42 069 | real compressor (−45 %) |
| gzip | 14 700 ms | 76 322 | fell back to uncompressed |
| lz4 | 14 637 ms | 76 322 | fell back to uncompressed |

Times are indistinguishable; the shuffle column is the finding. On this JVM only bzip2 and
snappy have working implementations, so the ablation measures *which codec is real here*,
not the algorithms. §13.7 of `docs/01-architecture.md` documents the probe that decides
this and the defect that made gzip and lz4 die inside `IFile` before it was fixed.

## 4. Small files

10 000 files of ~1.4 KB (14 443 296 B), tiny10k corpus (`abl-smallfiles-*`):

| Input format | Wall time | Status | Map input records |
|---|---|---|---|
| `TextInputFormat` (one split per file) | 619 950 ms | **failed** | — |
| `CombineTextInputFormat` | 1 048 994 ms | ok | 67 334 |

The failed run is reported as a failure, not as a timing: 10 000 map tasks overwhelmed the
local runner and it was killed. The completed run is the number to quote — at this file
size per-task overhead dominates, and combining makes the job merely slow rather than
impossible (0.014 MB/s).

## 5. Co-occurrence and inverted index: pairs vs stripes vs whole-file

10 MB, 2 reducers, single run each (`pattern-pairs`, `pattern-stripes`, `pattern-inverted`):

| Design | Wall time | Shuffle bytes | Map output records | Output records |
|---|---|---|---|---|
| pairs (J8p) | 51 476 ms | 68 712 576 | 8 979 690 | 506 215 |
| stripes (J8s) | 20 608 ms | 28 624 443 | 92 078 | 5 016 |
| inverted index (J6, whole-file) | 15 663 ms | 10 247 307 | 92 078 | 5 016 |

This is the classic design comparison and the numbers behave as the textbook predicts:
pairs emits one record per co-occurring pair (8 979 690 records, 68.7 MB shuffled) while
stripes emits one record per document-word carrying a map of neighbours
(92 078 records, 28.6 MB, `NEIGHBOURS_PER_WORD` 9 485 905), so stripes is **2.5× faster**
and shuffles **2.4× less**. `WholeFileInputFormat` is faster still here (1.3× over stripes)
because it never splits a document, which is what makes positional postings possible — but
it also serialises one document per mapper and cannot scale past the number of documents.

## 6. The Python reference as a baseline

The reference implementation is a single-process, stdlib-only Python program; it exists to
define correctness, but its runtime is a real measurement of the same tokenisation work
(`baseline-python.csv`):

| Corpus | Wall time | Tokens | Distinct terms | Tokens/s |
|---|---|---|---|---|
| 10 MB | 7 295.9 ms | 1 795 998 | 5 016 | 246 166 |
| 100 MB | 82 520.8 ms | 17 974 370 | 5 016 | 217 816 |

At 10 MB the single Python process beats a whole local MapReduce job (7.3 s vs 15.9 s),
and at 100 MB it is *slower* (82.5 s vs 66.6 s). That is an honest and expected result:
MapReduce's fixed per-job cost dominates small inputs, and its throughput advantage only
starts to show as the input grows.

## 7. Correctness against the reference

`python/verify_outputs.py` re-implements every normalisation rule and aggregation and is
compared with the Java output stage by stage. Re-run for this report on the 10 MB corpus
(output in `out/10m`, re-verified after the report was rewritten):

| Stage | Keys compared | Result |
|---|---|---|
| 01-wordcount | 5 016 | identical |
| 02-topn | 100 | identical |
| 03-word-length | 14 | identical |
| 04-char-freq | 30 | identical |
| 05a-bigrams | 135 940 | identical |
| 05b-trigrams | 121 478 | identical |
| 06-inverted-index | 5 016 | identical |
| 07c-tfidf | 92 078 | match, max relative delta 4.97e-07 |
| 08a-pairs | 506 215 | identical |
| 08b-stripes | 5 016 | identical |
| 09-sentiment | 80 | match, max relative delta 4.89e-07 |

`RESULT: all stages match the reference (diff = 0)`, exit code 0. The two float stages are
compared with a tolerance because TF-IDF and sentiment scores are floats; 5e-07 is double
rounding, not a semantic difference.

The 100 MB corpus was verified the same way when it was benchmarked (also diff = 0), but
its output directory has since been removed, so that row is not re-verified here — only the
10 MB result above is backed by artifacts currently on disk.

SequenceFile stages are rendered with `Main dump` before comparison, because every
SequenceFile file carries a random 16-byte sync marker (§13.3 of the architecture
document): the *contents* are deterministic, the bytes are not.

## 8. Figures and raw data

Seven figures are regenerated from the CSVs by `make plots`:

| Figure | Shows |
|---|---|
| `results/plots/runtime-vs-size.png` | word-count wall time, 10 MB vs 100 MB (§1) |
| `results/plots/runtime-vs-reducers.png` | reducer scaling, the no-scaling result of §1 |
| `results/plots/combiner-ablations.png` | the four combiner configurations of §2 |
| `results/plots/compression.png` | the five codecs of §3 |
| `results/plots/shuffle-ablations.png` | shuffle bytes per codec (§3) |
| `results/plots/small-files.png` | the 10 000-file ablation of §4 |
| `results/plots/pairs-vs-stripes.png` | co-occurrence designs of §5 |

`results/plots/summary.txt` is the text rendering of the same numbers. Every figure is drawn
from `results/csv/`, so a number in this report can always be traced back to a row.

## 9. Build, tests, coverage

`mvn -B -Pcoverage-gate verify` (gate: 80 % of lines):

| Check | Result |
|---|---|
| Compile + package | BUILD SUCCESS, `text-analytics.jar` 200 243 bytes |
| Unit tests | 89, 0 failures (`target/surefire-reports`) |
| Integration tests | 8, 0 failures (`target/failsafe-reports`) |
| Line coverage | **85.2 % — 1 835 of 2 153 lines** (`target/site/jacoco/jacoco.csv`) |

Lowest-covered classes (defensive error paths, worth knowing before a viva):
`SaltedTextPartitioner` 48.4 %, `JobBootstrap` 50.0 %, `SentimentLexicon` 59.6 %,
`StopWordDictionary` 65.5 %, `PipelineDriver` 71.8 %.

## 10. Cluster measurements — partially verified, no speedup claimed

The optional cluster (`docker/docker-compose.yml`: 1 NameNode + ResourceManager, 3
DataNode + NodeManager workers) was built and validated on the authoring host, then
stopped short of a full pipeline run. What exists:

| Check | Outcome |
|---|---|
| `docker compose build` | success (`results/logs/cluster-build.log`) |
| `hdfs dfsadmin -report` | 3 live DataNodes |
| `yarn node -list` | 3 RUNNING nodes |
| 10 MB corpus on HDFS | 20 files at replication 2 |
| `wordcount` on YARN | **1 m 23 s**, counters identical to local mode |
| Pipeline stages on YARN | J1 44 s, J2 58 s, J3 43 s, J4 42 s, J5(n=2) 41 s, J5(n=3) 44 s |
| Full 13-stage pipeline on YARN | **not run to completion** |

Why it stopped: the Docker VM had 3.5 GB of RAM for four containers plus task JVMs. The
DataNodes stopped answering RPC (`SocketTimeoutException: 60000 millis … waiting for
channel to be ready for read`), YARN killed tasks as timed out (exit 137/143), the Docker
engine started returning HTTP 500, and the rebuild failed with "There is not enough space
on the disk" — the host had ~3 GB free while Docker's disk image had grown to 59 GB.

**The YARN and container sizing in `conf/` and `docker/` is therefore unverified.** The
values (640 MB and 1 vcore per worker, 256 MB map containers, a 900 s task timeout, pinned
daemon heaps) were chosen to fit a 3.5 GB VM after two larger configurations failed, but
they were never exercised by a completed run, because the engine died before the first
container start. Treat them as a documented starting point, not a validated configuration.

Two rules for anyone filling Table 3 of the academic report with cluster numbers:

* Record the **node count** next to every time. A speedup without its node count is
  meaningless, and three containers on one laptop measure the laptop, not Hadoop.
* Do not extrapolate from the stage times above. They show fixed cost per stage
  (allocation + JVM start, ~40–60 s at 10 MB), not throughput.

Reproduce with:

```bash
make cluster-up
docker compose -f docker/docker-compose.yml ps          # expect 4 containers Up
bash scripts/04_upload_to_hdfs.sh                        # corpora into HDFS
make cluster-run DATASET=10m                             # pipeline on YARN
make fault-tolerance                                     # kill datanode2 mid-job
```
