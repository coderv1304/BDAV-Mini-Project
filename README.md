# Scalable Text Analytics Platform on Hadoop MapReduce

A multi-job text-analytics pipeline over large text corpora, built strictly against the
`org.apache.hadoop.mapreduce` API, with a single-machine Python oracle used to *prove*
the distributed results are correct.

| | |
|---|---|
| **Stack** | Hadoop 3.3.6, Java 11, Maven, `org.apache.hadoop.mapreduce` (never `mapred`) |
| **Runs in** | local standalone (LocalJobRunner) · pseudo-distributed · Docker multi-node (1 NN/RM + 3 DN/NM) |
| **Verified** | 97 automated tests, 85.2 % line coverage, and **diff = 0** against the Python reference on the 10 MB corpus, 11 stages |
| **Datasets** | generated at 10 MB / 100 MB / 500 MB / 1 GB + a 10 000-file corpus for the small-files study |

---

## 1. Quick start

Everything Java runs in a JDK-11 container because Hadoop 3.3.x does not support the
JDK 17+ that most hosts ship, and a real Maven is needed. Docker is the only
prerequisite for the local workflow.

```bash
make data          # generate 10 MB, 100 MB and the 10 000-file corpus
make build         # compile + unit tests + integration tests + 80 % coverage gate
make run           # whole pipeline on 10 MB (LocalJobRunner)
make verify        # diff every stage against the Python reference -> must print diff = 0
```

Everything at once:

```bash
make               # build -> data -> run -> verify
```

Real end-to-end run from this repository (10 MB corpus, 2 reducers, local mode):

```
[..] pipeline starting: corpus=data/generated/10m root=out/10m stages=core
[..] J1-normalized-word-count finished
[..] J2-stopword-topn finished
...
[..] pipeline finished: results under out/10m
```

Stage outputs:

```
out/10m/01-wordcount        SequenceFile<Text,LongWritable>   (chains into J2)
out/10m/02-topn             word<TAB>count, rank order
out/10m/03-word-length      length<TAB>count
out/10m/04-char-freq        U+XXXX<TAB>char<TAB>count
out/10m/05a-bigrams         ngram<TAB>count
out/10m/05b-trigrams        ngram<TAB>count
out/10m/06-inverted-index   word<TAB>doc:count:pos,pos
out/10m/07c-tfidf           term<TAB>doc<TAB>weight
out/10m/08a-pairs           w1<TAB>w2<TAB>count
out/10m/08b-stripes         word<TAB>neighbour:count,...
out/10m/09-sentiment        doc<TAB>polarity<TAB>tokens<TAB>pos<TAB>neg
```

Actual first lines of `out/10m/02-topn` from the verification run:

```
<num>	9575
one	7260
word	6636
said	4779
use	4636
```

`<num>` at the top is expected: the default number policy masks digit-only tokens, and
the generated corpus contains many years and page-style numbers. This is exactly the
kind of artefact the mask policy exists to expose rather than hide inside thousands of
distinct numeric tokens.

---

## 2. Correctness: what "verified" means here

The verifier recomputes every stage in Python and compares:

```
$ make verify
01-wordcount       PASS  5016 keys identical
02-topn            PASS  100 ranked entries identical
03-word-length     PASS  14 keys identical
04-char-freq       PASS  30 keys identical
05a-bigrams        PASS  135940 keys identical
05b-trigrams       PASS  121478 keys identical
06-inverted-index  PASS  5016 keys identical
07c-tfidf          PASS  92078 keys match (max relative delta 4.97e-07)
08a-pairs          PASS  506215 keys identical
08b-stripes        PASS  5016 keys identical
09-sentiment       PASS  80 keys match (max relative delta 4.89e-07)

RESULT: all stages match the reference (diff = 0)
```

Counts, tokens, postings and rendered stripes are compared **byte-exactly**. Only
floating-point fields (TF-IDF weights, sentiment polarity) use a 1e-6 relative
tolerance, because Java formats `%f` half-up while Python rounds the binary double
half-even — a difference in the last digit that is not a correctness failure. That
policy is documented at the top of `python/verify_outputs.py` rather than hidden.

---

## 3. Commands

### 3.1 Local mode (no cluster)

```bash
make data                       # corpora (add TA_BIG=1 for 500m and 1g)
make build                      # jar + tests + coverage gate
make run DATASET=100m           # pipeline on a bigger corpus
make verify DATASET=100m
make check                      # determinism + idempotency smoke test
python python/verify_outputs.py --corpus data/generated/10m --output-root out/10m-local \
       --wordcount-text out/10m-local/_verify-wordcount
```

Individual stages (useful when debugging one stage):

```bash
hadoop jar target/text-analytics.jar wordcount -Dta.overwrite=true corpus/ out-wc/
hadoop jar target/text-analytics.jar topn      out-wc/ out-topn/
hadoop jar target/text-analytics.jar tfidf     out-tf/ out-df/ out-tfidf/
```

No class name after the jar: its manifest already sets `Main-Class: edu.bdav.ta.Main`, so a
class argument would be read as the sub-command.

Or without `hadoop` on PATH, using the jar's classpath:

```bash
java -cp target/classes:$(cat target/runtime-classpath.txt) edu.bdav.ta.Main ngram \
  -Dta.ngram.n=3 -Dta.overwrite=true data/generated/10m out/trigrams
```

### 3.2 Multi-node Docker cluster (optional)

Nothing above needs this; the pipeline is complete in local mode. The cluster is provided
for the distributed comparison, and on the authoring host it was verified only up to a
point (DataNodes, YARN nodes and real YARN jobs, but not the full pipeline) because
the workstation ran out of RAM and disk — `docs/01-architecture.md` §13.6 records exactly
where it stopped. It wants a machine with ≥8 GB free for Docker and ≥10 GB of disk.

```bash
make cluster-up                 # builds the image (JDK 11 + Hadoop 3.3.6) and starts 4 containers
docker compose -f docker/docker-compose.yml ps
# NameNode UI: http://localhost:9870    YARN UI: http://localhost:8088

docker compose -f docker/docker-compose.yml exec namenode bash -lc \
  'hdfs dfs -put /data/generated/10m/doc-*.txt /data/raw/10m/'

make cluster-run DATASET=10m    # runs the pipeline on YARN
make fault-tolerance            # kills datanode2 mid-job, compares output hashes
make cluster-down
```

### 3.3 Benchmarks

```bash
make bench      # all experiments -> results/csv/*.csv + results/plots/*.png
make plots      # re-draw plots from existing CSVs
```

---

## 4. Configuration

No path, threshold or count is hardcoded: everything is a `-D` option, a value in
`src/main/resources/defaults.properties`, or a `conf/*.xml` Hadoop setting.

| Key | Default | Meaning |
|---|---|---|
| `ta.reducers` | 4 | reduce tasks |
| `ta.topn.n` | 100 | global top-N size |
| `ta.topn.strategy` | heap | `heap` (1 reducer) or `partitioned` (R reducers + merge) |
| `ta.ngram.n` | 2 | n-gram order (1–5) |
| `ta.ngram.minfreq` | 2 | prune n-grams below this count (applied in the reducer, on the global count) |
| `ta.ngram.padding` | true | emit `<s>` / `</s>` boundary n-grams |
| `ta.number.mode` | mask | `keep` / `drop` / `mask` (`<num>`) |
| `ta.min.token.length` | 1 | minimum token length in code points |
| `ta.combine.mode` | combiner | `none` / `combiner` / `inmapper` / `both` |
| `ta.codec` | none | `none` / `snappy` / `lz4` / `gzip` / `bzip2` for map output and SequenceFiles |
| `ta.stopwords.path` | — | stop-word file (shipped via DistributedCache) |
| `ta.lexicon.path` | — | sentiment lexicon (shipped via DistributedCache) |
| `ta.cooccur.window` | 5 | co-occurrence window |
| `ta.cooccur.mincount` | 2 | drop pairs below this count |
| `ta.cooccur.topk` | 20 | neighbours kept per stripe |
| `ta.tf.mode` | log | `raw` / `log` / `augmented` |
| `ta.overwrite` | false | replace an existing output directory (or pass `--overwrite`) |
| `ta.partitioner` | hash | `hash` or `salted` (skew experiment only) |

Exit codes: `0` ok, `2` usage, `3` configuration, `4` job failed, `5` I/O, `6`
verification failed. The driver prints one actionable line instead of a stack trace; the
stack trace is logged at DEBUG only.

---

## 5. Repository layout

```
pom.xml                     Maven build (Hadoop 3.3.6, Java 11, JaCoCo, shade)
Makefile                    build -> data -> run -> verify -> bench
conf/                       core-site, hdfs-site, mapred-site, yarn-site, hadoop-env.sh
docker/                     Dockerfile, docker-compose.yml (1 NN/RM + 3 DN/NM), entrypoints
scripts/                    01_build 02_fetch_data 03_check_stopwords 04_upload_to_hdfs
                            05_run_all 06_verify_reference 07_benchmark 08_fault_tolerance
                            09_determinism 99_clean + lib/common.sh + bench_inside.sh
python/                     generate_corpus.py, verify_outputs.py, bench_*.py,
                            reference/{tokenizer,jobs}.py, baselines/bench_python_baseline.py
src/main/java/edu/bdav/ta/   Main + config/counters/cli/io/text/types/partition/sort/
                            reduce/driver + job/{wordcount,topn,length,charfreq,ngram,
                            inverted,tfidf,cooccur,sentiment}
src/test/java/edu/bdav/ta/  unit tests per component + integration/*IT (LocalJobRunner)
docs/                       01-architecture, 02-report-outline-ieee, 03-viva-pack,
                            04-benchmark-report
results/                    csv/ (measurements), plots/ (figures), logs/ (runs)
```

---

## 6. Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| `mvn: command not found`, or Hadoop fails on Java 17+ | building outside the JDK-11 image | use `make build` / `scripts/01_build.sh` (runs in the container), or install JDK 11 + Maven 3.8+ |
| `output path already exists` | the driver is idempotent by design | pass `--overwrite` or `-Dta.overwrite=true` |
| `key ta.stopwords.path is required` | stop-word stage run without the file | `-Dta.stopwords.path=data/stopwords/en-stopwords.txt` |
| `codec 'snappy' unusable here` (warning) | the codec needs a native library the JVM does not have | harmless: the job continues uncompressed and says so. Only `bzip2` has a pure-Java implementation here; `gzip`, `snappy` and `lz4` need Hadoop's native libraries |
| `Output directory not set` | a stage ran without a positional output path | each command takes `<input> <output>`; see `Main` usage |
| `LocalJobRunner` fails on Windows with `winutils`/`HADOOP_HOME` errors | Hadoop local mode needs `winutils.exe` on native Windows | run inside the container or WSL2; `make` does this for you |
| `Container killed on request. Exit code 137` on the cluster | YARN memory limit too small for the inverted-index reducer | raise `yarn.nodemanager.resource.memory-mb` and `mapreduce.reduce.memory.mb` |
| `hdfs dfsadmin -report` shows 0 DataNodes; uploads fail with `could only be written to 0 of the 1 minReplication nodes` | a worker container is not running a DataNode at all. Check its log: if it shows `[namenode] formatting the filesystem` the role override never applied (an `ENTRYPOINT` in the image makes compose's `command:` only *append an argument*) | the image ends with `CMD ["/usr/local/bin/entrypoint-namenode.sh"]`, not `ENTRYPOINT`; rebuild with `make cluster-up` |
| `unknown command: edu.bdav.ta.Main` when submitting | `hadoop jar` already reads `Main-Class` from the manifest, so a class name becomes the first argument (the sub-command) | `hadoop jar target/text-analytics.jar wordcount <in> <out>` |
| verifier reports differing TF-IDF weights | mismatched `ta.tf.mode` or `ta.corpus.ndocs` between job and verifier | pass the same options to `python/verify_outputs.py` |

---

## 7. Honest limitations

* **Everything measured end-to-end is local mode.** Those numbers bound the pipeline's
  cost but say nothing about distributed speedup. The cluster was brought up and validated
  with real YARN jobs (identical counters to local mode, six pipeline stages), but the
  full 13-stage pipeline never finished there: the Docker VM had 3.5 GB of RAM and the
  host disk filled up. `docs/01-architecture.md` §13.6 documents that failure and the
  sizing it would need — so cluster throughput is not claimed anywhere.
* **Sentiment is lexicon-based** and the shipped lexicon is small (≈150 hand-written
  entries), so coverage is low by construction; the job reports
  `UNKNOWN_LEXICON_WORDS` rather than pretending otherwise.
* **Cosine similarity on TF-IDF is not implemented**; it is documented as future work
  (see `docs/01-architecture.md` §12). Everything in the mandatory list is implemented.
* **The Python verifier is the oracle for 11 stages**, not for every conceivable
  configuration: it mirrors the default policies. Non-default `ta.tf.mode`,
  `ta.ngram.n` and `ta.cooccur.window` are passed to it explicitly by the scripts.
