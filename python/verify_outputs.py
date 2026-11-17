#!/usr/bin/env python3
"""Verifies Hadoop pipeline output against the single-machine reference.

Comparison policy, stated explicitly because the project claims "diff = 0":

  * Counts, token strings, postings and rendered stripes are compared
    **byte-exactly** (as Python dicts of str -> str/int), so any divergence in the
    tokenisation contract shows up immediately.
  * Top-N is compared as an **ordered sequence**, because rank order is part of the
    result (count descending, then term ascending in code-point order).
  * Floating-point fields (TF-IDF weights, sentiment polarity) are compared
    numerically with a relative tolerance of 1e-6. Java formats `%f` half-up and
    Python rounds the binary double half-even, so byte comparison of a formatted
    double would report a false failure in the last digit. Everything else in those
    files is still compared exactly.

Exit code 0 means every checked stage matched.
"""
from __future__ import annotations

import argparse
import math
import os
import sys
from collections import Counter

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from reference import jobs  # noqa: E402
from reference import tokenizer as tk  # noqa: E402

RELATIVE_TOLERANCE = 1e-6


def read_parts(directory: str) -> list[str]:
    """Returns all records from a stage's part files, in file order."""
    if not os.path.isdir(directory):
        raise FileNotFoundError(f"output directory missing: {directory}")
    records: list[str] = []
    for name in sorted(os.listdir(directory)):
        if not name.startswith("part-"):
            continue
        with open(os.path.join(directory, name), "r", encoding="utf-8") as handle:
            records.extend(line.rstrip("\n") for line in handle)
    return records


class Report:
    """Collects per-stage verdicts and prints a summary table."""

    def __init__(self) -> None:
        self.rows: list[tuple[str, bool, str]] = []

    def add(self, stage: str, ok: bool, detail: str) -> None:
        self.rows.append((stage, ok, detail))

    def ok(self) -> bool:
        return all(row[1] for row in self.rows)

    def print(self) -> None:
        width = max((len(row[0]) for row in self.rows), default=10)
        for stage, ok, detail in self.rows:
            print(f"{stage.ljust(width)}  {'PASS' if ok else 'FAIL'}  {detail}")


def compare_dict(stage: str, expected: dict, actual: dict, report: Report) -> None:
    """Compares two dicts exactly; reports count and first differences."""
    if expected == actual:
        report.add(stage, True, f"{len(expected)} keys identical")
        return
    missing = [key for key in expected if key not in actual][:3]
    extra = [key for key in actual if key not in expected][:3]
    differing = [key for key in expected if key in actual and expected[key] != actual[key]][:3]
    report.add(stage, False,
               f"{len(expected)} expected / {len(actual)} actual; "
               f"missing={missing} extra={extra} differing={[(k, expected[k], actual[k]) for k in differing]}")


def compare_numeric(stage: str, expected: dict, actual: dict, report: Report) -> None:
    """Compares floats with a relative tolerance and everything else exactly."""
    if set(expected) != set(actual):
        report.add(stage, False, f"key sets differ: {len(expected)} vs {len(actual)}")
        return
    worst = 0.0
    worst_key = None
    for key, expected_value in expected.items():
        actual_value = actual[key]
        scale = max(1.0, abs(expected_value))
        delta = abs(expected_value - actual_value) / scale
        if delta > worst:
            worst = delta
            worst_key = key
    if worst <= RELATIVE_TOLERANCE:
        report.add(stage, True, f"{len(expected)} keys match (max relative delta {worst:.2e})")
    else:
        report.add(stage, False, f"max relative delta {worst:.3e} at {worst_key}")


def verify(corpus: str, output_root: str, cfg: dict, skip: set[str],
           wordcount_text: str | None = None) -> Report:
    report = Report()
    stopwords = jobs.load_stopwords(cfg.get("stopwords_path"))
    lexicon = jobs.load_lexicon(cfg.get("lexicon_path"))

    counts = jobs.word_count(corpus, cfg)
    if "wordcount" not in skip:
        actual = {}
        wordcount_dir = wordcount_text or os.path.join(output_root, "01-wordcount")
        for record in read_parts(wordcount_dir):
            word, _, value = record.partition("\t")
            actual[word] = int(value)
        compare_dict("01-wordcount", dict(counts), actual, report)

    if "topn" not in skip:
        expected = jobs.stopword_topn(counts, stopwords, cfg)
        actual = []
        for record in read_parts(os.path.join(output_root, "02-topn")):
            word, _, value = record.partition("\t")
            actual.append((word, int(value)))
        if expected == actual:
            report.add("02-topn", True, f"{len(expected)} ranked entries identical")
        else:
            report.add("02-topn", False, f"expected {expected[:3]} got {actual[:3]}")

    if "length" not in skip:
        expected = {str(length): count for length, count in jobs.word_length_distribution(corpus, cfg).items()}
        actual = {}
        for record in read_parts(os.path.join(output_root, "03-word-length")):
            length, _, value = record.partition("\t")
            actual[length] = int(value)
        compare_dict("03-word-length", expected, actual, report)

    if "charfreq" not in skip:
        expected = {f"U+{ord(ch):04X}": str(count)
                    for ch, count in jobs.character_frequency(corpus, cfg).items()}
        actual = {}
        for record in read_parts(os.path.join(output_root, "04-char-freq")):
            label, rest = record.split("\t", 1)
            _char, _, count = rest.rpartition("\t")
            actual[label] = count
        compare_dict("04-char-freq", expected, actual, report)

    for n, directory in ((2, "05a-bigrams"), (3, "05b-trigrams")):
        if directory in skip:
            continue
        expected = {tk.render_ngram(key): str(value) for key, value in jobs.ngram_counts(corpus, cfg, n).items()}
        actual = {}
        for record in read_parts(os.path.join(output_root, directory)):
            ngram, _, value = record.rpartition("\t")
            actual[ngram] = value
        compare_dict(directory, expected, actual, report)

    if "inverted" not in skip:
        expected = jobs.inverted_index(corpus, cfg)
        actual = {}
        for record in read_parts(os.path.join(output_root, "06-inverted-index")):
            word, _, postings = record.partition("\t")
            actual[word] = postings
        compare_dict("06-inverted-index", expected, actual, report)

    if "tfidf" not in skip:
        expected_weights, documents = jobs.tfidf(corpus, cfg)
        expected = {(term, doc): weight for (term, doc), weight in expected_weights.items()}
        actual = {}
        for record in read_parts(os.path.join(output_root, "07c-tfidf")):
            term, doc, value = record.split("\t")
            actual[(term, doc)] = float(value)
        compare_numeric("07c-tfidf", expected, actual, report)
        print(f"reference N (documents) = {documents}".ljust(0), file=sys.stderr)

    if "pairs" not in skip:
        expected = {f"{a}\t{b}": str(value) for (a, b), value in jobs.cooccurrence_pairs(corpus, cfg).items()}
        actual = {}
        for record in read_parts(os.path.join(output_root, "08a-pairs")):
            first, second, value = record.split("\t")
            actual[f"{first}\t{second}"] = value
        compare_dict("08a-pairs", expected, actual, report)

    if "stripes" not in skip:
        expected = jobs.cooccurrence_stripes(corpus, cfg)
        actual = {}
        for record in read_parts(os.path.join(output_root, "08b-stripes")):
            word, _, stripe = record.partition("\t")
            actual[word] = stripe
        compare_dict("08b-stripes", expected, actual, report)

    if "sentiment" not in skip and os.path.isdir(os.path.join(output_root, "09-sentiment")):
        expected = jobs.sentiment(corpus, cfg, lexicon)
        actual = {}
        for record in read_parts(os.path.join(output_root, "09-sentiment")):
            doc, rest = record.split("\t", 1)
            polarity, token_count, positive, negative = rest.split("\t")
            actual[doc] = (float(polarity), int(token_count), float(positive), float(negative))
        flattened_expected = {}
        flattened_actual = {}
        for doc, values in expected.items():
            for index, value in enumerate(values):
                flattened_expected[(doc, index)] = float(value)
        for doc, values in actual.items():
            for index, value in enumerate(values):
                flattened_actual[(doc, index)] = float(value)
        compare_numeric("09-sentiment", flattened_expected, flattened_actual, report)

    return report


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="verify pipeline output against the Python reference")
    parser.add_argument("--corpus", required=True, help="corpus directory used as job input")
    parser.add_argument("--output-root", required=True, help="pipeline output root")
    parser.add_argument("--stopwords", default="data/stopwords/en-stopwords.txt")
    parser.add_argument("--lexicon", default="data/lexicon/sentiment-lexicon.tsv")
    parser.add_argument("--topn", type=int, default=100)
    parser.add_argument("--ngram-min-freq", type=int, default=2)
    parser.add_argument("--cooccur-window", type=int, default=5)
    parser.add_argument("--cooccur-mincount", type=int, default=2)
    parser.add_argument("--cooccur-topk", type=int, default=20)
    parser.add_argument("--tf-mode", default="log")
    parser.add_argument("--skip", default="", help="comma separated stages to skip")
    parser.add_argument("--wordcount-text", default=None,
                        help="text rendering of 01-wordcount (produced by `Main dump`), "
                             "because that stage is a SequenceFile")
    args = parser.parse_args(argv)

    cfg = jobs.config(
        topn=args.topn,
        ngram_min_freq=args.ngram_min_freq,
        cooccur_window=args.cooccur_window,
        cooccur_mincount=args.cooccur_mincount,
        cooccur_topk=args.cooccur_topk,
        tf_mode=args.tf_mode,
    )
    cfg["stopwords_path"] = args.stopwords
    cfg["lexicon_path"] = args.lexicon
    skip = {name.strip() for name in args.skip.split(",") if name.strip()}

    report = verify(args.corpus, args.output_root, cfg, skip, args.wordcount_text)
    report.print()
    if report.ok():
        print("\nRESULT: all stages match the reference (diff = 0)")
        return 0
    print("\nRESULT: MISMATCHES FOUND")
    return 1


if __name__ == "__main__":
    sys.exit(main())
