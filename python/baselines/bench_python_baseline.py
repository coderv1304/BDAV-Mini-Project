#!/usr/bin/env python3
"""Times the single-machine reference word count on each generated corpus.

This is the denominator of the crossover analysis: at some input size the fixed cost
of a MapReduce job (JVM start, split planning, shuffle, commit) stops dominating and
distribution starts to win. The honest result on a laptop-sized corpus is that
Hadoop loses; the benchmark reports that rather than hiding it.
"""
from __future__ import annotations

import argparse
import csv
import os
import sys
import time
from collections import Counter

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from reference import jobs  # noqa: E402
from reference import tokenizer as tk  # noqa: E402


def count_words(root: str) -> tuple[Counter, int, int]:
    """Reference word count, returning (counts, tokens, bytes)."""
    counts = Counter()
    tokens = 0
    total_bytes = 0
    for _doc_id, text in jobs.iter_documents(root):
        total_bytes += len(text.encode("utf-8"))
        for line in tk.split_lines(text):
            line_tokens = tk.tokenize(line)
            tokens += len(line_tokens)
            counts.update(line_tokens)
    return counts, tokens, total_bytes


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="single-machine baseline timings")
    parser.add_argument("--datasets", nargs="+", default=["10m", "100m"])
    parser.add_argument("--root", default="data/generated")
    parser.add_argument("--repeats", type=int, default=1,
                        help="more repeats = more reliable numbers, at linear cost")
    parser.add_argument("--out", default="results/csv/baseline-python.csv")
    args = parser.parse_args(argv)

    os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)
    rows = []
    for dataset in args.datasets:
        directory = os.path.join(args.root, dataset)
        if not os.path.isdir(directory):
            print(f"skip {dataset}: {directory} not found", file=sys.stderr)
            continue
        best = None
        for _ in range(max(1, args.repeats)):
            start = time.perf_counter()
            counts, tokens, total_bytes = count_words(directory)
            elapsed_ms = (time.perf_counter() - start) * 1000.0
            best = (elapsed_ms, tokens, total_bytes, len(counts))
        elapsed_ms, tokens, total_bytes, distinct = best
        rows.append({
            "dataset": dataset,
            "bytes": total_bytes,
            "tokens": tokens,
            "distinct_terms": distinct,
            "wall_ms": round(elapsed_ms, 1),
            "tokens_per_second": round(tokens / (elapsed_ms / 1000.0), 1),
        })
        print(f"{dataset}: {elapsed_ms:.0f} ms, {tokens} tokens, {distinct} distinct terms")

    with open(args.out, "w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()) if rows else ["dataset"])
        writer.writeheader()
        writer.writerows(rows)
    print(f"written {args.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
