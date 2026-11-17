#!/usr/bin/env python3
"""Aggregates raw benchmark rows into the tables the report quotes.

Derived quantities, all computed from measured values only:

  * throughput (MB/s) = bytes / wall_ms
  * speedup(S) = T(1 reducer) / T(S reducers)
  * efficiency(S) = speedup(S) / S   (1.0 = perfect scaling)
  * shuffle reduction = 1 - shuffle_bytes(with combiner) / shuffle_bytes(without)

Nothing is extrapolated: if a measurement is missing the row is omitted and the
summary says so instead of inventing a number.
"""
from __future__ import annotations

import argparse
import csv
import os
import sys
from collections import defaultdict

COUNTERS_OF_INTEREST = [
    "MAP_OUTPUT_BYTES",
    "MAP_OUTPUT_RECORDS",
    "REDUCE_SHUFFLE_BYTES",
    "COMBINE_INPUT_RECORDS",
    "GC_TIME_MILLIS",
    "CPU_MILLISECONDS",
    "FILE_BYTES_READ",
    "FILE_BYTES_WRITTEN",
    "SPILLED_RECORDS",
    "PHYSICAL_MEMORY_BYTES",
]


def read_csv(path: str) -> list[dict]:
    if not os.path.exists(path):
        return []
    with open(path, newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="aggregate benchmark results")
    parser.add_argument("--raw", default="results/csv/bench-raw.csv")
    parser.add_argument("--metrics", default="results/csv/bench-metrics.csv")
    parser.add_argument("--out", default="results/csv/bench-summary.csv")
    args = parser.parse_args(argv)

    raw = read_csv(args.raw)
    metrics = read_csv(args.metrics)
    if not raw:
        print("no raw benchmark rows - run scripts/07_benchmark.sh first", file=sys.stderr)
        return 1

    counters: dict[tuple[str, str], dict[str, int]] = defaultdict(dict)
    for row in metrics:
        try:
            counters[(row["dataset"], row["experiment"])][row["counter"]] = int(row["value"])
        except (KeyError, ValueError):
            continue

    # Wall time per reducer count, per dataset, for the reducer-scaling study.
    by_reducers: dict[str, dict[int, int]] = defaultdict(dict)
    for row in raw:
        if row["experiment"].startswith("reducers-") and row["status"] == "ok":
            by_reducers[row["dataset"]][int(row["reducers"])] = int(row["wall_ms"])

    summary = []
    for row in raw:
        dataset = row["dataset"]
        experiment = row["experiment"]
        bytes_total = int(row["bytes"])
        wall = int(row["wall_ms"])
        status = row["status"]
        measured = counters.get((dataset, experiment), {})

        speedup = ""
        efficiency = ""
        baseline = by_reducers.get(dataset, {}).get(1)
        if baseline and wall > 0 and row["experiment"].startswith("reducers-"):
            speedup = round(baseline / wall, 3)
            efficiency = round(speedup / int(row["reducers"]), 3)

        summary.append({
            "dataset": dataset,
            "experiment": experiment,
            "variant": row["variant"],
            "reducers": row["reducers"],
            "status": status,
            "bytes": bytes_total,
            "files": row.get("files", ""),
            "wall_ms": wall,
            "mb_per_s": round((bytes_total / 1_000_000) / (wall / 1000.0), 3) if wall > 0 else "",
            "speedup_vs_1_reducer": speedup,
            "reducer_efficiency": efficiency,
            "shuffle_bytes": measured.get("REDUCE_SHUFFLE_BYTES", ""),
            "map_output_bytes": measured.get("MAP_OUTPUT_BYTES", ""),
            "map_output_records": measured.get("MAP_OUTPUT_RECORDS", ""),
            "gc_ms": measured.get("GC_TIME_MILLIS", ""),
            "cpu_ms": measured.get("CPU_MILLISECONDS", ""),
        })

    # Shuffle reduction from combining: measured, not assumed.
    for dataset in {row["dataset"] for row in summary}:
        without = counters.get((dataset, "abl-nocombiner"), {}).get("REDUCE_SHUFFLE_BYTES")
        with_combiner = counters.get((dataset, "abl-combiner"), {}).get("REDUCE_SHUFFLE_BYTES")
        if without and with_combiner:
            print(f"{dataset}: shuffle {without} -> {with_combiner} bytes "
                  f"({100.0 * (1 - with_combiner / without):.1f}% reduction from the combiner)")

    os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)
    with open(args.out, "w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(summary[0].keys()))
        writer.writeheader()
        writer.writerows(summary)
    print(f"written {args.out} ({len(summary)} rows)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
