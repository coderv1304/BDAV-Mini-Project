#!/usr/bin/env python3
"""Plots the measured benchmark results.

Every figure reads a CSV produced by an actual run. If a series is missing the figure
is skipped with a warning rather than drawn from invented data - a placeholder plot is
worse than no plot, because it looks like evidence.
"""
from __future__ import annotations

import argparse
import csv
import os
import sys

try:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
except ImportError:  # pragma: no cover - environment dependent
    print("matplotlib is required for plots: pip install matplotlib", file=sys.stderr)
    sys.exit(2)


def read_csv(path: str) -> list[dict]:
    if not os.path.exists(path):
        return []
    with open(path, newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def to_int(value, default=None):
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return default


def plot_size_scaling(summary: list[dict], baseline: list[dict], out_dir: str) -> None:
    hadoop = [(row) for row in summary if row["experiment"] == "size-wc"]
    if not hadoop:
        print("skip size-scaling plot: no size-wc rows")
        return
    hadoop.sort(key=lambda row: to_int(row["bytes"], 0))
    sizes_mb = [to_int(row["bytes"], 0) / 1e6 for row in hadoop]
    hadoop_s = [to_int(row["wall_ms"], 0) / 1000.0 for row in hadoop]

    fig, ax = plt.subplots(figsize=(7, 4.5))
    ax.plot(sizes_mb, hadoop_s, marker="o", label="Hadoop local mode (word count)")

    if baseline:
        base = sorted(baseline, key=lambda row: to_int(row["bytes"], 0))
        ax.plot([to_int(row["bytes"], 0) / 1e6 for row in base],
                [to_int(row["wall_ms"], 0) / 1000.0 for row in base],
                marker="s", linestyle="--", label="Python reference (single machine)")
    ax.set_xlabel("corpus size (MB)")
    ax.set_ylabel("wall-clock time (s)")
    ax.set_title("Runtime vs input size")
    ax.grid(alpha=0.3)
    ax.legend()
    fig.tight_layout()
    fig.savefig(os.path.join(out_dir, "runtime-vs-size.png"), dpi=150)
    plt.close(fig)


def plot_reducer_scaling(summary: list[dict], out_dir: str) -> None:
    rows = [row for row in summary if row["experiment"].startswith("reducers-")]
    if not rows:
        print("skip reducer-scaling plot: no reducer rows")
        return
    fig, (ax_time, ax_speed) = plt.subplots(1, 2, figsize=(11, 4.2))
    for dataset in sorted({row["dataset"] for row in rows}):
        series = sorted((row for row in rows if row["dataset"] == dataset),
                        key=lambda row: to_int(row["reducers"], 0))
        reducers = [to_int(row["reducers"], 0) for row in series]
        ax_time.plot(reducers, [to_int(row["wall_ms"], 0) / 1000.0 for row in series],
                     marker="o", label=f"{dataset}")
        speedups = [float(row["speedup_vs_1_reducer"]) for row in series
                    if row["speedup_vs_1_reducer"] not in ("", None)]
        if len(speedups) == len(reducers):
            ax_speed.plot(reducers, speedups, marker="o", label=f"{dataset}")
    ax_speed.plot([1, 4], [1, 4], linestyle=":", color="grey", label="linear scaling")
    ax_time.set_xlabel("reducers")
    ax_time.set_ylabel("wall-clock time (s)")
    ax_time.set_title("Runtime vs reducer count")
    ax_time.grid(alpha=0.3)
    ax_time.legend()
    ax_speed.set_xlabel("reducers")
    ax_speed.set_ylabel("speedup vs 1 reducer")
    ax_speed.set_title("Speedup (measured)")
    ax_speed.grid(alpha=0.3)
    ax_speed.legend()
    fig.tight_layout()
    fig.savefig(os.path.join(out_dir, "runtime-vs-reducers.png"), dpi=150)
    plt.close(fig)


def plot_shuffle_ablation(summary: list[dict], out_dir: str) -> None:
    labels = []
    values = []
    for experiment, label in (("abl-nocombiner", "no combiner"),
                              ("abl-combiner", "combiner"),
                              ("abl-inmapper", "in-mapper"),
                              ("abl-both", "both")):
        row = next((item for item in summary if item["experiment"] == experiment), None)
        shuffle = to_int(row.get("shuffle_bytes") if row else None)
        if shuffle:
            labels.append(label)
            values.append(shuffle / 1e6)
    if not values:
        print("skip shuffle ablation plot: no combiner rows")
        return
    fig, ax = plt.subplots(figsize=(6.5, 4))
    bars = ax.bar(labels, values, color=["#b5651d", "#2e7d32", "#1565c0", "#6a1b9a"])
    ax.bar_label(bars, fmt="%.2f MB")
    ax.set_ylabel("shuffle bytes (MB)")
    ax.set_title("Combiner ablation: shuffle volume (10 MB corpus)")
    ax.grid(axis="y", alpha=0.3)
    fig.tight_layout()
    fig.savefig(os.path.join(out_dir, "shuffle-ablations.png"), dpi=150)
    plt.close(fig)


def plot_pattern_comparison(summary: list[dict], out_dir: str) -> None:
    pairs = next((row for row in summary if row["experiment"] == "pattern-pairs"), None)
    stripes = next((row for row in summary if row["experiment"] == "pattern-stripes"), None)
    if not pairs or not stripes:
        print("skip pattern comparison: pairs/stripes rows missing")
        return
    labels = ["pairs", "stripes"]
    shuffle = [to_int(pairs.get("shuffle_bytes"), 0) / 1e6, to_int(stripes.get("shuffle_bytes"), 0) / 1e6]
    records = [to_int(pairs.get("map_output_records"), 0) / 1000.0,
               to_int(stripes.get("map_output_records"), 0) / 1000.0]
    time_s = [to_int(pairs["wall_ms"], 0) / 1000.0, to_int(stripes["wall_ms"], 0) / 1000.0]

    fig, axes = plt.subplots(1, 3, figsize=(12, 3.8))
    for ax, values, title, unit in zip(
            axes,
            (shuffle, records, time_s),
            ("shuffle bytes", "map output records", "wall-clock time"),
            ("MB", "thousand records", "s")):
        bars = ax.bar(labels, values, color=["#b5651d", "#1565c0"])
        ax.bar_label(bars, fmt="%.2f")
        ax.set_title(f"{title} ({unit})")
        ax.grid(axis="y", alpha=0.3)
    fig.suptitle("Co-occurrence: pairs vs stripes (10 MB corpus)")
    fig.tight_layout()
    fig.savefig(os.path.join(out_dir, "pairs-vs-stripes.png"), dpi=150)
    plt.close(fig)


def plot_small_files(summary: list[dict], out_dir: str) -> None:
    text_row = next((row for row in summary if row["experiment"] == "abl-smallfiles-text"), None)
    combine_row = next((row for row in summary if row["experiment"] == "abl-smallfiles-combine"), None)
    if not text_row or not combine_row:
        print("skip small-files plot: rows missing")
        return
    labels = ["TextInputFormat\n(one split per file)", "CombineTextInputFormat"]
    values = [to_int(text_row["wall_ms"], 0) / 1000.0, to_int(combine_row["wall_ms"], 0) / 1000.0]
    fig, ax = plt.subplots(figsize=(6.5, 4))
    bars = ax.bar(labels, values, color=["#b5651d", "#2e7d32"])
    ax.bar_label(bars, fmt="%.2f s")
    ax.set_ylabel("wall-clock time (s)")
    ax.set_title("Small-files problem: 10 000 tiny files")
    ax.grid(axis="y", alpha=0.3)
    fig.tight_layout()
    fig.savefig(os.path.join(out_dir, "small-files.png"), dpi=150)
    plt.close(fig)


def plot_codecs(summary: list[dict], out_dir: str) -> None:
    rows = [(row["experiment"].replace("abl-codec-", ""), to_int(row["wall_ms"], 0) / 1000.0)
            for row in summary if row["experiment"].startswith("abl-codec-")]
    if not rows:
        print("skip codec plot: rows missing")
        return
    labels = [name for name, _ in rows]
    values = [value for _, value in rows]
    fig, ax = plt.subplots(figsize=(6.5, 4))
    bars = ax.bar(labels, values, color="#37474f")
    ax.bar_label(bars, fmt="%.2f s")
    ax.set_ylabel("wall-clock time (s)")
    ax.set_title("Map-output compression ablation (10 MB corpus)")
    ax.grid(axis="y", alpha=0.3)
    fig.tight_layout()
    fig.savefig(os.path.join(out_dir, "compression.png"), dpi=150)
    plt.close(fig)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="plot benchmark results")
    parser.add_argument("--summary", default="results/csv/bench-summary.csv")
    parser.add_argument("--baseline", default="results/csv/baseline-python.csv")
    parser.add_argument("--metrics", default="results/csv/bench-metrics.csv")
    parser.add_argument("--out", default="results/plots")
    args = parser.parse_args(argv)

    os.makedirs(args.out, exist_ok=True)
    summary = read_csv(args.summary)
    baseline = read_csv(args.baseline)
    if not summary:
        print("no summary rows to plot", file=sys.stderr)
        return 1

    plot_size_scaling(summary, baseline, args.out)
    plot_reducer_scaling(summary, args.out)
    plot_shuffle_ablation(summary, args.out)
    plot_pattern_comparison(summary, args.out)
    plot_small_files(summary, args.out)
    plot_codecs(summary, args.out)
    print(f"plots written to {args.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
