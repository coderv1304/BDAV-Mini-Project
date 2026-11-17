#!/usr/bin/env python3
"""Deterministic corpus generator for the text-analytics platform.

Generates plain-text "documents" with a Zipfian word distribution, realistic
sentence/paragraph structure, and a controlled sprinkle of the edge cases the
project must handle (Unicode, emoji, contractions, hyphenation, numbers, very
long tokens). Determinism matters: the Java pipeline and the Python reference
must see byte-identical input across runs and machines, so every choice is made
from one seeded RNG.

Usage:
    python generate_corpus.py --out data/generated/10m --target-bytes 10000000
    python generate_corpus.py --out data/generated/tiny10k --target-bytes 10000000 \
        --files 10000 --max-file-bytes 1200

`--max-file-bytes` is a floor, not a ceiling: a file is filled with whole paragraphs
until it reaches the cap, so 1200 yields ~1444 bytes per file on average (10 000 files
-> ~14.4 MB). That overshoot is intentional - splitting a paragraph to hit an exact
byte count would make the small-files corpus differ linguistically from the others.
"""
from __future__ import annotations

import argparse
import json
import os
import random
import sys

SYLLABLES = ["ka", "to", "ri", "man", "pel", "sor", "tri", "vel", "dan", "quo",
             "ber", "lin", "sta", "nor", "vec", "ial", "mon", "tar", "sul", "gir",
             "ath", "eld", "ith", "orn", "ush", "yne", "bro", "cly", "dun", "fen"]

COMMON = """the of and a to in is you that it he was for on are as with his they I at be this have from
or one had by word but not what all were we when your can said there use an each which she do how their if
will up other about out many then them these so some her would make like him into time has look two more
write go see number no way could people my than first water been call who oil its now find long down day
did get come made may part book story man woman city river night light house world hand life eye
""".split()

EDGE_WORDS = ["don't", "it's", "state-of-the-art", "e-mail", "naïve", "café",
              "Zürich", "über", "résumé", "co-operate", "2024", "1984", "42",
              "3.14", "COVID-19", "x", "a", "🚀", "🙂", "👩‍👩‍👧", "€", "$", "©"]

LONG_TOKEN = "pneumonoultramicroscopicsilicovolcanoconiosis"


def build_vocabulary(size: int, rng: random.Random) -> list[str]:
    """Builds a vocabulary of `size` distinct tokens: common words plus coins."""
    vocab = []
    seen = set()
    for word in COMMON:
        if word not in seen:
            seen.add(word)
            vocab.append(word)
    while len(vocab) < size:
        parts = rng.choice([1, 2, 2, 3])
        word = "".join(rng.choice(SYLLABLES) for _ in range(parts))
        if rng.random() < 0.25:
            word += rng.choice(["ing", "ed", "ly", "ness", "tion", "s"])
        if word not in seen:
            seen.add(word)
            vocab.append(word)
    return vocab


def zipf_ranks(rng: random.Random, n: int, s: float = 1.07) -> list[int]:
    """Pre-computes a sampled rank table (inverse-CDF sampling over 1/r^s)."""
    cumulative = []
    total = 0.0
    for rank in range(1, n + 1):
        total += 1.0 / (rank ** s)
        cumulative.append(total)
    return cumulative


def pick_rank(rng: random.Random, cumulative: list[float]) -> int:
    """Samples a vocabulary rank from the Zipf CDF via binary search."""
    import bisect
    target = rng.random() * cumulative[-1]
    return bisect.bisect_left(cumulative, target)


def sentence(rng: random.Random, vocab: list[str], cumulative: list[float]) -> str:
    """Builds one sentence of 6..22 tokens with punctuation and edge cases."""
    length = rng.randint(6, 22)
    words = []
    for index in range(length):
        roll = rng.random()
        if roll < 0.02:
            words.append(rng.choice(EDGE_WORDS))
        elif roll < 0.021:
            words.append(LONG_TOKEN)
        else:
            rank = pick_rank(rng, cumulative)
            words.append(vocab[min(rank, len(vocab) - 1)])
    text = " ".join(words)
    text = text[0].upper() + text[1:]
    return text + rng.choice([".", ".", ".", "!", "?", ";"])


def paragraph(rng: random.Random, vocab: list[str], cumulative: list[float]) -> str:
    """Builds a paragraph of 3..8 sentences terminated by a blank line."""
    sentences = [sentence(rng, vocab, cumulative) for _ in range(rng.randint(3, 8))]
    return " ".join(sentences) + "\n\n"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="generate a deterministic text corpus")
    parser.add_argument("--out", required=True, help="output directory")
    parser.add_argument("--target-bytes", type=int, required=True, help="approximate corpus size")
    parser.add_argument("--files", type=int, default=0, help="exact number of files (0 = derive)")
    parser.add_argument("--max-file-bytes", type=int, default=0, help="cap per file (0 = derive)")
    parser.add_argument("--vocab-size", type=int, default=5000)
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args(argv)

    os.makedirs(args.out, exist_ok=True)
    rng = random.Random(args.seed)
    vocab = build_vocabulary(args.vocab_size, rng)
    cumulative = zipf_ranks(rng, len(vocab))

    files = args.files if args.files > 0 else max(1, args.target_bytes // 500_000)
    per_file = args.max_file_bytes if args.max_file_bytes > 0 else max(1024, args.target_bytes // files)

    written_total = 0
    for index in range(files):
        path = os.path.join(args.out, f"doc-{index:05d}.txt")
        chunk = bytearray()
        while len(chunk) < per_file:
            chunk.extend(paragraph(rng, vocab, cumulative).encode("utf-8"))
        with open(path, "wb") as handle:
            handle.write(chunk)
        written_total += len(chunk)
        if written_total >= args.target_bytes and args.files == 0:
            break

    manifest = {
        "directory": args.out,
        "files": len([n for n in os.listdir(args.out) if n.startswith("doc-")]),
        "bytes": written_total,
        "vocab_size": len(vocab),
        "seed": args.seed,
        "zipf_exponent": 1.07,
    }
    with open(os.path.join(args.out, "_manifest.json"), "w", encoding="utf-8") as handle:
        json.dump(manifest, handle, indent=2, sort_keys=True)
    print(json.dumps(manifest, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())
