"""Single-machine reference implementation of every pipeline stage.

Deliberately naive: `collections.Counter`, `dict`, plain loops. The point is
trustworthiness, not speed - the Hadoop pipeline must reproduce these numbers on
the 10 MB and 100 MB corpora, and the benchmark compares the two runtimes to find
the crossover point where distribution starts to pay.
"""
from __future__ import annotations

import math
import os
from collections import Counter, defaultdict

from . import tokenizer as tk

DEFAULT_CONFIG = {
    "number_mode": "mask",
    "min_token_length": 1,
    "topn": 100,
    "ngram_min_freq": 2,
    "cooccur_window": 5,
    "cooccur_ordered": True,
    "cooccur_mincount": 2,
    "cooccur_topk": 20,
    "tf_mode": "log",
    "numeric_format": "%.6f",
}


def config(**overrides):
    """Returns a config dict with project defaults, overridden by keyword."""
    merged = dict(DEFAULT_CONFIG)
    merged.update({k: v for k, v in overrides.items() if v is not None})
    return merged


def iter_documents(root: str):
    """Yields (document_id, text) with document id relative to `root`.

    Document order is sorted for reproducibility; the pipeline's results are order
    independent, so this only makes failure output stable.
    """
    for name in sorted(os.listdir(root)):
        if name.startswith("_") or name.startswith("."):
            continue
        path = os.path.join(root, name)
        if os.path.isdir(path):
            for nested, text in iter_documents(path):
                yield os.path.join(name, nested), text
            continue
        with open(path, "rb") as handle:
            yield name, handle.read().decode("utf-8", errors="replace")


def iter_lines(root: str):
    """Yields every line of every document, in document order."""
    for _, text in iter_documents(root):
        for line in tk.split_lines(text):
            yield line


def load_stopwords(path: str | None) -> set[str]:
    """Reads a stop-word file the same way StopWordDictionary.parse does."""
    if not path:
        return set()
    words = set()
    with open(path, "r", encoding="utf-8") as handle:
        for line in handle:
            word = line.strip()
            if not word or word.startswith("#"):
                continue
            words.add(word.lower())
    return words


def load_lexicon(path: str | None) -> dict[str, float]:
    """Reads a `word<TAB>score` sentiment lexicon."""
    if not path:
        return {}
    scores = {}
    with open(path, "r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            fields = line.split("\t")
            if len(fields) < 2:
                continue
            try:
                scores[fields[0].strip().lower()] = float(fields[1])
            except ValueError:
                continue
    return scores


# --------------------------------------------------------------------------- J1
def word_count(root: str, cfg: dict) -> Counter:
    counts = Counter()
    for line in iter_lines(root):
        counts.update(tk.tokenize(line, cfg["number_mode"], cfg["min_token_length"]))
    return counts


# --------------------------------------------------------------------------- J2
def stopword_topn(counts: Counter, stopwords: set[str], cfg: dict) -> list[tuple[str, int]]:
    filtered = [(word, count) for word, count in counts.items() if word not in stopwords]
    filtered.sort(key=lambda item: (-item[1], item[0]))
    return filtered[: cfg["topn"]]


# --------------------------------------------------------------------------- J3
def word_length_distribution(root: str, cfg: dict) -> Counter:
    histogram = Counter()
    for line in iter_lines(root):
        for token in tk.tokenize(line, cfg["number_mode"], cfg["min_token_length"]):
            histogram[len(token)] += 1
    return histogram


# --------------------------------------------------------------------------- J4
def character_frequency(root: str, cfg: dict) -> Counter:
    counts = Counter()
    for line in iter_lines(root):
        for token in tk.tokenize(line, cfg["number_mode"], cfg["min_token_length"]):
            counts.update(token)
    return counts


# --------------------------------------------------------------------------- J5
def ngram_counts(root: str, cfg: dict, n: int) -> Counter:
    counts = Counter()
    for line in iter_lines(root):
        tokens = tk.tokenize(line, cfg["number_mode"], cfg["min_token_length"])
        counts.update(tk.ngrams(tokens, n, padding=True))
    return Counter({key: value for key, value in counts.items() if value >= cfg["ngram_min_freq"]})


# --------------------------------------------------------------------------- J6
def inverted_index(root: str, cfg: dict) -> dict[str, str]:
    index: dict[str, dict[str, list[int]]] = defaultdict(dict)
    for doc_id, text in iter_documents(root):
        per_doc: dict[str, list[int]] = defaultdict(list)
        for token, position in tk.tokenize_positions(text, cfg["number_mode"], cfg["min_token_length"]):
            per_doc[token].append(position)
        for token, positions in per_doc.items():
            index[token][doc_id] = positions
    rendered = {}
    for token, docs in index.items():
        parts = []
        for doc_id in sorted(docs):
            positions = sorted(docs[doc_id])
            parts.append(f"{doc_id}:{len(positions)}:{','.join(str(p) for p in positions)}")
        rendered[token] = "\t".join(parts)
    return rendered


# --------------------------------------------------------------------------- J7
def term_frequencies(root: str, cfg: dict) -> tuple[dict[tuple[str, str], float], int]:
    """Returns ((term, doc) -> weighted tf, number_of_documents)."""
    weights: dict[tuple[str, str], float] = {}
    documents = 0
    for doc_id, text in iter_documents(root):
        documents += 1
        counts = Counter(tk.tokenize(text, cfg["number_mode"], cfg["min_token_length"]))
        maximum = max(counts.values()) if counts else 1
        for term, raw in counts.items():
            if cfg["tf_mode"] == "raw":
                weight = float(raw)
            elif cfg["tf_mode"] == "augmented":
                weight = 0.5 + 0.5 * (raw / max(1, maximum))
            else:
                weight = 1.0 + math.log10(raw)
            weights[(term, doc_id)] = weight
    return weights, documents


def document_frequencies(weights: dict[tuple[str, str], float]) -> Counter:
    df = Counter()
    for (term, _), _value in weights.items():
        df[term] += 1
    return df


def tfidf(root: str, cfg: dict) -> tuple[dict[tuple[str, str], float], int]:
    """Returns ((term, doc) -> tf-idf weight, documents)."""
    weights, documents = term_frequencies(root, cfg)
    df = document_frequencies(weights)
    result = {}
    for (term, doc_id), weight in weights.items():
        idf = math.log10(documents / df[term])
        result[(term, doc_id)] = weight * idf
    return result, documents


# --------------------------------------------------------------------------- J8
def cooccurrence_pairs(root: str, cfg: dict) -> Counter:
    pairs = Counter()
    window = cfg["cooccur_window"]
    for _doc_id, text in iter_documents(root):
        tokens = tk.tokenize(text, cfg["number_mode"], cfg["min_token_length"])
        size = len(tokens)
        for i in range(size):
            for j in range(i + 1, min(size, i + window + 1)):
                pairs[(tokens[i], tokens[j])] += 1
                if not cfg["cooccur_ordered"]:
                    pairs[(tokens[j], tokens[i])] += 1
    return Counter({key: value for key, value in pairs.items() if value >= cfg["cooccur_mincount"]})


def cooccurrence_stripes(root: str, cfg: dict) -> dict[str, str]:
    stripes: dict[str, Counter] = defaultdict(Counter)
    window = cfg["cooccur_window"]
    for _doc_id, text in iter_documents(root):
        tokens = tk.tokenize(text, cfg["number_mode"], cfg["min_token_length"])
        size = len(tokens)
        for i in range(size):
            word = tokens[i]
            stripes.setdefault(word, Counter())
            for j in range(i + 1, min(size, i + window + 1)):
                stripes[word][tokens[j]] += 1
                if not cfg["cooccur_ordered"]:
                    stripes[tokens[j]][word] += 1
    rendered = {}
    for word, stripe in stripes.items():
        kept = [(neighbour, count) for neighbour, count in stripe.items() if count >= cfg["cooccur_mincount"]]
        if not kept:
            continue
        kept.sort(key=lambda item: (-item[1], item[0]))
        limited = kept[: cfg["cooccur_topk"]] if cfg["cooccur_topk"] > 0 else kept
        rendered[word] = ",".join(f"{neighbour}:{count}" for neighbour, count in limited)
    return rendered


# --------------------------------------------------------------------------- J9
def sentiment(root: str, cfg: dict, lexicon: dict[str, float]) -> dict[str, tuple[float, int, float, float]]:
    scores = {}
    for doc_id, text in iter_documents(root):
        tokens = tk.tokenize(text, cfg["number_mode"], cfg["min_token_length"])
        positive = 0.0
        negative = 0.0
        for token in tokens:
            score = lexicon.get(token)
            if score is None:
                continue
            if score > 0:
                positive += score
            elif score < 0:
                negative += -score
        polarity = 0.0 if not tokens else (positive - negative) / len(tokens)
        scores[doc_id] = (polarity, len(tokens), positive, negative)
    return scores
