"""Python mirror of edu.bdav.ta.text.TextNormalizer and NGramBuilder.

This module is the normative counterpart of the Java tokeniser: the two must
produce identical token streams for identical bytes, otherwise the project's
"diff = 0 against the reference" claim is meaningless.

Rules (identical order to the Java implementation):
  1. Unicode NFC normalisation
  2. lowercasing
  3. deletion of intra-word apostrophes
  4. split on every code point whose category is not L*/M*/N*
  5. number policy: keep | drop | mask (default mask -> '<num>')
  6. minimum token length in code points

`str.lower()` and Java's `String.toLowerCase(Locale.ROOT)` agree for the
characters in scope; the verifier is what proves it for a given corpus.
"""
from __future__ import annotations

import unicodedata

APOSTROPHES = frozenset({"\u0027", "\u2019", "\u02bc", "\uff07"})
NUMBER_TOKEN = "<num>"
NGram_SEPARATOR = "\u0001"
START = "<s>"
END = "</s>"


def is_word_code_point(ch: str) -> bool:
    """Mirrors TextNormalizer.isWordCodePoint (categories L*, M*, N*)."""
    return unicodedata.category(ch)[0] in ("L", "M", "N")


def is_decimal_digit(ch: str) -> bool:
    """Java checks DECIMAL_DIGIT_NUMBER only; Python's str.isdigit() is broader."""
    return unicodedata.category(ch) == "Nd"


def normalise(text: str) -> str:
    """NFC, lowercase, apostrophe removal - the exact text the tokeniser scans."""
    text = unicodedata.normalize("NFC", text)
    text = text.lower()
    return "".join(ch for ch in text if ch not in APOSTROPHES)


def tokenize(text: str, number_mode: str = "mask", min_length: int = 1) -> list[str]:
    """Returns the accepted tokens of `text`, in order of appearance."""
    prepared = normalise(text)
    tokens: list[str] = []
    index = 0
    length = len(prepared)
    while index < length:
        char = prepared[index]
        if not is_word_code_point(char):
            index += 1
            continue
        start = index
        while index < length and is_word_code_point(prepared[index]):
            index += 1
        token = prepared[start:index]
        if token and all(is_decimal_digit(ch) for ch in token):
            if number_mode == "drop":
                continue
            if number_mode == "mask":
                if len(NUMBER_TOKEN) < min_length:
                    continue
                tokens.append(NUMBER_TOKEN)
                continue
        if len(token) < min_length:
            continue
        tokens.append(token)
    return tokens


def tokenize_positions(text: str, number_mode: str = "mask", min_length: int = 1) -> list[tuple[str, int]]:
    """Returns (token, position) pairs; position is the accepted-token ordinal."""
    return [(token, position) for position, token in enumerate(tokenize(text, number_mode, min_length))]


def ngrams(tokens: list[str], n: int, padding: bool = True) -> list[str]:
    """Emits n-grams joined by U+0001, mirroring NGramBuilder.add/finish."""
    stream: list[str] = []
    if padding:
        stream.extend([START] * (n - 1))
    stream.extend(tokens)
    if padding:
        stream.append(END)
    if len(stream) < n:
        return []
    return [NGram_SEPARATOR.join(stream[i:i + n]) for i in range(len(stream) - n + 1)]


def render_ngram(key: str) -> str:
    """Renders an n-gram key with spaces instead of U+0001."""
    return key.replace(NGram_SEPARATOR, " ")


def split_lines(text: str) -> list[str]:
    """Splits like Hadoop's LineRecordReader: on '\\n' only, dropping the final empty record."""
    if not text:
        return []
    lines = text.split("\n")
    if lines and lines[-1] == "":
        lines.pop()
    return [line[:-1] if line.endswith("\r") else line for line in lines]
