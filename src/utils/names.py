"""Utilities for normalizing and matching human names across data sources."""

from __future__ import annotations

import re
import unicodedata
from typing import TypeVar

T = TypeVar("T")


def normalize_person_name(name: str) -> str:
    """Normalize a person name for robust keying across sources.

    Args:
        name: Raw display name.

    Returns:
        Lower-cased ASCII name with punctuation removed and whitespace collapsed.
    """
    ascii_name = (
        unicodedata.normalize("NFKD", str(name))
        .encode("ascii", "ignore")
        .decode("ascii")
    )
    cleaned = re.sub(r"[^a-zA-Z\s]", "", ascii_name).strip().lower()
    return re.sub(r"\s+", " ", cleaned)


def from_last_first(name: str) -> str:
    """Convert `Last, First` to `First Last` when applicable.

    Args:
        name: Source display name.

    Returns:
        Name converted to `First Last` if comma-separated, otherwise unchanged.
    """
    if "," not in str(name):
        return str(name)
    last, first = (part.strip() for part in str(name).split(",", 1))
    return f"{first} {last}".strip()


def resolve_unique_name_match(
    candidate_name: str, value_by_normalized_name: dict[str, T]
) -> T | None:
    """Resolve a unique match using exact, first-initial, and last-name heuristics.

    Args:
        candidate_name: Target name to resolve.
        value_by_normalized_name: Mapping keyed by normalized full name.

    Returns:
        A unique matched value, otherwise `None`.
    """
    norm = normalize_person_name(candidate_name)
    if not norm:
        return None

    exact = value_by_normalized_name.get(norm)
    if exact is not None:
        return exact

    parts = norm.split()
    if len(parts) < 2:
        return None

    first, last = parts[0], parts[-1]
    matches: list[T] = []

    for raw_norm, value in value_by_normalized_name.items():
        raw_parts = raw_norm.split()
        if len(raw_parts) < 2:
            continue
        raw_first, raw_last = raw_parts[0], raw_parts[-1]
        if raw_last != last:
            continue
        if raw_first.startswith(first) or first.startswith(raw_first):
            matches.append(value)
        elif raw_first and first and raw_first[0] == first[0]:
            matches.append(value)

    unique = {id(v): v for v in matches}
    if len(unique) != 1:
        return None
    return next(iter(unique.values()))
