# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Canonical scalar validators shared by rooted-graph storage contracts."""

from __future__ import annotations

import hashlib
import json
import re

from process.provider_directory_rooted_graph_identity import SHA256_PATTERN


def _canonical_json(value: object) -> str:
    try:
        return json.dumps(
            value,
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )
    except (MemoryError, OverflowError, RecursionError, TypeError, ValueError):
        raise ValueError("provider_directory_rooted_graph_json_invalid") from None


def _sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _strict_identifier(candidate: object, pattern: re.Pattern[str]) -> str:
    if type(candidate) is not str or pattern.fullmatch(candidate) is None:
        raise ValueError("provider_directory_rooted_graph_identifier_invalid")
    return candidate


def _strict_hash(candidate: object) -> str:
    if type(candidate) is not str or SHA256_PATTERN.fullmatch(candidate) is None:
        raise ValueError("provider_directory_rooted_graph_hash_invalid")
    return candidate


def _strict_text(candidate: object, maximum: int) -> str:
    if (
        type(candidate) is not str
        or not candidate
        or len(candidate) > maximum
        or candidate != candidate.strip()
        or any(not character.isprintable() for character in candidate)
    ):
        raise ValueError("provider_directory_rooted_graph_text_invalid")
    return candidate


__all__ = (
    "_canonical_json",
    "_sha256_text",
    "_strict_hash",
    "_strict_identifier",
    "_strict_text",
)
