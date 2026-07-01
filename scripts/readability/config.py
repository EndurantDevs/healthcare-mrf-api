"""Configuration helpers and naming defaults for readability checks."""

from __future__ import annotations

import fnmatch
import re
from typing import Any, Iterable

DEFAULT_AMBIGUOUS_FUNCTION_NAMES = {
    "build",
    "callback",
    "do",
    "execute",
    "get",
    "get_all",
    "handle",
    "helper",
    "load",
    "main",
    "parse",
    "process",
    "process_data",
    "run",
    "save",
    "shutdown",
    "start",
}
DEFAULT_AMBIGUOUS_VARIABLE_NAMES = {
    "data",
    "item",
    "items",
    "obj",
    "payload",
    "record",
    "records",
    "res",
    "result",
    "results",
    "row",
    "rows",
    "source",
    "sources",
    "target",
    "targets",
    "tmp",
    "value",
    "values",
}
DEFAULT_BOOLEAN_PREFIXES = (
    "can_",
    "has_",
    "include_",
    "includes_",
    "is_",
    "needs_",
    "should_",
    "supports_",
    "use_",
    "uses_",
)
DEFAULT_ALLOWED_SHORT_NAMES = {"_", "i", "j", "k", "n", "x", "y", "z"}
DEFAULT_ALWAYS_BAD_SHORT_NAMES = {"l", "O"}
DEFAULT_COLLECTION_SINGULAR_EXCEPTIONS = {
    "data",
    "metadata",
    "payload",
}
DEFAULT_DICT_NAME_MARKERS = (
    "by_",
    "_by_",
    "_dict",
    "_index",
    "_lookup",
    "_map",
    "_registry",
)
DEFAULT_COMMENT_NOISE_PATTERNS = (
    r"^(?:increment|decrement|return|returns|loop|iterate|assign|set|get|create|call|print)\b",
    r"^(?:initialize|initialise) (?:variable|counter|list|dict|dictionary|set)\b",
)


def compile_suppression_patterns(config: dict[str, Any]) -> list[tuple[str, re.Pattern[str]]]:
    """Compile inline suppression patterns from readability config."""
    patterns = []
    for pattern in config.get("inline_suppression_patterns", []):
        patterns.append((pattern["name"], re.compile(pattern["pattern"])))
    return patterns


def is_matching_path_pattern(path: str, patterns: Iterable[str]) -> bool:
    """Return true when a path matches at least one configured glob."""
    return any(fnmatch.fnmatch(path, pattern) for pattern in patterns)


def readability_options(config: dict[str, Any]) -> dict[str, Any]:
    """Return optional readability rule overrides."""
    return config.get("readability", {})


def threshold(config: dict[str, Any], name: str, default: int) -> int:
    """Read an integer threshold from the supported config locations."""
    value = config.get("thresholds", {}).get(name)
    if value is None:
        value = readability_options(config).get(name, default)
    return int(value)


def name_list(config: dict[str, Any], name: str, default: Iterable[str]) -> set[str]:
    """Read a configured name list or return its default set."""
    configured = readability_options(config).get(name)
    if configured is None:
        return set(default)
    return set(configured)


def name_prefixes(config: dict[str, Any], name: str, default: Iterable[str]) -> tuple[str, ...]:
    """Read configured name prefixes or return their default tuple."""
    configured = readability_options(config).get(name)
    if configured is None:
        return tuple(default)
    return tuple(configured)


def split_name_tokens(name: str) -> list[str]:
    """Split snake-case and CamelCase identifiers into lowercase tokens."""
    cleaned = name.strip("_")
    if not cleaned:
        return []
    parts: list[str] = []
    for token in cleaned.split("_"):
        token_parts = re.findall(r"[A-Z]?[a-z]+|[A-Z]+(?=[A-Z]|$)|[0-9]+", token)
        parts.extend(part.lower() for part in token_parts if part)
    return parts or [cleaned.lower()]


def has_boolean_prefix(name: str, prefixes: Iterable[str]) -> bool:
    """Return true when an identifier starts with a predicate-style prefix."""
    normalized = name.strip("_")
    return any(normalized.startswith(prefix) for prefix in prefixes)


def is_boolean_method_name(name: str) -> bool:
    """Return true for known bool-returning builtins and predicate names."""
    normalized = name.strip("_")
    known_predicates = {
        "isalnum",
        "isalpha",
        "isascii",
        "isdecimal",
        "isdigit",
        "isidentifier",
        "islower",
        "isnumeric",
        "isspace",
        "istitle",
        "isupper",
        "empty",
        "full",
        "in_transaction",
        "startswith",
        "endswith",
    }
    return normalized in known_predicates or normalized.startswith(("is_", "has_"))
