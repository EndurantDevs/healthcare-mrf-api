# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Core carrier matching and coverage aggregation for the PTG audit."""

from __future__ import annotations

import json
import re
import sys
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping, Sequence

_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from process import mrf_source_discovery as discovery


DEFAULT_LINE_COLUMNS: tuple[tuple[str, str], ...] = (
    ("medical", "MEDICAL_CARRIERS"),
    ("dental", "DENTAL_CARRIERS"),
    ("vision", "VISION_CARRIERS"),
)

PLACEHOLDER_RE = re.compile(
    r"^(n/?a|na|none|no|not\s+offered|no\s+coverage|waived|unknown|tbd|--|-|"
    r"self[-\s]*administered|self[-\s]*funded|employer[-\s]*sponsored)$",
    re.IGNORECASE,
)


@dataclass(frozen=True)
class CarrierCoverageStats:
    line: str
    column: str
    mentions_total: int = 0
    placeholders: int = 0
    importable_mentions: int = 0
    catalog_mentions: int = 0
    unmatched_mentions: int = 0
    distinct_total: int = 0
    distinct_importable: int = 0
    distinct_catalog: int = 0
    distinct_unmatched: int = 0

    def to_dict(self) -> dict[str, Any]:
        """Return these coverage counters as a JSON-ready mapping."""
        return {
            "line": self.line,
            "column": self.column,
            "mentions_total": self.mentions_total,
            "placeholders": self.placeholders,
            "importable_mentions": self.importable_mentions,
            "catalog_mentions": self.catalog_mentions,
            "unmatched_mentions": self.unmatched_mentions,
            "distinct_total": self.distinct_total,
            "distinct_importable": self.distinct_importable,
            "distinct_catalog": self.distinct_catalog,
            "distinct_unmatched": self.distinct_unmatched,
        }


Matcher = Callable[[Any, str], bool]


def _source_tier(candidate: Any) -> str:
    return str(getattr(candidate, "source_tier", "") or "").strip().lower()


def _source_status(candidate: Any) -> str:
    return str(getattr(candidate, "status", "") or "").strip().lower()


def non_importable_reason_for_matches(matches: Sequence[Any]) -> str:
    """Classify why catalog evidence for a carrier is not importable."""
    if not matches:
        return "unmatched"
    if any(_source_status(candidate) == "archived" for candidate in matches):
        return "archived_source"
    if any(_source_tier(candidate) == "coverage_evidence" for candidate in matches):
        return "coverage_evidence_only"
    if any(_source_tier(candidate) == "directory_evidence" for candidate in matches):
        return "directory_evidence_only"
    if any(
        getattr(candidate, "index_url", None) or getattr(candidate, "human_url", None)
        for candidate in matches
    ):
        return "not_importable_by_policy"
    return "no_public_source_url"


def split_carrier_cell(value: str | None) -> list[str]:
    """Split one carrier cell without treating commas inside names as separators."""
    text = str(value or "").strip()
    if not text:
        return []
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        parsed = None
    if isinstance(parsed, list):
        return [str(item).strip() for item in parsed if str(item).strip()]
    return [
        part.strip(" \t-*")
        for part in re.split(r"\r?\n|;", text)
        if part.strip(" \t-*")
    ]


def normalize_carrier(value: str) -> str:
    """Normalize a carrier label for case-insensitive matching."""
    return re.sub(r"\s+", " ", str(value or "").strip().lower())


def is_placeholder_carrier(value: str) -> bool:
    """Return whether a carrier label is a placeholder value."""
    return bool(PLACEHOLDER_RE.match(normalize_carrier(value)))


def is_discovery_candidate_match(candidate: Any, carrier: str) -> bool:
    """Return whether discovery's payer text filter matches the carrier label."""
    return discovery._is_candidate_text_filter_match(
        candidate,
        entity_types=(),
        payer_query=carrier,
    )


def supports_candidate_benefit_line(candidate: Any, line: str) -> bool:
    """Return whether a candidate can cover the requested benefit line."""
    benefit_lines = getattr(candidate, "benefit_lines", None)
    if not benefit_lines:
        return True
    values = benefit_lines if isinstance(benefit_lines, (list, tuple, set)) else [benefit_lines]
    target = normalize_carrier(line)
    for value in values:
        normalized = normalize_carrier(str(value).replace("_", " ").replace("-", " "))
        if not normalized:
            continue
        tokens = set(re.split(r"[^a-z0-9]+", normalized))
        if target == normalized or target in tokens:
            return True
    return False


def has_catalog_source_candidate(candidate: Any) -> bool:
    """Return whether a candidate can appear as a source in the admin catalog."""
    return bool(
        getattr(candidate, "index_url", None)
        or getattr(candidate, "human_url", None)
    )


def _iter_carrier_mentions(
    client_rows: Iterable[Mapping[str, str]], column: str
) -> Iterable[tuple[str, str, bool]]:
    for client_row in client_rows:
        for carrier_label in split_carrier_cell(client_row.get(column)):
            if is_placeholder_carrier(carrier_label):
                yield carrier_label, "", True
                continue
            yield carrier_label, normalize_carrier(carrier_label), False


def _has_cached_carrier_match(
    cache_by_carrier: dict[str, bool],
    carrier_key: str,
    carrier_label: str,
    source_candidates: Sequence[Any],
    matcher: Matcher,
) -> bool:
    if carrier_key not in cache_by_carrier:
        cache_by_carrier[carrier_key] = any(
            matcher(candidate, carrier_label) for candidate in source_candidates
        )
    return cache_by_carrier[carrier_key]


def _filter_candidates_by_line(
    source_candidates: Sequence[Any],
    line: str,
) -> list[Any]:
    return [
        candidate
        for candidate in source_candidates
        if supports_candidate_benefit_line(candidate, line)
    ]


def _collect_distinct_carrier_matches(
    csv_rows: Sequence[Mapping[str, str]],
    *,
    column: str,
    line_all_candidates: Sequence[Any],
    line_importable_candidates: Sequence[Any],
    matcher: Matcher,
) -> dict[str, dict[str, Any]]:
    distinct: dict[str, dict[str, Any]] = defaultdict(
        lambda: {"label": "", "count": 0, "importable": False, "catalog": False}
    )
    importable_match_cache_by_carrier: dict[str, bool] = {}
    catalog_match_cache_by_carrier: dict[str, bool] = {}

    for carrier_label, carrier_key, is_placeholder in _iter_carrier_mentions(
        csv_rows, column
    ):
        if is_placeholder:
            continue
        entry = distinct[carrier_key]
        entry["label"] = entry["label"] or carrier_label
        entry["count"] += 1
        is_importable = _has_cached_carrier_match(
            importable_match_cache_by_carrier,
            carrier_key,
            carrier_label,
            line_importable_candidates,
            matcher,
        )
        entry["importable"] = is_importable
        if is_importable:
            catalog_match_cache_by_carrier[carrier_key] = True
        entry["catalog"] = is_importable or _has_cached_carrier_match(
            catalog_match_cache_by_carrier,
            carrier_key,
            carrier_label,
            line_all_candidates,
            matcher,
        )
    return distinct


def _audit_carrier_line(
    csv_rows: Sequence[Mapping[str, str]],
    line: str,
    column: str,
    all_candidates: Sequence[Any],
    importable_candidates: Sequence[Any],
    matcher: Matcher,
) -> tuple[CarrierCoverageStats, list[tuple[str, int]]]:
    line_all_candidates = _filter_candidates_by_line(all_candidates, line)
    line_importable_candidates = _filter_candidates_by_line(
        importable_candidates,
        line,
    )
    carrier_mentions = list(_iter_carrier_mentions(csv_rows, column))
    distinct = _collect_distinct_carrier_matches(
        csv_rows,
        column=column,
        line_all_candidates=line_all_candidates,
        line_importable_candidates=line_importable_candidates,
        matcher=matcher,
    )
    unmatched = sorted(
        (
            (str(entry["label"]), int(entry["count"]))
            for entry in distinct.values()
            if not entry["catalog"]
        ),
        key=lambda item: (-item[1], item[0].lower()),
    )
    distinct_catalog = sum(1 for entry in distinct.values() if entry["catalog"])
    return CarrierCoverageStats(
        line=line,
        column=column,
        mentions_total=len(carrier_mentions),
        placeholders=sum(1 for _label, _key, is_placeholder in carrier_mentions if is_placeholder),
        importable_mentions=sum(
            int(entry["count"]) for entry in distinct.values() if entry["importable"]
        ),
        catalog_mentions=sum(
            int(entry["count"]) for entry in distinct.values() if entry["catalog"]
        ),
        unmatched_mentions=sum(count for _label, count in unmatched),
        distinct_total=len(distinct),
        distinct_importable=sum(1 for entry in distinct.values() if entry["importable"]),
        distinct_catalog=distinct_catalog,
        distinct_unmatched=len(distinct) - distinct_catalog,
    ), unmatched


def audit_carrier_rows(
    client_rows: Iterable[Mapping[str, str]],
    *,
    all_candidates: Sequence[Any],
    importable_candidates: Sequence[Any],
    line_columns: Sequence[tuple[str, str]] = DEFAULT_LINE_COLUMNS,
    matcher: Matcher = is_discovery_candidate_match,
) -> tuple[list[CarrierCoverageStats], dict[str, list[tuple[str, int]]]]:
    """Classify carrier mentions by benefit line and return unmatched counts."""
    coverage_stats_list: list[CarrierCoverageStats] = []
    unmatched_by_line: dict[str, list[tuple[str, int]]] = {}
    csv_rows = list(client_rows)

    for line, column in line_columns:
        line_stats, unmatched = _audit_carrier_line(
            csv_rows,
            line,
            column,
            all_candidates,
            importable_candidates,
            matcher,
        )
        unmatched_by_line[line] = unmatched
        coverage_stats_list.append(line_stats)

    return coverage_stats_list, unmatched_by_line


def audit_non_importable_carrier_rows(
    client_rows: Iterable[Mapping[str, str]],
    *,
    all_candidates: Sequence[Any],
    importable_candidates: Sequence[Any],
    line_columns: Sequence[tuple[str, str]] = DEFAULT_LINE_COLUMNS,
    matcher: Matcher = is_discovery_candidate_match,
) -> dict[str, list[tuple[str, int]]]:
    """Return carrier labels that have catalog evidence but no importable source."""
    csv_rows = list(client_rows)
    non_importable_by_line: dict[str, list[tuple[str, int]]] = {}

    for line, column in line_columns:
        distinct = _collect_distinct_carrier_matches(
            csv_rows,
            column=column,
            line_all_candidates=_filter_candidates_by_line(all_candidates, line),
            line_importable_candidates=_filter_candidates_by_line(
                importable_candidates, line
            ),
            matcher=matcher,
        )

        non_importable_by_line[line] = sorted(
            (
                (str(entry["label"]), int(entry["count"]))
                for entry in distinct.values()
                if entry["catalog"] and not entry["importable"]
            ),
            key=lambda item: (-item[1], item[0].lower()),
        )

    return non_importable_by_line


def audit_non_importable_reason_summary(
    client_rows: Iterable[Mapping[str, str]],
    *,
    all_candidates: Sequence[Any],
    importable_candidates: Sequence[Any],
    line_columns: Sequence[tuple[str, str]] = DEFAULT_LINE_COLUMNS,
    matcher: Matcher = is_discovery_candidate_match,
) -> dict[str, dict[str, dict[str, int]]]:
    """Return aggregate-only reasons for catalog matches lacking importable sources."""
    csv_rows = list(client_rows)
    summary_by_line: dict[str, dict[str, dict[str, int]]] = {}

    for line, column in line_columns:
        line_all_candidates = _filter_candidates_by_line(all_candidates, line)
        line_importable_candidates = _filter_candidates_by_line(importable_candidates, line)
        distinct = _collect_distinct_carrier_matches(
            csv_rows,
            column=column,
            line_all_candidates=line_all_candidates,
            line_importable_candidates=line_importable_candidates,
            matcher=matcher,
        )
        reason_summary: dict[str, dict[str, int]] = defaultdict(
            lambda: {"distinct": 0, "mentions": 0}
        )
        for entry in distinct.values():
            if not entry["catalog"] or entry["importable"]:
                continue
            matches = [
                candidate
                for candidate in line_all_candidates
                if matcher(candidate, str(entry["label"]))
            ]
            reason = non_importable_reason_for_matches(matches)
            reason_summary[reason]["distinct"] += 1
            reason_summary[reason]["mentions"] += int(entry["count"])
        summary_by_line[line] = dict(sorted(reason_summary.items()))

    return summary_by_line
