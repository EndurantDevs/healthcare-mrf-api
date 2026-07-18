#!/usr/bin/env python3
# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Audit client carrier coverage against PTG source-discovery candidates.

This script is intentionally local/research-only. Pass the private client CSV at
runtime; do not commit that CSV or paste its row data into tests.
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import hashlib
import json
import re
import sys
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping, Sequence

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

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


def discovery_candidate_matches(candidate: Any, carrier: str) -> bool:
    """Return whether discovery's payer text filter matches the carrier label."""
    return discovery._is_candidate_text_filter_match(
        candidate,
        entity_types=(),
        payer_query=carrier,
    )


def candidate_supports_benefit_line(candidate: Any, line: str) -> bool:
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
        if candidate_supports_benefit_line(candidate, line)
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
        entry["importable"] = _has_cached_carrier_match(
            importable_match_cache_by_carrier,
            carrier_key,
            carrier_label,
            line_importable_candidates,
            matcher,
        )
        entry["catalog"] = _has_cached_carrier_match(
            catalog_match_cache_by_carrier,
            carrier_key,
            carrier_label,
            line_all_candidates,
            matcher,
        )
    return distinct


def audit_carrier_rows(
    client_rows: Iterable[Mapping[str, str]],
    *,
    all_candidates: Sequence[Any],
    importable_candidates: Sequence[Any],
    line_columns: Sequence[tuple[str, str]] = DEFAULT_LINE_COLUMNS,
    matcher: Matcher = discovery_candidate_matches,
) -> tuple[list[CarrierCoverageStats], dict[str, list[tuple[str, int]]]]:
    """Classify carrier mentions by benefit line and return unmatched counts."""
    stats_by_line: list[CarrierCoverageStats] = []
    unmatched_by_line: dict[str, list[tuple[str, int]]] = {}
    csv_rows = list(client_rows)

    for line, column in line_columns:
        line_all_candidates = _filter_candidates_by_line(all_candidates, line)
        line_importable_candidates = _filter_candidates_by_line(importable_candidates, line)
        mentions_total = 0
        placeholders = 0
        importable_mentions = 0
        catalog_mentions = 0
        unmatched_mentions = 0
        distinct: dict[str, dict[str, Any]] = defaultdict(
            lambda: {"label": "", "count": 0, "importable": False, "catalog": False}
        )
        importable_match_cache_by_carrier: dict[str, bool] = {}
        catalog_match_cache_by_carrier: dict[str, bool] = {}

        for carrier_label, carrier_key, is_placeholder in _iter_carrier_mentions(
            csv_rows, column
        ):
            mentions_total += 1
            if is_placeholder:
                placeholders += 1
                continue
            entry = distinct[carrier_key]
            entry["label"] = entry["label"] or carrier_label
            entry["count"] += 1
            has_importable_match = _has_cached_carrier_match(
                importable_match_cache_by_carrier,
                carrier_key,
                carrier_label,
                line_importable_candidates,
                matcher,
            )
            if has_importable_match:
                catalog_match_cache_by_carrier[carrier_key] = True
            has_catalog_match = catalog_match_cache_by_carrier.get(carrier_key)
            if has_catalog_match is None:
                has_catalog_match = _has_cached_carrier_match(
                    catalog_match_cache_by_carrier,
                    carrier_key,
                    carrier_label,
                    line_all_candidates,
                    matcher,
                )
            if has_importable_match:
                importable_mentions += 1
                entry["importable"] = True
            if has_catalog_match:
                catalog_mentions += 1
                entry["catalog"] = True
            if not has_catalog_match:
                unmatched_mentions += 1

        distinct_importable = sum(1 for entry in distinct.values() if entry["importable"])
        distinct_catalog = sum(1 for entry in distinct.values() if entry["catalog"])
        unmatched = sorted(
            (
                (str(entry["label"]), int(entry["count"]))
                for entry in distinct.values()
                if not entry["catalog"]
            ),
            key=lambda item: (-item[1], item[0].lower()),
        )
        unmatched_by_line[line] = unmatched
        stats_by_line.append(
            CarrierCoverageStats(
                line=line,
                column=column,
                mentions_total=mentions_total,
                placeholders=placeholders,
                importable_mentions=importable_mentions,
                catalog_mentions=catalog_mentions,
                unmatched_mentions=unmatched_mentions,
                distinct_total=len(distinct),
                distinct_importable=distinct_importable,
                distinct_catalog=distinct_catalog,
                distinct_unmatched=len(distinct) - distinct_catalog,
            )
        )

    return stats_by_line, unmatched_by_line


def audit_non_importable_carrier_rows(
    client_rows: Iterable[Mapping[str, str]],
    *,
    all_candidates: Sequence[Any],
    importable_candidates: Sequence[Any],
    line_columns: Sequence[tuple[str, str]] = DEFAULT_LINE_COLUMNS,
    matcher: Matcher = discovery_candidate_matches,
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
    matcher: Matcher = discovery_candidate_matches,
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


def read_csv_rows(path: Path) -> list[dict[str, str]]:
    """Read a UTF-8 CSV file into carrier-row mappings."""
    with path.open(newline="", encoding="utf-8-sig") as handle:
        return list(csv.DictReader(handle))

async def load_discovery_candidates(
    *,
    provider: str,
    limit: int,
) -> tuple[list[Any], list[Any]]:
    """Load catalog-backed discovery candidates and their importable subset."""
    candidates = await discovery._load_candidates(provider, test_mode=True, limit=limit)
    catalog_candidates = [
        candidate
        for candidate in candidates
        if has_catalog_source_candidate(candidate)
    ]
    importable = [
        candidate
        for candidate in catalog_candidates
        if discovery._is_candidate_importable_source(candidate)
    ]
    return catalog_candidates, importable


def build_arg_parser() -> argparse.ArgumentParser:
    """Build the command-line parser for the carrier coverage audit."""
    parser = argparse.ArgumentParser(
        description="Audit private client carrier coverage against PTG source discovery."
    )
    parser.add_argument("csv_path", type=Path, help="Private client/carrier CSV.")
    parser.add_argument("--provider", default="master-list", help="Discovery provider.")
    parser.add_argument("--candidate-limit", type=int, default=5000)
    parser.add_argument(
        "--show-unmatched",
        action="store_true",
        help="Print unmatched carrier labels. Keep output local.",
    )
    parser.add_argument("--top-unmatched", type=int, default=20)
    parser.add_argument(
        "--show-non-importable",
        action="store_true",
        help="Print catalog/evidence labels lacking importable sources. Keep output local.",
    )
    parser.add_argument("--top-non-importable", type=int, default=50)
    parser.add_argument("--json", action="store_true", help="Emit machine-readable JSON.")
    parser.add_argument("--redact-labels", action="store_true", help="Redact detail labels.")
    return parser


def _add_optional_report_sections(
    report_payload: dict[str, Any],
    *,
    parsed_args: argparse.Namespace,
    client_rows: Sequence[Mapping[str, str]],
    all_candidates: Sequence[Any],
    importable_candidates: Sequence[Any],
    unmatched: Mapping[str, Sequence[tuple[str, int]]],
    matcher: Matcher = discovery_candidate_matches,
) -> None:
    display_carrier = lambda label: (
        f"carrier:{hashlib.sha256(normalize_carrier(label).encode('utf-8')).hexdigest()[:12]}"
        if parsed_args.redact_labels
        else label
    )

    if parsed_args.show_unmatched:
        report_payload["top_unmatched"] = {
            benefit_line: [
                {"carrier": display_carrier(carrier_label), "mentions": mention_count}
                for carrier_label, mention_count in carrier_counts[
                    : parsed_args.top_unmatched
                ]
            ]
            for benefit_line, carrier_counts in unmatched.items()
        }
    if parsed_args.show_non_importable:
        non_importable = audit_non_importable_carrier_rows(
            client_rows,
            all_candidates=all_candidates,
            importable_candidates=importable_candidates,
            matcher=matcher,
        )
        report_payload["non_importable_reason_summary"] = (
            audit_non_importable_reason_summary(
                client_rows,
                all_candidates=all_candidates,
                importable_candidates=importable_candidates,
                matcher=matcher,
            )
        )
        report_payload["top_non_importable"] = {
            benefit_line: [
                {"carrier": display_carrier(carrier_label), "mentions": mention_count}
                for carrier_label, mention_count in carrier_counts[
                    : parsed_args.top_non_importable
                ]
            ]
            for benefit_line, carrier_counts in non_importable.items()
        }


def _print_human_report(
    report_payload: Mapping[str, Any],
    *,
    show_unmatched: bool,
    show_non_importable: bool,
) -> None:
    print(
        f"rows={report_payload['rows']} candidates={report_payload['candidates']} "
        f"importable_candidates={report_payload['importable_candidates']}"
    )
    for coverage_row in report_payload["coverage"]:
        print(
            f"{coverage_row['line']}: importable "
            f"{coverage_row['importable_mentions']}/{coverage_row['mentions_total']} mentions, "
            f"{coverage_row['distinct_importable']}/{coverage_row['distinct_total']} distinct; "
            f"catalog/evidence {coverage_row['catalog_mentions']}/"
            f"{coverage_row['mentions_total']} mentions, "
            f"{coverage_row['distinct_catalog']}/{coverage_row['distinct_total']} distinct; "
            f"unmatched {coverage_row['unmatched_mentions']} mentions, "
            f"{coverage_row['distinct_unmatched']} distinct"
        )
    if show_unmatched:
        for benefit_line, unmatched_rows in report_payload["top_unmatched"].items():
            print(f"{benefit_line} top unmatched:")
            for unmatched_row in unmatched_rows:
                print(f"  {unmatched_row['mentions']:>4}  {unmatched_row['carrier']}")
    if show_non_importable:
        print("non-importable reason summary:")
        for benefit_line, reason_counts_by_name in report_payload[
            "non_importable_reason_summary"
        ].items():
            reason_parts = [
                f"{reason}={reason_counts['mentions']} mentions/"
                f"{reason_counts['distinct']} distinct"
                for reason, reason_counts in reason_counts_by_name.items()
            ]
            print(f"  {benefit_line}: {', '.join(reason_parts) or 'none'}")
        for benefit_line, non_importable_rows in report_payload[
            "top_non_importable"
        ].items():
            print(f"{benefit_line} top non-importable:")
            for non_importable_row in non_importable_rows:
                print(
                    f"  {non_importable_row['mentions']:>4}  "
                    f"{non_importable_row['carrier']}"
                )


async def async_main(argv: Sequence[str] | None = None) -> int:
    """Run the audit command and emit its selected report format."""
    args = build_arg_parser().parse_args(argv)
    rows = read_csv_rows(args.csv_path)
    all_candidates, importable_candidates = await load_discovery_candidates(
        provider=args.provider,
        limit=args.candidate_limit,
    )
    stats, unmatched = audit_carrier_rows(
        rows,
        all_candidates=all_candidates,
        importable_candidates=importable_candidates,
    )
    payload = {
        "csv_path": "<redacted>" if args.redact_labels else str(args.csv_path),
        "rows": len(rows),
        "provider": args.provider,
        "candidates": len(all_candidates),
        "importable_candidates": len(importable_candidates),
        "coverage": [item.to_dict() for item in stats],
    }
    if args.json:
        payload["non_importable_reason_summary"] = (
            audit_non_importable_reason_summary(
                rows,
                all_candidates=all_candidates,
                importable_candidates=importable_candidates,
            )
        )
    _add_optional_report_sections(
        payload,
        parsed_args=args,
        client_rows=rows,
        all_candidates=all_candidates,
        importable_candidates=importable_candidates,
        unmatched=unmatched,
    )
    if args.json:
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        _print_human_report(
            payload,
            show_unmatched=args.show_unmatched,
            show_non_importable=args.show_non_importable,
        )
    return 0


def main() -> int:
    """Run the asynchronous carrier audit and return its exit code."""
    return asyncio.run(async_main())


if __name__ == "__main__":
    raise SystemExit(main())
