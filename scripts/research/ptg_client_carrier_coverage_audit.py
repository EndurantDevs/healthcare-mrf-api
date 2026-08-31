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

from scripts.research.ptg_client_carrier_coverage_audit_core import (
    DEFAULT_LINE_COLUMNS,
    PLACEHOLDER_RE,
    CarrierCoverageStats,
    Matcher,
    _audit_carrier_line,
    _collect_distinct_carrier_matches,
    _filter_candidates_by_line,
    _has_cached_carrier_match,
    _iter_carrier_mentions,
    _source_status,
    _source_tier,
    audit_carrier_rows,
    audit_non_importable_carrier_rows,
    audit_non_importable_reason_summary,
    has_catalog_source_candidate,
    is_discovery_candidate_match,
    is_placeholder_carrier,
    non_importable_reason_for_matches,
    normalize_carrier,
    split_carrier_cell,
    supports_candidate_benefit_line,
)


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
    importable_candidates = [
        candidate
        for candidate in catalog_candidates
        if discovery._is_candidate_importable_source(candidate)
    ]
    return catalog_candidates, importable_candidates


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
    matcher: Matcher = is_discovery_candidate_match,
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
    client_carrier_rows = read_csv_rows(args.csv_path)
    all_candidates, importable_candidates = await load_discovery_candidates(
        provider=args.provider,
        limit=args.candidate_limit,
    )
    stats, unmatched = audit_carrier_rows(
        client_carrier_rows,
        all_candidates=all_candidates,
        importable_candidates=importable_candidates,
    )
    report_by_field = {
        "csv_path": "<redacted>" if args.redact_labels else str(args.csv_path),
        "rows": len(client_carrier_rows),
        "provider": args.provider,
        "candidates": len(all_candidates),
        "importable_candidates": len(importable_candidates),
        "coverage": [coverage_stat.to_dict() for coverage_stat in stats],
    }
    if args.json:
        report_by_field["non_importable_reason_summary"] = (
            audit_non_importable_reason_summary(
                client_carrier_rows,
                all_candidates=all_candidates,
                importable_candidates=importable_candidates,
            )
        )
    _add_optional_report_sections(
        report_by_field,
        parsed_args=args,
        client_rows=client_carrier_rows,
        all_candidates=all_candidates,
        importable_candidates=importable_candidates,
        unmatched=unmatched,
    )
    if args.json:
        print(json.dumps(report_by_field, indent=2, sort_keys=True))
    else:
        _print_human_report(
            report_by_field,
            show_unmatched=args.show_unmatched,
            show_non_importable=args.show_non_importable,
        )
    return 0


def main() -> int:
    """Run the asynchronous carrier audit and return its exit code."""
    return asyncio.run(async_main())


if __name__ == "__main__":
    raise SystemExit(main())
