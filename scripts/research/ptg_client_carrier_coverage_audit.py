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
    return re.sub(r"\s+", " ", str(value or "").strip().lower())


def is_placeholder_carrier(value: str) -> bool:
    return bool(PLACEHOLDER_RE.match(normalize_carrier(value)))


def discovery_candidate_matches(candidate: Any, carrier: str) -> bool:
    return discovery._candidate_matches_text_filters(
        candidate,
        entity_types=(),
        payer_query=carrier,
    )


def candidate_supports_benefit_line(candidate: Any, line: str) -> bool:
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


def audit_carrier_rows(
    client_rows: Iterable[Mapping[str, str]],
    *,
    all_candidates: Sequence[Any],
    importable_candidates: Sequence[Any],
    line_columns: Sequence[tuple[str, str]] = DEFAULT_LINE_COLUMNS,
    matcher: Matcher = discovery_candidate_matches,
) -> tuple[list[CarrierCoverageStats], dict[str, list[tuple[str, int]]]]:
    stats_by_line: list[CarrierCoverageStats] = []
    unmatched_by_line: dict[str, list[tuple[str, int]]] = {}
    csv_rows = list(client_rows)

    for line, column in line_columns:
        line_all_candidates = [
            candidate
            for candidate in all_candidates
            if candidate_supports_benefit_line(candidate, line)
        ]
        line_importable_candidates = [
            candidate
            for candidate in importable_candidates
            if candidate_supports_benefit_line(candidate, line)
        ]
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


def read_csv_rows(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8-sig") as handle:
        return list(csv.DictReader(handle))


async def load_discovery_candidates(
    *,
    provider: str,
    limit: int,
) -> tuple[list[Any], list[Any]]:
    candidates = await discovery._load_candidates(provider, test_mode=True, limit=limit)
    importable = [
        candidate
        for candidate in candidates
        if discovery._candidate_is_importable_source(candidate)
    ]
    return list(candidates), importable


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Audit private client carrier coverage against PTG source discovery."
    )
    parser.add_argument("csv_path", type=Path, help="Path to the private client/carrier CSV.")
    parser.add_argument("--provider", default="master-list", help="Source-discovery provider to load.")
    parser.add_argument("--candidate-limit", type=int, default=5000)
    parser.add_argument(
        "--show-unmatched",
        action="store_true",
        help="Print unmatched carrier labels. Keep output local because labels come from the private CSV.",
    )
    parser.add_argument("--top-unmatched", type=int, default=20)
    parser.add_argument("--json", action="store_true", help="Emit machine-readable JSON.")
    return parser


async def async_main(argv: Sequence[str] | None = None) -> int:
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
        "csv_path": str(args.csv_path),
        "rows": len(rows),
        "provider": args.provider,
        "candidates": len(all_candidates),
        "importable_candidates": len(importable_candidates),
        "coverage": [item.to_dict() for item in stats],
    }
    if args.show_unmatched:
        payload["top_unmatched"] = {
            line: [{"carrier": label, "mentions": count} for label, count in items[: args.top_unmatched]]
            for line, items in unmatched.items()
        }
    if args.json:
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        print(
            f"rows={payload['rows']} candidates={payload['candidates']} "
            f"importable_candidates={payload['importable_candidates']}"
        )
        for item in stats:
            print(
                f"{item.line}: importable {item.importable_mentions}/{item.mentions_total} mentions, "
                f"{item.distinct_importable}/{item.distinct_total} distinct; "
                f"catalog/evidence {item.catalog_mentions}/{item.mentions_total} mentions, "
                f"{item.distinct_catalog}/{item.distinct_total} distinct; "
                f"unmatched {item.unmatched_mentions} mentions, {item.distinct_unmatched} distinct"
            )
        if args.show_unmatched:
            for line, items in unmatched.items():
                print(f"{line} top unmatched:")
                for label, count in items[: args.top_unmatched]:
                    print(f"  {count:>4}  {label}")
    return 0


def main() -> int:
    return asyncio.run(async_main())


if __name__ == "__main__":
    raise SystemExit(main())
