#!/usr/bin/env python
# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Audit whether raw TiC artifacts directly contain provider address fields."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from process.ptg_parts.provider_location_evidence import audit_tic_provider_location_evidence


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("paths", nargs="+", help="Raw TiC JSON, JSON.GZ, or ZIP artifact path(s).")
    parser.add_argument("--max-samples", type=int, default=5, help="Maximum direct-location samples per artifact.")
    parser.add_argument(
        "--skip-inline-provider-groups",
        action="store_true",
        help="Only inspect top-level provider_references; skip inline negotiated-rate provider_groups.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    summaries = []
    exit_code = 0
    for raw_path in args.paths:
        path = Path(raw_path)
        if not path.exists():
            summaries.append({"path": str(path), "error": "file_not_found"})
            exit_code = 2
            continue
        try:
            summaries.append(
                audit_tic_provider_location_evidence(
                    path,
                    max_samples=max(args.max_samples, 0),
                    scan_inline_provider_groups=not args.skip_inline_provider_groups,
                )
            )
        except Exception as exc:  # pragma: no cover - CLI safety net.
            summaries.append({"path": str(path), "error": type(exc).__name__, "message": str(exc)})
            exit_code = 1
    json.dump(summaries if len(summaries) != 1 else summaries[0], sys.stdout, indent=2, sort_keys=True)
    sys.stdout.write("\n")
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
