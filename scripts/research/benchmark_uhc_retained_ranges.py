#!/usr/bin/env python3
"""Measure fresh UHC retained-range admission, including process startup and fsync."""

from __future__ import annotations

import argparse
import hashlib
import json
import statistics
import subprocess
import tempfile
import time
from pathlib import Path


CONTRACT_ID = "healthporta.uhc.retained-json-ranges.v2"


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        while chunk := source.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--scanner", type=Path, required=True)
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--expected-sha256", required=True)
    parser.add_argument("--expected-byte-count", type=int, required=True)
    parser.add_argument("--range-count", type=int, default=4)
    parser.add_argument("--trials", type=int, default=8)
    parser.add_argument("--minimum-rows-per-second", type=float, default=100_000.0)
    return parser.parse_args()


def validate_inputs(args: argparse.Namespace) -> None:
    if not args.scanner.is_file():
        raise SystemExit(f"scanner is not a file: {args.scanner}")
    if not args.source.is_file():
        raise SystemExit(f"source is not a file: {args.source}")
    if args.trials < 1:
        raise SystemExit("--trials must be positive")
    observed_bytes = args.source.stat().st_size
    if observed_bytes != args.expected_byte_count:
        raise SystemExit(
            f"source byte count mismatch: {observed_bytes} != {args.expected_byte_count}"
        )
    observed_sha256 = sha256_file(args.source)
    if observed_sha256 != args.expected_sha256:
        raise SystemExit(
            f"source SHA-256 mismatch: {observed_sha256} != {args.expected_sha256}"
        )


def run_trial(args: argparse.Namespace, trial: int) -> dict[str, object]:
    with tempfile.TemporaryDirectory(prefix="uhc-retained-cold-") as temporary:
        output_root = Path(temporary)
        started = time.perf_counter()
        completed = subprocess.run(
            [
                str(args.scanner),
                "--uhc-retain",
                str(args.source),
                str(output_root),
                args.expected_sha256,
                str(args.expected_byte_count),
                str(args.range_count),
            ],
            check=False,
            capture_output=True,
            text=True,
        )
        wall_seconds = time.perf_counter() - started
        if completed.returncode != 0:
            raise SystemExit(
                f"trial {trial} failed ({completed.returncode}): {completed.stderr.strip()}"
            )
        stdout_lines = completed.stdout.splitlines()
        if len(stdout_lines) != 1:
            raise SystemExit(f"trial {trial} emitted {len(stdout_lines)} stdout lines")
        summary = json.loads(stdout_lines[0])
        expected_fields = {
            "record_kind": "uhc_retained_summary",
            "contract_id": CONTRACT_ID,
            "contract_version": 2,
            "raw_artifact_sha256": args.expected_sha256,
            "raw_artifact_byte_count": args.expected_byte_count,
            "range_count": args.range_count,
            "raw_reused": False,
            "manifest_reused": False,
        }
        for field, expected in expected_fields.items():
            if summary.get(field) != expected:
                raise SystemExit(
                    f"trial {trial} summary mismatch for {field}: "
                    f"{summary.get(field)!r} != {expected!r}"
                )
        record_count = summary.get("record_count")
        if not isinstance(record_count, int) or record_count <= 0:
            raise SystemExit(f"trial {trial} reported invalid record_count: {record_count!r}")
        for field in ("raw_artifact_path", "manifest_path"):
            artifact_path = Path(summary[field]).resolve()
            if output_root.resolve() not in artifact_path.parents or not artifact_path.is_file():
                raise SystemExit(f"trial {trial} reported invalid {field}: {artifact_path}")
        rows_per_second = record_count / wall_seconds
        result = {
            "record_kind": "uhc_retained_benchmark_trial",
            "trial": trial,
            "record_count": record_count,
            "wall_seconds": wall_seconds,
            "rows_per_second": rows_per_second,
            "native_total_seconds": summary["timings_seconds"]["total"],
        }
        print(json.dumps(result, separators=(",", ":")), flush=True)
        return result


def main() -> None:
    args = parse_args()
    validate_inputs(args)
    results = [run_trial(args, trial) for trial in range(1, args.trials + 1)]
    rates = [float(result["rows_per_second"]) for result in results]
    walls = [float(result["wall_seconds"]) for result in results]
    summary = {
        "record_kind": "uhc_retained_benchmark_summary",
        "trials": args.trials,
        "minimum_required_rows_per_second": args.minimum_rows_per_second,
        "rows_per_second": {
            "minimum": min(rates),
            "median": statistics.median(rates),
            "maximum": max(rates),
        },
        "wall_seconds": {
            "minimum": min(walls),
            "median": statistics.median(walls),
            "maximum": max(walls),
        },
        "passed": min(rates) >= args.minimum_rows_per_second,
    }
    print(json.dumps(summary, separators=(",", ":")), flush=True)
    if not summary["passed"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
