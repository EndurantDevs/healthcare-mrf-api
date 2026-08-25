#!/usr/bin/env python3
# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Generate and verify equivalent synthetic hospital-price MRF inputs."""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
if str(REPOSITORY_ROOT) not in sys.path:
    sys.path.insert(0, str(REPOSITORY_ROOT))

from scripts.research.hospital_hpt_corpus import (
    Hospital,
    PriceFact,
    _filename,
    _group_facts,
    build_corpus,
    semantic_digest,
    write_json,
)
from scripts.research.hospital_hpt_codecs import READERS
from scripts.research.hospital_hpt_csv import (
    write_tall_csv,
    write_wide_csv,
)


def generate(
    output_dir: Path, *, hospitals: int, facts_per_hospital: int, payers: int
) -> dict[str, object]:
    """Write all three source formats and verify their exact parity."""
    hospital_rows, facts = build_corpus(
        hospitals=hospitals, facts_per_hospital=facts_per_hospital, payers=payers
    )
    facts_by_hospital = _group_facts(facts)
    entries = []
    for format_name in READERS:
        (output_dir / format_name).mkdir(parents=True, exist_ok=True)
    for hospital in hospital_rows:
        json_path = output_dir / "json" / _filename(hospital, "json")
        tall_path = output_dir / "tall_csv" / _filename(hospital, "csv")
        wide_path = output_dir / "wide_csv" / _filename(hospital, "csv")
        write_json(json_path, hospital, facts_by_hospital[hospital.hospital_id])
        write_tall_csv(tall_path, hospital, facts_by_hospital[hospital.hospital_id])
        write_wide_csv(wide_path, hospital, facts_by_hospital[hospital.hospital_id])
        entries.append(
            {
                "hospital_id": hospital.hospital_id,
                "json": str(json_path.relative_to(output_dir)),
                "tall_csv": str(tall_path.relative_to(output_dir)),
                "wide_csv": str(wide_path.relative_to(output_dir)),
            }
        )
    manifest_by_field = {
        "schema_version": 1,
        "hospitals": hospitals,
        "facts": len(facts),
        "payers": payers,
        "semantic_sha256": semantic_digest(hospital_rows, facts),
        "entries": entries,
    }
    (output_dir / "manifest.json").write_text(
        json.dumps(manifest_by_field, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return verify(output_dir / "manifest.json")


def verify(manifest_path: Path) -> dict[str, object]:
    """Verify every encoded format against the manifest digest and counts."""
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    root = manifest_path.parent
    formats_by_name = {}
    for format_name, reader in READERS.items():
        started = time.perf_counter()
        hospitals: list[Hospital] = []
        facts: list[PriceFact] = []
        input_bytes = 0
        for entry in manifest["entries"]:
            input_path = root / entry[format_name]
            hospital, hospital_facts = reader(
                input_path, entry["hospital_id"]
            )
            hospitals.append(hospital)
            facts.extend(hospital_facts)
            input_bytes += input_path.stat().st_size
        digest = semantic_digest(hospitals, facts)
        elapsed_seconds = time.perf_counter() - started
        formats_by_name[format_name] = {
            "hospitals": len(hospitals),
            "facts": len(facts),
            "semantic_sha256": digest,
            "input_bytes": input_bytes,
            "parse_seconds": elapsed_seconds,
            "facts_per_second": len(facts) / elapsed_seconds,
        }
        if (
            len(hospitals) != manifest["hospitals"]
            or len(facts) != manifest["facts"]
            or digest != manifest["semantic_sha256"]
        ):
            raise ValueError(f"{format_name} does not match the canonical corpus")
    return {
        "status": "passed",
        "manifest": str(manifest_path),
        "formats": formats_by_name,
    }


def build_parser() -> argparse.ArgumentParser:
    """Build the corpus experiment command-line parser."""
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    generate_parser = subparsers.add_parser("generate")
    generate_parser.add_argument("--output-dir", type=Path, required=True)
    generate_parser.add_argument("--hospitals", type=int, default=3)
    generate_parser.add_argument("--facts-per-hospital", type=int, default=100)
    generate_parser.add_argument("--payers", type=int, default=5)
    verify_parser = subparsers.add_parser("verify")
    verify_parser.add_argument("--manifest", type=Path, required=True)
    benchmark_parser = subparsers.add_parser("benchmark")
    benchmark_parser.add_argument("--manifest", type=Path, required=True)
    benchmark_parser.add_argument(
        "--postgres-dsn", default=os.getenv("HLTHPRT_HPT_BENCHMARK_POSTGRES_DSN")
    )
    benchmark_parser.add_argument("--work-dir", type=Path, required=True)
    benchmark_parser.add_argument("--report", type=Path, required=True)
    benchmark_parser.add_argument("--trials", type=int, default=5)
    benchmark_parser.add_argument("--warmup-trials", type=int, default=1)
    benchmark_parser.add_argument("--query-iterations", type=int, default=5)
    benchmark_parser.add_argument(
        "--format", choices=tuple(READERS), default="json", dest="format_name"
    )
    return parser


def _run_command(args: argparse.Namespace) -> dict[str, object]:
    """Run the parsed research command and persist its optional report."""
    if args.command == "generate":
        return generate(
            args.output_dir,
            hospitals=args.hospitals,
            facts_per_hospital=args.facts_per_hospital,
            payers=args.payers,
        )
    if args.command == "verify":
        return verify(args.manifest)
    if not args.postgres_dsn:
        raise SystemExit(
            "provide --postgres-dsn or HLTHPRT_HPT_BENCHMARK_POSTGRES_DSN"
        )
    from scripts.research.hospital_hpt_postgres import run_benchmark

    benchmark_receipt = run_benchmark(
        args.postgres_dsn,
        args.manifest,
        args.work_dir,
        measured_trials=args.trials,
        warmup_trials=args.warmup_trials,
        query_iterations=args.query_iterations,
        format_name=args.format_name,
    )
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(
        json.dumps(benchmark_receipt, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return benchmark_receipt


def main(argv: list[str] | None = None) -> int:
    """Run the hospital-price research CLI."""
    command_receipt = _run_command(build_parser().parse_args(argv))
    print(json.dumps(command_receipt, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
