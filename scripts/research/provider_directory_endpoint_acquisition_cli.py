"""Command-line entrypoint for the Provider Directory acquisition harness."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

try:
    from scripts.research import (
        provider_directory_endpoint_acquisition_harness as harness,
    )
except ModuleNotFoundError:
    import provider_directory_endpoint_acquisition_harness as harness


def parse_acquisition_arguments(
    argv: list[str] | None = None,
) -> argparse.Namespace:
    """Parse local manifest and operator-input controls."""

    parser = argparse.ArgumentParser(description=harness.__doc__)
    parser.add_argument("--manifest", type=Path, default=harness.DEFAULT_MANIFEST)
    parser.add_argument(
        "--entry",
        action="append",
        default=[],
        help="Include only this manifest entry; repeatable.",
    )
    parser.add_argument(
        "--operator-input",
        type=Path,
        help="Verify credential-free result records from a local JSON file.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        help="Write the generated plan or verification report as JSON.",
    )
    parser.add_argument("--validate-only", action="store_true")
    return parser.parse_args(argv)


def run_acquisition_cli(argv: list[str] | None = None) -> int:
    """Validate a manifest, emit operator inputs, or verify local results."""

    args = parse_acquisition_arguments(argv)
    if args.validate_only and args.operator_input:
        raise SystemExit("--validate-only cannot be combined with --operator-input")

    manifest = harness.load_manifest(args.manifest)
    selected_entry_ids = frozenset(args.entry)
    plan = harness.build_operator_plan(manifest, selected_entry_ids)
    if args.validate_only:
        output_by_field = {
            "valid": True,
            "entries": len(plan["entries"]),
            "manifest_sha256": plan["manifest_sha256"],
        }
        exit_code = 0
    elif args.operator_input:
        operator_input = harness.load_operator_input(args.operator_input)
        output_by_field = harness.evaluate_operator_input(
            manifest, operator_input, selected_entry_ids
        )
        exit_code = 0 if output_by_field["ok"] else 2
    else:
        output_by_field = plan
        exit_code = 0

    if args.output:
        harness.write_json(args.output, output_by_field)
    print(json.dumps(output_by_field, sort_keys=True))
    return exit_code


if __name__ == "__main__":
    raise SystemExit(run_acquisition_cli())
