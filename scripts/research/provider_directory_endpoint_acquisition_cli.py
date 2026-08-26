"""Command-line entrypoint for the Provider Directory acquisition harness."""

from __future__ import annotations

import argparse
import datetime as dt
import json
import re
from pathlib import Path
from typing import Any

try:
    from scripts.research import (
        provider_directory_endpoint_acquisition_harness as harness,
    )
    from scripts.research.provider_directory_endpoint_acquisition_reporting import (
        RAW_RUN_STATUSES,
        run_summary,
    )
    from scripts.research.provider_directory_endpoint_acquisition_support import (
        result_param_errors,
        result_source_identity_errors,
    )
except ModuleNotFoundError:
    import provider_directory_endpoint_acquisition_harness as harness
    from provider_directory_endpoint_acquisition_reporting import (
        RAW_RUN_STATUSES,
        run_summary,
    )
    from provider_directory_endpoint_acquisition_support import (
        result_param_errors,
        result_source_identity_errors,
    )

ENVIRONMENT_PATTERN = re.compile(r"^[a-z0-9]+(?:[-_][a-z0-9]+)*$")


def _fresh_observed_at(value: object) -> str:
    if not isinstance(value, str):
        raise harness.ManifestError("operator input observed_at is required")
    try:
        observed_at = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise harness.ManifestError("operator input observed_at must be ISO-8601") from exc
    if observed_at.tzinfo is None:
        raise harness.ManifestError("operator input observed_at requires a timezone")
    observation_age = dt.datetime.now(dt.UTC) - observed_at.astimezone(dt.UTC)
    if observation_age < -dt.timedelta(minutes=5) or observation_age > dt.timedelta(
        hours=1
    ):
        raise harness.ManifestError("operator input observation is not current")
    return observed_at.astimezone(dt.UTC).isoformat(timespec="seconds").replace(
        "+00:00", "Z"
    )


def _validated_observation_time(
    manifest: dict[str, Any],
    operator_input: dict[str, Any],
    selected_entry_ids: frozenset[str],
) -> tuple[str, str]:
    operator_plan = harness.build_operator_plan(manifest, selected_entry_ids)
    if operator_input.get("campaign_id") != operator_plan["campaign_id"]:
        raise harness.ManifestError("operator input campaign_id does not match the plan")
    if operator_input.get("manifest_sha256") != operator_plan["manifest_sha256"]:
        raise harness.ManifestError("operator input manifest_sha256 does not match the plan")
    if operator_input.get("observation_method") != "operator-attested-read-only-export":
        raise harness.ManifestError("operator input observation method is not operator-attested")
    environment = operator_input.get("environment")
    if not isinstance(environment, str) or not ENVIRONMENT_PATTERN.fullmatch(environment):
        raise harness.ManifestError("operator input environment is invalid")
    if environment != manifest.get("catalog_confirmation", {}).get("environment"):
        raise harness.ManifestError("operator input environment does not match the manifest")
    plan_entry_by_id = {
        entry_plan["entry_id"]: entry_plan for entry_plan in operator_plan["entries"]
    }
    operator_result_by_entry_id = operator_input["results"]
    if set(operator_result_by_entry_id) != set(plan_entry_by_id):
        raise harness.ManifestError("operator results do not exactly match the selected plan")
    for entry_id, entry_plan in plan_entry_by_id.items():
        if operator_result_by_entry_id.get(entry_id, {}).get("spec_sha256") != entry_plan[
            "spec_sha256"
        ]:
            raise harness.ManifestError(
                f"{entry_id}: operator result does not match the planned entry"
            )
    return _fresh_observed_at(operator_input.get("observed_at")), environment


def _verification_update_metadata(
    report_path: Path,
    manifest_path: Path,
    environment: str,
    terminal_entry_ids: list[str],
    nonterminal_entry_ids: list[str],
) -> dict[str, Any]:
    return {
        "eligible": not nonterminal_entry_ids,
        "selected_entry_ids": terminal_entry_ids + nonterminal_entry_ids,
        "terminal_entry_ids": terminal_entry_ids,
        "nonterminal_entry_ids": nonterminal_entry_ids,
        "argv": [
            "python",
            "scripts/update_provider_directory_verification.py",
            "--manifest",
            str(manifest_path),
            "--report",
            str(report_path),
            "--environment",
            environment,
        ],
    }


def _operator_verification_entry(
    manifest: dict[str, Any],
    manifest_entry: dict[str, Any],
    validation_entry: dict[str, Any],
    operator_result_by_field: dict[str, Any],
) -> dict[str, Any]:
    run_status = operator_result_by_field.get("status")
    if not isinstance(run_status, str) or run_status not in RAW_RUN_STATUSES:
        raise harness.ManifestError(
            f"{manifest_entry['entry_id']}: run status is not controlled"
        )
    run_id = operator_result_by_field.get("run_id")
    if not isinstance(run_id, str) or not harness.RUN_ID_PATTERN.fullmatch(run_id):
        raise harness.ManifestError(
            f"{manifest_entry['entry_id']}: observed result requires a run_id"
        )
    is_manual_observation = (
        manifest_entry["classification"] == harness.MANUAL_CLASSIFICATION
    )
    is_plan_bound = True
    if run_status != "succeeded" or is_manual_observation:
        identity_errors = result_source_identity_errors(
            manifest, manifest_entry, operator_result_by_field
        )
        if identity_errors:
            raise harness.ManifestError(
                f"{manifest_entry['entry_id']}: observed run identity is invalid: "
                + "; ".join(identity_errors)
            )
        expected_params = harness.entry_params(manifest, manifest_entry)
        is_plan_bound = bool(expected_params) and not result_param_errors(
            manifest,
            manifest_entry,
            operator_result_by_field,
            expected_params,
        )
    return {
        "status": (
            "observed"
            if is_manual_observation
            else "succeeded"
            if run_status == "succeeded" and validation_entry["status"] == "passed"
            else "metric_validation_failed" if run_status == "succeeded" else "observed"
        ),
        "current_run_id": run_id,
        "last_run": run_summary(operator_result_by_field),
        "metric_errors": validation_entry["errors"] if run_status == "succeeded" else [],
        "access_verification": "not_verified",
        **(
            {"plan_bound": is_plan_bound}
            if run_status != "succeeded" or is_manual_observation
            else {}
        ),
    }


def _verification_entry_ids_by_state(
    report_entry_by_id: dict[str, dict[str, Any]],
) -> tuple[list[str], list[str]]:
    terminal_entry_ids = [
        entry_id
        for entry_id, report_entry in report_entry_by_id.items()
        if report_entry["status"] != "observed"
    ]
    nonterminal_entry_ids = [
        entry_id
        for entry_id, report_entry in report_entry_by_id.items()
        if report_entry["status"] == "observed"
    ]
    return terminal_entry_ids, nonterminal_entry_ids


def _build_verification_report(
    manifest: dict[str, Any],
    operator_input: dict[str, Any],
    report_path: Path,
    manifest_path: Path,
    selected_entry_ids: frozenset[str],
) -> dict[str, Any]:
    """Build an updater-compatible report from operator-attested run observations."""

    observed_at, environment = _validated_observation_time(
        manifest, operator_input, selected_entry_ids
    )
    validation_report = harness.evaluate_operator_input(
        manifest, operator_input, selected_entry_ids
    )
    operator_result_by_entry_id = operator_input["results"]
    manifest_entry_by_id = {
        entry["entry_id"]: entry for entry in manifest["entries"]
    }
    report_entry_by_id = {
        entry_id: _operator_verification_entry(
            manifest,
            manifest_entry_by_id[entry_id],
            validation_entry,
            operator_result_by_entry_id[entry_id],
        )
        for entry_id, validation_entry in validation_report["entries"].items()
    }

    terminal_entry_ids, nonterminal_entry_ids = _verification_entry_ids_by_state(
        report_entry_by_id
    )
    return {
        "schema_version": 1,
        "generated_at": observed_at,
        "mode": "dry-run",
        "campaign_id": validation_report["campaign_id"],
        "manifest_sha256": validation_report["manifest_sha256"],
        "observation": {
            "method": operator_input["observation_method"],
            "environment": environment,
            "observed_at": observed_at,
            "operator_input_sha256": harness._json_hash(operator_input),
        },
        "entries": report_entry_by_id,
        "verification_update": _verification_update_metadata(
            report_path,
            manifest_path,
            environment,
            terminal_entry_ids,
            nonterminal_entry_ids,
        ),
    }


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
    parser.add_argument(
        "--verification-report",
        action="store_true",
        help="Emit an updater-compatible report from operator-attested run observations.",
    )
    parser.add_argument("--validate-only", action="store_true")
    return parser.parse_args(argv)


def run_acquisition_cli(argv: list[str] | None = None) -> int:
    """Validate a manifest, emit operator inputs, or verify local results."""

    args = parse_acquisition_arguments(argv)
    if args.validate_only and args.operator_input:
        raise SystemExit("--validate-only cannot be combined with --operator-input")
    if args.verification_report and not args.operator_input:
        raise SystemExit("--verification-report requires --operator-input")
    if args.verification_report and not args.output:
        raise SystemExit("--verification-report requires --output")

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
        if args.verification_report:
            output_by_field = _build_verification_report(
                manifest,
                operator_input,
                args.output,
                args.manifest,
                selected_entry_ids,
            )
            exit_code = 0
        else:
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
