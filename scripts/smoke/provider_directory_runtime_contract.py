#!/usr/bin/env python
# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Verify Provider Directory runtime contract without requiring a database."""

from __future__ import annotations

import argparse
import importlib
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
os.environ.setdefault("HLTHPRT_LOG_CFG", str(ROOT / "logging.yaml"))


PROVIDER_DIRECTORY_IMPORTER = "provider-directory-fhir"
REQUIRED_PROVIDER_DIRECTORY_CLI_OPTIONS = (
    "--refresh-preset",
    "--include-supplemental-catalogs",
    "--retest-results-url",
    "--credential-config-file",
    "--linked-resource-limit",
    "--publish-corroboration",
)
REQUIRED_COVERAGE_AUDIT_OPTIONS = (
    "--fast-serving-readiness",
    "--require-serving-ready",
)
REQUIRED_PROVIDER_DIRECTORY_HARNESS_OPTIONS = (
    "--local-cli",
    "--coverage-audit",
    "--coverage-audit-full",
    "--coverage-audit-require-serving-ready",
    "--coverage-audit-fast-serving-readiness",
    "--no-cli-test-mode",
    "--seed-db-url",
    "--retest-results-url",
    "--credential-config-file",
    "--refresh-preset",
    "--include-supplemental-catalogs",
    "--resource-deadline-seconds",
    "--linked-resource-deadline-seconds",
)
REQUIRED_PROVIDER_DIRECTORY_SCHEMA_PARAMS = (
    "refresh_preset",
    "include_supplemental_catalogs",
    "import_resources",
    "full_refresh",
    "stale_cleanup",
    "publish_artifacts",
    "publish_corroboration",
    "retest_results_url",
    "resource_limit",
    "resource_deadline_seconds",
    "linked_resource_limit",
    "linked_resource_deadline_seconds",
    "page_limit",
    "page_count",
    "stream_batch_size",
    "bulk_export",
    "source_concurrency",
    "concurrency",
    "timeout",
)
REQUIRED_SERVING_READINESS_CHECKS = (
    "source_catalog_seeded",
    "resource_rows_imported",
    "searchable_address_overlay",
    "searchable_phone_overlay",
    "source_detail_attribution",
    "network_catalog_published",
    "ptg_corroboration_table",
    "ptg_network_name_overlap",
)


def _command_text(args: list[str]) -> str:
    result = subprocess.run(
        [sys.executable, *args],
        cwd=ROOT,
        check=True,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    return result.stdout


def _provider_directory_cli_report() -> dict[str, Any]:
    help_text = _command_text(["main.py", "start", PROVIDER_DIRECTORY_IMPORTER, "--help"])
    missing = [option for option in REQUIRED_PROVIDER_DIRECTORY_CLI_OPTIONS if option not in help_text]
    return {
        "ok": not missing,
        "missing": missing,
        "required": list(REQUIRED_PROVIDER_DIRECTORY_CLI_OPTIONS),
    }


def _coverage_audit_cli_report() -> dict[str, Any]:
    help_text = _command_text(["scripts/research/provider_directory_coverage_audit.py", "--help"])
    missing = [option for option in REQUIRED_COVERAGE_AUDIT_OPTIONS if option not in help_text]
    return {
        "ok": not missing,
        "missing": missing,
        "required": list(REQUIRED_COVERAGE_AUDIT_OPTIONS),
    }


def _harness_cli_report() -> dict[str, Any]:
    help_text = _command_text(["scripts/research/provider_directory_fhir_harness.py", "--help"])
    missing_options = [option for option in REQUIRED_PROVIDER_DIRECTORY_HARNESS_OPTIONS if option not in help_text]
    return {
        "ok": not missing_options,
        "missing": missing_options,
        "required": list(REQUIRED_PROVIDER_DIRECTORY_HARNESS_OPTIONS),
    }


def _importer_schema_report() -> dict[str, Any]:
    from api.control_imports import importer_registry

    items = {
        str(item.get("name") or ""): item
        for item in importer_registry()
        if isinstance(item, dict)
    }
    provider_directory = items.get(PROVIDER_DIRECTORY_IMPORTER) or {}
    params_schema = list(provider_directory.get("params_schema") or [])
    by_name = {
        str(param.get("name") or ""): param
        for param in params_schema
        if isinstance(param, dict)
    }
    missing = [name for name in REQUIRED_PROVIDER_DIRECTORY_SCHEMA_PARAMS if name not in by_name]
    refresh_preset = by_name.get("refresh_preset") or {}
    refresh_choices = list(refresh_preset.get("choices") or [])
    if "monthly-full" not in refresh_choices:
        missing.append("refresh_preset.choice.monthly-full")
    if (by_name.get("include_supplemental_catalogs") or {}).get("type") != "boolean":
        missing.append("include_supplemental_catalogs.type.boolean")
    return {
        "ok": not missing,
        "missing": missing,
        "required": list(REQUIRED_PROVIDER_DIRECTORY_SCHEMA_PARAMS),
        "refresh_preset_choices": refresh_choices,
    }


def _monthly_preset_report() -> dict[str, Any]:
    provider_directory_fhir = importlib.import_module("process.provider_directory_fhir")

    task = provider_directory_fhir._apply_provider_directory_refresh_preset(  # pylint: disable=protected-access
        {"refresh_preset": "monthly_full"}
    )
    expected = {
        "refresh_preset": "monthly-full",
        "import_resources": True,
        "full_refresh": True,
        "stale_cleanup": True,
        "publish_artifacts": True,
        "publish_corroboration": True,
        "bulk_export": True,
        "include_supplemental_catalogs": True,
    }
    mismatched = {
        key: {"expected": expected_value, "observed": task.get(key)}
        for key, expected_value in expected.items()
        if task.get(key) != expected_value
    }
    return {
        "ok": not mismatched,
        "mismatched": mismatched,
        "expected": expected,
    }


def _serving_readiness_contract_report() -> dict[str, Any]:
    coverage_audit = importlib.import_module("scripts.research.provider_directory_coverage_audit")
    skipped_ptg_summary = getattr(coverage_audit, "_skipped_ptg_summary")
    serving_readiness_summary = getattr(coverage_audit, "_serving_readiness_summary")
    readiness_payload_map = {
        "source_summary": {
            "available": True,
            "source_count": 1,
            "live_valid_count": 1,
        },
        "source_resource_coverage_summary": {
            "available": True,
            "source_count": 1,
            "sources_with_resource_rows": 1,
        },
        "unified_summary": {
            "available": True,
            "provider_directory_rows": 1,
            "provider_directory_keyed_rows": 1,
            "provider_directory_phone_rows": 1,
            "provider_directory_source_record_id_rows": 1,
        },
        "plan_network_context_summary": {
            "available": True,
            "network_ref_rows": 0,
            "resolved_network_names": 0,
        },
        "network_catalog_summary": {
            "available": False,
        },
        "ptg_summary": skipped_ptg_summary(),
    }
    readiness = serving_readiness_summary(readiness_payload_map)
    checks_by_name = {
        str(check.get("name") or ""): check
        for check in readiness.get("checks") or []
        if isinstance(check, dict)
    }
    missing_checks = [name for name in REQUIRED_SERVING_READINESS_CHECKS if name not in checks_by_name]
    phone_check = checks_by_name.get("searchable_phone_overlay") or {}
    if phone_check.get("required") is not True:
        missing_checks.append("searchable_phone_overlay.required.true")
    if phone_check.get("status") != "pass":
        missing_checks.append("searchable_phone_overlay.status.pass")
    return {
        "ok": not missing_checks,
        "missing": missing_checks,
        "required": list(REQUIRED_SERVING_READINESS_CHECKS),
        "status": readiness.get("status"),
    }


def build_report() -> dict[str, Any]:
    checks = {
        "provider_directory_cli": _provider_directory_cli_report(),
        "coverage_audit_cli": _coverage_audit_cli_report(),
        "provider_directory_harness_cli": _harness_cli_report(),
        "importer_schema": _importer_schema_report(),
        "monthly_preset": _monthly_preset_report(),
        "serving_readiness_contract": _serving_readiness_contract_report(),
    }
    failures = {
        name: check
        for name, check in checks.items()
        if not check.get("ok")
    }
    return {
        "ok": not failures,
        "failures": failures,
        "checks": checks,
    }


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--format", choices=("text", "json"), default="text")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    report = build_report()
    if args.format == "json":
        print(json.dumps(report, indent=2, sort_keys=True))
    else:
        print("Provider Directory runtime contract: " + ("PASS" if report["ok"] else "FAIL"))
        for name, check in report["checks"].items():
            status = "PASS" if check.get("ok") else "FAIL"
            print(f"- {name}: {status}")
            if check.get("missing"):
                print(f"  missing: {', '.join(check['missing'])}")
            if check.get("mismatched"):
                print(f"  mismatched: {json.dumps(check['mismatched'], sort_keys=True)}")
    return 0 if report["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
