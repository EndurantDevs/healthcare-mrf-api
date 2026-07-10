"""Command-line entrypoint for the Provider Directory acquisition harness."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

try:
    from scripts.research import provider_directory_endpoint_acquisition_harness as harness
except ModuleNotFoundError:
    import provider_directory_endpoint_acquisition_harness as harness


def _default_control_url() -> str | None:
    """Prefer the current runtime variable while retaining legacy compatibility."""
    return os.getenv("HLTHPRT_IMPORT_CONTROL_URL") or os.getenv("HP_IMPORT_CONTROL_URL")


def _default_token_environment_name() -> str:
    """Select the current token key without reading its secret value."""
    if "HLTHPRT_IMPORT_CONTROL_TOKEN" in os.environ or "HP_IMPORT_CONTROL_TOKEN" not in os.environ:
        return "HLTHPRT_IMPORT_CONTROL_TOKEN"
    return "HP_IMPORT_CONTROL_TOKEN"


def parse_acquisition_arguments(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse campaign controls without reading credentials."""
    parser = argparse.ArgumentParser(description=harness.__doc__)
    parser.add_argument("--manifest", type=Path, default=harness.DEFAULT_MANIFEST)
    parser.add_argument("--state", type=Path, default=harness.DEFAULT_STATE)
    parser.add_argument("--report", type=Path, default=harness.DEFAULT_REPORT)
    parser.add_argument("--control-url", default=_default_control_url())
    parser.add_argument("--token-env", default=_default_token_environment_name())
    parser.add_argument("--entry", action="append", default=[], help="Run only this manifest entry; repeatable.")
    parser.add_argument("--restart-entry", action="append", default=[], help="Start a fresh root after a guarded terminal failure; repeatable and requires --apply.")
    parser.add_argument("--adopt-run", action="append", default=[], metavar="ENTRY=RUN_ID", help="Adopt an exact existing run as a guarded campaign lineage tip; repeatable and requires --apply.")
    parser.add_argument("--apply", action="store_true", help="Permit POST /v1/runs. The default is GET-only dry-run.")
    parser.add_argument("--validate-only", action="store_true")
    parser.add_argument("--poll-interval-seconds", type=float, default=30.0)
    parser.add_argument("--retry-wait-seconds", type=float, default=900.0)
    parser.add_argument("--request-timeout-seconds", type=float, default=60.0)
    return parser.parse_args(argv)


def run_acquisition_cli(argv: list[str] | None = None) -> int:
    """Validate or run an audited endpoint acquisition campaign."""
    args = parse_acquisition_arguments(argv)
    if args.restart_entry and not args.apply:
        raise SystemExit("--restart-entry requires --apply")
    if args.adopt_run and not args.apply:
        raise SystemExit("--adopt-run requires --apply")
    manifest = harness.load_manifest(args.manifest)
    if args.validate_only:
        print(json.dumps({"valid": True, "entries": len(manifest["entries"]), "manifest_sha256": harness._json_hash(manifest)}, sort_keys=True))
        return 0
    if not args.control_url:
        raise SystemExit(
            "--control-url, HLTHPRT_IMPORT_CONTROL_URL, or HP_IMPORT_CONTROL_URL is required"
        )
    known_entry_ids = {entry["entry_id"] for entry in manifest["entries"]}
    restart_entry_ids = frozenset(args.restart_entry)
    adopt_run_ids = harness.parse_adopt_runs(args.adopt_run, harness.SLUG_PATTERN)
    adopted_entry_ids = frozenset(entry_id for entry_id, _run_id in adopt_run_ids)
    selected_entry_ids = frozenset(args.entry) | restart_entry_ids | adopted_entry_ids
    unknown_ids = selected_entry_ids - known_entry_ids
    if unknown_ids:
        raise SystemExit("unknown manifest entries: " + ",".join(sorted(unknown_ids)))
    conflicting_entry_ids = restart_entry_ids & adopted_entry_ids
    if conflicting_entry_ids:
        raise SystemExit("entries cannot be both restarted and adopted: " + ",".join(sorted(conflicting_entry_ids)))
    config = harness.HarnessConfig(args.state, args.report, args.apply, selected_entry_ids, args.poll_interval_seconds, args.retry_wait_seconds, restart_entry_ids, adopt_run_ids)
    client = harness.ImportControlHttpClient(args.control_url, args.token_env, args.request_timeout_seconds)
    final_state = harness.AcquisitionHarness(manifest, client, config).execute_campaign()
    selected_states = [state for entry_id, state in final_state["entries"].items() if not selected_entry_ids or entry_id in selected_entry_ids]
    accepted_statuses = harness.FINISHED_STATE_STATUSES | ({"planned"} if not args.apply else set())
    is_successful = all(state.get("status") in accepted_statuses for state in selected_states)
    print(json.dumps({"ok": is_successful, "mode": "apply" if args.apply else "dry-run", "state": str(args.state), "report": str(args.report)}, sort_keys=True))
    return 0 if is_successful else 2
