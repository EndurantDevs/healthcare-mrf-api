#!/usr/bin/env python3
"""Read-only driver for PTG V4 import and serving acceptance."""

from __future__ import annotations

import asyncio
import json
import sys
from pathlib import Path
from typing import Any, Mapping
from urllib.parse import quote, urlsplit

import httpx


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.ptg_v4_dev_canary_cli import build_parser
from scripts.ptg_v4_dev_canary_api import (
    api_parameters as _api_parameters,
    api_spec as _api_spec,
    public_warm_probe as _public_warm_probe,
    reference_spec as _reference_spec,
    require_same_runtime_identity as _require_same_runtime_identity,
)
from scripts.ptg_v4_dev_canary_cold_capture import (
    record_cold_sample as _record_cold_sample,
)
from scripts.ptg_v4_dev_canary_budget import elapsed_budget as _elapsed_budget
from scripts.ptg_v4_dev_canary_db import (
    capture_storage_baseline,
    collect_v4_database_evidence,
)
from scripts.ptg_v4_dev_canary_progress import (
    PROGRESS_EVIDENCE_CONTRACT,
    evaluate_progress_timeline,
)
from scripts.ptg_v4_dev_canary_publication import (
    evaluate_v4_evidence,
)
from scripts.ptg_v4_dev_canary_reference import (
    build_v3_reference_evidence,
    evaluate_database_reference_evidence,
    validate_reference_evidence,
)
from scripts.ptg_v4_dev_canary_io import (
    CanaryInfrastructureError,
    get_json as _get_json,
    load_json as _load_json,
    parse_datetime as _parse_datetime,
    required_environment as _required_environment,
    utc_now_text as _utc_now_text,
    write_json as _write_json,
)
from scripts.ptg_v4_dev_canary_identity import (
    bind_terminal_run_identity,
    normalize_terminal_run_identity,
    require_matching_database_identity,
)
from scripts.ptg_v4_dev_canary_internal_evaluation import (
    validate_internal_evidence,
)
from scripts.ptg_v4_dev_canary_graph_budget import (
    evaluate_graph_read_budget,
)
from scripts.ptg_v4_dev_canary_storage_budget import storage_budget
from scripts.ptg_v4_dev_canary_kubernetes import (
    collect_frozen_v3_candidate_evidence,
    require_frozen_v3_service_host,
)
from scripts.ptg_v4_dev_canary_support import (
    RELATION_COUNT_FIELDS,
    ROOT_COUNT_FIELDS,
    CanaryConfigurationError,
    headers_from_environment,
    parse_count_expectations,
    validate_base_url,
    validate_identifier,
)


TERMINAL_RUN_STATUSES = frozenset(
    {"succeeded", "failed", "canceled", "cancelled", "dead_letter"}
)


async def _monitor_progress(args) -> dict[str, Any]:
    control_url = validate_base_url(
        args.control_base_url,
        allow_insecure_http=args.allow_insecure_http,
    )
    if not 0 < args.poll_interval_seconds <= 5:
        raise CanaryConfigurationError("progress polling interval must be within 5s")
    headers, _environment_names = headers_from_environment(args.control_header_env)
    evidence_by_field = {
        "contract": PROGRESS_EVIDENCE_CONTRACT,
        "control_run_id": args.control_run_id,
        "poll_interval_seconds": args.poll_interval_seconds,
        "samples": [],
        "run": {},
    }
    clock = asyncio.get_running_loop().time
    deadline = clock() + args.maximum_monitor_seconds
    timeout = httpx.Timeout(args.request_timeout_seconds)
    async with httpx.AsyncClient(timeout=timeout) as client:
        while clock() <= deadline:
            run, _elapsed_ms = await _get_json(
                client,
                f"{control_url}/control/v1/imports/{quote(args.control_run_id, safe='')}",
                headers=headers,
                parameters=None,
                maximum_bytes=2 << 20,
            )
            evidence_by_field["samples"].append(_progress_sample(run))
            evidence_by_field["run"] = _run_summary(run)
            _write_json(Path(args.output), evidence_by_field)
            if str(run.get("status") or "").lower() in TERMINAL_RUN_STATUSES:
                return evidence_by_field
            await asyncio.sleep(args.poll_interval_seconds)
    raise CanaryInfrastructureError("progress monitor exceeded its bounded duration")


def _progress_sample(run: Mapping[str, Any]) -> dict[str, Any]:
    progress = run.get("progress")
    return {
        "observed_at": _utc_now_text(),
        "status": run.get("status"),
        "progress": dict(progress) if isinstance(progress, Mapping) else None,
    }


def _run_summary(run: Mapping[str, Any]) -> dict[str, Any]:
    summary_by_field = {
        field_name: run.get(field_name)
        for field_name in ("run_id", "status", "started_at", "finished_at")
    }
    if str(run.get("status") or "").lower() == "succeeded":
        summary_by_field["terminal_identity"] = normalize_terminal_run_identity(
            run
        )
    return summary_by_field


async def _capture_baseline(args) -> dict[str, Any]:
    database_url = _required_environment(args.database_url_env)
    schema_name = validate_identifier(args.database_schema, label="database schema")
    evidence = await capture_storage_baseline(
        database_url,
        schema_name=schema_name,
    )
    _write_json(Path(args.output), evidence)
    return evidence


async def _capture_v3_reference(args) -> dict[str, Any]:
    """Capture one exact semantic page from the frozen, V3-only candidate."""

    api_url = validate_base_url(
        args.api_base_url,
        allow_insecure_http=args.allow_insecure_http,
    )
    require_frozen_v3_service_host(urlsplit(api_url).hostname)
    api_headers, _environment_names = headers_from_environment(args.api_header_env)
    spec = _reference_spec(args)
    timeout = httpx.Timeout(args.request_timeout_seconds)
    runtime_before = await collect_frozen_v3_candidate_evidence()
    async with httpx.AsyncClient(timeout=timeout) as client:
        document, _elapsed_ms = await _get_json(
            client,
            f"{api_url}/api/v1/pricing/providers/by-procedure",
            headers=api_headers,
            parameters=spec.parameters({}),
            maximum_bytes=args.maximum_response_bytes,
        )
    runtime_after = await collect_frozen_v3_candidate_evidence()
    if runtime_before != runtime_after:
        raise CanaryInfrastructureError(
            "frozen V3 candidate changed during reference capture"
        )
    evidence = build_v3_reference_evidence(
        document,
        spec,
        runtime_evidence=runtime_after,
    )
    _write_json(Path(args.output), evidence)
    return evidence


async def _accept(args) -> dict[str, Any]:
    """Evaluate one identity-bound terminal import and serving evidence set."""

    timeline = _load_json(Path(args.progress_evidence))
    baseline = _load_json(Path(args.storage_baseline))
    cold_evidence = _load_json(Path(args.cold_evidence))
    reference_evidence = _load_json(Path(args.reference_evidence))
    internal_evidence = _load_json(Path(args.internal_owner_evidence))
    database_evidence = await _collect_bound_database_evidence(
        args,
        timeline,
        baseline,
        reference_evidence,
    )
    progress_by_field = evaluate_progress_timeline(
        timeline,
        budget=_elapsed_budget(database_evidence),
        maximum_update_gap_seconds=args.maximum_progress_gap_seconds,
    )
    reference_equivalence_by_field = evaluate_database_reference_evidence(
        database_evidence.get("reference_equivalence", {})
    )
    cold_sample_evidence, cold_not_before = _cold_acceptance_inputs(
        cold_evidence,
        timeline,
    )
    serving_by_field = await _public_warm_probe(
        args,
        cold_sample_evidence,
        cold_not_before=cold_not_before,
    )
    publication_by_field = _publication_from_serving(
        args, database_evidence, serving_by_field
    )
    internal_by_field, graph_read_budget_by_field = (
        _graph_serving_sections(
            args,
            database_evidence,
            reference_evidence,
            internal_evidence,
            cold_sample_evidence,
            serving_by_field,
        )
    )
    sections_by_name = {
        "import_progress": progress_by_field,
        "publication_and_storage": publication_by_field,
        "v3_reference_equivalence": reference_equivalence_by_field,
        "public_cpt_70553": serving_by_field,
        "internal_worst_owner": internal_by_field,
        "graph_read_budget": graph_read_budget_by_field,
    }
    failures = _section_failures(sections_by_name.values())
    return {
        "contract": "ptg_v4_dev_canary_acceptance_v1",
        "passed": not failures,
        "failures": failures,
        **sections_by_name,
    }


def _graph_serving_sections(
    args: Any,
    database_evidence: Mapping[str, Any],
    reference_evidence: Mapping[str, Any],
    internal_evidence: Mapping[str, Any],
    cold_sample_evidence: list[Mapping[str, Any]],
    serving_by_field: Mapping[str, Any],
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Validate internal evidence and evaluate the separate graph budget."""

    image_identity = serving_by_field["bounded_read_counts"][
        "runtime_identity"
    ]["image_identity"]
    reference_snapshot_id = str(
        reference_evidence.get("reference_snapshot_id") or ""
    )
    internal_by_field = validate_internal_evidence(
        internal_evidence,
        args.snapshot_id,
        expected_reference_snapshot_id=reference_snapshot_id,
        expected_image_identity=image_identity,
    )
    graph_read_budget_by_field = _evaluate_graph_read_policy(
        database_evidence,
        reference_snapshot_id=reference_snapshot_id,
        image_identity=image_identity,
        cold_sample_evidence=cold_sample_evidence,
        serving_by_field=serving_by_field,
        internal_by_field=internal_by_field,
    )
    return internal_by_field, graph_read_budget_by_field


def _evaluate_graph_read_policy(
    database_evidence_by_field: Mapping[str, Any],
    *,
    reference_snapshot_id: str,
    image_identity: str,
    cold_sample_evidence: list[Mapping[str, Any]],
    serving_by_field: Mapping[str, Any],
    internal_by_field: Mapping[str, Any],
) -> dict[str, Any]:
    """Bind measured graph reads to the sealed import and reviewed case."""

    snapshot_by_field = database_evidence_by_field.get("snapshot")
    if not isinstance(snapshot_by_field, Mapping):
        raise CanaryConfigurationError(
            "published V4 snapshot evidence is missing"
        )
    return evaluate_graph_read_budget(
        reference_snapshot_id=reference_snapshot_id,
        snapshot_id=str(snapshot_by_field.get("snapshot_id") or ""),
        import_run_id=str(snapshot_by_field.get("import_run_id") or ""),
        image_identity=image_identity,
        public_cold_samples=cold_sample_evidence,
        public_warm_graph_evidence=serving_by_field[
            "bounded_read_counts"
        ],
        internal_owner_evidence=internal_by_field,
    )


def _evaluate_publication(
    args: Any,
    database_evidence_by_field: Mapping[str, Any],
    *,
    measurement_image_identity: str,
) -> dict[str, Any]:
    """Apply exact count and physical-storage gates to sealed DB evidence."""

    budget = storage_budget(database_evidence_by_field)
    return evaluate_v4_evidence(
        database_evidence_by_field,
        storage_budget=budget,
        measurement_image_identity=measurement_image_identity,
        expected_root_counts=parse_count_expectations(
            args.expect_root_count,
            allowed_fields=ROOT_COUNT_FIELDS,
            relation_scoped=False,
        ),
        expected_relation_counts=parse_count_expectations(
            args.expect_relation_count,
            allowed_fields=RELATION_COUNT_FIELDS,
            relation_scoped=True,
        ),
    )


def _publication_from_serving(
    args: Any,
    database_evidence_by_field: Mapping[str, Any],
    serving_by_field: Mapping[str, Any],
) -> dict[str, Any]:
    """Bind the storage measurement to the serving image under test."""

    image_identity = serving_by_field["bounded_read_counts"][
        "runtime_identity"
    ]["image_identity"]
    return _evaluate_publication(
        args,
        database_evidence_by_field,
        measurement_image_identity=image_identity,
    )


def _cold_acceptance_inputs(
    cold_evidence_by_field: Mapping[str, Any],
    timeline_by_field: Mapping[str, Any],
) -> tuple[list[Mapping[str, Any]], str]:
    """Extract persisted public samples and their terminal freshness bound."""

    cases_by_name = cold_evidence_by_field.get("cases")
    cold_sample_evidence = (
        list(cases_by_name.get("public", []))
        if isinstance(cases_by_name, Mapping)
        else []
    )
    run_evidence_by_field = timeline_by_field.get("run")
    cold_not_before = (
        str(run_evidence_by_field.get("finished_at") or "")
        if isinstance(run_evidence_by_field, Mapping)
        else ""
    )
    return cold_sample_evidence, cold_not_before


def _section_failures(
    sections_by_field: Any,
) -> list[str]:
    """Flatten deterministic failure rows from every acceptance section."""

    return [
        str(failure)
        for section_by_field in sections_by_field
        for failure in section_by_field.get("failures", [])
    ]


async def _collect_bound_database_evidence(
    args: Any,
    timeline_by_field: Mapping[str, Any],
    storage_baseline_by_field: Mapping[str, Any],
    reference_evidence_by_field: Mapping[str, Any],
) -> dict[str, Any]:
    """Collect sealed rows only after binding the terminal control identity."""

    terminal_identity_by_field = bind_terminal_run_identity(
        timeline_by_field,
        expected_snapshot_id=args.snapshot_id,
    )
    control_run_by_field = timeline_by_field.get("run")
    if not isinstance(control_run_by_field, Mapping):
        raise CanaryConfigurationError("progress evidence lacks a control run")
    reference_failures = validate_reference_evidence(reference_evidence_by_field)
    if reference_failures:
        raise CanaryConfigurationError("; ".join(reference_failures))
    database_evidence_by_field = await collect_v4_database_evidence(
        _required_environment(args.database_url_env),
        schema_name=validate_identifier(args.database_schema, label="database schema"),
        snapshot_id=args.snapshot_id,
        storage_baseline=storage_baseline_by_field,
        import_started_at=_parse_datetime(control_run_by_field.get("started_at")),
        import_finished_at=_parse_datetime(control_run_by_field.get("finished_at")),
        reference_snapshot_id=str(
            reference_evidence_by_field.get("reference_snapshot_id") or ""
        ),
        rollback_owner_id=str(args.rollback_owner_id).strip(),
    )
    require_matching_database_identity(
        database_evidence_by_field,
        terminal_identity_by_field,
    )
    return database_evidence_by_field
async def _run(args) -> dict[str, Any]:
    if args.command == "monitor-progress":
        return await _monitor_progress(args)
    if args.command == "capture-storage-baseline":
        return await _capture_baseline(args)
    if args.command == "capture-v3-reference":
        return await _capture_v3_reference(args)
    if args.command == "record-cold-sample":
        return await _record_cold_sample(args)
    if args.command == "internal-owner-probe":
        from scripts.ptg_v4_dev_canary_internal import run_internal_owner_probe

        return await run_internal_owner_probe(args)
    if args.command == "accept":
        return await _accept(args)
    raise CanaryConfigurationError("unsupported command")


def main() -> int:
    """Run one canary command and return a stable shell status."""

    args = build_parser().parse_args()
    try:
        report = asyncio.run(_run(args))
        if getattr(args, "output", None) and args.command == "accept":
            _write_json(Path(args.output), report)
        print(json.dumps(report, indent=2, sort_keys=True))
        return 0 if report.get("passed", True) else 1
    except CanaryConfigurationError as exc:
        print(json.dumps({"passed": False, "error": str(exc)}))
        return 2
    except (CanaryInfrastructureError, RuntimeError, ValueError) as exc:
        print(
            json.dumps(
                {
                    "passed": False,
                    "error": "canary evidence collection failed",
                    "error_type": type(exc).__name__,
                }
            )
        )
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
