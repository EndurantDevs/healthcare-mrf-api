#!/usr/bin/env python3
# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Verify current Provider Directory evidence through the client-facing api-layer contract."""

from __future__ import annotations

import argparse
import asyncio
import datetime as dt
import json
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping


ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.research.provider_directory_api_evidence_support import (
    ApiConfig,
    MappedEvidenceWitness,
    OverlaySample,
    ProviderDirectoryApiClient,
    SourceEvaluationContext,
    SourceSelection,
    evaluate_source,
    redact_sensitive,
)
from scripts.research.provider_directory_api_evidence_db import (
    fetch_current_dataset_completion_proofs,
    fetch_mapped_evidence_witnesses,
    fetch_overlay_samples,
)
from scripts.research.provider_directory_api_evidence_report import (
    database_failure_result,
    mapped_completion_summary,
)
from scripts.research.provider_directory_api_evidence_selection import (
    HarnessConfig,
    _normalized_ids,
    _validate_harness_config,
    resolve_source_selection,
)
from scripts.research.provider_directory_endpoint_acquisition_harness import (
    DEFAULT_MANIFEST,
    load_manifest,
)


DEFAULT_API_BASE_URL_ENV = "PROVIDER_DIRECTORY_API_BASE_URL"
DEFAULT_API_BEARER_TOKEN_ENV = "PROVIDER_DIRECTORY_API_BEARER_TOKEN"
DEFAULT_API_KEY_ENV = "PROVIDER_DIRECTORY_API_KEY"


@dataclass(frozen=True)
class CurrentEvidenceProbe:
    """Current overlay, mapped evidence, and fail-closed probe outcomes."""

    samples_by_source: Mapping[str, list[OverlaySample]]
    witnesses_by_source: Mapping[str, list[MappedEvidenceWitness]]
    completion_proofs_by_source: Mapping[
        str, Mapping[str, Mapping[str, Any]]
    ]
    database_error: str | None = None
    witness_probe_error: str | None = None
    completion_probe_error: str | None = None


def _utc_now() -> str:
    return (
        dt.datetime.now(dt.UTC)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


async def _current_samples(
    config: HarnessConfig,
    conn: Any,
    selections: list[SourceSelection],
) -> CurrentEvidenceProbe:
    """Probe current evidence without letting one optional stage hide another."""
    empty_samples_by_source = {selection.source_id: [] for selection in selections}
    empty_witnesses_by_source = {
        selection.source_id: [] for selection in selections
    }
    try:
        samples_by_source = await fetch_overlay_samples(
            conn,
            schema=config.schema,
            selections=selections,
            samples_per_source=config.samples_per_source,
        )
    except Exception as exc:
        return CurrentEvidenceProbe(
            empty_samples_by_source,
            empty_witnesses_by_source,
            {},
            database_error=type(exc).__name__,
        )
    try:
        witnesses_by_source = await fetch_mapped_evidence_witnesses(
            conn,
            schema=config.schema,
            selections=selections,
            witnesses_per_resource=config.samples_per_source,
        )
    except Exception as exc:
        return CurrentEvidenceProbe(
            samples_by_source,
            empty_witnesses_by_source,
            {},
            witness_probe_error=type(exc).__name__,
        )
    if not config.require_mapped_evidence:
        return CurrentEvidenceProbe(samples_by_source, witnesses_by_source, {})
    try:
        completion_proofs_by_source = await fetch_current_dataset_completion_proofs(
            conn,
            schema=config.schema,
            selections=selections,
        )
        return CurrentEvidenceProbe(
            samples_by_source,
            witnesses_by_source,
            completion_proofs_by_source,
        )
    except Exception as exc:
        return CurrentEvidenceProbe(
            samples_by_source,
            witnesses_by_source,
            {},
            completion_probe_error=type(exc).__name__,
        )


def _active_api_client(
    api_config: ApiConfig,
    api_client: ProviderDirectoryApiClient | None,
    database_error: str | None,
) -> tuple[ProviderDirectoryApiClient | None, str | None]:
    if database_error:
        return None, "database_probe_failed"
    if api_client is not None and api_config.is_enabled:
        return api_client, None
    if api_config.is_enabled:
        return ProviderDirectoryApiClient(api_config), None
    skip_reason = (
        "data_only_mode" if api_config.data_only else "api_credentials_unavailable"
    )
    return None, skip_reason


def _source_results(
    config: HarnessConfig,
    selections: list[SourceSelection],
    current_probe: CurrentEvidenceProbe,
    api_client: ProviderDirectoryApiClient | None,
    api_skip_reason: str | None,
) -> list[dict[str, Any]]:
    if current_probe.database_error:
        return [database_failure_result(selection) for selection in selections]
    evaluation_context = SourceEvaluationContext(
        config.candidate_limit,
        config.api_latency_slo_ms,
        api_skip_reason,
        current_probe.witness_probe_error,
        current_probe.completion_probe_error,
    )
    return [
        evaluate_source(
            selection,
            current_probe.samples_by_source.get(selection.source_id, []),
            api_client,
            evaluation_context,
            witnesses=current_probe.witnesses_by_source.get(selection.source_id, []),
            completion_proofs=current_probe.completion_proofs_by_source.get(
                selection.source_id
            ),
        )
        for selection in selections
    ]


def _report_payload(
    config: HarnessConfig,
    selections: list[SourceSelection],
    source_results: list[dict[str, Any]],
    current_probe: CurrentEvidenceProbe,
    api_client: ProviderDirectoryApiClient | None,
) -> dict[str, Any]:
    required_failures = sum(
        source_result["status"] == "fail" and source_result["required"]
        for source_result in source_results
    )
    mapped_summary = mapped_completion_summary(
        require_mapped_evidence=config.require_mapped_evidence,
        source_result_list=source_results,
        witness_list_by_source=current_probe.witnesses_by_source,
        witness_probe_error=current_probe.witness_probe_error,
    )
    mapped_summary["current_dataset_completion_probe_failed"] = bool(
        current_probe.completion_probe_error
    )
    return {
        "schema_version": 1,
        "generated_at": _utc_now(),
        "mode": (
            "api"
            if api_client is not None and not current_probe.database_error
            else "data_only"
        ),
        "selection": {
            "entry_ids": list(config.entry_ids),
            "source_ids": list(config.source_ids),
            "selected_source_count": len(selections),
            "max_sources": config.max_sources,
            "samples_per_source": config.samples_per_source,
            "phone_candidate_limit": config.candidate_limit,
            "api_latency_slo_ms": config.api_latency_slo_ms,
            "require_mapped_evidence": config.require_mapped_evidence,
        },
        "summary": {
            "pass": sum(
                source_result["status"] == "pass" for source_result in source_results
            ),
            "fail": sum(
                source_result["status"] == "fail" for source_result in source_results
            ),
            "skip": sum(
                source_result["status"] == "skip" for source_result in source_results
            ),
            "required_sources_failed": required_failures,
            **mapped_summary,
        },
        "sources": source_results,
    }


async def build_report(
    config: HarnessConfig,
    conn: Any,
    api_config: ApiConfig,
    api_client: ProviderDirectoryApiClient | None = None,
) -> dict[str, Any]:
    """Build a credential-safe report, failing required sources without evidence."""
    _validate_harness_config(config)
    manifest = load_manifest(config.manifest_path)
    selections = resolve_source_selection(
        manifest,
        requested_entry_ids=config.entry_ids,
        requested_source_ids=config.source_ids,
        max_sources=config.max_sources,
    )
    if not selections:
        raise ValueError("no maintained sources selected")
    current_probe = await _current_samples(config, conn, selections)
    active_client, api_skip_reason = _active_api_client(
        api_config, api_client, current_probe.database_error
    )
    source_results = _source_results(
        config,
        selections,
        current_probe,
        active_client,
        api_skip_reason,
    )
    return redact_sensitive(
        _report_payload(
            config,
            selections,
            source_results,
            current_probe,
            active_client,
        )
    )


async def _connect(args: argparse.Namespace) -> Any:
    try:
        import asyncpg
    except ImportError as exc:  # pragma: no cover - runtime dependency.
        raise RuntimeError("asyncpg is required for database probes") from exc
    return await asyncpg.connect(
        host=args.db_host,
        port=args.db_port,
        user=args.db_user,
        password=args.db_password or None,
        database=args.db_database,
        timeout=args.db_timeout_seconds,
        server_settings={
            "statement_timeout": str(int(args.statement_timeout_seconds * 1000))
        },
    )


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse explicit database/API settings without printing secret values."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, default=DEFAULT_MANIFEST)
    parser.add_argument("--entry-id", action="append", default=[])
    parser.add_argument("--source-id", action="append", default=[])
    parser.add_argument("--max-sources", type=int, default=100)
    parser.add_argument("--samples-per-source", type=int, default=1)
    parser.add_argument("--phone-candidate-limit", type=int, default=5)
    parser.add_argument(
        "--db-host", default=os.getenv("HLTHPRT_DB_HOST") or "127.0.0.1"
    )
    parser.add_argument(
        "--db-port", type=int, default=int(os.getenv("HLTHPRT_DB_PORT") or "5432")
    )
    parser.add_argument(
        "--db-database", default=os.getenv("HLTHPRT_DB_DATABASE") or "healthporta"
    )
    parser.add_argument("--db-user", default=os.getenv("HLTHPRT_DB_USER") or "postgres")
    parser.add_argument("--db-password", default=os.getenv("HLTHPRT_DB_PASSWORD") or "")
    parser.add_argument("--db-schema", default=os.getenv("HLTHPRT_DB_SCHEMA") or "mrf")
    parser.add_argument("--db-timeout-seconds", type=float, default=10.0)
    parser.add_argument("--statement-timeout-seconds", type=float, default=10.0)
    parser.add_argument(
        "--api-base-url", default=os.getenv(DEFAULT_API_BASE_URL_ENV) or ""
    )
    parser.add_argument(
        "--api-bearer-token", default=os.getenv(DEFAULT_API_BEARER_TOKEN_ENV) or ""
    )
    parser.add_argument("--api-key", default=os.getenv(DEFAULT_API_KEY_ENV) or "")
    parser.add_argument("--api-key-header", default="X-API-Key")
    parser.add_argument("--api-timeout-seconds", type=float, default=15.0)
    parser.add_argument("--api-latency-slo-ms", type=float, default=40.0)
    parser.add_argument("--data-only", action="store_true")
    parser.add_argument(
        "--require-mapped-evidence",
        action="store_true",
        help=(
            "Return inconclusive when selected sources have no mapped "
            "role, plan, or network witness."
        ),
    )
    return parser.parse_args(argv)


def _harness_config(args: argparse.Namespace) -> HarnessConfig:
    return HarnessConfig(
        manifest_path=args.manifest,
        schema=args.db_schema,
        entry_ids=_normalized_ids(args.entry_id),
        source_ids=_normalized_ids(args.source_id),
        max_sources=args.max_sources,
        samples_per_source=args.samples_per_source,
        candidate_limit=args.phone_candidate_limit,
        api_latency_slo_ms=args.api_latency_slo_ms,
        require_mapped_evidence=args.require_mapped_evidence,
    )


async def run(args: argparse.Namespace) -> dict[str, Any]:
    """Connect, probe current evidence, and close the database session."""
    if args.db_timeout_seconds <= 0 or args.statement_timeout_seconds <= 0:
        raise ValueError("database timeouts must be greater than zero")
    api_config = ApiConfig(
        args.api_base_url,
        args.api_bearer_token or None,
        args.api_key or None,
        args.api_key_header,
        args.api_timeout_seconds,
        args.data_only,
    )
    conn = await _connect(args)
    try:
        return await build_report(_harness_config(args), conn, api_config)
    finally:
        await conn.close()


def main(argv: list[str] | None = None) -> int:
    """Print the safe JSON report and return nonzero for required failures."""
    try:
        report = asyncio.run(run(parse_args(argv)))
        print(json.dumps(redact_sensitive(report), indent=2, sort_keys=True))
        return (
            1
            if report["summary"]["required_sources_failed"]
            or report["summary"].get("completion_inconclusive")
            else 0
        )
    except Exception as exc:  # Never serialize command arguments or exception text.
        print(
            json.dumps({"status": "error", "error": type(exc).__name__}, sort_keys=True)
        )
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
