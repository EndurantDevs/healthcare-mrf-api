# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Automatic release audit and activation for one strict PTG V3 candidate."""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import json
import os
import re
import tempfile
from contextlib import asynccontextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, AsyncIterator, Mapping, Sequence

from db.connection import db
from process.control_lifecycle import mark_control_run
from process.ptg_parts.artifacts import PTG2ArtifactStore, sha256_file
from process.ptg_parts.db_tables import _quote_ident
from process.ptg_parts.domain import PTG2_ARTIFACT_RAW, PTG2_CANDIDATE_ACTIVATION_CONTRACT
from process.ptg_parts.input_artifact_retention import (
    artifact_lease_context,
    guard_artifact_lease,
    protect_existing_artifact,
)
from process.ptg_parts.ptg2_candidate_attestation import (
    record_candidate_audit_attestation,
)
from process.ptg_parts.ptg2_provider_quarantine import (
    validate_provider_identifier_quarantine,
)
from process.ptg_parts.source_snapshot_control import promote_ptg2_source_snapshot
from scripts.validation import ptg2_v3_source_api_audit


IMPORTER_NAME = "ptg-candidate-audit"
ARCH_VERSION = "postgres_binary_v3"
STORAGE_GENERATION = "shared_blocks_v3"
API_BASE_URL_ENV = "HLTHPRT_PTG2_CANDIDATE_AUDIT_API_BASE_URL"
AUTH_HEADER_ENV = "HLTHPRT_PTG2_CANDIDATE_AUDIT_AUTH_HEADER"
AUTH_SCHEME_ENV = "HLTHPRT_PTG2_CANDIDATE_AUDIT_AUTH_SCHEME"
TRUSTED_CLUSTER_HTTP_ENV = "HLTHPRT_PTG2_CANDIDATE_AUDIT_TRUSTED_CLUSTER_HTTP"
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_SAFE_FAILURE_REASON_RE = re.compile(r"^[a-z0-9_]{1,128}$")


@dataclass(frozen=True)
class CandidateAuditTarget:
    candidate_run_id: str
    snapshot_id: str
    snapshot_status: str
    source_key: str
    plan_id: str
    plan_market_type: str
    expected_current_snapshot_id: str | None
    current_snapshot_id: str | None
    raw_container_sha256: tuple[str, ...]
    provider_identifier_quarantine: Mapping[str, Any]
    activated: bool
    audit_report: Mapping[str, Any] | None = None
    audit_report_digest: str | None = None


class CandidateAuditReleaseGateError(RuntimeError):
    """A deterministic release-audit mismatch that must not be retried."""

    control_error_code = "ptg_candidate_audit_release_gate_failed"
    retryable = False


def _mapping(value: Any) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return {}
        return dict(parsed) if isinstance(parsed, dict) else {}
    return {}


def _row_mapping(row: Any) -> dict[str, Any]:
    if isinstance(row, Mapping):
        return dict(row)
    return dict(getattr(row, "_mapping", row))


def _normalized_digest(value: Any, *, field: str) -> str:
    if isinstance(value, (bytes, bytearray, memoryview)):
        normalized = bytes(value).hex()
    else:
        normalized = str(value or "").strip().lower()
    if not _SHA256_RE.fullmatch(normalized):
        raise ValueError(f"candidate {field} is invalid")
    return normalized


def _candidate_import_id(candidate_run_id: str) -> str:
    return candidate_run_id.removeprefix("ptg2:")


def _validate_corroboration(
    *,
    candidate_run_id: str,
    observed_snapshot_id: str,
    snapshot_id: str | None,
    import_id: str | None,
) -> None:
    expected_snapshot_id = str(snapshot_id or "").strip()
    if expected_snapshot_id and expected_snapshot_id != observed_snapshot_id:
        raise ValueError("snapshot_id does not corroborate candidate_run_id")
    expected_import_id = str(import_id or "").strip()
    if expected_import_id and expected_import_id not in {
        candidate_run_id,
        _candidate_import_id(candidate_run_id),
    }:
        raise ValueError("import_id does not corroborate candidate_run_id")


async def _candidate_rows(candidate_run_id: str) -> list[dict[str, Any]]:
    schema = _quote_ident(os.getenv("HLTHPRT_DB_SCHEMA") or "mrf")
    candidate_rows = await db.all(
        f"""
        SELECT snapshot.snapshot_id,
               snapshot.import_run_id,
               snapshot.status,
               snapshot.previous_snapshot_id,
               snapshot.manifest,
               scope.plan_id,
               scope.plan_market_type,
               layout.state AS layout_state,
               layout.generation AS layout_generation,
               layout.layout_manifest,
               current_pointer.snapshot_id AS current_snapshot_id,
               attestation.report_digest AS audit_report_digest,
               attestation.report AS audit_report,
               attestation.activated_at AS audit_activated_at
          FROM {schema}.ptg2_snapshot AS snapshot
          JOIN {schema}.ptg2_v3_snapshot_binding AS binding
            ON binding.snapshot_id = snapshot.snapshot_id
          JOIN {schema}.ptg2_v3_snapshot_scope AS scope
            ON scope.snapshot_id = snapshot.snapshot_id
          JOIN {schema}.ptg2_v3_snapshot_layout AS layout
            ON layout.snapshot_key = binding.snapshot_key
          LEFT JOIN {schema}.ptg2_current_source_snapshot AS current_pointer
            ON current_pointer.source_key = lower(
                snapshot.manifest->'activation'->>'source_key'
            )
          LEFT JOIN {schema}.ptg2_v3_candidate_audit_attestation AS attestation
            ON attestation.snapshot_id = snapshot.snapshot_id
         WHERE snapshot.import_run_id = :candidate_run_id
         ORDER BY snapshot.snapshot_id
        """,
        candidate_run_id=candidate_run_id,
    )
    return [_row_mapping(candidate_row) for candidate_row in candidate_rows]


async def _candidate_raw_sources(snapshot_id: str) -> tuple[str, ...]:
    schema = _quote_ident(os.getenv("HLTHPRT_DB_SCHEMA") or "mrf")
    rows = await db.all(
        f"""
        SELECT source_key, raw_container_sha256
          FROM {schema}.ptg2_v3_snapshot_source
         WHERE snapshot_id = :snapshot_id
         ORDER BY source_key
        """,
        snapshot_id=snapshot_id,
    )
    source_rows = [_row_mapping(row) for row in rows]
    try:
        source_keys = [int(row.get("source_key")) for row in source_rows]
    except (TypeError, ValueError) as exc:
        raise ValueError("candidate source scope contains an invalid ordinal") from exc
    if source_keys != list(range(len(source_rows))):
        raise ValueError("candidate source scope is not dense")
    digests = tuple(
        _normalized_digest(row.get("raw_container_sha256"), field="raw container digest")
        for row in source_rows
    )
    if not digests:
        raise ValueError("candidate has no public raw source bindings")
    if len(digests) != len(set(digests)):
        raise ValueError("candidate raw source bindings are ambiguous")
    return digests


def _candidate_target_from_row(
    candidate_row: Mapping[str, Any],
    *,
    candidate_run_id: str,
    raw_container_sha256: tuple[str, ...],
) -> CandidateAuditTarget:
    """Validate a resolved candidate row and return its exact audit target."""

    observed_run_id = str(candidate_row.get("import_run_id") or "").strip()
    if observed_run_id != candidate_run_id:
        raise ValueError("candidate run binding changed during resolution")
    snapshot_id = str(candidate_row.get("snapshot_id") or "").strip()
    manifest = _mapping(candidate_row.get("manifest"))
    serving_index = _mapping(manifest.get("serving_index"))
    activation = _mapping(manifest.get("activation"))
    layout_manifest = _mapping(candidate_row.get("layout_manifest"))
    layout_serving_index = _mapping(layout_manifest.get("serving_index"))
    try:
        provider_identifier_quarantine = (
            validate_provider_identifier_quarantine(
                serving_index.get("provider_identifier_quarantine")
            )
        )
        layout_provider_identifier_quarantine = (
            validate_provider_identifier_quarantine(
                layout_serving_index.get("provider_identifier_quarantine")
            )
        )
    except ValueError as exc:
        raise ValueError(
            "candidate provider identifier quarantine is invalid"
        ) from exc
    if provider_identifier_quarantine != layout_provider_identifier_quarantine:
        raise ValueError(
            "candidate provider identifier quarantine changed after layout sealing"
        )
    if (
        not snapshot_id
        or serving_index.get("arch_version") != ARCH_VERSION
        or serving_index.get("storage_generation") != STORAGE_GENERATION
        or layout_serving_index.get("arch_version") != ARCH_VERSION
        or layout_serving_index.get("storage_generation") != STORAGE_GENERATION
        or str(candidate_row.get("layout_state") or "") != "sealed"
        or str(candidate_row.get("layout_generation") or "") != STORAGE_GENERATION
        or activation.get("contract") != PTG2_CANDIDATE_ACTIVATION_CONTRACT
    ):
        raise ValueError("candidate is not an exact strict postgres_binary_v3 snapshot")

    source_key = str(activation.get("source_key") or "").strip().lower()
    plan_id = str(candidate_row.get("plan_id") or "").strip()
    plan_market_type = str(candidate_row.get("plan_market_type") or "").strip().lower()
    expected_current = str(
        activation.get("expected_previous_snapshot_id") or ""
    ).strip() or None
    row_previous = str(candidate_row.get("previous_snapshot_id") or "").strip() or None
    current_snapshot = str(candidate_row.get("current_snapshot_id") or "").strip() or None
    if not source_key or not plan_id or not plan_market_type:
        raise ValueError("candidate public source scope is incomplete")
    if row_previous != expected_current:
        raise ValueError("candidate predecessor binding is inconsistent")

    status = str(candidate_row.get("status") or "").strip().lower()
    activation_state = str(activation.get("state") or "").strip().lower()
    is_activated = status == "published" and activation_state == "activated"
    if is_activated:
        if (
            activation.get("mode") != "audited_control"
            or current_snapshot != snapshot_id
            or candidate_row.get("audit_activated_at") is None
        ):
            raise ValueError("activated candidate cannot be corroborated")
    elif (
        status != "validated"
        or activation_state != "validated"
        or current_snapshot != expected_current
        or current_snapshot == snapshot_id
    ):
        raise ValueError("candidate is not validated with deferred activation")

    report = _mapping(candidate_row.get("audit_report")) if is_activated else None
    report_digest = (
        _normalized_digest(
            candidate_row.get("audit_report_digest"), field="audit report digest"
        )
        if is_activated
        else None
    )
    if is_activated and not report:
        raise ValueError("activated candidate has no corroborating audit report")
    return CandidateAuditTarget(
        candidate_run_id=candidate_run_id,
        snapshot_id=snapshot_id,
        snapshot_status=status,
        source_key=source_key,
        plan_id=plan_id,
        plan_market_type=plan_market_type,
        expected_current_snapshot_id=expected_current,
        current_snapshot_id=current_snapshot,
        raw_container_sha256=raw_container_sha256,
        provider_identifier_quarantine=provider_identifier_quarantine,
        activated=is_activated,
        audit_report=report,
        audit_report_digest=report_digest,
    )


async def load_candidate_audit_target(
    *,
    candidate_run_id: str,
    snapshot_id: str | None = None,
    import_id: str | None = None,
) -> CandidateAuditTarget:
    """Derive one exact candidate solely from public PostgreSQL snapshot state."""

    normalized_run_id = str(candidate_run_id or "").strip()
    if not normalized_run_id:
        raise ValueError("candidate_run_id is required")
    rows = await _candidate_rows(normalized_run_id)
    if not rows:
        raise ValueError("candidate_run_id did not resolve a candidate")
    if len(rows) != 1:
        raise ValueError("candidate_run_id does not resolve exactly one candidate")
    observed_snapshot_id = str(rows[0].get("snapshot_id") or "").strip()
    _validate_corroboration(
        candidate_run_id=normalized_run_id,
        observed_snapshot_id=observed_snapshot_id,
        snapshot_id=snapshot_id,
        import_id=import_id,
    )
    raw_digests = await _candidate_raw_sources(observed_snapshot_id)
    return _candidate_target_from_row(
        rows[0],
        candidate_run_id=normalized_run_id,
        raw_container_sha256=raw_digests,
    )


def resolve_retained_raw_files(
    store: PTG2ArtifactStore,
    raw_container_sha256: Sequence[str],
) -> tuple[Path, ...]:
    """Resolve and lease exactly one verified content-addressed file per digest."""

    resolved_paths: list[Path] = []
    for raw_digest in raw_container_sha256:
        digest = _normalized_digest(raw_digest, field="raw container digest")
        expected_path = store.artifact_path(digest, kind=PTG2_ARTIFACT_RAW)
        parent = expected_path.parent
        candidates = sorted(
            path
            for path in parent.glob(f"{digest}*")
            if (path.name == digest or path.name.startswith(f"{digest}."))
            and path.is_file()
            and not path.is_symlink()
        ) if parent.is_dir() else []
        if len(candidates) != 1:
            raise ValueError(
                f"candidate raw artifact {digest} resolved to {len(candidates)} retained files"
            )
        path = candidates[0]
        if not protect_existing_artifact(store, path):
            raise ValueError(f"candidate raw artifact {digest} is unavailable")
        observed_digest, _byte_count = sha256_file(path)
        if observed_digest != digest:
            raise ValueError(f"candidate raw artifact {digest} failed SHA-256 verification")
        resolved_paths.append(path)
    return tuple(resolved_paths)


def _audit_configuration() -> tuple[str, str, str, str, bool]:
    api_base_url = str(
        os.getenv(API_BASE_URL_ENV) or os.getenv("PTG_AUDIT_API_BASE_URL") or ""
    ).strip().rstrip("/")
    token = str(os.getenv("HLTHPRT_CONTROL_API_TOKEN") or "").strip()
    auth_header = str(os.getenv(AUTH_HEADER_ENV) or "Authorization").strip()
    default_scheme = "Bearer" if auth_header.lower() == "authorization" else ""
    auth_scheme = str(os.getenv(AUTH_SCHEME_ENV, default_scheme)).strip()
    trusted_cluster_http_text = str(
        os.getenv(TRUSTED_CLUSTER_HTTP_ENV, "false")
    ).strip().lower()
    if not api_base_url:
        raise ValueError(f"{API_BASE_URL_ENV} is required")
    if not token:
        raise ValueError("HLTHPRT_CONTROL_API_TOKEN is required")
    if not auth_header or any(character in auth_header for character in "\r\n"):
        raise ValueError("candidate audit auth header is invalid")
    if trusted_cluster_http_text not in {"true", "false"}:
        raise ValueError(f"{TRUSTED_CLUSTER_HTTP_ENV} must be true or false")
    return (
        api_base_url,
        token,
        auth_header,
        auth_scheme,
        trusted_cluster_http_text == "true",
    )


def _fatal_audit_reason(report: Mapping[str, Any]) -> str | None:
    failures = _mapping(report.get("failures"))
    examples = failures.get("examples")
    if not isinstance(examples, list) or not examples:
        return None
    example = examples[0]
    reason = str(example.get("reason") or "") if isinstance(example, Mapping) else ""
    return reason if _SAFE_FAILURE_REASON_RE.fullmatch(reason) else None


def run_release_audit(
    target: CandidateAuditTarget,
    source_paths: Sequence[Path],
) -> dict[str, Any]:
    """Invoke the independent release audit directly using temporary-only work."""

    (
        api_base_url,
        token,
        auth_header,
        auth_scheme,
        trusted_cluster_http,
    ) = _audit_configuration()
    with tempfile.TemporaryDirectory(prefix="ptg-candidate-audit-") as temp_dir:
        work_dir = Path(temp_dir)
        report_path = work_dir / "report.json"
        audit_arguments = [
            *(str(path) for path in source_paths),
            "--report",
            str(report_path),
            "--profile",
            "release",
            "--api-base-url",
            api_base_url,
            "--api-path",
            ptg2_v3_source_api_audit.DEFAULT_CANDIDATE_API_PATH,
            "--plan-id",
            target.plan_id,
            "--snapshot-id",
            target.snapshot_id,
            "--plan-market-type",
            target.plan_market_type,
            "--source-key",
            target.source_key,
            "--auth-token",
            token,
            "--auth-header",
            auth_header,
            "--auth-scheme",
            auth_scheme,
            "--validated-candidate",
            "--max-invalid-npis",
            str(target.provider_identifier_quarantine["occurrence_count"]),
            "--work-dir",
            str(work_dir),
        ]
        if trusted_cluster_http:
            audit_arguments.append("--trusted-cluster-http")
        exit_code = ptg2_v3_source_api_audit.run_audit_cli(audit_arguments)
        try:
            report = json.loads(report_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise RuntimeError("candidate release audit did not produce a valid report") from exc
    if not isinstance(report, dict):
        raise RuntimeError("candidate release audit report is not an object")
    if exit_code != 0 or report.get("status") == "error":
        failure_reason = _fatal_audit_reason(report)
        suffix = f": {failure_reason}" if failure_reason else ""
        raise CandidateAuditReleaseGateError(
            f"candidate release audit did not pass the release gate{suffix}"
        )
    report_source = _mapping(report.get("source"))
    try:
        observed_quarantine = validate_provider_identifier_quarantine(
            report_source.get("provider_identifier_quarantine")
        )
    except ValueError as exc:
        raise CandidateAuditReleaseGateError(
            "candidate release audit has invalid provider identifier quarantine evidence"
        ) from exc
    if observed_quarantine != dict(target.provider_identifier_quarantine):
        raise CandidateAuditReleaseGateError(
            "candidate release audit provider identifier quarantine does not match publication"
        )
    if report.get("status") != "pass" or report.get("release_gate_eligible") is not True:
        raise CandidateAuditReleaseGateError(
            "candidate release audit did not pass the release gate"
        )
    return report


def _integer_metrics(mapping: Mapping[str, Any], keys: Sequence[str]) -> dict[str, int]:
    return {
        key: value
        for key in keys
        if isinstance((value := mapping.get(key)), int) and not isinstance(value, bool)
    }


def _audit_summary(report: Mapping[str, Any], report_digest: str) -> dict[str, Any]:
    checks = _mapping(report.get("checks"))
    http = _mapping(report.get("http"))
    counts = _integer_metrics(
        checks,
        (
            "source_occurrence_ids",
            "api_occurrence_ids",
            "negative_queries",
            "random_api_requests_executed",
        ),
    )
    counts.update(_integer_metrics(http, ("standard_api_actual_http_requests",)))
    audit_timings_by_metric: dict[str, Any] = {}
    duration = report.get("duration_seconds")
    if isinstance(duration, (int, float)) and not isinstance(duration, bool):
        audit_timings_by_metric["duration_seconds"] = float(duration)
    latency = _mapping(report.get("latency"))
    cold = _mapping(latency.get("cold"))
    for output_key, source_key in (
        ("cold_first_page_p95_ms", "first_page_first_observation"),
        ("cold_logical_query_p95_ms", "logical_query"),
    ):
        p95_milliseconds = _mapping(cold.get(source_key)).get("p95_ms")
        if isinstance(p95_milliseconds, (int, float)) and not isinstance(
            p95_milliseconds, bool
        ):
            audit_timings_by_metric[output_key] = float(p95_milliseconds)
    return {
        "audit_report_digest": report_digest,
        "audit_counts": counts,
        "audit_timings": audit_timings_by_metric,
    }


def _success_result(
    target: CandidateAuditTarget,
    *,
    report: Mapping[str, Any],
    report_digest: str,
    idempotent: bool,
) -> dict[str, Any]:
    summary = _audit_summary(report, report_digest)
    audit_metrics_by_name = {
        "arch_version": ARCH_VERSION,
        "snapshot_status": "published",
        "activation_status": "activated",
        "snapshot_id": target.snapshot_id,
        "import_run_id": target.candidate_run_id,
        "candidate_run_id": target.candidate_run_id,
        "idempotent": idempotent,
        **summary,
    }
    return {
        **audit_metrics_by_name,
        "metrics": dict(audit_metrics_by_name),
    }


async def _progress(
    run_id: str | None,
    *,
    snapshot_id: str | None,
    phase: str,
    message: str,
    pct: int,
) -> None:
    if not run_id:
        return
    await mark_control_run(
        run_id,
        status="running",
        phase_detail=phase,
        progress_message=message,
        snapshot_id=snapshot_id,
        progress={
            "unit": "phase",
            "done": pct,
            "total": 100,
            "pct": pct,
            "message": message,
            "phase": phase,
        },
    )


@asynccontextmanager
async def candidate_audit_guard(candidate_run_id: str) -> AsyncIterator[None]:
    """Serialize duplicate audits for one candidate across workers and nodes."""

    if db.engine is None:
        await db.connect()
    assert db.engine is not None
    lock_name = f"ptg-candidate-audit:{candidate_run_id}"
    async with db.engine.connect() as connection:
        autocommit = connection.execution_options(isolation_level="AUTOCOMMIT")
        if hasattr(autocommit, "__await__"):
            autocommit = await autocommit
        acquired = await autocommit.scalar(
            db.text("SELECT pg_advisory_lock(hashtextextended(:lock_name, 0))"),
            {"lock_name": lock_name},
        )
        if acquired not in (None, True):
            raise RuntimeError("candidate audit database guard was not acquired")
        try:
            yield
        finally:
            with contextlib.suppress(Exception):
                await autocommit.scalar(
                    db.text("SELECT pg_advisory_unlock(hashtextextended(:lock_name, 0))"),
                    {"lock_name": lock_name},
                )


async def _audit_and_activate(
    candidate_target: CandidateAuditTarget,
    *,
    control_run_id: str | None,
    store: PTG2ArtifactStore,
) -> dict[str, Any]:
    """Audit retained sources, attest the report, and promote the candidate."""

    await _progress(
        control_run_id,
        snapshot_id=candidate_target.snapshot_id,
        phase="candidate source validation",
        message="verifying retained source artifacts",
        pct=20,
    )
    source_paths = await asyncio.to_thread(
        resolve_retained_raw_files,
        store,
        candidate_target.raw_container_sha256,
    )
    await _progress(
        control_run_id,
        snapshot_id=candidate_target.snapshot_id,
        phase="candidate release audit",
        message="indexing sources and auditing public API responses",
        pct=35,
    )
    report = await asyncio.to_thread(
        run_release_audit, candidate_target, source_paths
    )
    await _progress(
        control_run_id,
        snapshot_id=candidate_target.snapshot_id,
        phase="candidate attestation",
        message="recording passing audit attestation",
        pct=85,
    )
    attestation = await record_candidate_audit_attestation(
        snapshot_id=candidate_target.snapshot_id,
        source_key=candidate_target.source_key,
        plan_id=candidate_target.plan_id,
        plan_market_type=candidate_target.plan_market_type,
        report=report,
    )
    await _progress(
        control_run_id,
        snapshot_id=candidate_target.snapshot_id,
        phase="candidate promotion",
        message="atomically promoting audited candidate",
        pct=92,
    )
    promotion = await promote_ptg2_source_snapshot(
        source_key=candidate_target.source_key,
        snapshot_id=candidate_target.snapshot_id,
        expected_current_snapshot_id=(
            candidate_target.expected_current_snapshot_id
        ),
    )
    if promotion.get("status") != "promoted":
        raise RuntimeError("candidate promotion did not complete")
    report_digest = _normalized_digest(
        attestation.get("report_digest"),
        field="audit report digest",
    )
    return _success_result(
        candidate_target,
        report=report,
        report_digest=report_digest,
        idempotent=False,
    )


async def main(
    *,
    candidate_run_id: str,
    snapshot_id: str | None = None,
    import_id: str | None = None,
    run_id: str | None = None,
) -> dict[str, Any]:
    """Audit, attest, and atomically activate one strict V3 candidate."""

    normalized_candidate_run_id = str(candidate_run_id or "").strip()
    if not normalized_candidate_run_id:
        raise ValueError("candidate_run_id is required")
    async with candidate_audit_guard(normalized_candidate_run_id):
        await _progress(
            run_id,
            snapshot_id=None,
            phase="candidate resolution",
            message="loading candidate from PostgreSQL",
            pct=10,
        )
        candidate_target = await load_candidate_audit_target(
            candidate_run_id=normalized_candidate_run_id,
            snapshot_id=snapshot_id,
            import_id=import_id,
        )
        if candidate_target.activated:
            assert candidate_target.audit_report is not None
            assert candidate_target.audit_report_digest is not None
            return _success_result(
                candidate_target,
                report=candidate_target.audit_report,
                report_digest=candidate_target.audit_report_digest,
                idempotent=True,
            )

        _audit_configuration()
        store = PTG2ArtifactStore()
        lease_owner = hashlib.sha256(
            f"{IMPORTER_NAME}:{normalized_candidate_run_id}".encode("utf-8")
        ).hexdigest()[:32]
        with artifact_lease_context(owner=f"{IMPORTER_NAME}:{lease_owner}", store=store) as lease:
            return await guard_artifact_lease(
                lease,
                _audit_and_activate(
                    candidate_target,
                    control_run_id=run_id,
                    store=store,
                ),
            )


__all__ = [
    "ARCH_VERSION",
    "CandidateAuditTarget",
    "IMPORTER_NAME",
    "candidate_audit_guard",
    "load_candidate_audit_target",
    "main",
    "resolve_retained_raw_files",
    "run_release_audit",
]
