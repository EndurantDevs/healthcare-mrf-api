# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Automatic release audit and activation for one strict PTG V3 candidate."""

from __future__ import annotations

import contextlib
import json
import os
import re
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any, AsyncIterator, Mapping, Sequence
from urllib.parse import urlsplit

from db.connection import db
from process.control_lifecycle import mark_control_run
from process.ptg_parts.db_tables import _quote_ident
from process.ptg_parts.domain import PTG2_CANDIDATE_ACTIVATION_CONTRACT
from process.ptg_parts.ptg2_fast_candidate_audit import (
    FastAuditHttpConfig,
    FastAuditTarget,
    FastCandidateAuditError,
    run_fast_candidate_audit,
)
from process.ptg_parts.ptg2_candidate_attestation import (
    PTG2_TRUSTED_CLUSTER_HTTP_TRANSPORT,
    PTG2_VERIFIED_HTTPS_TRANSPORT,
    record_candidate_audit_attestation,
)
from process.ptg_parts.ptg2_provider_quarantine import (
    validate_provider_identifier_quarantine,
)
from process.ptg_parts.ptg2_source_witness import (
    PTG2_V3_SOURCE_WITNESS_PAYLOAD_CONTRACT,
    source_set_digest,
)
from process.ptg_parts.ptg2_source_witness_store import load_shared_source_witness
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


@dataclass(frozen=True)
class CandidateAuditTarget:
    candidate_run_id: str
    snapshot_id: str
    snapshot_status: str
    snapshot_key: int
    source_key: str
    plan_id: str
    plan_market_type: str
    expected_current_snapshot_id: str | None
    current_snapshot_id: str | None
    raw_container_sha256: tuple[str, ...]
    provider_identifier_quarantine: Mapping[str, Any]
    source_witness: Mapping[str, Any]
    audit_sample: Mapping[str, Any]
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
               binding.snapshot_key,
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
    source_set = _mapping(serving_index.get("source_set"))
    source_witness = _mapping(serving_index.get("source_witness"))
    layout_source_witness = _mapping(
        layout_serving_index.get("source_witness")
    )
    audit_sample = _mapping(serving_index.get("audit_sample"))
    layout_audit_sample = _mapping(layout_serving_index.get("audit_sample"))
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
    expected_source_set_digest = source_set_digest(raw_container_sha256)
    if (
        source_witness != layout_source_witness
        or audit_sample != layout_audit_sample
        or source_witness.get("contract")
        != PTG2_V3_SOURCE_WITNESS_PAYLOAD_CONTRACT
        or int(source_witness.get("source_count") or -1)
        != len(raw_container_sha256)
        or source_witness.get("source_set_digest") != expected_source_set_digest
        or source_set.get("raw_container_sha256_digest")
        != expected_source_set_digest
    ):
        raise ValueError(
            "candidate source witness changed after layout sealing"
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
        snapshot_key=int(candidate_row["snapshot_key"]),
        source_key=source_key,
        plan_id=plan_id,
        plan_market_type=plan_market_type,
        expected_current_snapshot_id=expected_current,
        current_snapshot_id=current_snapshot,
        raw_container_sha256=raw_container_sha256,
        provider_identifier_quarantine=provider_identifier_quarantine,
        source_witness=source_witness,
        audit_sample=audit_sample,
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


def _audit_configuration(snapshot_id: str) -> FastAuditHttpConfig:
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
    parsed = urlsplit(api_base_url)
    is_trusted_cluster_http = trusted_cluster_http_text == "true"
    if parsed.scheme == "https" and parsed.netloc:
        should_verify_tls = True
        transport_contract = PTG2_VERIFIED_HTTPS_TRANSPORT
    elif (
        is_trusted_cluster_http
        and ptg2_v3_source_api_audit._is_cluster_http_api_origin(api_base_url)
    ):
        should_verify_tls = False
        transport_contract = PTG2_TRUSTED_CLUSTER_HTTP_TRANSPORT
    else:
        raise ValueError(
            "candidate audit requires verified HTTPS or explicit cluster HTTP"
        )
    token_value = f"{auth_scheme} {token}".strip()
    return FastAuditHttpConfig(
        api_base_url=api_base_url,
        headers={
            auth_header: token_value,
            ptg2_v3_source_api_audit.CANDIDATE_AUDIT_HEADER: snapshot_id,
            "Accept": "application/json",
            "User-Agent": "ptg2-v3-fast-candidate-audit/1.0",
        },
        verify_tls=should_verify_tls,
        transport_contract=transport_contract,
    )


async def run_release_audit(
    candidate_target: CandidateAuditTarget,
    witness: Any,
) -> dict[str, Any]:
    """Run the bounded PostgreSQL witness audit without opening source files."""

    try:
        audit_report = await run_fast_candidate_audit(
            witness=witness,
            audit_target=FastAuditTarget(
                snapshot_id=candidate_target.snapshot_id,
                source_key=candidate_target.source_key,
                plan_id=candidate_target.plan_id,
                plan_market_type=candidate_target.plan_market_type,
                source_count=len(candidate_target.raw_container_sha256),
                source_set_digest=source_set_digest(
                    candidate_target.raw_container_sha256
                ),
                audit_sample=candidate_target.audit_sample,
                provider_identifier_quarantine=(
                    candidate_target.provider_identifier_quarantine
                ),
            ),
            http=_audit_configuration(candidate_target.snapshot_id),
        )
    except FastCandidateAuditError as exc:
        raise CandidateAuditReleaseGateError(
            f"candidate release audit failed: {exc.reason}"
        ) from exc
    report_source = _mapping(audit_report.get("source"))
    try:
        observed_quarantine = validate_provider_identifier_quarantine(
            report_source.get("provider_identifier_quarantine")
        )
    except ValueError as exc:
        raise CandidateAuditReleaseGateError(
            "candidate release audit has invalid provider identifier quarantine evidence"
        ) from exc
    if observed_quarantine != dict(
        candidate_target.provider_identifier_quarantine
    ):
        raise CandidateAuditReleaseGateError(
            "candidate release audit provider identifier quarantine does not match publication"
        )
    if (
        audit_report.get("status") != "pass"
        or audit_report.get("release_gate_eligible") is not True
    ):
        raise CandidateAuditReleaseGateError(
            "candidate release audit did not pass the release gate"
        )
    return audit_report


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
            "source_witnesses",
            "api_witnesses_matched",
            "api_challenges_executed",
            "provider_witnesses_validated",
            "api_audit_occurrences_validated",
        ),
    )
    counts.update(_integer_metrics(http, ("standard_api_actual_http_requests",)))
    audit_timings_by_metric: dict[str, Any] = {}
    duration = report.get("duration_seconds")
    if isinstance(duration, (int, float)) and not isinstance(duration, bool):
        audit_timings_by_metric["duration_seconds"] = float(duration)
    latency = _mapping(report.get("latency"))
    for output_key, source_key in (
        ("request_p50_ms", "request_p50_ms"),
        ("request_p95_ms", "request_p95_ms"),
        ("request_max_ms", "request_max_ms"),
    ):
        p95_milliseconds = latency.get(source_key)
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
) -> dict[str, Any]:
    """Audit sealed source witnesses, attest the report, and promote."""

    await _progress(
        control_run_id,
        snapshot_id=candidate_target.snapshot_id,
        phase="candidate source validation",
        message="loading sealed source witnesses from PostgreSQL",
        pct=20,
    )
    witness = await load_shared_source_witness(
        schema_name=os.getenv("HLTHPRT_DB_SCHEMA") or "mrf",
        snapshot_key=candidate_target.snapshot_key,
        expected_raw_source_sha256=candidate_target.raw_container_sha256,
        expected_metadata=candidate_target.source_witness,
    )
    await _progress(
        control_run_id,
        snapshot_id=candidate_target.snapshot_id,
        phase="candidate release audit",
        message=(
            f"auditing {len(witness.occurrence_records):,} sealed source occurrences "
            "through concurrent public API requests"
        ),
        pct=35,
    )
    report = await run_release_audit(candidate_target, witness)
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

        _audit_configuration(candidate_target.snapshot_id)
        return await _audit_and_activate(
            candidate_target,
            control_run_id=run_id,
        )


__all__ = [
    "ARCH_VERSION",
    "CandidateAuditTarget",
    "IMPORTER_NAME",
    "candidate_audit_guard",
    "load_candidate_audit_target",
    "main",
    "run_release_audit",
]
