# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import importlib
import json
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, Mock

import pytest

from api import control_imports, control_workers
from process import PTGCandidateAudit
from process.ptg_parts.ptg2_provider_quarantine import (
    provider_identifier_quarantine_payload,
)
from process.ptg_parts.ptg2_source_witness import source_set_digest


ptg_candidate_audit = importlib.import_module("process.ptg_candidate_audit")


RAW_DIGEST = "ab" * 32
SOURCE_SET_DIGEST = source_set_digest((RAW_DIGEST,))
SOURCE_WITNESS_DIGEST = "cd" * 32
SOURCE_WITNESS_SAMPLE_DIGEST = "de" * 32
AUDIT_SAMPLE_DIGEST = "ef" * 32
EMPTY_PROVIDER_IDENTIFIER_QUARANTINE = provider_identifier_quarantine_payload({})
NONEMPTY_PROVIDER_IDENTIFIER_QUARANTINE = provider_identifier_quarantine_payload(
    {123456789: 2}
)


def _source_witness_by_field() -> dict[str, object]:
    return {
        "contract": "ptg2_v3_source_witness_payload_v2",
        "format_version": 2,
        "selection_method": "bottom_k_atomic_occurrence_exponential_priority_v2",
        "population_semantics": "queryable_emitted_price_provider_occurrence_v1",
        "unqueryable_rate_policy": "count_but_exclude_from_npi_api_challenges_v1",
        "source_count": 1,
        "source_set_digest": SOURCE_SET_DIGEST,
        "total_target": 2_048,
        "provider_quota": 48,
        "queryable_occurrence_population_count": 5_000,
        "provider_population_count": 100,
        "emitted_rate_row_count": 1_000,
        "unqueryable_rate_row_count": 0,
        "occurrence_witness_count": 2_000,
        "provider_witness_count": 48,
        "record_count": 2_048,
        "sample_digest": SOURCE_WITNESS_SAMPLE_DIGEST,
        "payload_sha256": SOURCE_WITNESS_DIGEST,
        "payload_bytes": 123_456,
        "compression": "per_record_zlib",
    }


def _audit_sample_by_field() -> dict[str, object]:
    return {
        "contract": "persisted_served_occurrence_sample_v2",
        "sample_digest": AUDIT_SAMPLE_DIGEST,
    }


def _candidate_row(
    *,
    activated: bool = False,
    provider_identifier_quarantine=EMPTY_PROVIDER_IDENTIFIER_QUARANTINE,
) -> dict[str, object]:
    """Build one sealed candidate database row fixture."""

    snapshot_id = "candidate-snapshot"
    activation_map = {
        "contract": "ptg2_candidate_activation_v1",
        "state": "activated" if activated else "validated",
        "source_key": "derived-source",
        "expected_previous_snapshot_id": "previous-snapshot",
    }
    if activated:
        activation_map["mode"] = "audited_control"
    serving_index_by_field = {
        "arch_version": "postgres_binary_v3",
        "storage_generation": "shared_blocks_v3",
        "source_set": {
            "contract": "sorted_raw_container_sha256_bytes_v1",
            "source_count": 1,
            "raw_container_sha256_digest": SOURCE_SET_DIGEST,
        },
        "source_witness": _source_witness_by_field(),
        "audit_sample": _audit_sample_by_field(),
        "provider_identifier_quarantine": provider_identifier_quarantine,
    }
    return {
        "snapshot_id": snapshot_id,
        "import_run_id": "ptg2:derived-import",
        "status": "published" if activated else "validated",
        "previous_snapshot_id": "previous-snapshot",
        "manifest": {
            "activation": activation_map,
            "serving_index": serving_index_by_field,
        },
        "snapshot_key": 17,
        "plan_id": "12-3456789",
        "plan_market_type": "group",
        "layout_state": "sealed",
        "layout_generation": "shared_blocks_v3",
        "layout_manifest": {"serving_index": serving_index_by_field},
        "current_snapshot_id": snapshot_id if activated else "previous-snapshot",
        "audit_report_digest": bytes.fromhex("cd" * 32) if activated else None,
        "audit_report": (
            _passing_report(
                provider_identifier_quarantine=provider_identifier_quarantine
            )
            if activated
            else None
        ),
        "audit_activated_at": "2026-07-13T12:00:00+00:00" if activated else None,
    }


def _passing_report(
    *,
    provider_identifier_quarantine=EMPTY_PROVIDER_IDENTIFIER_QUARANTINE,
) -> dict[str, object]:
    return {
        "status": "pass",
        "release_gate_eligible": True,
        "duration_seconds": 12.5,
        "checks": {
            "source_witnesses": 2_048,
            "api_witnesses_matched": 2_000,
            "api_challenges_executed": 2_000,
            "provider_witnesses_validated": 48,
            "api_audit_occurrences_validated": 1,
        },
        "http": {"standard_api_actual_http_requests": 2_001},
        "latency": {
            "request_p50_ms": 18.2,
            "request_p95_ms": 31.2,
            "request_max_ms": 35.4,
        },
        "source": {
            "private_detail": "must-not-leak",
            "source_set_digest": SOURCE_SET_DIGEST,
            "source_witness_payload_sha256": SOURCE_WITNESS_DIGEST,
            "provider_identifier_quarantine": provider_identifier_quarantine,
        },
        "failures": {"examples": [{"body": "must-not-leak"}]},
    }


def _target(
    *,
    activated: bool = False,
    provider_identifier_quarantine=EMPTY_PROVIDER_IDENTIFIER_QUARANTINE,
) -> ptg_candidate_audit.CandidateAuditTarget:
    return ptg_candidate_audit.CandidateAuditTarget(
        candidate_run_id="ptg2:derived-import",
        snapshot_id="candidate-snapshot",
        snapshot_status="published" if activated else "validated",
        snapshot_key=17,
        source_key="derived-source",
        plan_id="12-3456789",
        plan_market_type="group",
        expected_current_snapshot_id="previous-snapshot",
        current_snapshot_id="candidate-snapshot" if activated else "previous-snapshot",
        raw_container_sha256=(RAW_DIGEST,),
        provider_identifier_quarantine=provider_identifier_quarantine,
        source_witness=_candidate_row(
            provider_identifier_quarantine=provider_identifier_quarantine
        )["manifest"]["serving_index"]["source_witness"],
        audit_sample=_candidate_row(
            provider_identifier_quarantine=provider_identifier_quarantine
        )["manifest"]["serving_index"]["audit_sample"],
        activated=activated,
        audit_report=(
            _passing_report(
                provider_identifier_quarantine=provider_identifier_quarantine
            )
            if activated
            else None
        ),
        audit_report_digest="cd" * 32 if activated else None,
    )


def test_registry_and_dedicated_worker_contract():
    importers_map = {item["name"]: item for item in control_imports.importer_registry()}
    workers_map = {item["queue"]: item for item in control_workers.worker_registry()}
    adapter = control_imports._SINGLE_JOB_ADAPTERS["ptg-candidate-audit"]

    item = importers_map["ptg-candidate-audit"]
    assert item["family"] == "mrf"
    assert item["enqueue_adapter"] == "arq_single_job"
    assert item["cancelable"] is True
    assert item["queue"] == "arq:PTGCandidateAudit"
    assert [param["name"] for param in item["params_schema"]] == [
        "candidate_run_id",
        "snapshot_id",
        "import_id",
    ]
    assert item["params_schema"][0]["required"] is True
    assert adapter["target_module"] == "process.ptg_candidate_audit"
    assert adapter["target_function"] == "main"
    assert adapter["job_prefix"] == "ptg_candidate_audit"
    assert workers_map["arq:PTGCandidateAudit"]["worker_class"] == "process.PTGCandidateAudit"
    assert PTGCandidateAudit.max_jobs == 1
    assert PTGCandidateAudit.queue_read_limit == 1


@pytest.mark.asyncio
async def test_generic_enqueue_uses_dedicated_queue_and_stable_job_id(monkeypatch):
    calls: list[tuple[tuple[object, ...], dict[str, object]]] = []

    class Job:
        job_id = "ptg_candidate_audit_control-run"

    class Redis:
        async def enqueue_job(self, *args, **kwargs):
            calls.append((args, kwargs))
            return Job()

    async def create_pool(*_args, **_kwargs):
        return Redis()

    monkeypatch.setattr(control_imports, "create_pool", create_pool)
    enqueue_response = await control_imports._enqueue_import_start(
        {
            "run_id": "control-run",
            "importer": "ptg-candidate-audit",
            "family": "mrf",
            "params": {
                "candidate_run_id": "ptg2:derived-import",
                "snapshot_id": "candidate-snapshot",
            },
        }
    )

    assert enqueue_response["status"] == "queued"
    assert enqueue_response["metrics"]["queue"] == "arq:PTGCandidateAudit"
    args, kwargs = calls[0]
    assert args[0] == "control_single_job_start"
    assert args[1]["task"]["candidate_run_id"] == "ptg2:derived-import"
    assert kwargs == {
        "_queue_name": "arq:PTGCandidateAudit",
        "_job_id": "ptg_candidate_audit_control-run",
    }


@pytest.mark.asyncio
async def test_candidate_scope_is_derived_and_corroboration_cannot_spoof(monkeypatch):
    candidate_rows = AsyncMock(return_value=[_candidate_row()])
    raw_sources = AsyncMock(return_value=(RAW_DIGEST,))
    monkeypatch.setattr(ptg_candidate_audit, "_candidate_rows", candidate_rows)
    monkeypatch.setattr(ptg_candidate_audit, "_candidate_raw_sources", raw_sources)

    target = await ptg_candidate_audit.load_candidate_audit_target(
        candidate_run_id="ptg2:derived-import",
        snapshot_id="candidate-snapshot",
        import_id="derived-import",
    )

    assert target.plan_id == "12-3456789"
    assert target.plan_market_type == "group"
    assert target.source_key == "derived-source"
    assert target.raw_container_sha256 == (RAW_DIGEST,)
    candidate_rows.assert_awaited_once_with("ptg2:derived-import")
    raw_sources.assert_awaited_once_with("candidate-snapshot")

    with pytest.raises(ValueError, match="snapshot_id does not corroborate"):
        await ptg_candidate_audit.load_candidate_audit_target(
            candidate_run_id="ptg2:derived-import",
            snapshot_id="caller-spoofed-snapshot",
        )
    with pytest.raises(ValueError, match="import_id does not corroborate"):
        await ptg_candidate_audit.load_candidate_audit_target(
            candidate_run_id="ptg2:derived-import",
            import_id="caller-spoofed-import",
        )


@pytest.mark.asyncio
async def test_candidate_scope_rejects_snapshot_layout_quarantine_mismatch(monkeypatch):
    candidate_row = _candidate_row()
    candidate_row["layout_manifest"] = {
        "serving_index": {
            "arch_version": "postgres_binary_v3",
            "storage_generation": "shared_blocks_v3",
            "provider_identifier_quarantine": provider_identifier_quarantine_payload(
                {123456789: 1}
            ),
        }
    }
    monkeypatch.setattr(
        ptg_candidate_audit,
        "_candidate_rows",
        AsyncMock(return_value=[candidate_row]),
    )
    monkeypatch.setattr(
        ptg_candidate_audit,
        "_candidate_raw_sources",
        AsyncMock(return_value=(RAW_DIGEST,)),
    )

    with pytest.raises(ValueError, match="changed after layout sealing"):
        await ptg_candidate_audit.load_candidate_audit_target(
            candidate_run_id="ptg2:derived-import",
        )


def test_candidate_audit_has_no_retained_source_file_dependency():
    assert not hasattr(ptg_candidate_audit, "resolve_retained_raw_files")


@pytest.mark.asyncio
async def test_release_audit_uses_postgres_witness_and_bounded_http_configuration(
    monkeypatch,
):
    expected_report = _passing_report()
    runner = AsyncMock(return_value=expected_report)
    witness = Mock(occurrence_records=())
    monkeypatch.setenv(
        "HLTHPRT_PTG2_CANDIDATE_AUDIT_API_BASE_URL",
        "http://candidate-api.default.svc.cluster.local:8080",
    )
    monkeypatch.setenv(
        "HLTHPRT_PTG2_CANDIDATE_AUDIT_TRUSTED_CLUSTER_HTTP",
        "true",
    )
    monkeypatch.setenv("HLTHPRT_CONTROL_API_TOKEN", "public-control-token")
    monkeypatch.setattr(ptg_candidate_audit, "run_fast_candidate_audit", runner)

    report = await ptg_candidate_audit.run_release_audit(_target(), witness)

    assert report is expected_report
    kwargs = runner.await_args.kwargs
    assert kwargs["witness"] is witness
    assert kwargs["audit_target"].snapshot_id == "candidate-snapshot"
    assert kwargs["audit_target"].source_set_digest == SOURCE_SET_DIGEST
    assert kwargs["http"].concurrency == 32
    assert kwargs["http"].deadline_seconds == 55.0
    assert kwargs["http"].verify_tls is False
    assert kwargs["http"].require_uvloop is True
    assert kwargs["http"].headers["Authorization"] == "Bearer public-control-token"


@pytest.mark.asyncio
async def test_release_audit_accepts_exact_nonempty_provider_quarantine(monkeypatch):
    expected_report = _passing_report(
        provider_identifier_quarantine=NONEMPTY_PROVIDER_IDENTIFIER_QUARANTINE
    )
    runner = AsyncMock(return_value=expected_report)
    monkeypatch.setenv(
        "HLTHPRT_PTG2_CANDIDATE_AUDIT_API_BASE_URL",
        "https://public-api.internal.example",
    )
    monkeypatch.setenv("HLTHPRT_CONTROL_API_TOKEN", "public-control-token")
    monkeypatch.setattr(ptg_candidate_audit, "run_fast_candidate_audit", runner)

    report = await ptg_candidate_audit.run_release_audit(
        _target(
            provider_identifier_quarantine=(
                NONEMPTY_PROVIDER_IDENTIFIER_QUARANTINE
            )
        ),
        object(),
    )

    assert report is expected_report
    assert (
        runner.await_args.kwargs["audit_target"].provider_identifier_quarantine
        == NONEMPTY_PROVIDER_IDENTIFIER_QUARANTINE
    )


@pytest.mark.asyncio
async def test_release_audit_failure_is_deterministic_and_not_retryable(monkeypatch):
    monkeypatch.setenv(
        "HLTHPRT_PTG2_CANDIDATE_AUDIT_API_BASE_URL",
        "https://public-api.internal.example",
    )
    monkeypatch.setenv("HLTHPRT_CONTROL_API_TOKEN", "public-control-token")
    monkeypatch.setattr(
        ptg_candidate_audit,
        "run_fast_candidate_audit",
        AsyncMock(
            side_effect=ptg_candidate_audit.FastCandidateAuditError(
                "source_witness_missing_from_api"
            )
        ),
    )

    with pytest.raises(ptg_candidate_audit.CandidateAuditReleaseGateError) as exc_info:
        await ptg_candidate_audit.run_release_audit(_target(), object())

    assert str(exc_info.value).endswith("source_witness_missing_from_api")
    assert exc_info.value.control_error_code == "ptg_candidate_audit_release_gate_failed"
    assert exc_info.value.retryable is False


def test_release_audit_rejects_untrusted_plain_http_configuration(monkeypatch):
    monkeypatch.setenv(
        "HLTHPRT_PTG2_CANDIDATE_AUDIT_API_BASE_URL",
        "http://public-api.example",
    )
    monkeypatch.setenv("HLTHPRT_CONTROL_API_TOKEN", "public-control-token")

    with pytest.raises(ValueError, match="verified HTTPS or explicit cluster HTTP"):
        ptg_candidate_audit._audit_configuration("candidate-snapshot")


@pytest.mark.asyncio
async def test_passing_audit_loads_postgres_witness_attests_then_activates(monkeypatch):
    events: list[str] = []
    report = _passing_report(
        provider_identifier_quarantine=NONEMPTY_PROVIDER_IDENTIFIER_QUARANTINE
    )
    witness = Mock(occurrence_records=())
    monkeypatch.setattr(ptg_candidate_audit, "_progress", AsyncMock())
    monkeypatch.setattr(
        ptg_candidate_audit,
        "load_shared_source_witness",
        AsyncMock(return_value=witness),
    )

    async def audit(target, observed_witness):
        events.append("audit")
        assert target.snapshot_key == 17
        assert observed_witness is witness
        return report

    monkeypatch.setattr(ptg_candidate_audit, "run_release_audit", audit)

    async def attest(**kwargs):
        events.append("attest")
        assert kwargs["plan_id"] == "12-3456789"
        assert kwargs["source_key"] == "derived-source"
        assert kwargs["report"] is report
        return {"report_digest": "ef" * 32}

    async def promote(**kwargs):
        events.append("promote")
        assert kwargs["expected_current_snapshot_id"] == "previous-snapshot"
        return {"status": "promoted"}

    monkeypatch.setattr(ptg_candidate_audit, "record_candidate_audit_attestation", attest)
    monkeypatch.setattr(ptg_candidate_audit, "promote_ptg2_source_snapshot", promote)

    activation_response = await ptg_candidate_audit._audit_and_activate(
        _target(
            provider_identifier_quarantine=(
                NONEMPTY_PROVIDER_IDENTIFIER_QUARANTINE
            )
        ),
        control_run_id="control-run",
    )

    assert events == ["audit", "attest", "promote"]
    assert activation_response["arch_version"] == "postgres_binary_v3"
    assert activation_response["snapshot_status"] == "published"
    assert activation_response["activation_status"] == "activated"
    assert activation_response["audit_report_digest"] == "ef" * 32
    assert activation_response["audit_counts"]["standard_api_actual_http_requests"] == 2_001
    assert activation_response["metrics"]["candidate_run_id"] == "ptg2:derived-import"


@pytest.mark.asyncio
async def test_failing_audit_never_attests_or_activates(monkeypatch):
    monkeypatch.setattr(ptg_candidate_audit, "_progress", AsyncMock())
    monkeypatch.setattr(
        ptg_candidate_audit,
        "load_shared_source_witness",
        AsyncMock(return_value=Mock(occurrence_records=())),
    )
    monkeypatch.setattr(
        ptg_candidate_audit,
        "run_release_audit",
        AsyncMock(
            side_effect=RuntimeError(
                "candidate release audit did not pass the release gate"
            )
        ),
    )
    attest = AsyncMock()
    promote = AsyncMock()
    monkeypatch.setattr(ptg_candidate_audit, "record_candidate_audit_attestation", attest)
    monkeypatch.setattr(ptg_candidate_audit, "promote_ptg2_source_snapshot", promote)

    with pytest.raises(RuntimeError, match="did not pass"):
        await ptg_candidate_audit._audit_and_activate(
            _target(),
            control_run_id="control-run",
        )

    attest.assert_not_awaited()
    promote.assert_not_awaited()


@pytest.mark.asyncio
async def test_already_active_redelivery_returns_corroborated_success(monkeypatch):
    @asynccontextmanager
    async def guard(_candidate_run_id):
        yield

    audit_configuration = Mock(side_effect=AssertionError("redelivery must not require API configuration"))
    monkeypatch.setattr(ptg_candidate_audit, "_audit_configuration", audit_configuration)
    monkeypatch.setattr(ptg_candidate_audit, "candidate_audit_guard", guard)
    monkeypatch.setattr(
        ptg_candidate_audit,
        "load_candidate_audit_target",
        AsyncMock(return_value=_target(activated=True)),
    )
    audit = AsyncMock()
    monkeypatch.setattr(ptg_candidate_audit, "_audit_and_activate", audit)
    monkeypatch.setattr(ptg_candidate_audit, "_progress", AsyncMock())

    redelivery_response = await ptg_candidate_audit.main(
        candidate_run_id="ptg2:derived-import",
        snapshot_id="candidate-snapshot",
        import_id="derived-import",
        run_id="control-run",
    )

    assert redelivery_response["activation_status"] == "activated"
    assert redelivery_response["idempotent"] is True
    assert redelivery_response["audit_report_digest"] == "cd" * 32
    audit.assert_not_awaited()
    audit_configuration.assert_not_called()


def test_scheduler_result_is_redacted():
    result = ptg_candidate_audit._success_result(
        _target(),
        report=_passing_report(),
        report_digest="ef" * 32,
        idempotent=False,
    )
    encoded = json.dumps(result, sort_keys=True)

    assert "must-not-leak" not in encoded
    assert "derived-source" not in encoded
    assert "12-3456789" not in encoded
    assert "source_path" not in encoded
    assert "Authorization" not in encoded
    assert "response" not in encoded
    assert set(result) == {
        "arch_version",
        "snapshot_status",
        "activation_status",
        "snapshot_id",
        "import_run_id",
        "candidate_run_id",
        "idempotent",
        "audit_report_digest",
        "audit_counts",
        "audit_timings",
        "metrics",
    }
