# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import asyncio
import hashlib
import importlib
import json
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, Mock

import pytest

from api import control_imports, control_workers
from process import PTGCandidateAudit
from process.ptg_parts.artifacts import PTG2ArtifactStore
from process.ptg_parts.ptg2_provider_quarantine import (
    provider_identifier_quarantine_payload,
)


ptg_candidate_audit = importlib.import_module("process.ptg_candidate_audit")


RAW_DIGEST = "ab" * 32
EMPTY_PROVIDER_IDENTIFIER_QUARANTINE = provider_identifier_quarantine_payload({})
NONEMPTY_PROVIDER_IDENTIFIER_QUARANTINE = provider_identifier_quarantine_payload(
    {123456789: 2}
)


def _candidate_row(
    *,
    activated: bool = False,
    provider_identifier_quarantine=EMPTY_PROVIDER_IDENTIFIER_QUARANTINE,
) -> dict[str, object]:
    snapshot_id = "candidate-snapshot"
    activation_map = {
        "contract": "ptg2_candidate_activation_v1",
        "state": "activated" if activated else "validated",
        "source_key": "derived-source",
        "expected_previous_snapshot_id": "previous-snapshot",
    }
    if activated:
        activation_map["mode"] = "audited_control"
    serving_index = {
        "arch_version": "postgres_binary_v3",
        "storage_generation": "shared_blocks_v3",
        "provider_identifier_quarantine": provider_identifier_quarantine,
    }
    return {
        "snapshot_id": snapshot_id,
        "import_run_id": "ptg2:derived-import",
        "status": "published" if activated else "validated",
        "previous_snapshot_id": "previous-snapshot",
        "manifest": {"activation": activation_map, "serving_index": serving_index},
        "plan_id": "12-3456789",
        "plan_market_type": "group",
        "layout_state": "sealed",
        "layout_generation": "shared_blocks_v3",
        "layout_manifest": {"serving_index": serving_index},
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
        "duration_seconds": 321.5,
        "checks": {
            "source_occurrence_ids": 2500,
            "api_occurrence_ids": 2500,
            "negative_queries": 500,
            "random_api_requests_executed": 2500,
        },
        "http": {"standard_api_actual_http_requests": 3000},
        "latency": {
            "cold": {
                "first_page_first_observation": {"p95_ms": 31.2},
                "logical_query": {"p95_ms": 35.4},
            }
        },
        "source": {
            "private_detail": "must-not-leak",
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
        source_key="derived-source",
        plan_id="12-3456789",
        plan_market_type="group",
        expected_current_snapshot_id="previous-snapshot",
        current_snapshot_id="candidate-snapshot" if activated else "previous-snapshot",
        raw_container_sha256=(RAW_DIGEST,),
        provider_identifier_quarantine=provider_identifier_quarantine,
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
    result = await control_imports._enqueue_import_start(
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

    assert result["status"] == "queued"
    assert result["metrics"]["queue"] == "arq:PTGCandidateAudit"
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


def test_retained_raw_resolution_fails_closed_on_missing_ambiguous_and_mismatch(tmp_path):
    store = PTG2ArtifactStore(tmp_path)
    with pytest.raises(ValueError, match="resolved to 0 retained files"):
        ptg_candidate_audit.resolve_retained_raw_files(store, (RAW_DIGEST,))

    first = store.artifact_path(RAW_DIGEST, suffix=".json")
    first.parent.mkdir(parents=True, exist_ok=True)
    first.write_bytes(b"wrong")
    second = store.artifact_path(RAW_DIGEST, suffix=".json.gz")
    second.write_bytes(b"also-wrong")
    with pytest.raises(ValueError, match="resolved to 2 retained files"):
        ptg_candidate_audit.resolve_retained_raw_files(store, (RAW_DIGEST,))

    second.unlink()
    with pytest.raises(ValueError, match="failed SHA-256 verification"):
        ptg_candidate_audit.resolve_retained_raw_files(store, (RAW_DIGEST,))

    payload = b"exact retained source"
    digest = hashlib.sha256(payload).hexdigest()
    exact = store.artifact_path(digest, suffix=".json.gz")
    exact.parent.mkdir(parents=True, exist_ok=True)
    exact.write_bytes(payload)
    assert ptg_candidate_audit.resolve_retained_raw_files(store, (digest,)) == (exact,)


def test_release_audit_uses_validated_candidate_and_unmodified_release_defaults(
    monkeypatch,
    tmp_path,
):
    captured_list: list[str] = []
    work_dir_list = [None]

    def run_cli(argv):
        captured_list.extend(argv)
        report_path = tmp_path / "unused"
        report_path = type(report_path)(argv[argv.index("--report") + 1])
        work_dir_list[0] = type(report_path)(argv[argv.index("--work-dir") + 1])
        report_path.write_text(json.dumps(_passing_report()), encoding="utf-8")
        return 0

    monkeypatch.setenv(
        "HLTHPRT_PTG2_CANDIDATE_AUDIT_API_BASE_URL",
        "http://candidate-api:8080",
    )
    monkeypatch.setenv(
        "HLTHPRT_PTG2_CANDIDATE_AUDIT_TRUSTED_CLUSTER_HTTP",
        "true",
    )
    monkeypatch.setenv("HLTHPRT_CONTROL_API_TOKEN", "public-control-token")
    monkeypatch.setattr(ptg_candidate_audit.ptg2_v3_source_api_audit, "run_audit_cli", run_cli)

    report = ptg_candidate_audit.run_release_audit(
        _target(),
        (tmp_path / "source.json.gz",),
    )

    assert report["status"] == "pass"
    assert "--validated-candidate" in captured_list
    assert captured_list[captured_list.index("--max-invalid-npis") + 1] == "0"
    assert captured_list[captured_list.index("--profile") + 1] == "release"
    assert captured_list[captured_list.index("--api-base-url") + 1] == "http://candidate-api:8080"
    assert captured_list[captured_list.index("--auth-header") + 1] == "Authorization"
    assert captured_list[captured_list.index("--auth-scheme") + 1] == "Bearer"
    assert "--trusted-cluster-http" in captured_list
    assert "--source-occurrence-samples" not in captured_list
    assert "--api-occurrence-samples" not in captured_list
    assert "--negative-samples" not in captured_list
    assert "--random-api-calls" not in captured_list
    assert work_dir_list[0] is not None and not work_dir_list[0].exists()


def test_release_audit_accepts_exact_nonempty_provider_quarantine(
    monkeypatch,
    tmp_path,
):
    captured_arguments: list[str] = []
    expected_report = _passing_report(
        provider_identifier_quarantine=NONEMPTY_PROVIDER_IDENTIFIER_QUARANTINE
    )

    def run_cli(audit_arguments):
        captured_arguments.extend(audit_arguments)
        report_path = type(tmp_path)(
            audit_arguments[audit_arguments.index("--report") + 1]
        )
        report_path.write_text(json.dumps(expected_report), encoding="utf-8")
        return 0

    monkeypatch.setenv(
        "HLTHPRT_PTG2_CANDIDATE_AUDIT_API_BASE_URL",
        "https://public-api.internal.example",
    )
    monkeypatch.setenv("HLTHPRT_CONTROL_API_TOKEN", "public-control-token")
    monkeypatch.setattr(
        ptg_candidate_audit.ptg2_v3_source_api_audit,
        "run_audit_cli",
        run_cli,
    )

    report = ptg_candidate_audit.run_release_audit(
        _target(
            provider_identifier_quarantine=(
                NONEMPTY_PROVIDER_IDENTIFIER_QUARANTINE
            )
        ),
        (tmp_path / "source.json.gz",),
    )

    assert report == expected_report
    assert captured_arguments[
        captured_arguments.index("--max-invalid-npis") + 1
    ] == "2"


def test_release_audit_failure_is_deterministic_and_not_retryable(monkeypatch, tmp_path):
    def run_cli(argv):
        report_path = type(tmp_path)(argv[argv.index("--report") + 1])
        report_path.write_text(
            json.dumps({"status": "fail", "release_gate_eligible": False}),
            encoding="utf-8",
        )
        return 1

    monkeypatch.setenv(
        "HLTHPRT_PTG2_CANDIDATE_AUDIT_API_BASE_URL",
        "https://public-api.internal.example",
    )
    monkeypatch.setenv("HLTHPRT_CONTROL_API_TOKEN", "public-control-token")
    monkeypatch.setattr(ptg_candidate_audit.ptg2_v3_source_api_audit, "run_audit_cli", run_cli)

    with pytest.raises(ptg_candidate_audit.CandidateAuditReleaseGateError) as exc_info:
        ptg_candidate_audit.run_release_audit(
            _target(),
            (tmp_path / "source.json.gz",),
        )

    assert str(exc_info.value) == "candidate release audit did not pass the release gate"
    assert exc_info.value.control_error_code == "ptg_candidate_audit_release_gate_failed"
    assert exc_info.value.retryable is False


def test_release_audit_preserves_safe_fatal_configuration_reason(
    monkeypatch,
    tmp_path,
):
    def run_cli(argv):
        report_path = type(tmp_path)(argv[argv.index("--report") + 1])
        report_path.write_text(
            json.dumps(
                {
                    "status": "error",
                    "failures": {
                        "counts": {"configuration": 1},
                        "examples": [
                            {
                                "category": "configuration",
                                "exception_type": "ConfigurationError",
                                "reason": "release_api_base_url_must_be_https_origin",
                            }
                        ],
                    },
                }
            ),
            encoding="utf-8",
        )
        return 2

    monkeypatch.setenv(
        "HLTHPRT_PTG2_CANDIDATE_AUDIT_API_BASE_URL",
        "https://public-api.internal.example",
    )
    monkeypatch.setenv("HLTHPRT_CONTROL_API_TOKEN", "public-control-token")
    monkeypatch.setattr(
        ptg_candidate_audit.ptg2_v3_source_api_audit,
        "run_audit_cli",
        run_cli,
    )

    with pytest.raises(
        ptg_candidate_audit.CandidateAuditReleaseGateError,
        match="release_api_base_url_must_be_https_origin",
    ):
        ptg_candidate_audit.run_release_audit(
            _target(),
            (tmp_path / "source.json.gz",),
        )


@pytest.mark.asyncio
async def test_isolated_audit_subprocess_returns_validated_report_without_token_argv(
    monkeypatch,
    tmp_path,
):
    report = _passing_report()
    subprocess_invocation_by_field: dict[str, object] = {}

    class SuccessfulProcess:
        pid = 12345
        returncode = 0

        async def wait(self):
            return self.returncode

    async def create_subprocess_exec(*arguments, **kwargs):
        subprocess_invocation_by_field["arguments"] = arguments
        subprocess_invocation_by_field["kwargs"] = kwargs
        report_path = type(tmp_path)(
            arguments[arguments.index("--report") + 1]
        )
        report_path.write_text(json.dumps(report), encoding="utf-8")
        return SuccessfulProcess()

    monkeypatch.setenv(
        "HLTHPRT_PTG2_CANDIDATE_AUDIT_API_BASE_URL",
        "https://public-api.internal.example",
    )
    monkeypatch.setenv("HLTHPRT_CONTROL_API_TOKEN", "public-control-token")
    monkeypatch.setattr(
        ptg_candidate_audit.asyncio,
        "create_subprocess_exec",
        create_subprocess_exec,
    )

    observed = await ptg_candidate_audit.run_isolated_release_audit(
        _target(),
        (tmp_path / "source.json.gz",),
    )

    assert observed == report
    arguments = subprocess_invocation_by_field["arguments"]
    kwargs = subprocess_invocation_by_field["kwargs"]
    assert isinstance(arguments, tuple)
    assert isinstance(kwargs, dict)
    assert arguments[:3] == (
        ptg_candidate_audit.sys.executable,
        "-m",
        "scripts.validation.ptg2_v3_source_api_audit",
    )
    assert "public-control-token" not in arguments
    assert kwargs["env"]["PTG_AUDIT_AUTH_TOKEN"] == "public-control-token"


@pytest.mark.asyncio
async def test_isolated_audit_cancellation_terminates_process_group(
    monkeypatch,
    tmp_path,
):
    """Cancellation must reap the isolated audit before the worker can retry."""

    class HangingProcess:
        pid = 12345
        returncode = None

        def __init__(self):
            self.exited = asyncio.Event()
            self.signals = []

        async def wait(self):
            await self.exited.wait()
            self.returncode = -15
            return self.returncode

    process = HangingProcess()
    subprocess_invocation_by_field: dict[str, object] = {}

    async def create_subprocess_exec(*arguments, **kwargs):
        subprocess_invocation_by_field["arguments"] = arguments
        subprocess_invocation_by_field["kwargs"] = kwargs
        return process

    def signal_process_group(observed_process, observed_signal):
        assert observed_process is process
        process.signals.append(observed_signal)
        process.exited.set()

    monkeypatch.setattr(
        ptg_candidate_audit.asyncio,
        "create_subprocess_exec",
        create_subprocess_exec,
    )
    monkeypatch.setattr(
        ptg_candidate_audit,
        "_signal_process_group",
        signal_process_group,
    )
    monkeypatch.setenv(
        "HLTHPRT_PTG2_CANDIDATE_AUDIT_API_BASE_URL",
        "https://public-api.internal.example",
    )
    monkeypatch.setenv("HLTHPRT_CONTROL_API_TOKEN", "public-control-token")
    task = asyncio.create_task(
        ptg_candidate_audit.run_isolated_release_audit(
            _target(),
            (tmp_path / "source.json.gz",),
        )
    )
    await asyncio.sleep(0)
    await asyncio.sleep(0)

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert process.signals == [ptg_candidate_audit.signal.SIGTERM]
    arguments = subprocess_invocation_by_field["arguments"]
    kwargs = subprocess_invocation_by_field["kwargs"]
    assert isinstance(arguments, tuple)
    assert isinstance(kwargs, dict)
    assert "public-control-token" not in arguments
    assert kwargs["start_new_session"] is True


@pytest.mark.asyncio
async def test_passing_audit_attests_then_activates(monkeypatch, tmp_path):
    events: list[str] = []
    report = _passing_report(
        provider_identifier_quarantine=NONEMPTY_PROVIDER_IDENTIFIER_QUARANTINE
    )
    monkeypatch.setattr(ptg_candidate_audit, "_progress", AsyncMock())
    monkeypatch.setattr(
        ptg_candidate_audit,
        "resolve_retained_raw_files",
        Mock(return_value=(tmp_path / "source.json.gz",)),
    )
    monkeypatch.setattr(
        ptg_candidate_audit,
        "run_isolated_release_audit",
        AsyncMock(return_value=report),
    )

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

    result = await ptg_candidate_audit._audit_and_activate(
        _target(
            provider_identifier_quarantine=(
                NONEMPTY_PROVIDER_IDENTIFIER_QUARANTINE
            )
        ),
        control_run_id="control-run",
        store=PTG2ArtifactStore(tmp_path / "store"),
    )

    assert events == ["attest", "promote"]
    assert result["arch_version"] == "postgres_binary_v3"
    assert result["snapshot_status"] == "published"
    assert result["activation_status"] == "activated"
    assert result["audit_report_digest"] == "ef" * 32
    assert result["audit_counts"]["standard_api_actual_http_requests"] == 3000
    assert result["metrics"]["candidate_run_id"] == "ptg2:derived-import"


@pytest.mark.asyncio
async def test_failing_audit_never_attests_or_activates(monkeypatch, tmp_path):
    monkeypatch.setattr(ptg_candidate_audit, "_progress", AsyncMock())
    monkeypatch.setattr(
        ptg_candidate_audit,
        "resolve_retained_raw_files",
        Mock(return_value=(tmp_path / "source.json.gz",)),
    )
    monkeypatch.setattr(
        ptg_candidate_audit,
        "run_isolated_release_audit",
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
            store=PTG2ArtifactStore(tmp_path / "store"),
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

    result = await ptg_candidate_audit.main(
        candidate_run_id="ptg2:derived-import",
        snapshot_id="candidate-snapshot",
        import_id="derived-import",
        run_id="control-run",
    )

    assert result["activation_status"] == "activated"
    assert result["idempotent"] is True
    assert result["audit_report_digest"] == "cd" * 32
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
