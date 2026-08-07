# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Failure, cancellation, and evidence defenses for the synthetic canary."""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from dataclasses import replace

import pytest

import process.formulary_fhir.synthetic_canary as canary_module
import process.formulary_fhir.synthetic_canary_contract as contract_module
from process.formulary_fhir.manual_lock import ManualSourceLockError
from process.formulary_fhir.synchronizer import SynchronizationResult
from process.formulary_fhir.synthetic_canary import SyntheticCanaryError
from process.formulary_fhir.synthetic_canary import SyntheticSeedCandidateResult
from process.formulary_fhir.synthetic_canary import candidate_result_json
from process.formulary_fhir.synthetic_canary import (
    verify_synthetic_seed_candidate,
)
from process.formulary_fhir.synthetic_canary_contract import CANARY_ENABLED_ENV
from process.formulary_fhir.synthetic_canary_contract import CANARY_SOURCE_BASE
from process.formulary_fhir.synthetic_canary_contract import CANARY_SOURCE_ID
from process.formulary_fhir.synthetic_canary_contract import (
    SyntheticCanaryContractError,
)
from process.formulary_fhir.synthetic_canary_contract import expected_evidence


def _synchronization_result(
    *,
    full_aliases: int = 2,
    resumed_aliases: int = 0,
    request_count: int = 9,
) -> SynchronizationResult:
    expected_by_field = expected_evidence()
    return SynchronizationResult(
        dataset_id=expected_by_field["dataset_id"],
        acquisition_contract_hash=expected_by_field[
            "acquisition_contract_hash"
        ],
        list_count=1,
        alias_count=2,
        medication_membership_count=2,
        coverage_hash=expected_by_field["coverage_hash"],
        membership_hash=expected_by_field["membership_hash"],
        full_aliases=full_aliases,
        reused_aliases=0,
        resumed_aliases=resumed_aliases,
        request_count=request_count,
        transient_retry_count=0,
        throttle_count=0,
    )


def _candidate_result() -> SyntheticSeedCandidateResult:
    expected_by_field = expected_evidence()
    return SyntheticSeedCandidateResult(
        expected_by_field["dataset_id"],
        expected_by_field["source_configuration_hash"],
        expected_by_field["acquisition_contract_hash"],
        1,
        2,
        2,
        expected_by_field["coverage_hash"],
        expected_by_field["membership_hash"],
        2,
        0,
        9,
    )


def test_fixture_and_evidence_loaders_reject_malformed_contracts(
    monkeypatch,
    tmp_path,
):
    monkeypatch.setattr(contract_module, "FIXTURE_ROOT", tmp_path)
    (tmp_path / "coverage_plan.json").write_text("[]", encoding="utf-8")
    (tmp_path / "medication_a.json").write_text("{", encoding="utf-8")

    with pytest.raises(SyntheticCanaryContractError):
        contract_module.fixture_object("unknown.json")
    with pytest.raises(SyntheticCanaryContractError):
        contract_module.fixture_object("medication_b.json")
    with pytest.raises(SyntheticCanaryContractError):
        contract_module.fixture_object("coverage_plan.json")
    with pytest.raises(SyntheticCanaryContractError):
        contract_module.fixture_object("medication_a.json")


@pytest.mark.parametrize(
    "changed_field,changed_value",
    [
        ("contract_version", "different-contract"),
        ("request_count", True),
        ("request_count", 8),
        ("coverage_hash", "not-a-hash"),
        ("unexpected", "field"),
    ],
)
def test_expected_evidence_requires_exact_schema(
    monkeypatch,
    changed_field,
    changed_value,
):
    invalid_evidence_by_field = expected_evidence()
    invalid_evidence_by_field[changed_field] = changed_value
    monkeypatch.setattr(
        contract_module,
        "fixture_object",
        lambda _file_name: invalid_evidence_by_field,
    )

    with pytest.raises(SyntheticCanaryContractError):
        contract_module.expected_evidence()


@pytest.mark.asyncio
@pytest.mark.parametrize("disabled_value", [None, "", "0", "false", "typo"])
async def test_canary_gate_rejects_before_source_lease(monkeypatch, disabled_value):
    if disabled_value is None:
        monkeypatch.delenv(CANARY_ENABLED_ENV, raising=False)
    else:
        monkeypatch.setenv(CANARY_ENABLED_ENV, disabled_value)
    lease_calls: list[bool] = []

    def forbidden_lease(*_args, **_kwargs):
        lease_calls.append(True)
        raise AssertionError("source lease opened")

    monkeypatch.setattr(
        canary_module.manual_lock,
        "manual_source_lease",
        forbidden_lease,
    )
    with pytest.raises(SyntheticCanaryError) as caught:
        await verify_synthetic_seed_candidate(database=object())

    assert caught.value.code == "disabled"
    assert lease_calls == []


@pytest.mark.asyncio
async def test_candidate_holds_lease_and_disables_after_success(monkeypatch):
    monkeypatch.setenv(CANARY_ENABLED_ENV, "yes")
    events: list[str] = []

    @asynccontextmanager
    async def source_lease(*_args, **_kwargs):
        events.append("lock")
        yield
        events.append("unlock")

    async def enable(_database):
        events.append("enable")

    async def verify(_database):
        events.append("verify")
        return _candidate_result()

    async def disable(
        _database,
        *,
        require_verified_graph,
        is_reserved_source_claimed,
    ):
        assert require_verified_graph is True
        assert is_reserved_source_claimed is True
        events.append("disable")

    monkeypatch.setattr(
        canary_module.manual_lock,
        "manual_source_lease",
        source_lease,
    )
    monkeypatch.setattr(canary_module, "_enable_exact_source", enable)
    monkeypatch.setattr(canary_module, "_verify_enabled_candidate", verify)
    monkeypatch.setattr(canary_module, "_disable_exact_source", disable)

    candidate_result = await verify_synthetic_seed_candidate(database=object())

    assert candidate_result == _candidate_result()
    assert events == ["lock", "enable", "verify", "disable", "unlock"]


@pytest.mark.asyncio
async def test_candidate_failure_preserves_error_and_attempts_disable(monkeypatch):
    monkeypatch.setenv(CANARY_ENABLED_ENV, "true")
    original_error = RuntimeError("private fixture detail")
    events: list[str] = []

    @asynccontextmanager
    async def source_lease(*_args, **_kwargs):
        yield

    async def enable(_database):
        events.append("enable")

    async def fail(_database):
        raise original_error

    async def disable(
        _database,
        *,
        require_verified_graph,
        is_reserved_source_claimed,
    ):
        assert require_verified_graph is False
        assert is_reserved_source_claimed is True
        events.append("disable")

    monkeypatch.setattr(
        canary_module.manual_lock,
        "manual_source_lease",
        source_lease,
    )
    monkeypatch.setattr(canary_module, "_enable_exact_source", enable)
    monkeypatch.setattr(canary_module, "_verify_enabled_candidate", fail)
    monkeypatch.setattr(canary_module, "_disable_exact_source", disable)

    with pytest.raises(RuntimeError) as caught:
        await verify_synthetic_seed_candidate(database=object())

    assert caught.value is original_error
    assert events == ["enable", "disable"]


@pytest.mark.asyncio
async def test_candidate_cleanup_failure_is_explicit(monkeypatch):
    monkeypatch.setenv(CANARY_ENABLED_ENV, "true")

    @asynccontextmanager
    async def source_lease(*_args, **_kwargs):
        yield

    async def enable(_database):
        return None

    async def fail(_database):
        raise RuntimeError("private operation detail")

    async def fail_cleanup(
        _database,
        *,
        require_verified_graph,
        is_reserved_source_claimed,
    ):
        assert require_verified_graph is False
        assert is_reserved_source_claimed is True
        raise RuntimeError("private cleanup detail")

    monkeypatch.setattr(
        canary_module.manual_lock,
        "manual_source_lease",
        source_lease,
    )
    monkeypatch.setattr(canary_module, "_enable_exact_source", enable)
    monkeypatch.setattr(canary_module, "_verify_enabled_candidate", fail)
    monkeypatch.setattr(canary_module, "_disable_exact_source", fail_cleanup)

    with pytest.raises(SyntheticCanaryError) as caught:
        await verify_synthetic_seed_candidate(database=object())

    assert caught.value.code == "cleanup"
    assert "private" not in str(caught.value)


@pytest.mark.asyncio
async def test_candidate_cancellation_drains_source_disablement(monkeypatch):
    monkeypatch.setenv(CANARY_ENABLED_ENV, "true")
    verification_started = asyncio.Event()
    cleanup_started = asyncio.Event()
    release_cleanup = asyncio.Event()
    events: list[str] = []

    @asynccontextmanager
    async def source_lease(*_args, **_kwargs):
        yield

    async def enable(_database):
        return None

    async def block(_database):
        verification_started.set()
        await asyncio.Event().wait()

    async def disable(
        _database,
        *,
        require_verified_graph,
        is_reserved_source_claimed,
    ):
        assert require_verified_graph is False
        assert is_reserved_source_claimed is True
        cleanup_started.set()
        await release_cleanup.wait()
        events.append("disabled")

    monkeypatch.setattr(
        canary_module.manual_lock,
        "manual_source_lease",
        source_lease,
    )
    monkeypatch.setattr(canary_module, "_enable_exact_source", enable)
    monkeypatch.setattr(canary_module, "_verify_enabled_candidate", block)
    monkeypatch.setattr(canary_module, "_disable_exact_source", disable)
    canary_task = asyncio.create_task(
        verify_synthetic_seed_candidate(database=object())
    )
    await verification_started.wait()
    canary_task.cancel()
    await cleanup_started.wait()
    canary_task.cancel()
    release_cleanup.set()

    with pytest.raises(asyncio.CancelledError):
        await canary_task
    assert events == ["disabled"]


@pytest.mark.asyncio
async def test_candidate_cancellation_during_enable_attempts_targeted_cleanup(
    monkeypatch,
):
    monkeypatch.setenv(CANARY_ENABLED_ENV, "true")
    enable_started = asyncio.Event()
    cleanup_calls: list[bool] = []

    @asynccontextmanager
    async def source_lease(*_args, **_kwargs):
        yield

    async def block_enable(_database):
        enable_started.set()
        await asyncio.Event().wait()

    async def targeted_cleanup(
        _database,
        *,
        require_verified_graph,
        is_reserved_source_claimed,
    ):
        cleanup_calls.append(require_verified_graph)
        assert is_reserved_source_claimed is False

    monkeypatch.setattr(
        canary_module.manual_lock,
        "manual_source_lease",
        source_lease,
    )
    monkeypatch.setattr(canary_module, "_enable_exact_source", block_enable)
    monkeypatch.setattr(
        canary_module,
        "_disable_exact_source",
        targeted_cleanup,
    )
    canary_task = asyncio.create_task(
        verify_synthetic_seed_candidate(database=object())
    )
    await enable_started.wait()
    canary_task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await canary_task
    assert cleanup_calls == [False]


@pytest.mark.asyncio
async def test_final_cleanup_failure_wins_over_command_cancellation(monkeypatch):
    monkeypatch.setenv(CANARY_ENABLED_ENV, "true")
    cleanup_started = asyncio.Event()
    release_cleanup = asyncio.Event()

    @asynccontextmanager
    async def source_lease(*_args, **_kwargs):
        yield

    async def enable(_database):
        return None

    async def verify(_database):
        return _candidate_result()

    async def fail_cleanup(
        _database,
        *,
        require_verified_graph,
        is_reserved_source_claimed,
    ):
        assert require_verified_graph is True
        assert is_reserved_source_claimed is True
        cleanup_started.set()
        await release_cleanup.wait()
        raise RuntimeError("private cleanup failure")

    monkeypatch.setattr(
        canary_module.manual_lock,
        "manual_source_lease",
        source_lease,
    )
    monkeypatch.setattr(canary_module, "_enable_exact_source", enable)
    monkeypatch.setattr(canary_module, "_verify_enabled_candidate", verify)
    monkeypatch.setattr(canary_module, "_disable_exact_source", fail_cleanup)
    canary_task = asyncio.create_task(
        verify_synthetic_seed_candidate(database=object())
    )
    await cleanup_started.wait()
    canary_task.cancel()
    release_cleanup.set()

    with pytest.raises(SyntheticCanaryError) as caught:
        await canary_task
    assert caught.value.code == "cleanup"


@pytest.mark.asyncio
async def test_candidate_maps_source_lock_failure(monkeypatch):
    monkeypatch.setenv(CANARY_ENABLED_ENV, "true")

    @asynccontextmanager
    async def unavailable_lease(*_args, **_kwargs):
        raise ManualSourceLockError("busy")
        yield

    monkeypatch.setattr(
        canary_module.manual_lock,
        "manual_source_lease",
        unavailable_lease,
    )
    with pytest.raises(SyntheticCanaryError) as caught:
        await verify_synthetic_seed_candidate(database=object())

    assert caught.value.code == "busy"


def test_candidate_evidence_accepts_exact_replay_and_rejects_drift():
    expected_hash = expected_evidence()["source_configuration_hash"]
    first = canary_module._candidate_result(
        expected_hash,
        _synchronization_result(),
    )
    replay = canary_module._candidate_result(
        expected_hash,
        _synchronization_result(
            full_aliases=0,
            resumed_aliases=2,
            request_count=3,
        ),
    )
    partial_replay = canary_module._candidate_result(
        expected_hash,
        _synchronization_result(
            full_aliases=2,
            resumed_aliases=1,
            request_count=6,
        ),
    )

    assert first.full_aliases == 2 and replay.resumed_aliases == 2
    assert partial_replay.full_aliases == 2
    assert partial_replay.resumed_aliases == 1
    with pytest.raises(SyntheticCanaryError, match="evidence"):
        canary_module._candidate_result(
            expected_hash,
            replace(_synchronization_result(), membership_hash="0" * 64),
        )
    with pytest.raises(SyntheticCanaryError, match="evidence"):
        canary_module._candidate_result(
            expected_hash,
            _synchronization_result(full_aliases=1, request_count=9),
        )


def test_candidate_json_is_fixed_and_rejects_other_objects():
    rendered_result = candidate_result_json(_candidate_result())

    assert '"status":"verified"' in rendered_result
    assert CANARY_SOURCE_ID not in rendered_result
    assert CANARY_SOURCE_BASE not in rendered_result
    with pytest.raises(SyntheticCanaryError):
        candidate_result_json(object())
