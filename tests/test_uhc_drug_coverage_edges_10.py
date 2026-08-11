# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Fail-closed edge coverage for durable formulary source ownership."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from process import uhc_drug_file_catalog as drug_catalog
from process.formulary_fhir import source_artifact_binding
from process.formulary_fhir import uhc_drug_acquisition_lease as lease
from process.formulary_fhir import uhc_drug_acquisition_lease_contract as lease_contract
from process.formulary_fhir import uhc_drug_acquisition_lease_store as lease_store
from process.formulary_fhir.source_artifact_contract import identity_fields
from process.formulary_fhir.source_artifact_contract import SourceArtifactIdentity


SOURCE_ID = "synthetic-formulary-source"
TOKEN = "a" * 64


class _Transaction:
    async def __aenter__(self):
        return self

    async def __aexit__(self, *_error) -> bool:
        return False


class _LeaseDatabase:
    def __init__(self, *, scalars=(), statuses=()) -> None:
        self.scalars = iter(scalars)
        self.statuses = iter(statuses)

    def transaction(self) -> _Transaction:
        return _Transaction()

    async def scalar(self, _statement: str, **_parameters: object):
        return next(self.scalars, "")

    async def status(self, _statement: str, **_parameters: object) -> int:
        return next(self.statuses, 1)


def _claim(generation: int = 1) -> lease_contract.UHCDrugSourceAcquisitionClaim:
    return lease_contract.UHCDrugSourceAcquisitionClaim(
        source_id=SOURCE_ID,
        lease_generation=generation,
        lease_token=TOKEN,
    )


def _artifact_identity() -> SourceArtifactIdentity:
    return SourceArtifactIdentity(
        source_id=SOURCE_ID,
        source_file_set_sha256="1" * 64,
        source_file_id="2" * 64,
        raw_listing_projection_sha256="3" * 64,
        family="ifp",
        file_name="synthetic-formulary.json",
        source_url="https://example.test/synthetic-formulary.json",
        catalog_modified_at="2026-08-10T00:00:00Z",
        catalog_entry_sha256="4" * 64,
        expected_byte_count=5,
    )


def _artifact_row(
    identity: SourceArtifactIdentity, *, status: str
) -> dict[str, object]:
    return {
        **identity_fields(identity),
        "artifact_sha256": "5" * 64,
        "artifact_byte_count": 5,
        "status": status,
        "verified_at": datetime(2026, 8, 10, tzinfo=UTC),
    }


@pytest.mark.asyncio
async def test_fenced_verified_artifact_requires_verified_state() -> None:
    """Replay succeeds only after its transaction fence sees a verified row."""

    identity = _artifact_identity()
    transaction_fence = AsyncMock()
    verified_database = SimpleNamespace(
        transaction=lambda: _Transaction(),
        first=AsyncMock(return_value=_artifact_row(identity, status="verified")),
    )
    verified = await source_artifact_binding._fenced_verified_source_artifact(
        verified_database,
        identity,
        "5" * 64,
        5,
        transaction_fence,
    )
    assert verified.identity == identity
    transaction_fence.assert_awaited_once()

    pending_database = SimpleNamespace(
        transaction=lambda: _Transaction(),
        first=AsyncMock(return_value=_artifact_row(identity, status="pending")),
    )
    with pytest.raises(RuntimeError, match="state is invalid"):
        await source_artifact_binding._fenced_verified_source_artifact(
            pending_database,
            identity,
            "5" * 64,
            5,
            AsyncMock(),
        )


def test_lease_contract_rejects_invalid_generations_and_windows() -> None:
    """Every numeric source-ownership boundary rejects bools and zero values."""

    with pytest.raises(ValueError, match="claim is invalid"):
        _claim(0)
    with pytest.raises(ValueError, match="lease is invalid"):
        lease_contract._lease_seconds(0)
    with pytest.raises(ValueError, match="interval is invalid"):
        lease_contract._positive_seconds(True, "heartbeat interval")
    with pytest.raises(ValueError, match="timeout is invalid"):
        lease_contract._positive_seconds(0, "heartbeat timeout")
    with pytest.raises(lease_contract.UHCDrugSourceAcquisitionLeaseError) as caught:
        lease_contract._claim_from_row(
            {
                "source_id": SOURCE_ID,
                "lease_generation": 0,
                "lease_token": TOKEN,
            }
        )
    assert caught.value.code == "state"


@pytest.mark.asyncio
async def test_lease_store_type_guards_and_successful_heartbeat() -> None:
    """Store APIs reject forged claims while preserving the successful branch."""

    with pytest.raises(ValueError, match="claim is invalid"):
        await lease_store.require_active_uhc_drug_source_acquisition(
            object(),
            database=_LeaseDatabase(),
        )
    with pytest.raises(ValueError, match="claim is invalid"):
        await lease_store.heartbeat_uhc_drug_source_acquisition(
            object(),
            database=_LeaseDatabase(),
        )
    await lease_store.heartbeat_uhc_drug_source_acquisition(
        _claim(),
        lease_seconds=300,
        database=_LeaseDatabase(statuses=(1,)),
    )
    with pytest.raises(ValueError, match="claim is invalid"):
        await lease_store.release_uhc_drug_source_acquisition(
            object(),
            database=_LeaseDatabase(),
        )


@pytest.mark.asyncio
async def test_heartbeat_loop_propagates_direct_cancellation(monkeypatch) -> None:
    """Cancellation remains cancellation rather than being mapped to lease loss."""

    async def immediate_sleep(_seconds: float) -> None:
        return None

    monkeypatch.setattr(lease.asyncio, "sleep", immediate_sleep)
    monkeypatch.setattr(
        lease,
        "heartbeat_uhc_drug_source_acquisition",
        AsyncMock(side_effect=asyncio.CancelledError),
    )
    with pytest.raises(asyncio.CancelledError):
        await lease._heartbeat_loop(
            _claim(),
            database=object(),
            lease_seconds=300,
            heartbeat_seconds=1.0,
            heartbeat_timeout_seconds=1.0,
        )


@pytest.mark.asyncio
async def test_failure_cleanup_stops_heartbeat_before_release(monkeypatch) -> None:
    """The failure helper runs both ordered cleanup phases."""

    stop_heartbeat = AsyncMock()
    best_effort_release = AsyncMock()
    monkeypatch.setattr(lease, "_stop_heartbeat", stop_heartbeat)
    monkeypatch.setattr(lease, "_best_effort_release", best_effort_release)
    heartbeat_task = object()
    database = object()

    await lease._stop_heartbeat_and_best_effort_release(
        heartbeat_task,
        _claim(),
        database=database,
    )

    stop_heartbeat.assert_awaited_once_with(heartbeat_task)
    best_effort_release.assert_awaited_once_with(_claim(), database=database)


@pytest.mark.asyncio
async def test_detached_owned_drain_stops_when_heartbeat_finishes_first(
    monkeypatch,
) -> None:
    """A completed heartbeat prevents a stale detached release."""

    event_loop = asyncio.get_running_loop()
    operation_task = event_loop.create_future()
    heartbeat_task = event_loop.create_future()
    operation_task.set_result(None)
    heartbeat_task.set_result(None)
    join_tasks = AsyncMock()
    best_effort_release = AsyncMock()
    monkeypatch.setattr(lease, "_join_tasks", join_tasks)
    monkeypatch.setattr(lease, "_best_effort_release", best_effort_release)

    await lease._finish_owned_detached_drain(
        operation_task,
        heartbeat_task,
        _claim(),
        database=object(),
    )

    join_tasks.assert_awaited_once_with(operation_task, heartbeat_task)
    best_effort_release.assert_not_awaited()


@pytest.mark.asyncio
async def test_cancel_drain_observes_an_immediately_expired_deadline(
    monkeypatch,
) -> None:
    """The bounded caller wait detaches an undrained, already unowned task."""

    operation_task = Mock()
    operation_task.done.return_value = False
    heartbeat_task = Mock()
    heartbeat_task.done.return_value = True
    heartbeat_task.exception.return_value = lease.UHCDrugSourceAcquisitionLeaseError(
        "lease_lost"
    )
    detached_drain = object()
    finish_unowned_drain = Mock(return_value=detached_drain)
    retained_operation_list: list[object] = []

    def retain_and_close(operation) -> None:
        retained_operation_list.append(operation)

    monkeypatch.setattr(
        lease,
        "_finish_unowned_detached_drain",
        finish_unowned_drain,
    )
    monkeypatch.setattr(lease, "_retain_background_drain", retain_and_close)

    await lease._cancel_operation_under_lease(
        operation_task,
        heartbeat_task,
        _claim(),
        database=object(),
        failure_drain_seconds=0.0,
    )

    operation_task.cancel.assert_called_once_with()
    finish_unowned_drain.assert_called_once_with(operation_task, heartbeat_task)
    assert retained_operation_list == [detached_drain]


@pytest.mark.asyncio
async def test_finish_operation_drains_failure_cleanup_before_reraising(
    monkeypatch,
) -> None:
    """Operation errors are preserved after cancellation-safe cleanup."""

    async def fail_operation() -> None:
        raise RuntimeError("synthetic operation failure")

    operation_task = asyncio.create_task(fail_operation())
    await asyncio.sleep(0)
    cleanup = AsyncMock()
    monkeypatch.setattr(
        lease,
        "_stop_heartbeat_and_best_effort_release",
        cleanup,
    )
    heartbeat_task = object()
    database = object()

    with pytest.raises(RuntimeError, match="synthetic operation failure"):
        await lease._finish_operation(
            operation_task,
            heartbeat_task,
            _claim(),
            database=database,
        )

    cleanup.assert_awaited_once_with(
        heartbeat_task,
        _claim(),
        database=database,
    )


@pytest.mark.asyncio
async def test_supervisor_rejects_a_noncallable_operation() -> None:
    """Ownership is never claimed for a malformed operation."""

    with pytest.raises(ValueError, match="operation is invalid"):
        await lease.run_with_source_lease(
            SOURCE_ID,
            object(),
            database=object(),
            lease_seconds=3,
            heartbeat_seconds=0.1,
            heartbeat_timeout_seconds=0.1,
            failure_drain_seconds=0.1,
        )


def test_drug_catalog_normalizes_url_constructor_errors(monkeypatch) -> None:
    """Canonical URL parser failures remain bounded catalog errors."""

    def reject_url(_source_url: str):
        raise ValueError("synthetic URL failure")

    monkeypatch.setattr(drug_catalog, "URL", reject_url)
    with pytest.raises(drug_catalog.UHCFileCatalogError, match="URL is invalid"):
        drug_catalog._canonical_drug_source_url(
            "https://legacy.providerlookuponline.com/synthetic-formulary.json"
        )
