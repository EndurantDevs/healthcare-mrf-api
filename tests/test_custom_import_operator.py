# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import datetime as dt
from dataclasses import fields

import pytest

from process.custom_import.operator import (
    OperatorInspectionError,
    OperatorInvariantError,
    OperatorObjectNotFound,
    OperatorTransactionRequired,
    inspect_execution,
    inspect_generation,
)

_NOW = dt.datetime(2026, 9, 21, 12, 0, tzinfo=dt.UTC)
_DIGEST = bytes(range(32))


class _Result:
    def __init__(self, row):
        self.row = row

    def mappings(self):
        return self

    def one_or_none(self):
        return self.row


class _Session:
    def __init__(self, row, *, active: bool = True):
        self.row = row
        self.active = active
        self.statements = []

    def in_transaction(self):
        return self.active

    async def execute(self, statement):
        self.statements.append(statement)
        return _Result(self.row)


def _execution_row(*, state: str = "running", lease: bool = True):
    return {
        "execution_id": 17,
        "dataset_id": 3,
        "definition_revision_id": 5,
        "schema_revision_id": 7,
        "capture_bundle_id": 11,
        "mechanism": "queued",
        "state": state,
        "started_at": _NOW,
        "finished_at": _NOW if state in {"canceled", "failed", "completed", "no_change"} else None,
        "created_at": _NOW,
        "updated_at": _NOW,
        "lease_fence": 4 if lease else None,
        "lease_heartbeat_at": _NOW if lease else None,
        "lease_expires_at": _NOW + dt.timedelta(minutes=5) if lease else None,
    }


def _generation_row(
    *,
    sealed: bool = False,
    current_generation_id: int | None = None,
    ever_published: bool = False,
    no_change: bool = False,
):
    seal = "custom-import-generation-seal/v1" if sealed else None
    no_change_contract = "custom-import-no-change-seal/v1" if no_change else None
    has_current = current_generation_id is not None
    return {
        "generation_id": 19,
        "dataset_id": 3,
        "definition_revision_id": 5,
        "schema_revision_id": 7,
        "execution_id": 17,
        "capture_bundle_id": 11,
        "base_generation_id": 13,
        "root_count": 2,
        "family_count": 2,
        "created_at": _NOW,
        "seal_contract": seal,
        "sealing_fence": 4 if sealed else None,
        "sealed_root_count": 2 if sealed else None,
        "sealed_family_count": 2 if sealed else None,
        "generation_family_count": 2 if sealed else None,
        "family_child_count": 3 if sealed else None,
        "winner_count": 2 if sealed else None,
        "profile_count": 1 if sealed else None,
        "root_scalar_count": 4 if sealed else None,
        "child_scalar_count": 6 if sealed else None,
        "materialization_sha256": _DIGEST if sealed else None,
        "sealed_effective_output_sha256": _DIGEST if sealed else None,
        "sealed_at": _NOW if sealed else None,
        "current_generation_id": current_generation_id,
        "current_definition_revision_id": 5 if has_current else None,
        "current_schema_revision_id": 7 if has_current else None,
        "pointer_version": 8 if has_current else None,
        "pointer_changed_at": _NOW if has_current else None,
        "no_change_contract": no_change_contract,
        "no_change_base_generation_id": 13 if no_change else None,
        "no_change_base_pointer_version": 8 if no_change else None,
        "no_change_effective_output_sha256": _DIGEST if no_change else None,
        "no_change_receipt_sha256": _DIGEST if no_change else None,
        "no_change_sealed_at": _NOW if no_change else None,
        "ever_published": ever_published,
        "no_change_event_exists": no_change,
    }


@pytest.mark.asyncio
async def test_execution_status_is_safe_and_uses_one_caller_owned_snapshot():
    session = _Session(_execution_row())

    status = await inspect_execution(session, dataset_id=3, execution_id=17)

    assert len(session.statements) == 1
    assert session.statements[0].get_execution_options()["autoflush"] is False
    assert status.state == "running"
    assert status.lease is not None and status.lease.fence == 4
    assert {field.name for field in fields(status)}.isdisjoint({"idempotency_key", "terminal_reason", "token_sha256"})
    assert {field.name for field in fields(status.lease)}.isdisjoint({"token", "token_sha256"})


@pytest.mark.asyncio
async def test_terminal_execution_can_retain_status_without_a_lease_row():
    status = await inspect_execution(
        _Session(_execution_row(state="completed", lease=False)), dataset_id=3, execution_id=17
    )

    assert status.state == "completed"
    assert status.lease is None


@pytest.mark.asyncio
async def test_active_execution_can_report_a_repairable_missing_lease():
    status = await inspect_execution(_Session(_execution_row(lease=False)), dataset_id=3, execution_id=17)

    assert status.state == "running"
    assert status.lease is None


@pytest.mark.asyncio
@pytest.mark.parametrize("dataset_id, execution_id", [(0, 1), (1, 0), (True, 1), (1, 2**63)])
async def test_execution_status_rejects_invalid_identifiers_without_sql(dataset_id, execution_id):
    session = _Session(_execution_row())

    with pytest.raises(OperatorInspectionError):
        await inspect_execution(session, dataset_id=dataset_id, execution_id=execution_id)

    assert session.statements == []


@pytest.mark.asyncio
async def test_inspection_requires_a_transaction_and_hides_dataset_mismatch():
    inactive = _Session(_execution_row(), active=False)
    with pytest.raises(OperatorTransactionRequired):
        await inspect_execution(inactive, dataset_id=3, execution_id=17)
    assert inactive.statements == []

    missing = _Session(None)
    with pytest.raises(OperatorObjectNotFound):
        await inspect_execution(missing, dataset_id=4, execution_id=17)
    assert len(missing.statements) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "row, expected",
    [
        (_generation_row(), "unsealed"),
        (_generation_row(sealed=True), "sealed_unpublished"),
        (_generation_row(sealed=True, current_generation_id=19, ever_published=True), "current"),
        (_generation_row(sealed=True, current_generation_id=23, ever_published=True), "superseded"),
        (_generation_row(current_generation_id=19, ever_published=True), "current"),
        (_generation_row(current_generation_id=23, ever_published=True), "superseded"),
        (_generation_row(sealed=True, current_generation_id=13, no_change=True), "no_change"),
    ],
)
async def test_generation_status_distinguishes_durable_publication_states(row, expected):
    session = _Session(row)

    status = await inspect_generation(session, dataset_id=3, generation_id=19)

    assert len(session.statements) == 1
    assert status.publication_state == expected
    if expected == "unsealed" or status.seal is None:
        assert status.seal is None
    else:
        assert status.seal is not None
    if expected == "no_change":
        assert status.no_change is not None and status.no_change.base_generation_id == 13
    if status.seal is not None:
        assert {field.name for field in fields(status.seal)}.isdisjoint({"sealing_token", "sealing_token_sha256"})


@pytest.mark.asyncio
async def test_generation_status_rejects_no_change_candidate_as_current():
    with pytest.raises(OperatorInvariantError):
        await inspect_generation(
            _Session(_generation_row(sealed=True, current_generation_id=19, no_change=True)),
            dataset_id=3,
            generation_id=19,
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "row",
    [
        {**_generation_row(sealed=True, current_generation_id=13, no_change=True), "no_change_event_exists": False},
        {**_generation_row(sealed=True), "no_change_event_exists": True},
        _generation_row(sealed=True, current_generation_id=13, ever_published=True, no_change=True),
    ],
)
async def test_generation_status_requires_exact_unambiguous_no_change_evidence(row):
    with pytest.raises(OperatorInvariantError):
        await inspect_generation(_Session(row), dataset_id=3, generation_id=19)


@pytest.mark.asyncio
async def test_generation_status_rejects_non_binary_digest_evidence():
    row = _generation_row(sealed=True)
    row["materialization_sha256"] = 32

    with pytest.raises(OperatorInvariantError):
        await inspect_generation(_Session(row), dataset_id=3, generation_id=19)


@pytest.mark.asyncio
async def test_generation_status_hides_missing_or_cross_dataset_objects():
    session = _Session(None)

    with pytest.raises(OperatorObjectNotFound):
        await inspect_generation(session, dataset_id=4, generation_id=19)

    assert len(session.statements) == 1
