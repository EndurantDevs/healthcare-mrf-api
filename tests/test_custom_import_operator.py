# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import datetime as dt
from dataclasses import fields

import pytest

from process.custom_import.operator import (
    _NO_CHANGE_EVIDENCE_FIELDS,
    _SEAL_EVIDENCE_FIELDS,
    OperatorInspectionError,
    OperatorInvariantError,
    OperatorObjectNotFound,
    OperatorTransactionRequired,
    inspect_execution_evidence,
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

    def all(self):
        if self.row is None:
            return []
        return list(self.row) if isinstance(self.row, tuple) else [self.row]


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


def _execution_evidence_generation_values(generation: bool) -> dict[str, object]:
    return {
        "evidence_generation_id": 19 if generation else None,
        "generation_dataset_id": 3 if generation else None,
        "generation_definition_revision_id": 5 if generation else None,
        "generation_schema_revision_id": 7 if generation else None,
        "generation_execution_id": 17 if generation else None,
        "generation_capture_bundle_id": 11 if generation else None,
        "generation_producing_fence": 4 if generation else None,
        "generation_producing_token_sha256": _DIGEST if generation else None,
        "generation_source_bundle_sha256": _DIGEST if generation else None,
        "seal_generation_id": 19 if generation else None,
        "seal_dataset_id": 3 if generation else None,
        "seal_definition_revision_id": 5 if generation else None,
        "seal_schema_revision_id": 7 if generation else None,
        "seal_execution_id": 17 if generation else None,
        "seal_capture_bundle_id": 11 if generation else None,
        "seal_token_sha256": _DIGEST if generation else None,
        "seal_contract": "custom-import-generation-seal/v1" if generation else None,
        "sealing_fence": 4 if generation else None,
        "sealed_root_count": 2 if generation else None,
        "sealed_family_count": 2 if generation else None,
        "generation_family_count": 2 if generation else None,
        "family_child_count": 3 if generation else None,
        "winner_count": 2 if generation else None,
        "profile_count": 1 if generation else None,
        "root_scalar_count": 4 if generation else None,
        "child_scalar_count": 6 if generation else None,
        "materialization_sha256": _DIGEST if generation else None,
        "sealed_effective_output_sha256": _DIGEST if generation else None,
        "sealed_at": _NOW if generation else None,
    }


def _execution_evidence_row(*, generation: bool = True, current_generation_id: int | None = 19):
    """Return a synthetic execution-evidence statement mapping."""

    return {
        **_execution_row(state="completed"),
        "execution_source_binding_revision_id": 23,
        "definition_id": 5,
        "definition_dataset_id": 3,
        "definition_schema_revision_id": 7,
        "definition_sha256": _DIGEST,
        "stored_schema_revision_id": 7,
        "schema_dataset_id": 3,
        "schema_sha256": _DIGEST,
        "binding_revision_id": 23,
        "binding_dataset_id": 3,
        "binding_definition_revision_id": 5,
        "binding_schema_revision_id": 7,
        "binding_definition_sha256": _DIGEST,
        "binding_schema_sha256": _DIGEST,
        "source_binding_sha256": _DIGEST,
        "capture_id": 11,
        "capture_dataset_id": 3,
        "capture_definition_revision_id": 5,
        "capture_schema_revision_id": 7,
        "capture_manifest_sha256": _DIGEST,
        **_execution_evidence_generation_values(generation),
        "current_generation_id": current_generation_id,
        "current_definition_revision_id": 5 if current_generation_id is not None else None,
        "current_schema_revision_id": 7 if current_generation_id is not None else None,
        "pointer_version": 8 if current_generation_id is not None else None,
        "pointer_changed_at": _NOW if current_generation_id is not None else None,
        "no_change_execution_id": None,
        "no_change_dataset_id": None,
        "no_change_definition_revision_id": None,
        "no_change_schema_revision_id": None,
        "no_change_capture_bundle_id": None,
        "no_change_candidate_generation_id": None,
        "no_change_contract": None,
        "no_change_base_generation_id": None,
        "no_change_base_pointer_version": None,
        "no_change_effective_output_sha256": None,
        "no_change_receipt_sha256": None,
        "no_change_sealed_at": None,
        "ever_published": generation,
        "no_change_event_exists": False,
        "current_event_exists": generation and current_generation_id == 19,
    }


def _unsealed_execution_evidence_row():
    row = _execution_evidence_row(current_generation_id=None)
    row.update(dict.fromkeys(_SEAL_EVIDENCE_FIELDS))
    row.update(state="running", finished_at=None, ever_published=False)
    return row


def _no_change_execution_evidence_row():
    row = _execution_evidence_row(current_generation_id=13)
    row.update(
        no_change_execution_id=17,
        no_change_dataset_id=3,
        no_change_definition_revision_id=5,
        no_change_schema_revision_id=7,
        no_change_capture_bundle_id=11,
        no_change_candidate_generation_id=19,
        no_change_contract="custom-import-no-change-seal/v1",
        no_change_base_generation_id=13,
        no_change_base_pointer_version=8,
        no_change_effective_output_sha256=_DIGEST,
        no_change_receipt_sha256=_DIGEST,
        no_change_sealed_at=_NOW,
        no_change_event_exists=True,
        ever_published=False,
        state="no_change",
    )
    return row


def _without_optional_evidence_row():
    row = _execution_evidence_row(generation=False, current_generation_id=None)
    row.update(state="running", finished_at=None, capture_bundle_id=None, execution_source_binding_revision_id=None)
    for field in (
        "binding_revision_id",
        "binding_dataset_id",
        "binding_definition_revision_id",
        "binding_schema_revision_id",
        "binding_definition_sha256",
        "binding_schema_sha256",
        "source_binding_sha256",
        "capture_id",
        "capture_dataset_id",
        "capture_definition_revision_id",
        "capture_schema_revision_id",
        "capture_manifest_sha256",
    ):
        row[field] = None
    return row


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
async def test_inactive_lease_can_retain_heartbeat():
    execution_map = {**_execution_row(state="completed"), "lease_fence": 0, "lease_expires_at": None}
    status = await inspect_execution(_Session(execution_map), dataset_id=3, execution_id=17)

    assert status.lease is not None
    assert (status.lease.fence, status.lease.heartbeat_at, status.lease.expires_at) == (0, _NOW, None)


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
async def test_execution_evidence_is_one_safe_retained_snapshot():
    session = _Session(_execution_evidence_row())

    evidence = await inspect_execution_evidence(session, dataset_id=3, execution_id=17)

    assert len(session.statements) == 1
    statement = session.statements[0]
    assert statement.get_execution_options()["autoflush"] is False
    assert evidence.execution.execution_id == 17
    assert {field.name for field in fields(evidence.execution)}.isdisjoint(
        {"lease", "idempotency_key", "terminal_reason"}
    )
    assert evidence.definition_sha256 == _DIGEST.hex()
    assert evidence.schema_sha256 == _DIGEST.hex()
    assert evidence.source_binding_revision_id == 23
    assert evidence.source_binding_sha256 == _DIGEST.hex()
    assert evidence.capture_manifest_sha256 == _DIGEST.hex()
    assert evidence.current is not None and evidence.current.generation_id == 19
    assert evidence.generation is not None
    assert evidence.generation.generation_id == 19
    assert evidence.generation.source_bundle_sha256 == _DIGEST.hex()
    assert evidence.generation.seal is not None
    assert evidence.generation.seal.materialization_sha256 == _DIGEST.hex()
    assert evidence.generation.publication_state == "current"
    rendered = str(statement)
    for forbidden in (
        "canonical_binding",
        "canonical_definition",
        "canonical_manifest",
        "canonical_schema",
        "idempotency_key",
        "request_identity_sha256",
        "snapshot_token",
        ".producing_token AS ",
        ".sealing_token AS ",
        "custom_import_lease",
    ):
        assert forbidden not in rendered


@pytest.mark.asyncio
async def test_execution_evidence_keeps_an_incomplete_execution_distinct_from_invalid_evidence():
    row = _execution_evidence_row(generation=False)
    row["state"] = "running"
    row["finished_at"] = None
    evidence = await inspect_execution_evidence(
        _Session(row),
        dataset_id=3,
        execution_id=17,
    )

    assert evidence.generation is None
    assert evidence.current is not None and evidence.current.generation_id == 19


@pytest.mark.asyncio
@pytest.mark.parametrize("state", ["completed", "no_change"])
async def test_execution_evidence_rejects_terminal_execution_without_generation(state):
    row = _execution_evidence_row(generation=False)
    row["state"] = state

    with pytest.raises(OperatorInvariantError):
        await inspect_execution_evidence(_Session(row), dataset_id=3, execution_id=17)


@pytest.mark.asyncio
async def test_execution_evidence_rejects_orphaned_generation_evidence():
    row = _execution_evidence_row(generation=False)
    row["sealed_root_count"] = 2

    with pytest.raises(OperatorInvariantError):
        await inspect_execution_evidence(_Session(row), dataset_id=3, execution_id=17)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "field, value",
    [
        ("binding_definition_sha256", bytes(reversed(_DIGEST))),
        ("capture_definition_revision_id", 13),
        ("generation_capture_bundle_id", 13),
        ("seal_execution_id", 13),
    ],
)
async def test_execution_evidence_rejects_mismatched_joins(field, value):
    mismatched_evidence_by_field = {**_execution_evidence_row(), field: value}
    with pytest.raises(OperatorInvariantError):
        await inspect_execution_evidence(_Session(mismatched_evidence_by_field), dataset_id=3, execution_id=17)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "field, value",
    [
        ("execution_id", 0),
        ("state", "unknown"),
        ("definition_dataset_id", 13),
        ("binding_dataset_id", 13),
        ("capture_dataset_id", 13),
        ("sealing_fence", 0),
        ("sealed_root_count", -1),
        ("seal_generation_id", 13),
        ("no_change_event_exists", None),
        ("current_definition_revision_id", 13),
        ("dataset_id", 13),
    ],
)
async def test_execution_evidence_rejects_invalid_retained_values(field, value):
    row = _execution_evidence_row()
    row[field] = value

    with pytest.raises(OperatorInvariantError):
        await inspect_execution_evidence(_Session(row), dataset_id=3, execution_id=17)


@pytest.mark.asyncio
async def test_execution_evidence_distinguishes_absent_optional_binding_and_capture():
    evidence = await inspect_execution_evidence(
        _Session(_without_optional_evidence_row()), dataset_id=3, execution_id=17
    )

    assert evidence.source_binding_revision_id is None
    assert evidence.source_binding_sha256 is None
    assert evidence.capture_manifest_sha256 is None
    assert evidence.generation is None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "orphan_field, value",
    [("binding_dataset_id", 3), ("capture_dataset_id", 3)],
)
async def test_execution_evidence_rejects_orphaned_optional_evidence(orphan_field, value):
    row = _without_optional_evidence_row()
    row[orphan_field] = value

    with pytest.raises(OperatorInvariantError):
        await inspect_execution_evidence(_Session(row), dataset_id=3, execution_id=17)


@pytest.mark.asyncio
async def test_execution_evidence_distinguishes_an_unsealed_generation():
    evidence = await inspect_execution_evidence(
        _Session(_unsealed_execution_evidence_row()), dataset_id=3, execution_id=17
    )

    assert evidence.generation is not None
    assert evidence.generation.seal is None
    assert evidence.generation.publication_state == "unsealed"


@pytest.mark.asyncio
@pytest.mark.parametrize("state", ["completed", "no_change"])
async def test_execution_evidence_rejects_terminal_generation_without_seal(state):
    row = _unsealed_execution_evidence_row()
    row.update(state=state, finished_at=_NOW)

    with pytest.raises(OperatorInvariantError):
        await inspect_execution_evidence(_Session(row), dataset_id=3, execution_id=17)


@pytest.mark.asyncio
@pytest.mark.parametrize("orphaned_contract", [True, False])
async def test_execution_evidence_rejects_incomplete_or_orphaned_seal(orphaned_contract):
    row = _unsealed_execution_evidence_row()
    if orphaned_contract:
        row["seal_contract"] = "custom-import-generation-seal/v1"
    else:
        row.update(
            seal_generation_id=19,
            seal_dataset_id=3,
            seal_definition_revision_id=5,
            seal_schema_revision_id=7,
            seal_execution_id=17,
            seal_capture_bundle_id=11,
        )

    with pytest.raises(OperatorInvariantError):
        await inspect_execution_evidence(_Session(row), dataset_id=3, execution_id=17)


@pytest.mark.asyncio
async def test_execution_evidence_reports_retained_no_change():
    evidence = await inspect_execution_evidence(
        _Session(_no_change_execution_evidence_row()), dataset_id=3, execution_id=17
    )

    assert evidence.generation is not None
    assert evidence.generation.publication_state == "no_change"
    assert evidence.generation.no_change is not None
    assert evidence.generation.no_change.base_generation_id == 13


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "field, value",
    [
        ("no_change_base_generation_id", 0),
        ("no_change_candidate_generation_id", 13),
        ("no_change_contract", None),
    ],
)
async def test_execution_evidence_rejects_inconsistent_no_change(field, value):
    row = _no_change_execution_evidence_row()
    row[field] = value

    with pytest.raises(OperatorInvariantError):
        await inspect_execution_evidence(_Session(row), dataset_id=3, execution_id=17)


@pytest.mark.asyncio
async def test_execution_evidence_rejects_orphaned_no_change_columns():
    row = _execution_evidence_row()
    row.update(dict.fromkeys(_NO_CHANGE_EVIDENCE_FIELDS))
    row["no_change_receipt_sha256"] = _DIGEST

    with pytest.raises(OperatorInvariantError):
        await inspect_execution_evidence(_Session(row), dataset_id=3, execution_id=17)


@pytest.mark.asyncio
async def test_execution_evidence_refuses_ambiguous_multi_generation_evidence():
    recovered = _execution_evidence_row()
    recovered["evidence_generation_id"] = 23
    recovered["seal_generation_id"] = 23
    session = _Session((_execution_evidence_row(), recovered))

    with pytest.raises(OperatorInvariantError, match="ambiguous"):
        await inspect_execution_evidence(
            session,
            dataset_id=3,
            execution_id=17,
        )
    assert "LIMIT 2" in str(session.statements[0].compile(compile_kwargs={"literal_binds": True}))


@pytest.mark.asyncio
async def test_execution_evidence_selects_an_exact_recovery_candidate():
    recovered = _execution_evidence_row()
    recovered["evidence_generation_id"] = 23
    recovered["seal_generation_id"] = 23
    recovered["ever_published"] = False
    session = _Session(recovered)

    evidence = await inspect_execution_evidence(session, dataset_id=3, execution_id=17, candidate_generation_id=23)

    assert evidence.generation is not None
    assert evidence.generation.generation_id == 23
    assert evidence.generation.publication_state == "sealed_unpublished"
    statement = str(session.statements[0].compile(compile_kwargs={"literal_binds": True}))
    assert "mrf.custom_import_generation.execution_id = mrf.custom_import_execution.execution_id" in statement
    assert "custom_import_generation.generation_id = 23" in statement


@pytest.mark.asyncio
@pytest.mark.parametrize("candidate_generation_id", [23, 29])
async def test_execution_evidence_rejects_missing_or_foreign_candidate(candidate_generation_id):
    session = _Session(_execution_evidence_row(generation=False))

    with pytest.raises(OperatorObjectNotFound):
        await inspect_execution_evidence(
            session, dataset_id=3, execution_id=17, candidate_generation_id=candidate_generation_id
        )


@pytest.mark.asyncio
async def test_execution_evidence_requires_a_transaction_and_exact_execution():
    inactive = _Session(_execution_evidence_row(), active=False)
    with pytest.raises(OperatorTransactionRequired):
        await inspect_execution_evidence(inactive, dataset_id=3, execution_id=17)
    assert inactive.statements == []

    missing = _Session(None)
    with pytest.raises(OperatorObjectNotFound):
        await inspect_execution_evidence(missing, dataset_id=3, execution_id=17)
    assert len(missing.statements) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("dataset_id, execution_id", [(0, 1), (1, 0), (True, 1), (1, 2**63)])
async def test_execution_evidence_rejects_invalid_identifiers_without_sql(dataset_id, execution_id):
    session = _Session(_execution_evidence_row())

    with pytest.raises(OperatorInspectionError):
        await inspect_execution_evidence(session, dataset_id=dataset_id, execution_id=execution_id)

    assert session.statements == []


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
