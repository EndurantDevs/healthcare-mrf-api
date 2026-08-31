# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

from dataclasses import fields, replace
from datetime import UTC, datetime

import pytest

from process import provider_directory_rooted_graph_twin_store as twin_store
from process.provider_directory_fhir_root_policy import (
    REVIEWED_ROOT_POLICY_METADATA_KEY,
)
from process.provider_directory_rooted_graph_single_root_contract import (
    derive_single_root_identity,
)
from process.provider_directory_rooted_graph_publication import (
    build_provider_directory_rooted_graph_dataset_identity,
    provider_directory_rooted_graph_publication_metadata,
)
from process.provider_directory_rooted_graph_twin_contract import (
    build_provider_directory_rooted_graph_twin_admission,
    build_provider_directory_rooted_graph_twin_attempt,
    ProviderDirectoryRootedGraphTwinError,
)
from tests.provider_directory_rooted_graph_publication_test_support import (
    exact_current,
    resource_counts,
    sealed_roots,
)


class _Transaction:
    async def __aenter__(self):
        return self

    async def __aexit__(self, *_error) -> bool:
        return False


class _ScriptedDatabase:
    def __init__(self, *, rows=(), first_rows=(), scalars=()) -> None:
        self.rows = list(rows)
        self.first_rows = list(first_rows)
        self.scalars = list(scalars)
        self.status_calls: list[tuple[str, dict[str, object]]] = []

    def transaction(self) -> _Transaction:
        return _Transaction()

    async def all(self, _statement: str, **_parameters: object):
        return self.rows.pop(0)

    async def first(self, _statement: str, **_parameters: object):
        return self.first_rows.pop(0)

    async def scalar(self, _statement: str, **_parameters: object):
        return self.scalars.pop(0)

    async def status(self, statement: str, **parameters: object) -> int:
        self.status_calls.append((statement, parameters))
        return 1


def _root_row(root) -> dict[str, object]:
    row_by_column = {field.name: getattr(root, field.name) for field in fields(root)}
    row_by_column.update(
        status="sealed",
        rooted_graph_complete=True,
        endpoint_collection_complete=False,
        endpoint_complete=False,
        sealed_at=datetime(2026, 8, 10, tzinfo=UTC),
    )
    return row_by_column


def _attempt_and_admission():
    baseline, candidate = sealed_roots()
    recorded_at = datetime(2026, 8, 10, 12, tzinfo=UTC)
    attempt = build_provider_directory_rooted_graph_twin_attempt(
        baseline,
        candidate,
        attempted_at=recorded_at,
    )
    admission = build_provider_directory_rooted_graph_twin_admission(
        attempt,
        candidate,
        admitted_at=recorded_at,
    )
    return baseline, candidate, attempt, admission


def _record(candidate) -> dict[str, object]:
    return {field.name: getattr(candidate, field.name) for field in fields(candidate)}


def test_schema_row_and_timestamp_boundaries(monkeypatch) -> None:
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "rooted")
    monkeypatch.setenv("DB_SCHEMA", "rooted")
    assert twin_store._table("evidence") == '"rooted"."evidence"'
    monkeypatch.setenv("DB_SCHEMA", "other")
    with pytest.raises(ProviderDirectoryRootedGraphTwinError):
        twin_store._schema()
    monkeypatch.delenv("DB_SCHEMA")
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "bad-name")
    with pytest.raises(ProviderDirectoryRootedGraphTwinError):
        twin_store._schema()

    assert twin_store._row_fields(None) == {}
    with pytest.raises(ProviderDirectoryRootedGraphTwinError):
        twin_store._row_fields(object())
    aware = datetime(2026, 8, 10, tzinfo=UTC)
    assert twin_store._timestamp(aware) is aware
    for invalid_timestamp in (None, datetime(2026, 8, 10)):
        with pytest.raises(ProviderDirectoryRootedGraphTwinError):
            twin_store._timestamp(invalid_timestamp)


def test_row_decoders_reject_drift_and_preserve_exact_records() -> None:
    baseline, _candidate, attempt, admission = _attempt_and_admission()
    assert twin_store._root_from_row(_root_row(baseline)) == baseline
    malformed_root = _root_row(baseline)
    malformed_root["status"] = "running"
    with pytest.raises(ProviderDirectoryRootedGraphTwinError):
        twin_store._root_from_row(malformed_root)

    assert twin_store._attempt_from_row(_record(attempt)) == attempt
    malformed_attempt = _record(attempt)
    malformed_attempt["attempted_at"] = None
    with pytest.raises(ProviderDirectoryRootedGraphTwinError):
        twin_store._attempt_from_row(malformed_attempt)
    assert twin_store._admission_from_row(_record(admission)) == admission
    with pytest.raises(ProviderDirectoryRootedGraphTwinError, match="missing"):
        twin_store._admission_from_row(None)


@pytest.mark.asyncio
async def test_root_locking_is_ordered_complete_and_distinct() -> None:
    baseline, candidate = sealed_roots()
    database = _ScriptedDatabase(rows=((_root_row(baseline), _root_row(candidate)),))
    assert await twin_store._lock_roots(
        database,
        (candidate.acquisition_id, baseline.acquisition_id),
    ) == (baseline, candidate)
    with pytest.raises(ProviderDirectoryRootedGraphTwinError, match="identity"):
        await twin_store._lock_roots(
            database,
            (baseline.acquisition_id, baseline.acquisition_id),
        )
    missing_database = _ScriptedDatabase(rows=((_root_row(baseline),),))
    with pytest.raises(ProviderDirectoryRootedGraphTwinError, match="state"):
        await twin_store._lock_roots(
            missing_database,
            (baseline.acquisition_id, candidate.acquisition_id),
        )


@pytest.mark.asyncio
async def test_insert_and_read_helpers_bound_exact_persisted_records() -> None:
    _baseline, _candidate, attempt, admission = _attempt_and_admission()
    database = _ScriptedDatabase(
        first_rows=(_record(attempt), _record(admission), None)
    )
    await twin_store._insert_attempt(database, attempt)
    await twin_store._insert_authority(database, admission)
    assert len(database.status_calls) == 2
    assert await twin_store._read_attempt(database, attempt.attempt_id) == attempt
    assert (
        await twin_store._read_admission(
            database,
            admission.publication_acquisition_id,
        )
        == admission
    )
    with pytest.raises(ProviderDirectoryRootedGraphTwinError, match="missing"):
        await twin_store._read_attempt(database, attempt.attempt_id)


def test_candidate_request_and_exact_comparison_boundaries() -> None:
    baseline, candidate, attempt, _admission = _attempt_and_admission()
    assert twin_store._candidate_root((baseline, candidate)) == candidate
    with pytest.raises(ProviderDirectoryRootedGraphTwinError, match="identity"):
        twin_store._candidate_root((baseline, baseline))
    twin_store._validate_admission_request(
        baseline.acquisition_id,
        candidate.acquisition_id,
    )
    with pytest.raises(ValueError, match="acquisition_id_invalid"):
        twin_store._validate_admission_request("bad")
    twin_store._require_exact(
        attempt,
        attempt,
        twin_store._ATTEMPT_IDENTITY_COLUMNS,
    )
    with pytest.raises(ProviderDirectoryRootedGraphTwinError, match="state"):
        twin_store._require_exact(
            attempt,
            object(),
            twin_store._ATTEMPT_IDENTITY_COLUMNS,
        )


async def _exercise_admission(monkeypatch, *, current, roots):
    recorded_at = datetime(2026, 8, 10, 12, tzinfo=UTC)
    stored_by_kind: dict[str, object] = {}

    async def lock_current(_database):
        return current

    async def lock_roots(_database, _acquisition_ids):
        return roots

    async def insert_attempt(_database, attempt):
        stored_by_kind["attempt"] = attempt

    async def read_attempt(_database, _attempt_id):
        return stored_by_kind["attempt"]

    async def insert_admission(_database, admission):
        stored_by_kind["admission"] = admission

    async def read_admission(_database, _publication_acquisition_id):
        return stored_by_kind["admission"]

    monkeypatch.setattr(twin_store, "_lock_logical_current", lock_current)
    monkeypatch.setattr(twin_store, "_lock_roots", lock_roots)
    monkeypatch.setattr(twin_store, "_insert_attempt", insert_attempt)
    monkeypatch.setattr(twin_store, "_read_attempt", read_attempt)
    monkeypatch.setattr(twin_store, "_insert_authority", insert_admission)
    monkeypatch.setattr(twin_store, "_read_admission", read_admission)
    database = _ScriptedDatabase(scalars=(recorded_at,))
    return await twin_store.admit_provider_directory_rooted_graph_twins(
        roots[0].acquisition_id,
        roots[1].acquisition_id,
        database=database,
    )


@pytest.mark.asyncio
async def test_admission_accepts_match_and_rejects_mismatch_or_stale(
    monkeypatch,
) -> None:
    matched_roots = sealed_roots()
    admission = await _exercise_admission(
        monkeypatch,
        current=exact_current(),
        roots=matched_roots,
    )
    assert admission.publication_acquisition_id == matched_roots[1].acquisition_id

    mismatched_roots = (
        sealed_roots()[0],
        sealed_roots(second_resource_hash="e" * 64)[1],
    )
    with pytest.raises(ProviderDirectoryRootedGraphTwinError) as mismatch_error:
        await _exercise_admission(
            monkeypatch,
            current=exact_current(),
            roots=mismatched_roots,
        )
    assert mismatch_error.value.code == "mismatch"
    with pytest.raises(ProviderDirectoryRootedGraphTwinError) as stale_error:
        await _exercise_admission(
            monkeypatch,
            current=None,
            roots=matched_roots,
        )
    assert stale_error.value.code == "stale"


@pytest.mark.asyncio
async def test_twin_admission_rejects_partial_current_root(monkeypatch) -> None:
    with pytest.raises(ProviderDirectoryRootedGraphTwinError, match="state"):
        await _exercise_admission(
            monkeypatch,
            current=exact_current(retry_exhausted_count=1),
            roots=sealed_roots(),
        )


@pytest.mark.asyncio
async def test_required_admission_rebuilds_or_maps_missing_evidence(
    monkeypatch,
) -> None:
    baseline, candidate, attempt, admission = _attempt_and_admission()

    async def read_admission(_database, _publication_acquisition_id):
        return admission

    async def read_attempt(_database, _attempt_id):
        return attempt

    async def lock_roots(_database, _acquisition_ids):
        return baseline, candidate

    monkeypatch.setattr(twin_store, "_read_admission", read_admission)
    monkeypatch.setattr(twin_store, "_read_attempt", read_attempt)
    monkeypatch.setattr(twin_store, "_lock_roots", lock_roots)
    assert (
        await twin_store.require_provider_directory_rooted_graph_admission(
            admission.publication_acquisition_id,
            database=_ScriptedDatabase(),
        )
        == admission
    )

    async def missing_admission(_database, _publication_acquisition_id):
        raise ProviderDirectoryRootedGraphTwinError("missing")

    monkeypatch.setattr(twin_store, "_read_admission", missing_admission)
    with pytest.raises(ProviderDirectoryRootedGraphTwinError, match="missing"):
        await twin_store.require_provider_directory_rooted_graph_admission(
            admission.publication_acquisition_id,
            database=_ScriptedDatabase(),
        )


def _single_root_candidate(current, operation_key):
    """Project one deterministic identity onto synthetic sealed proof."""

    identity = derive_single_root_identity(current, operation_key=operation_key)
    proof = sealed_roots()[1]
    return type(proof)(
        **{
            field.name: (
                getattr(identity.candidate, field.name)
                if hasattr(identity.candidate, field.name)
                else getattr(proof, field.name)
            )
            for field in fields(proof)
        }
    )


def test_partial_single_root_intent_is_bound_without_changing_complete_identity() -> (
    None
):
    operation_key = "f" * 64
    complete = exact_current()
    partial = replace(
        complete,
        cohort_complete=False,
        retry_exhausted_count=8,
    )

    complete_identity = derive_single_root_identity(
        complete,
        operation_key=operation_key,
    )
    partial_identity = derive_single_root_identity(
        partial,
        operation_key=operation_key,
    )
    different_partial = derive_single_root_identity(
        replace(partial, retry_exhausted_count=9),
        operation_key=operation_key,
    )

    assert complete_identity.dataset_intent_id == (
        "pdrgi_051f89ecd1ea11a695cc6ab36122a388ecf554bd7c13a45e"
    )
    assert partial_identity.dataset_intent_id != complete_identity.dataset_intent_id
    assert different_partial.dataset_intent_id != partial_identity.dataset_intent_id


@pytest.mark.asyncio
async def test_single_root_admission_rederives_identity_and_publishes_metadata(
    monkeypatch,
) -> None:
    """Bind admission, metadata, and stale-seal rejection to one identity."""

    operation_key = "f" * 64
    current = exact_current()
    candidate = _single_root_candidate(current, operation_key)
    locked_roots = [candidate]
    stored_by_kind: dict[str, object] = {}

    async def lock_current(_database):
        return current

    async def lock_root(_database, _acquisition_id):
        return locked_roots[0]

    async def insert_admission(_database, admission):
        stored_by_kind["admission"] = admission

    async def read_admission(_database, _acquisition_id):
        return stored_by_kind["admission"]

    monkeypatch.setattr(twin_store, "_lock_logical_current", lock_current)
    monkeypatch.setattr(twin_store, "_lock_single_root", lock_root)
    monkeypatch.setattr(twin_store, "_insert_authority", insert_admission)
    monkeypatch.setattr(twin_store, "_read_admission", read_admission)
    recorded_at = datetime(2026, 8, 10, 12, tzinfo=UTC)
    admission = await twin_store.admit_rooted_graph_single_root(
        candidate.acquisition_id,
        acquisition_operation_key=operation_key,
        database=_ScriptedDatabase(scalars=(recorded_at,)),
    )

    assert admission.attempt_id is None
    assert admission.comparison_acquisition_id is None
    assert admission.acquisition_operation_key == operation_key
    identity = build_provider_directory_rooted_graph_dataset_identity(
        admission,
        current,
    )
    metadata = provider_directory_rooted_graph_publication_metadata(
        identity,
        admission,
        previous_dataset_id=current.dataset_id,
        resource_counts=resource_counts(),
    )
    assert metadata[REVIEWED_ROOT_POLICY_METADATA_KEY]["required_root_count"] == 1
    assert metadata["acquisition_operation_key"] == operation_key

    locked_roots[0] = replace(candidate, run_id="pdrgr_" + "0" * 48)
    with pytest.raises(ProviderDirectoryRootedGraphTwinError, match="identity"):
        await twin_store.admit_rooted_graph_single_root(
            candidate.acquisition_id,
            acquisition_operation_key=operation_key,
            database=_ScriptedDatabase(scalars=(recorded_at,)),
        )
