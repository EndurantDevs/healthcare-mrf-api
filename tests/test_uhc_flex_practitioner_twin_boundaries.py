# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Fail-closed row and replay boundaries for Practitioner twin admission."""

from __future__ import annotations

from contextlib import asynccontextmanager
import copy
from dataclasses import fields, replace
from datetime import date
from datetime import timedelta
from types import SimpleNamespace

import pytest

from process import uhc_flex_practitioner_twin_identity as twin_identity
from process import uhc_flex_practitioner_twin_store as twin_store
from process import uhc_flex_practitioner_twin_store_contract as twin_contract
from process.uhc_flex_practitioner_single_root_contract import (
    build_single_root_admission,
    single_root_dataset_intent_id,
    single_root_run_id,
    UHCFlexPractitionerSingleRootError,
    UHCFlexPractitionerSingleRootReceipt,
)
from process.uhc_flex_practitioner_store_contract import _acquisition_id
from tests.provider_directory_uhc_flex_npi_cohort_pg_support import cohort_fixture
from tests.test_uhc_flex_practitioner_twin_store_contract import (
    _root,
    OPERATION_KEY,
    PROJECTION_DATE,
    TIMESTAMP,
)


def _mutated(value, **changes):
    changed = copy.copy(value)
    for field_name, field_value in changes.items():
        object.__setattr__(changed, field_name, field_value)
    return changed


def _field_map(value) -> dict[str, object]:
    return {field.name: getattr(value, field.name) for field in fields(type(value))}


class _Database:
    def __init__(self, *, all_rows=(), first_rows=()) -> None:
        self.all_rows = list(all_rows)
        self.all_calls = []
        self.first_rows = list(first_rows)
        self.first_calls = []
        self.status_calls = []

    @asynccontextmanager
    async def transaction(self):
        yield self

    async def scalar(self, _statement, **_parameters):
        return TIMESTAMP

    async def first(self, statement, **parameters):
        self.first_calls.append((statement, parameters))
        return self.first_rows.pop(0) if self.first_rows else None

    async def all(self, statement, **parameters):
        self.all_calls.append((statement, parameters))
        return self.all_rows

    async def status(self, statement, **parameters):
        self.status_calls.append((statement, parameters))


def _sealed_database_row(root):
    return {
        **_field_map(root),
        "status": "sealed",
        "cohort_complete": root.cohort_complete,
        "pending_count": 0,
        "leased_count": 0,
        "error_count": root.error_count,
        "endpoint_collection_complete": False,
        "endpoint_complete": False,
        "sealed_at": TIMESTAMP,
    }


def _single_root(cohort_id: str, expected_npi_count: int):
    intent_id = single_root_dataset_intent_id(
        cohort_id,
        PROJECTION_DATE,
        OPERATION_KEY,
    )
    run_id = single_root_run_id(intent_id)
    return replace(
        _root("candidate", cohort_id=cohort_id),
        acquisition_id=_acquisition_id(
            cohort_id=cohort_id,
            acquisition_role="candidate",
            run_id=run_id,
            dataset_intent_id=intent_id,
            expected_npi_count=expected_npi_count,
        ),
        dataset_intent_id=intent_id,
        expected_npi_count=expected_npi_count,
        run_id=run_id,
    )


def _single_root_lock_row():
    cohort = cohort_fixture()
    candidate = _single_root(cohort.cohort_id, cohort.npi_count)
    snapshot = SimpleNamespace(
        endpoint_id=cohort.official_endpoint_id,
        dataset_id=cohort.official_dataset_id,
        acquisition_root_run_id=cohort.official_acquisition_root_run_id,
        dataset_hash=cohort.official_dataset_hash,
        content_proof_sha256=cohort.official_content_proof_sha256,
        practitioner_resource_count=cohort.practitioner_resource_count,
    )
    return (
        candidate,
        _sealed_database_row(candidate),
        _field_map(cohort),
        snapshot,
    )


def _attempt_and_admission():
    attempt = twin_contract.build_uhc_flex_practitioner_twin_attempt(
        _root("baseline"),
        _root("candidate"),
        semantic_projection_as_of=PROJECTION_DATE,
        operation_key=OPERATION_KEY,
        attempted_at=TIMESTAMP,
    )
    admission = twin_contract.build_uhc_flex_practitioner_twin_admission(
        attempt,
        admitted_at=TIMESTAMP,
    )
    return attempt, admission


def test_twin_identity_and_contract_fail_closed_on_malformed_coordinates():
    for invalid in (object(), "2026-8-10", "2026-02-30", "2026-W33-1"):
        with pytest.raises(ValueError):
            twin_identity.canonical_semantic_projection_as_of(invalid)
    with pytest.raises(ValueError):
        twin_identity.practitioner_dataset_intent_id(
            "invalid",
            PROJECTION_DATE,
            OPERATION_KEY,
        )
    with pytest.raises(ValueError):
        twin_identity.practitioner_dataset_intent_id(
            _root("baseline").cohort_id,
            PROJECTION_DATE,
            object(),
        )
    with pytest.raises(ValueError):
        twin_identity.build_uhc_flex_practitioner_run_id("invalid", "other")

    baseline = _root("baseline")
    candidate = _root("candidate")
    with pytest.raises(ValueError):
        replace(baseline, terminal_set_sha256="invalid")
    with pytest.raises(twin_contract.UHCFlexPractitionerTwinStoreError):
        twin_contract._validated_pair_context(
            baseline,
            _mutated(candidate, source_id="other"),
            semantic_projection_as_of=PROJECTION_DATE,
            operation_key=OPERATION_KEY,
        )
    assert twin_contract._has_valid_lineage(object()) is False
    attempt, _admission = _attempt_and_admission()
    with pytest.raises(twin_contract.UHCFlexPractitionerTwinStoreError):
        twin_contract.uhc_flex_practitioner_twin_admission_id(
            _mutated(attempt, matched=False)
        )


def test_single_root_contract_rejects_invalid_coordinates_and_receipts():
    bounded_error = UHCFlexPractitionerSingleRootError("private")
    assert bounded_error.code == "state"
    assert "private" not in str(bounded_error)
    cohort = cohort_fixture()
    candidate = _single_root(cohort.cohort_id, cohort.npi_count)

    with pytest.raises(ValueError):
        single_root_dataset_intent_id("invalid", PROJECTION_DATE, OPERATION_KEY)
    with pytest.raises(ValueError):
        single_root_run_id("invalid")
    with pytest.raises(UHCFlexPractitionerSingleRootError) as caught:
        build_single_root_admission(
            _mutated(candidate, acquisition_role="baseline"),
            semantic_projection_as_of=PROJECTION_DATE,
            operation_key=OPERATION_KEY,
            admitted_at=TIMESTAMP,
        )
    assert caught.value.code == "identity"

    with pytest.raises(ValueError):
        UHCFlexPractitionerSingleRootReceipt(
            operation_key=OPERATION_KEY,
            semantic_projection_as_of=PROJECTION_DATE,
            source_id="invalid",
            endpoint_id="0" * 64,
            cohort_id=cohort.cohort_id,
            official_dataset_id="dataset",
            official_dataset_hash="0" * 64,
            official_content_proof_sha256="1" * 64,
            dataset_intent_id=single_root_dataset_intent_id(
                cohort.cohort_id, PROJECTION_DATE, OPERATION_KEY
            ),
            expected_npi_count=cohort.npi_count,
            candidate=object(),
            admission_id="pdufpad_" + "2" * 48,
            reviewed_root_policy_json={},
            elapsed_seconds=0.0,
        )


def test_twin_store_row_and_exact_replay_validation_boundaries():
    attempt, admission = _attempt_and_admission()
    assert twin_store._date_text(date.fromisoformat(PROJECTION_DATE)) == PROJECTION_DATE
    assert twin_store._date_text(PROJECTION_DATE) == PROJECTION_DATE
    for invalid in (object(), TIMESTAMP.replace(tzinfo=None)):
        with pytest.raises(twin_contract.UHCFlexPractitionerTwinStoreError):
            twin_store._timestamp(invalid)
    with pytest.raises(twin_contract.UHCFlexPractitionerTwinStoreError):
        twin_store._sealed_root({})
    malformed_root = _sealed_database_row(_root("baseline"))
    malformed_root["terminal_set_sha256"] = "invalid"
    with pytest.raises(twin_contract.UHCFlexPractitionerTwinStoreError):
        twin_store._sealed_root(malformed_root)

    partial_root = replace(
        _root("candidate"),
        error_count=1,
        cohort_complete=False,
    )
    assert twin_store._sealed_root(_sealed_database_row(partial_root)) == partial_root
    inconsistent_partial_row = _sealed_database_row(partial_root)
    inconsistent_partial_row["cohort_complete"] = True
    with pytest.raises(twin_contract.UHCFlexPractitionerTwinStoreError):
        twin_store._sealed_root(inconsistent_partial_row)

    with pytest.raises(twin_contract.UHCFlexPractitionerTwinStoreError):
        twin_store._attempt_from_row(None)
    malformed_attempt = _field_map(attempt)
    malformed_attempt["attempt_id"] = "invalid"
    with pytest.raises(twin_contract.UHCFlexPractitionerTwinStoreError):
        twin_store._attempt_from_row(malformed_attempt)
    with pytest.raises(twin_contract.UHCFlexPractitionerTwinStoreError):
        twin_store._admission_from_row(None)
    malformed_admission = _field_map(admission)
    malformed_admission["admission_id"] = "invalid"
    with pytest.raises(twin_contract.UHCFlexPractitionerTwinStoreError):
        twin_store._admission_from_row(malformed_admission)

    with pytest.raises(twin_contract.UHCFlexPractitionerTwinStoreError):
        twin_store._require_exact_attempt(
            _mutated(attempt, operation_key="0" * 64),
            attempt,
        )
    with pytest.raises(twin_contract.UHCFlexPractitionerTwinStoreError):
        twin_store._require_exact_admission(
            _mutated(admission, operation_key="0" * 64),
            admission,
        )


@pytest.mark.asyncio
async def test_twin_store_lock_and_public_require_input_boundaries():
    baseline = _root("baseline")
    candidate = _root("candidate")
    with pytest.raises(twin_contract.UHCFlexPractitionerTwinStoreError):
        await twin_store._lock_sealed_roots(
            _Database(),
            baseline.acquisition_id,
            baseline.acquisition_id,
        )
    with pytest.raises(twin_contract.UHCFlexPractitionerTwinStoreError):
        await twin_store._lock_sealed_roots(
            _Database(all_rows=()),
            baseline.acquisition_id,
            candidate.acquisition_id,
        )
    with pytest.raises(twin_contract.UHCFlexPractitionerTwinStoreError):
        await twin_store.require_uhc_flex_practitioner_admission(
            candidate.acquisition_id,
            semantic_projection_as_of=PROJECTION_DATE,
            operation_key=None,
        )


@pytest.mark.asyncio
async def test_single_root_lock_revalidates_current_official_dataset(monkeypatch):
    candidate, candidate_row, cohort_row, snapshot = _single_root_lock_row()
    database = _Database(first_rows=(candidate_row, cohort_row))

    async def current_snapshot(_database):
        assert _database is database
        return snapshot

    monkeypatch.setattr(
        twin_store.official_cohort,
        "_current_official_snapshot",
        current_snapshot,
    )

    assert await twin_store._lock_single_root(
        database,
        candidate.acquisition_id,
    ) == candidate
    assert len(database.first_calls) == 2
    assert "provider_directory_uhc_flex_practitioner_acquisition" in (
        database.first_calls[0][0]
    )
    assert "FOR SHARE" in database.first_calls[0][0]
    assert database.first_calls[0][1] == {
        "candidate_acquisition_id": candidate.acquisition_id,
    }
    assert "provider_directory_uhc_flex_npi_cohort" in database.first_calls[1][0]
    assert "FOR SHARE" in database.first_calls[1][0]
    assert database.first_calls[1][1] == {"cohort_id": candidate.cohort_id}

    drifted_snapshot = SimpleNamespace(**vars(snapshot))
    drifted_snapshot.content_proof_sha256 = "0" * 64

    async def drifted_current_snapshot(_database):
        return drifted_snapshot

    monkeypatch.setattr(
        twin_store.official_cohort,
        "_current_official_snapshot",
        drifted_current_snapshot,
    )
    with pytest.raises(twin_contract.UHCFlexPractitionerTwinStoreError):
        await twin_store._lock_single_root(
            _Database(first_rows=(candidate_row, cohort_row)),
            candidate.acquisition_id,
        )


@pytest.mark.asyncio
async def test_single_root_admission_replay_returns_stored_identity(monkeypatch):
    cohort = cohort_fixture()
    candidate = _single_root(cohort.cohort_id, cohort.npi_count)
    stored_admission = build_single_root_admission(
        candidate,
        semantic_projection_as_of=PROJECTION_DATE,
        operation_key=OPERATION_KEY,
        admitted_at=TIMESTAMP - timedelta(minutes=1),
    )

    async def lock_single_root(*_args, **_kwargs):
        return candidate

    async def no_write(*_args, **_kwargs):
        return None

    async def read_admission(*_args, **_kwargs):
        return stored_admission

    monkeypatch.setattr(twin_store, "_lock_single_root", lock_single_root)
    monkeypatch.setattr(twin_store, "_insert_admission", no_write)
    monkeypatch.setattr(twin_store, "_read_admission", read_admission)

    replay = await twin_store.admit_uhc_flex_practitioner_single_root(
        candidate.acquisition_id,
        semantic_projection_as_of=PROJECTION_DATE,
        operation_key=OPERATION_KEY,
        database=_Database(),
    )

    assert replay is stored_admission


@pytest.mark.asyncio
async def test_twin_store_impossible_post_transaction_states(monkeypatch):
    baseline = _root("baseline")
    candidate = _root("candidate")
    attempt, admission = _attempt_and_admission()

    async def locked_roots(*_args, **_kwargs):
        return baseline, candidate

    async def no_write(*_args, **_kwargs):
        return None

    async def read_attempt(*_args, **_kwargs):
        return attempt

    async def no_admission(*_args, **_kwargs):
        return None

    monkeypatch.setattr(twin_store, "_lock_sealed_roots", locked_roots)
    monkeypatch.setattr(twin_store, "_insert_attempt", no_write)
    monkeypatch.setattr(twin_store, "_require_exact_attempt", lambda *_args: None)
    monkeypatch.setattr(twin_store, "_read_attempt", read_attempt)
    monkeypatch.setattr(twin_store, "_insert_admission", no_write)
    monkeypatch.setattr(twin_store, "_read_admission", no_admission)
    monkeypatch.setattr(twin_store, "_require_exact_admission", lambda *_args: None)
    with pytest.raises(twin_contract.UHCFlexPractitionerTwinStoreError):
        await twin_store.admit_uhc_flex_practitioner_twins(
            baseline.acquisition_id,
            candidate.acquisition_id,
            semantic_projection_as_of=PROJECTION_DATE,
            operation_key=OPERATION_KEY,
            database=_Database(),
        )

    unmatched_attempt = _mutated(attempt, matched=False)

    async def read_unmatched_attempt(*_args, **_kwargs):
        return unmatched_attempt

    async def read_admission(*_args, **_kwargs):
        return admission

    monkeypatch.setattr(twin_store, "_read_attempt", read_unmatched_attempt)
    monkeypatch.setattr(twin_store, "_read_admission", read_admission)
    with pytest.raises(twin_contract.UHCFlexPractitionerTwinStoreError):
        await twin_store.require_uhc_flex_practitioner_admission(
            candidate.acquisition_id,
            database=_Database(),
        )
