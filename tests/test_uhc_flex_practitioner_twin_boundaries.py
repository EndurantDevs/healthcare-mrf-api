# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Fail-closed row and replay boundaries for Practitioner twin admission."""

from __future__ import annotations

from contextlib import asynccontextmanager
import copy
from dataclasses import fields, replace
from datetime import date

import pytest

from process import uhc_flex_practitioner_twin_identity as twin_identity
from process import uhc_flex_practitioner_twin_store as twin_store
from process import uhc_flex_practitioner_twin_store_contract as twin_contract
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
    def __init__(self, *, all_rows=()) -> None:
        self.all_rows = list(all_rows)

    @asynccontextmanager
    async def transaction(self):
        yield self

    async def scalar(self, _statement, **_parameters):
        return TIMESTAMP

    async def all(self, _statement, **_parameters):
        return self.all_rows


def _sealed_database_row(root):
    return {
        **_field_map(root),
        "status": "sealed",
        "cohort_complete": True,
        "pending_count": 0,
        "leased_count": 0,
        "error_count": 0,
        "endpoint_collection_complete": False,
        "endpoint_complete": False,
        "sealed_at": TIMESTAMP,
    }


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
