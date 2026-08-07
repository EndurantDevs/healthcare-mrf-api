# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Exact dataset-verification orchestration tests."""

from __future__ import annotations

import datetime as dt
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process.formulary_fhir import repository_verify
from process.formulary_fhir.repository import AliasRef
from process.formulary_fhir.repository import DatasetRef
from process.formulary_fhir.repository import DatasetVerification
from process.formulary_fhir.repository import FHIRFormularyRepository
from process.formulary_fhir.repository_shared import CompletedAliasCheckpoint


CUTOFF = dt.datetime(2026, 8, 7, 12, tzinfo=dt.UTC)


@asynccontextmanager
async def _transaction():
    yield


def _dataset() -> DatasetRef:
    return DatasetRef(
        "source-a",
        "ffd_" + "a" * 48,
        "run-a",
        None,
        CUTOFF,
        "c" * 64,
        "none",
        "building",
    )


def _alias() -> AliasRef:
    return AliasRef(
        "source-a",
        "fhir_" + "a" * 26,
        "ffa_" + "a" * 48,
        "SYNTHETIC-PLAN",
    )


def _dataset_row(**overrides):
    values_by_field = {
        "status": "building",
        "list_count": 0,
        "alias_count": 0,
        "medication_count": 0,
        "coverage_hash": None,
        "membership_hash": None,
    }
    values_by_field.update(overrides)
    return values_by_field


@pytest.mark.asyncio
async def test_alias_and_dataset_verification_paths(monkeypatch):
    dataset = _dataset()
    alias_by_field = {
        "public_id": _alias().public_id,
        "source_plan_identifier": _alias().source_plan_identifier,
        "alias_id": _alias().alias_id,
    }
    monkeypatch.setattr(
        repository_verify,
        "completed_checkpoint",
        AsyncMock(
            return_value=CompletedAliasCheckpoint(
                "source-a",
                dataset.dataset_id,
                _alias().alias_id,
                "version-a",
                1,
                "a" * 64,
                "full",
            )
        ),
    )
    observed, proof_rows, count = await repository_verify._alias_proof(
        object(),
        "source-a",
        dataset,
        [alias_by_field],
    )
    assert observed == {_alias().public_id: {"SYNTHETIC-PLAN"}}
    assert len(proof_rows) == count == 1
    repository_verify.completed_checkpoint.return_value = None
    with pytest.raises(RuntimeError, match="checkpoint is incomplete"):
        await repository_verify._alias_proof(
            object(),
            "source-a",
            dataset,
            [alias_by_field],
        )
    verification = repository_verify._verification_result(
        "source-a",
        dataset.dataset_id,
        ["coverage"],
        ["membership"],
        1,
    )
    stored = _dataset_row(
        list_count=1,
        alias_count=1,
        medication_count=1,
        coverage_hash=verification.coverage_hash,
        membership_hash=verification.membership_hash,
    )
    assert repository_verify._is_stored_verification_exact(stored, verification)


@pytest.mark.asyncio
async def test_verify_dataset_building_and_verified_branches(monkeypatch):
    """Exercise exact verification, replay, mismatch, and empty-plan paths."""

    database = SimpleNamespace(transaction=_transaction)
    repository = FHIRFormularyRepository(source_id="source-a", database=database)
    verification = DatasetVerification(
        "source-a", _dataset().dataset_id, 1, 1, 1, "a" * 64, "b" * 64
    )
    monkeypatch.setattr(
        repository_verify,
        "lock_dataset",
        AsyncMock(return_value=_dataset_row()),
    )
    monkeypatch.setattr(
        repository_verify,
        "_coverage_rows",
        AsyncMock(return_value=[{"row": 1}]),
    )
    monkeypatch.setattr(
        repository_verify,
        "_coverage_proof",
        lambda *_args: ({"plan": {"alias"}}, ["coverage"]),
    )
    monkeypatch.setattr(
        repository_verify,
        "snapshot_alias_rows",
        AsyncMock(return_value=[]),
    )
    monkeypatch.setattr(
        repository_verify,
        "_alias_proof",
        AsyncMock(return_value=({"plan": {"alias"}}, ["membership"], 1)),
    )
    monkeypatch.setattr(
        repository_verify,
        "_verification_result",
        lambda *_args: verification,
    )
    monkeypatch.setattr(repository_verify, "_mark_verified", AsyncMock())
    assert await repository.verify_dataset(dataset=_dataset()) == verification
    repository_verify._mark_verified.assert_awaited_once()
    repository_verify.lock_dataset.return_value = _dataset_row(
        status="verified",
        list_count=1,
        alias_count=1,
        medication_count=1,
        coverage_hash="a" * 64,
        membership_hash="b" * 64,
    )
    assert await repository.verify_dataset(dataset=_dataset()) == verification
    repository_verify.lock_dataset.return_value["coverage_hash"] = "c" * 64
    with pytest.raises(RuntimeError, match="stored verification changed"):
        await repository.verify_dataset(dataset=_dataset())
    repository_verify._coverage_rows.return_value = []
    with pytest.raises(RuntimeError, match="no coverage plans"):
        await repository.verify_dataset(dataset=_dataset())
