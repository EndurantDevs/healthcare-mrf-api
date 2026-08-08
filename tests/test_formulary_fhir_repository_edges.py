# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Focused edge coverage for dormant formulary repository helpers."""

from __future__ import annotations

import datetime as dt
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process.formulary_fhir import repository as repository_module
from process.formulary_fhir import repository_publish
from process.formulary_fhir import repository_shared
from process.formulary_fhir import repository_verify
from process.formulary_fhir.repository import DatasetRef
from process.formulary_fhir.repository import DatasetVerification
from process.formulary_fhir.repository import FHIRFormularyRepository
from process.formulary_fhir.repository import PriorAliasState
from process.formulary_fhir.repository_shared import membership_hash


CUTOFF = dt.datetime(2026, 8, 7, 12, tzinfo=dt.UTC)


def _dataset(*, status: str = "building") -> DatasetRef:
    return DatasetRef(
        "source-a",
        "ffd_" + "a" * 48,
        "run-a",
        None,
        CUTOFF,
        "c" * 64,
        "none",
        status,
    )


@pytest.mark.asyncio
async def test_dataset_query_helpers_return_exact_mappings():
    database = SimpleNamespace(
        first=AsyncMock(
            side_effect=[
                {"dataset_id": "current-dataset"},
                {"run_id": "run-a"},
            ]
        )
    )

    current_by_field = await repository_module._current_dataset_row(
        database,
        "source-a",
    )
    dataset_by_field = await repository_module._dataset_by_run(
        database,
        "source-a",
        "run-a",
    )

    assert current_by_field == {"dataset_id": "current-dataset"}
    assert dataset_by_field == {"run_id": "run-a"}


@pytest.mark.asyncio
async def test_dataset_insert_and_error_clear_validate_affected_rows():
    database = SimpleNamespace(status=AsyncMock(side_effect=[1, 1, 2, 2]))
    await repository_module._insert_dataset(
        database,
        "source-a",
        _dataset().dataset_id,
        "run-a",
        None,
        CUTOFF,
        "none",
        "c" * 64,
    )
    await repository_module._clear_dataset_error(database, "source-a", _dataset())
    with pytest.raises(RuntimeError, match="insert count"):
        await repository_module._insert_dataset(
            database,
            "source-a",
            _dataset().dataset_id,
            "run-a",
            None,
            CUTOFF,
            "none",
            "c" * 64,
        )
    with pytest.raises(RuntimeError, match="resume failed"):
        await repository_module._clear_dataset_error(database, "source-a", _dataset())


@pytest.mark.asyncio
async def test_error_clear_accepts_verified_and_already_clear_building():
    database = SimpleNamespace(status=AsyncMock(return_value=0))

    await repository_module._clear_dataset_error(
        database,
        "source-a",
        _dataset(status="verified"),
    )
    assert database.status.await_args.kwargs["dataset_status"] == "verified"

    await repository_module._clear_dataset_error(database, "source-a", _dataset())
    assert "error_json IS NOT NULL" in database.status.await_args.args[0]
    assert database.status.await_args.kwargs["dataset_status"] == "building"

    with pytest.raises(RuntimeError, match="not resumable"):
        await repository_module._clear_dataset_error(
            database,
            "source-a",
            _dataset(status="failed"),
        )


@pytest.mark.asyncio
async def test_prior_membership_load_returns_recomputed_evidence(monkeypatch):
    variants_by_id = {"med-1": "a" * 64}
    prior = PriorAliasState(
        "source-a",
        "fhir_" + "a" * 26,
        "ffa_" + "a" * 48,
        "SYNTHETIC-PLAN",
        "version-a",
        1,
        CUTOFF,
        {},
        membership_hash(variants_by_id),
    )
    monkeypatch.setattr(
        repository_module,
        "persisted_membership_proof",
        AsyncMock(
            return_value=(1, prior.membership_hash, variants_by_id)
        ),
    )
    repository = FHIRFormularyRepository(source_id="source-a", database=object())

    loaded = await repository.load_prior_alias_state(prior)

    assert loaded.variants_by_medication_id == variants_by_id


def test_idempotent_publication_returns_exact_current_generation():
    published_at = CUTOFF + dt.timedelta(minutes=1)
    result = repository_publish._idempotent_result(
        "source-a",
        _dataset(status="published"),
        {"status": "published"},
        {
            "dataset_id": _dataset().dataset_id,
            "generation": 4,
            "published_at": published_at,
        },
    )

    assert result is not None
    assert (result.generation, result.published_at) == (4, published_at)


@pytest.mark.asyncio
async def test_membership_reader_handles_empty_and_rejects_duplicate_pages(
    monkeypatch,
):
    monkeypatch.setattr(repository_shared, "WRITE_BATCH_SIZE", 2)
    database = SimpleNamespace(all=AsyncMock(return_value=[]))
    empty_count, empty_hash, empty_variants = (
        await repository_shared.persisted_membership_proof(
            database,
            "source-a",
            "version-empty",
        )
    )
    assert (empty_count, empty_hash, empty_variants) == (
        0,
        membership_hash({}),
        {},
    )
    database.all.side_effect = [
        [
            {"upstream_medication_id": "med-a", "variant_hash": "a" * 64},
            {"upstream_medication_id": "med-b", "variant_hash": "b" * 64},
        ],
        [{"upstream_medication_id": "med-b", "variant_hash": "b" * 64}],
    ]
    with pytest.raises(RuntimeError, match="contains duplicates"):
        await repository_shared.persisted_membership_proof(
            database,
            "source-a",
            "version-duplicate",
        )


@pytest.mark.asyncio
async def test_verification_transition_rejects_lost_candidate():
    database = SimpleNamespace(status=AsyncMock(return_value=0))
    verification = DatasetVerification(
        "source-a",
        _dataset().dataset_id,
        1,
        1,
        1,
        "a" * 64,
        "b" * 64,
    )

    with pytest.raises(RuntimeError, match="verification transition"):
        await repository_verify._mark_verified(
            database,
            _dataset(),
            verification,
        )
