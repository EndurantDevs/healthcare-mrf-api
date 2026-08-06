# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

from contextlib import asynccontextmanager
from unittest.mock import AsyncMock

import pytest

from process.formulary_fhir import repository as repository_module
from process.formulary_fhir import repository_publish as publish_module
from process.formulary_fhir.repository import FHIRFormularyRepository


@asynccontextmanager
async def _transaction():
    yield


def _coverage_row(
    alias: str,
    *,
    dataset_alias_version_id: str | None,
) -> dict:
    return {
        "public_id": "fhir_abcdefghijklmnopqrstuvwxyz",
        "canonical_identity": "https://fhir.example.invalid/r4/List/list-a",
        "content_hash": "c" * 64,
        "metadata_json": {
            "source_plan_identifiers": ["CURRENT-ALIAS"],
        },
        "alias_id": f"alias-{alias}",
        "dataset_alias_version_id": dataset_alias_version_id,
        "source_plan_identifier": alias,
        "expected_count": 1 if dataset_alias_version_id else None,
        "membership_count": 1 if dataset_alias_version_id else None,
        "membership_hash": "m" * 64 if dataset_alias_version_id else None,
    }


@pytest.mark.asyncio
async def test_verification_ignores_removed_historical_aliases(monkeypatch):
    rows = [
        _coverage_row(
            "CURRENT-ALIAS",
            dataset_alias_version_id="alias-version-current",
        ),
        _coverage_row("REMOVED-ALIAS", dataset_alias_version_id=None),
    ]
    all_rows = AsyncMock(return_value=rows)
    status = AsyncMock(return_value=1)
    monkeypatch.setattr(repository_module.db, "all", all_rows)
    monkeypatch.setattr(repository_module.db, "status", status)

    summary = await FHIRFormularyRepository().verify_dataset("candidate")

    assert summary["list_count"] == 1
    assert summary["alias_count"] == 1
    assert summary["medication_membership_count"] == 1
    assert "fhir_formulary_coverage_plan_version" in all_rows.await_args.args[0]
    assert status.await_count == 1


@pytest.mark.asyncio
async def test_verification_rejects_missing_current_alias(monkeypatch):
    rows = [_coverage_row("CURRENT-ALIAS", dataset_alias_version_id=None)]
    monkeypatch.setattr(
        repository_module.db,
        "all",
        AsyncMock(return_value=rows),
    )
    status = AsyncMock(return_value=1)
    monkeypatch.setattr(repository_module.db, "status", status)

    with pytest.raises(RuntimeError, match="List-to-alias coverage"):
        await FHIRFormularyRepository().verify_dataset("candidate")

    status.assert_not_awaited()


@pytest.mark.asyncio
async def test_current_snapshot_loads_only_one_alias_membership_on_demand(
    monkeypatch,
):
    first = AsyncMock(
        return_value={
            "dataset_id": "published-dataset",
            "cutoff_at": "2026-08-05T12:00:00Z",
        }
    )
    all_rows = AsyncMock(
        side_effect=[
            [
                {
                    "public_id": "fhir_abcdefghijklmnopqrstuvwxyz",
                    "source_plan_identifier": "SYNTHETIC-PLAN",
                    "alias_id": "alias-a",
                    "alias_version_id": "alias-version-a",
                    "expected_count": 2,
                    "membership_hash": "c" * 64,
                    "cutoff_at": "2026-08-05T12:00:00Z",
                }
            ],
            [
                {"upstream_medication_id": "MI-a", "variant_hash": "a" * 64},
                {"upstream_medication_id": "MI-b", "variant_hash": "b" * 64},
            ],
        ]
    )
    monkeypatch.setattr(repository_module.db, "first", first)
    monkeypatch.setattr(repository_module.db, "all", all_rows)
    repository = FHIRFormularyRepository()

    snapshot = await repository.current_snapshot()
    prior = snapshot.aliases[("fhir_abcdefghijklmnopqrstuvwxyz", "SYNTHETIC-PLAN")]
    assert prior.variants_by_medication_id == {}
    assert prior.membership_hash_value == "c" * 64
    assert "fhir_formulary_alias_membership" not in all_rows.await_args_list[0].args[0]

    loaded = await repository.load_prior_alias_state(prior)
    assert loaded.variants_by_medication_id == {
        "MI-a": "a" * 64,
        "MI-b": "b" * 64,
    }
    assert "fhir_formulary_alias_membership" in all_rows.await_args_list[1].args[0]


@pytest.mark.asyncio
async def test_verified_manual_seed_publishes_only_into_an_empty_pointer(
    monkeypatch,
):
    first = AsyncMock(
        side_effect=[
            {
                "source_id": repository_module.SOURCE_ID,
                "status": "verified",
                "publish_requested": False,
                "seed_eligible": True,
            },
            None,
        ]
    )
    status = AsyncMock(side_effect=[1, 1])
    monkeypatch.setattr(publish_module.db, "transaction", _transaction)
    monkeypatch.setattr(publish_module.db, "first", first)
    monkeypatch.setattr(publish_module.db, "status", status)

    generation = await FHIRFormularyRepository().publish_verified_seed(
        "ffd_" + "a" * 48
    )

    assert generation == 1
    assert "FOR UPDATE" in first.await_args_list[0].args[0]
    assert "fhir_formulary_current" in first.await_args_list[1].args[0]
    assert "fhir_formulary_current" in status.await_args_list[0].args[0]
    assert "status = 'published'" in status.await_args_list[1].args[0]


@pytest.mark.asyncio
async def test_seed_publication_rejects_an_existing_pointer_without_mutation(
    monkeypatch,
):
    first = AsyncMock(
        side_effect=[
            {
                "source_id": repository_module.SOURCE_ID,
                "status": "verified",
                "publish_requested": False,
                "seed_eligible": True,
            },
            {"dataset_id": "already-published", "generation": 1},
        ]
    )
    status = AsyncMock()
    monkeypatch.setattr(publish_module.db, "transaction", _transaction)
    monkeypatch.setattr(publish_module.db, "first", first)
    monkeypatch.setattr(publish_module.db, "status", status)

    with pytest.raises(RuntimeError, match="requires an empty pointer"):
        await FHIRFormularyRepository().publish_verified_seed("ffd_" + "a" * 48)

    status.assert_not_awaited()


@pytest.mark.asyncio
async def test_seed_publication_rejects_an_ordinary_verified_dataset(
    monkeypatch,
):
    first = AsyncMock(
        return_value={
            "source_id": repository_module.SOURCE_ID,
            "status": "verified",
            "publish_requested": False,
            "seed_eligible": False,
        }
    )
    status = AsyncMock()
    monkeypatch.setattr(publish_module.db, "transaction", _transaction)
    monkeypatch.setattr(publish_module.db, "first", first)
    monkeypatch.setattr(publish_module.db, "status", status)

    with pytest.raises(RuntimeError, match="not publishable"):
        await FHIRFormularyRepository().publish_verified_seed("ffd_" + "a" * 48)

    assert first.await_count == 1
    status.assert_not_awaited()
