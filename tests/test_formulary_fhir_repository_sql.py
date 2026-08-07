# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Bounded SQL orchestration contracts for dormant formulary persistence."""

from __future__ import annotations

import datetime as dt
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process.formulary_fhir import repository_batch
from process.formulary_fhir import repository_checkpoint
from process.formulary_fhir import repository_coverage
from process.formulary_fhir import repository_write
from process.formulary_fhir.repository import AliasRef
from process.formulary_fhir.repository import AliasVersionWrite
from process.formulary_fhir.repository import CheckpointWrite
from process.formulary_fhir.repository import DatasetRef
from process.formulary_fhir.repository import FHIRFormularyRepository
from process.formulary_fhir.repository import PriorAliasState
from process.formulary_fhir.types import CoveragePlanRecord, MedicationRecord


CUTOFF = dt.datetime(2026, 8, 7, 12, tzinfo=dt.UTC)


@asynccontextmanager
async def _transaction():
    yield


def _dataset(**overrides) -> DatasetRef:
    values_by_field = {
        "source_id": "source-a",
        "dataset_id": "ffd_" + "a" * 48,
        "run_id": "run-a",
        "previous_dataset_id": None,
        "cutoff_at": CUTOFF,
        "acquisition_contract_hash": "c" * 64,
        "intent": "none",
        "status": "building",
    }
    values_by_field.update(overrides)
    return DatasetRef(**values_by_field)


def _alias(**overrides) -> AliasRef:
    values_by_field = {
        "source_id": "source-a",
        "public_id": "fhir_" + "a" * 26,
        "alias_id": "ffa_" + "a" * 48,
        "source_plan_identifier": "SYNTHETIC-PLAN",
    }
    values_by_field.update(overrides)
    return AliasRef(**values_by_field)


def _medication(index: int = 1, **overrides) -> MedicationRecord:
    values_by_field = {
        "upstream_medication_id": f"med-{index}",
        "upstream_version_id": "1",
        "upstream_last_updated": CUTOFF,
        "status": "active",
        "drug_name": "Synthetic medication",
        "rxnorm_id": str(index),
        "ndc11": None,
        "codings": (),
        "raw_extensions": (),
        "source_plan_identifiers": ("SYNTHETIC-PLAN",),
        "drug_tier": "preferred",
        "prior_authorization": False,
        "step_therapy": False,
        "quantity_limit": False,
        "alternative_references": (),
        "content_hash": f"{index:064x}",
    }
    values_by_field.update(overrides)
    return MedicationRecord(**values_by_field)


def _plan() -> CoveragePlanRecord:
    return CoveragePlanRecord(
        upstream_list_id="list-a",
        public_id="fhir_" + "a" * 26,
        canonical_identity="https://a.example.invalid/fhir/List/list-a",
        upstream_version_id="1",
        upstream_last_updated=CUTOFF,
        status="current",
        title="Synthetic plan",
        name="Synthetic",
        upstream_date=CUTOFF,
        period_start=None,
        period_end=None,
        source_plan_identifiers=("SYNTHETIC-PLAN",),
        raw_identifiers=(),
        raw_extensions=(),
        content_hash="a" * 64,
    )


@pytest.mark.asyncio
async def test_medication_membership_and_alternative_batches_execute(monkeypatch):
    medications = (_medication(1), _medication(2))
    variants_by_id = {
        medication.upstream_medication_id: "a" * 64
        for medication in medications
    }
    database = SimpleNamespace(status=AsyncMock(return_value=2))
    monkeypatch.setattr(
        repository_batch,
        "_assert_medication_batch",
        AsyncMock(),
    )
    await repository_batch._insert_medications(database, "source-a", medications)
    await repository_batch._insert_memberships(
        database,
        "source-a",
        "version-a",
        medications,
        variants_by_id,
    )
    evidence_medication = _medication(
        1,
        alternative_references=("MedicationKnowledge/med-2",),
    )
    evidence_rows = repository_batch._alternative_rows(
        (evidence_medication, medications[1]),
        {"med-1", "med-2"},
    )
    values_sql, params_by_name = repository_batch._alternative_values(
        "version-a",
        evidence_rows,
    )
    assert values_sql and params_by_name["resolved_0"] is True
    await repository_batch._insert_alternatives(
        database,
        "version-a",
        evidence_rows,
    )
    assert database.status.await_count == 3


@pytest.mark.asyncio
async def test_insert_alias_content_orders_membership_before_alternatives(monkeypatch):
    events: list[str] = []

    monkeypatch.setattr(
        repository_batch,
        "_insert_medications",
        AsyncMock(side_effect=lambda *_args: events.append("medications")),
    )
    monkeypatch.setattr(
        repository_batch,
        "_insert_memberships",
        AsyncMock(side_effect=lambda *_args: events.append("memberships")),
    )
    monkeypatch.setattr(
        repository_batch,
        "_insert_alternatives",
        AsyncMock(side_effect=lambda *_args: events.append("alternatives")),
    )
    monkeypatch.setattr(repository_batch, "_assert_alternatives", AsyncMock())
    medication = _medication()
    await repository_batch.insert_alias_content(
        object(),
        "source-a",
        "version-a",
        {medication.upstream_medication_id: medication},
        {medication.upstream_medication_id: "a" * 64},
    )
    assert events == ["medications", "memberships", "alternatives"]


@pytest.mark.asyncio
async def test_alias_lookup_and_checkpoint_update_insert_paths(monkeypatch):
    alias = _alias()
    database = SimpleNamespace(
        first=AsyncMock(
            return_value={
                "source_id": alias.source_id,
                "public_id": alias.public_id,
                "alias_id": alias.alias_id,
                "source_plan_identifier": alias.source_plan_identifier,
            }
        ),
        status=AsyncMock(side_effect=[1, 0, 1]),
    )
    assert (
        await repository_checkpoint.require_alias(database, "source-a", alias)
    )["alias_id"] == alias.alias_id
    with pytest.raises(RuntimeError, match="source is inconsistent"):
        await repository_checkpoint.require_alias(database, "source-b", alias)
    database.first.return_value = None
    with pytest.raises(RuntimeError, match="ownership is invalid"):
        await repository_checkpoint.require_alias(database, "source-a", alias)
    params_by_name = {
        "source_id": "source-a",
        "alias_id": alias.alias_id,
        "source_plan_identifier": alias.source_plan_identifier,
        "run_id": "run-a",
        "dataset_id": _dataset().dataset_id,
        "fence_token": 1,
        "cutoff_at": CUTOFF,
        "acquisition_mode": "full",
        "expected_count": 1,
        "processed_count": 0,
        "membership_hash": None,
        "completed": False,
    }
    assert (
        await repository_checkpoint._is_checkpoint_updated(
            database,
            params_by_name,
        )
        is True
    )
    assert (
        await repository_checkpoint._is_checkpoint_updated(
            database,
            params_by_name,
        )
        is False
    )
    assert (
        await repository_checkpoint._is_checkpoint_inserted(
            database,
            params_by_name,
        )
        is True
    )


@pytest.mark.asyncio
async def test_checkpoint_save_and_completed_readback_succeed(monkeypatch):
    checkpoint = CheckpointWrite(
        _dataset(),
        _alias(),
        1,
        "full",
        1,
        0,
        None,
        False,
    )
    monkeypatch.setattr(
        repository_checkpoint,
        "_is_checkpoint_updated",
        AsyncMock(return_value=True),
    )
    monkeypatch.setattr(
        repository_checkpoint,
        "_assert_checkpoint_write",
        AsyncMock(),
    )
    await repository_checkpoint.save_checkpoint_row(
        object(),
        "source-a",
        checkpoint,
    )
    repository_checkpoint._assert_checkpoint_write.assert_awaited_once()
    database = SimpleNamespace(
        first=AsyncMock(
            return_value={
                "expected_count": 1,
                "processed_count": 1,
                "membership_count": 1,
                "membership_hash": "a" * 64,
                "alias_membership_hash": "a" * 64,
                "alias_version_id": "version-a",
                "acquisition_mode": "full",
            }
        )
    )
    monkeypatch.setattr(
        repository_checkpoint,
        "persisted_membership_proof",
        AsyncMock(return_value=(1, "a" * 64, {"med-1": "b" * 64})),
    )
    completed_alias = await repository_checkpoint.completed_checkpoint(
        database,
        "source-a",
        _dataset(),
        _alias(),
    )
    assert completed_alias is not None
    assert completed_alias.alias_version_id == "version-a"
    assert database.first.await_args.kwargs["public_id"] == _alias().public_id


@pytest.mark.asyncio
async def test_coverage_helpers_validate_exact_readbacks(monkeypatch):
    plan = _plan()
    identity_by_field = {
        "source_id": "source-a",
        "public_id": plan.public_id,
        "upstream_list_id": plan.upstream_list_id,
        "canonical_identity": plan.canonical_identity,
    }
    version_by_field = {
        "coverage_version_id": "version-a",
        "content_hash": "a" * 64,
    }
    link_by_field = {
        "source_id": "source-a",
        "coverage_version_id": "version-a",
    }
    database = SimpleNamespace(
        status=AsyncMock(return_value=1),
        first=AsyncMock(
            side_effect=[identity_by_field, version_by_field, link_by_field]
        ),
    )
    await repository_coverage._insert_identity(database, "source-a", plan)
    await repository_coverage._insert_version(
        database,
        "source-a",
        "version-a",
        plan,
    )
    await repository_coverage._link_version(
        database,
        "source-a",
        _dataset(),
        plan,
        "version-a",
    )
    monkeypatch.setattr(repository_coverage, "require_alias", AsyncMock())
    aliases = await repository_coverage._put_aliases(database, "source-a", plan)
    assert aliases[0].source_plan_identifier == "SYNTHETIC-PLAN"


@pytest.mark.asyncio
async def test_put_coverage_plan_orchestrates_one_locked_transaction(monkeypatch):
    events: list[str] = []

    database = SimpleNamespace(transaction=_transaction)
    monkeypatch.setattr(
        repository_coverage,
        "lock_dataset",
        AsyncMock(side_effect=lambda *_args, **_kwargs: events.append("lock")),
    )
    monkeypatch.setattr(
        repository_coverage,
        "_insert_identity",
        AsyncMock(side_effect=lambda *_args: events.append("identity")),
    )
    monkeypatch.setattr(
        repository_coverage,
        "_insert_version",
        AsyncMock(side_effect=lambda *_args: events.append("version")),
    )
    monkeypatch.setattr(
        repository_coverage,
        "_link_version",
        AsyncMock(side_effect=lambda *_args: events.append("link")),
    )
    monkeypatch.setattr(
        repository_coverage,
        "_put_aliases",
        AsyncMock(
            side_effect=lambda *_args: (
                events.append("aliases"),
                (_alias(),),
            )[1]
        ),
    )
    coverage_result = await repository_coverage.put_coverage_plan(
        database,
        "source-a",
        _dataset(),
        _plan(),
    )
    assert events == ["lock", "identity", "version", "link", "aliases"]
    assert coverage_result.aliases == (_alias(),)


@pytest.mark.asyncio
async def test_alias_version_helpers_accept_exact_persisted_rows(monkeypatch):
    write = AliasVersionWrite(_dataset(), _alias(), 1, (_medication(),), 1)
    prepared = repository_write._prepare_alias_version("source-a", write)
    database = SimpleNamespace(
        status=AsyncMock(return_value=1),
        first=AsyncMock(
            side_effect=[
                {
                    "alias_version_id": prepared.alias_version_id,
                    "expected_count": 1,
                    "membership_count": 1,
                    "membership_hash": prepared.membership_hash,
                },
                {
                    "source_id": "source-a",
                    "alias_version_id": prepared.alias_version_id,
                },
            ]
        ),
    )
    version_id = await repository_write._insert_alias_version(
        database,
        "source-a",
        write,
        prepared,
    )
    await repository_write._link_alias_version(
        database,
        "source-a",
        _dataset(),
        _alias(),
        version_id,
    )
    monkeypatch.setattr(
        repository_write,
        "persisted_membership_proof",
        AsyncMock(return_value=(1, prepared.membership_hash, {})),
    )
    await repository_write._assert_persisted_membership(
        database,
        "source-a",
        version_id,
        1,
        prepared.membership_hash,
    )
    repository_write.persisted_membership_proof.return_value = (0, "b" * 64, {})
    with pytest.raises(RuntimeError, match="membership is inconsistent"):
        await repository_write._assert_persisted_membership(
            database,
            "source-a",
            version_id,
            1,
            prepared.membership_hash,
        )


@pytest.mark.asyncio
async def test_write_mixin_full_and_reuse_paths_are_atomic(monkeypatch):
    database = SimpleNamespace(transaction=_transaction)
    repository = FHIRFormularyRepository(source_id="source-a", database=database)
    monkeypatch.setattr(repository_write, "lock_dataset", AsyncMock())
    monkeypatch.setattr(repository_write, "require_alias", AsyncMock())
    monkeypatch.setattr(
        repository_write,
        "_insert_alias_version",
        AsyncMock(return_value="version-a"),
    )
    monkeypatch.setattr(repository_write, "insert_alias_content", AsyncMock())
    monkeypatch.setattr(
        repository_write,
        "_assert_persisted_membership",
        AsyncMock(),
    )
    monkeypatch.setattr(repository_write, "_require_predecessor_alias", AsyncMock())
    monkeypatch.setattr(repository_write, "_link_alias_version", AsyncMock())
    monkeypatch.setattr(repository_write, "save_checkpoint_row", AsyncMock())
    write = AliasVersionWrite(_dataset(), _alias(), 1, (_medication(),), 1)
    full_result = await repository.put_alias_version(write)
    assert full_result.acquisition_mode == "full"
    prepared = repository_write._prepare_alias_version("source-a", write)
    prior = PriorAliasState(
        "source-a",
        _alias().public_id,
        _alias().alias_id,
        _alias().source_plan_identifier,
        "version-a",
        1,
        CUTOFF,
        {},
        prepared.membership_hash,
    )
    reuse_result = await repository.link_reused_alias(
        dataset=_dataset(),
        alias=_alias(),
        prior=prior,
        fence_token=2,
    )
    assert reuse_result.acquisition_mode == "reuse"
    wrong_prior = PriorAliasState(
        "source-a",
        _alias().public_id,
        "other-alias",
        _alias().source_plan_identifier,
        "version-a",
        1,
        CUTOFF,
        {},
        prepared.membership_hash,
    )
    with pytest.raises(RuntimeError, match="prior alias ownership"):
        await repository.link_reused_alias(
            dataset=_dataset(),
            alias=_alias(),
            prior=wrong_prior,
            fence_token=3,
        )
