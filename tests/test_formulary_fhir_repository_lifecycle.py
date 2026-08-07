# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Lifecycle, verification, and source-first publication contracts."""

from __future__ import annotations

import datetime as dt
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process.formulary_fhir import repository as repository_module
from process.formulary_fhir import repository_checkpoint
from process.formulary_fhir import repository_publish
from process.formulary_fhir import repository_write
from process.formulary_fhir.repository import AliasRef
from process.formulary_fhir.repository import CheckpointWrite
from process.formulary_fhir.repository import DatasetRef
from process.formulary_fhir.repository import FHIRFormularyRepository
from process.formulary_fhir.repository import PriorAliasState
from process.formulary_fhir.repository_shared import membership_hash


CUTOFF = dt.datetime(2026, 8, 7, 12, tzinfo=dt.UTC)


@asynccontextmanager
async def _transaction(events: list[str] | None = None):
    if events is not None:
        events.append("transaction-enter")
    try:
        yield
    finally:
        if events is not None:
            events.append("transaction-exit")


def _database(*, events=None, **methods):
    return SimpleNamespace(
        transaction=lambda: _transaction(events),
        **methods,
    )


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


def _dataset_row(**overrides):
    values_by_field = {
        "source_id": "source-a",
        "dataset_id": "ffd_" + "a" * 48,
        "run_id": "run-a",
        "previous_dataset_id": None,
        "cutoff_at": CUTOFF,
        "status": "building",
        "publish_requested": False,
        "seed_eligible": False,
        "summary_json": {"acquisition_contract_hash": "c" * 64},
        "list_count": 0,
        "alias_count": 0,
        "medication_count": 0,
        "coverage_hash": None,
        "membership_hash": None,
        "published_at": None,
    }
    values_by_field.update(overrides)
    return values_by_field


def _prior(**overrides) -> PriorAliasState:
    values_by_field = {
        "source_id": "source-a",
        "public_id": _alias().public_id,
        "alias_id": _alias().alias_id,
        "source_plan_identifier": _alias().source_plan_identifier,
        "alias_version_id": "version-a",
        "expected_count": 1,
        "cutoff_at": CUTOFF,
        "variants_by_medication_id": {},
        "membership_hash": membership_hash({"med-1": "a" * 64}),
    }
    values_by_field.update(overrides)
    return PriorAliasState(**values_by_field)


@pytest.mark.asyncio
async def test_reuse_requires_exact_published_predecessor_alias():
    dataset = _dataset(previous_dataset_id="ffd_" + "b" * 48)
    alias = _alias()
    prior = _prior()
    predecessor_by_field = {
        "source_id": "source-a",
        "public_id": alias.public_id,
        "alias_id": alias.alias_id,
        "source_plan_identifier": alias.source_plan_identifier,
        "predecessor_dataset_id": dataset.previous_dataset_id,
        "alias_version_id": prior.alias_version_id,
        "expected_count": prior.expected_count,
        "membership_count": prior.expected_count,
        "membership_hash": prior.membership_hash,
        "cutoff_at": prior.cutoff_at,
    }
    database = SimpleNamespace(first=AsyncMock(return_value=predecessor_by_field))
    await repository_write._require_predecessor_alias(
        database, "source-a", dataset, alias, prior
    )
    database.first.return_value = {
        **predecessor_by_field,
        "alias_version_id": "sibling-version",
    }
    with pytest.raises(RuntimeError, match="predecessor alias"):
        await repository_write._require_predecessor_alias(
            database, "source-a", dataset, alias, prior
        )
    with pytest.raises(RuntimeError, match="requires a predecessor"):
        await repository_write._require_predecessor_alias(
            database, "source-a", _dataset(), alias, prior
        )


@pytest.mark.asyncio
async def test_begin_dataset_locks_source_and_validates_exact_resume(monkeypatch):
    events: list[str] = []
    expected_dataset_id = repository_module.stable_id(
        "ffd_",
        "source-a",
        "run-a",
    )
    database = _database(events=events)
    repository = FHIRFormularyRepository(source_id="source-a", database=database)
    monkeypatch.setattr(
        repository_module,
        "lock_source",
        AsyncMock(side_effect=lambda *_args: events.append("source-lock")),
    )
    monkeypatch.setattr(
        repository_module,
        "_current_dataset_row",
        AsyncMock(return_value={}),
    )
    monkeypatch.setattr(repository_module, "_insert_dataset", AsyncMock(return_value=True))
    monkeypatch.setattr(
        repository_module,
        "_dataset_by_run",
        AsyncMock(return_value=_dataset_row(dataset_id=expected_dataset_id)),
    )
    monkeypatch.setattr(repository_module, "_clear_dataset_error", AsyncMock())
    dataset = await repository.begin_dataset(
        run_id="run-a",
        cutoff_at=CUTOFF,
        acquisition_contract_hash="c" * 64,
    )
    assert dataset.dataset_id == expected_dataset_id
    assert events == ["transaction-enter", "source-lock", "transaction-exit"]
    repository_module._dataset_by_run.return_value = {}
    with pytest.raises(RuntimeError, match="run identity collision"):
        await repository.begin_dataset(
            run_id="run-a",
            cutoff_at=CUTOFF,
            acquisition_contract_hash="c" * 64,
        )


def test_resumed_dataset_validation_rejects_identity_parameters_and_state():
    dataset = _dataset()
    repository_module._validate_resumed_dataset(
        dataset,
        expected_dataset_id=dataset.dataset_id,
        cutoff_at=CUTOFF,
        intent="none",
        acquisition_contract_hash="c" * 64,
    )
    cases = (
        (_dataset(), "other", CUTOFF, "none", "c" * 64, "identity collision"),
        (_dataset(), _dataset().dataset_id, CUTOFF, "seed", "c" * 64, "parameters"),
        (
            _dataset(status="published"),
            _dataset().dataset_id,
            CUTOFF,
            "none",
            "c" * 64,
            "not resumable",
        ),
    )
    for candidate, dataset_id, cutoff, intent, contract_hash, message in cases:
        with pytest.raises(RuntimeError, match=message):
            repository_module._validate_resumed_dataset(
                candidate,
                expected_dataset_id=dataset_id,
                cutoff_at=cutoff,
                intent=intent,
                acquisition_contract_hash=contract_hash,
            )


@pytest.mark.asyncio
async def test_current_snapshot_and_prior_membership_load_paths(monkeypatch):
    database = _database()
    repository = FHIRFormularyRepository(source_id="source-a", database=database)
    monkeypatch.setattr(
        repository_module,
        "_current_dataset_row",
        AsyncMock(return_value={}),
    )
    assert (await repository.current_snapshot()).dataset is None
    repository_module._current_dataset_row.return_value = _dataset_row(
        status="verified"
    )
    with pytest.raises(RuntimeError, match="not published"):
        await repository.current_snapshot()
    repository_module._current_dataset_row.return_value = _dataset_row(
        status="published"
    )
    monkeypatch.setattr(
        repository_module,
        "snapshot_alias_rows",
        AsyncMock(
            return_value=[
                {
                    "public_id": _alias().public_id,
                    "source_plan_identifier": _alias().source_plan_identifier,
                    "alias_id": _alias().alias_id,
                    "alias_version_id": "version-a",
                    "expected_count": 1,
                    "membership_hash": membership_hash({"med-1": "a" * 64}),
                    "cutoff_at": CUTOFF,
                }
            ]
        ),
    )
    snapshot = await repository.current_snapshot()
    assert len(snapshot.aliases) == 1
    loaded = _prior(variants_by_medication_id={"med-1": "a" * 64})
    assert await repository.load_prior_alias_state(loaded) is loaded
    with pytest.raises(RuntimeError, match="source is invalid"):
        await repository.load_prior_alias_state(_prior(source_id="source-b"))
    with pytest.raises(RuntimeError, match="membership is invalid"):
        await repository.load_prior_alias_state(
            _prior(variants_by_medication_id={"med-1": "b" * 64})
        )
    monkeypatch.setattr(
        repository_module,
        "persisted_membership_proof",
        AsyncMock(return_value=(0, membership_hash({}), {})),
    )
    with pytest.raises(RuntimeError, match="membership is incomplete"):
        await repository.load_prior_alias_state(_prior())


@pytest.mark.asyncio
async def test_fail_and_interrupt_store_only_exception_type(monkeypatch):
    status = AsyncMock(return_value=1)
    repository = FHIRFormularyRepository(
        source_id="source-a",
        database=_database(status=status),
    )
    monkeypatch.setattr(repository_module, "lock_dataset", AsyncMock())
    error = RuntimeError("https://sensitive.example.invalid/path")
    await repository.fail_dataset(_dataset(), error)
    await repository.interrupt_dataset(_dataset(), error)
    assert "sensitive" not in status.await_args_list[0].kwargs["error_json"]
    assert "resumable" in status.await_args_list[1].kwargs["error_json"]
    status.return_value = 0
    with pytest.raises(RuntimeError, match="failure transition"):
        await repository.fail_dataset(_dataset(), error)
    with pytest.raises(RuntimeError, match="interruption write"):
        await repository.interrupt_dataset(_dataset(), error)


@pytest.mark.asyncio
async def test_checkpoint_mixin_progress_and_restart_contracts(monkeypatch):
    repository = FHIRFormularyRepository(
        source_id="source-a",
        database=_database(),
    )
    progress = CheckpointWrite(
        _dataset(),
        _alias(),
        1,
        "full",
        2,
        1,
        None,
        False,
    )
    monkeypatch.setattr(repository_checkpoint, "lock_dataset", AsyncMock())
    monkeypatch.setattr(repository_checkpoint, "require_alias", AsyncMock())
    monkeypatch.setattr(repository_checkpoint, "save_checkpoint_row", AsyncMock())
    await repository.save_checkpoint(progress)
    repository_checkpoint.save_checkpoint_row.assert_awaited_once()
    completed = CheckpointWrite(
        _dataset(),
        _alias(),
        2,
        "full",
        2,
        2,
        "a" * 64,
        True,
    )
    with pytest.raises(ValueError, match="progress checkpoint"):
        await repository.save_checkpoint(completed)
    with pytest.raises(RuntimeError, match="restart source"):
        await repository.completed_alias_checkpoint(
            dataset=_dataset(source_id="source-b"),
            alias=_alias(source_id="source-b"),
        )
    monkeypatch.setattr(
        repository_checkpoint,
        "completed_checkpoint",
        AsyncMock(return_value=None),
    )
    assert (
        await repository.completed_alias_checkpoint(
            dataset=_dataset(),
            alias=_alias(),
        )
        is None
    )


@pytest.mark.asyncio
async def test_publication_sql_helpers_enforce_exact_row_counts():
    published_at = CUTOFF
    database = SimpleNamespace(
        first=AsyncMock(
            side_effect=[
                {
                    "dataset_id": "old",
                    "generation": 1,
                    "published_at": published_at,
                    "cutoff_at": CUTOFF,
                },
                {
                    "dataset_id": _dataset().dataset_id,
                    "generation": 1,
                    "published_at": published_at,
                },
                {},
            ]
        ),
        status=AsyncMock(side_effect=[1, 0]),
    )
    current = await repository_publish._locked_current(database, "source-a")
    assert current["generation"] == 1
    initial = await repository_publish._insert_initial_pointer(
        database,
        "source-a",
        _dataset().dataset_id,
    )
    assert initial["generation"] == 1
    with pytest.raises(RuntimeError, match="compare-and-switch"):
        await repository_publish._advance_pointer(
            database,
            "source-a",
            _dataset().dataset_id,
            {"dataset_id": "old", "generation": 1},
        )
    await repository_publish._mark_published(
        database,
        "source-a",
        _dataset().dataset_id,
        published_at,
    )
    with pytest.raises(RuntimeError, match="transition failed"):
        await repository_publish._mark_published(
            database,
            "source-a",
            _dataset().dataset_id,
            published_at,
        )


@pytest.mark.asyncio
async def test_publication_locks_source_before_dataset_and_pointer(monkeypatch):
    events: list[str] = []
    database = _database(events=events)
    dataset = _dataset(intent="requested", status="verified")
    monkeypatch.setattr(
        repository_publish,
        "lock_source",
        AsyncMock(side_effect=lambda *_args: events.append("source-lock")),
    )
    monkeypatch.setattr(
        repository_publish,
        "lock_dataset",
        AsyncMock(
            side_effect=lambda *_args, **_kwargs: (
                events.append("dataset-lock"),
                _dataset_row(status="verified", publish_requested=True),
            )[1]
        ),
    )
    monkeypatch.setattr(
        repository_publish,
        "_locked_current",
        AsyncMock(side_effect=lambda *_args: (events.append("pointer-lock"), {})[1]),
    )
    monkeypatch.setattr(
        repository_publish,
        "_switch_pointer",
        AsyncMock(
            side_effect=lambda *_args: (
                events.append("pointer-switch"),
                {"generation": 1, "published_at": CUTOFF},
            )[1]
        ),
    )
    monkeypatch.setattr(
        repository_publish,
        "_mark_published",
        AsyncMock(side_effect=lambda *_args: events.append("dataset-published")),
    )
    publication_result = await repository_publish._publish(
        database,
        "source-a",
        dataset,
        seed_proof=False,
    )
    assert publication_result.generation == 1
    assert events == [
        "transaction-enter",
        "source-lock",
        "dataset-lock",
        "pointer-lock",
        "pointer-switch",
        "dataset-published",
        "transaction-exit",
    ]
