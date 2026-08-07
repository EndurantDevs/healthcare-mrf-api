# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Defensive branch tests for verify-only formulary synchronization."""

from __future__ import annotations

import asyncio
import datetime as dt
from dataclasses import replace

import asyncpg
import pytest
from sqlalchemy import exc as sqlalchemy_error

import process.formulary_fhir.synchronizer as sync_module
from process.formulary_fhir.repository import AliasCompletionFence
from process.formulary_fhir.repository import AliasRef
from process.formulary_fhir.repository import AliasVersionResult
from process.formulary_fhir.repository import CoveragePlanWriteResult
from process.formulary_fhir.repository import DatasetRef
from process.formulary_fhir.repository import PriorAliasState
from process.formulary_fhir.repository_checkpoint import next_checkpoint_fence
from process.formulary_fhir.planner import plan_alias_census
from process.formulary_fhir.planner import plan_coverage_census
from process.formulary_fhir.planner import AliasCensusPlan
from process.formulary_fhir.parser import parse_medication_knowledge
from process.formulary_fhir.repository_shared import medication_variant_hash
from process.formulary_fhir.repository_shared import membership_hash
from process.formulary_fhir.source import load_enabled_source
from process.formulary_fhir.source import EnabledSourceBinding
from process.formulary_fhir.source import require_source_unchanged
from tests.test_formulary_fhir_synchronizer import _Client
from tests.test_formulary_fhir_synchronizer import _Repository
from tests.test_formulary_fhir_synchronizer import _SourceDatabase
from tests.test_formulary_fhir_synchronizer import _run
from tests.test_formulary_fhir_synchronizer import _medication_resource
from tests.test_formulary_fhir_synchronizer import _published_snapshot
from tests.test_formulary_fhir_planner import CUTOFF
from tests.test_formulary_fhir_planner import _binding
from tests.test_formulary_fhir_planner import _coverage_census
from tests.test_formulary_fhir_planner import _medication_census
from tests.test_formulary_fhir_planner import _one_alias_plan


class _BlockingClient(_Client):
    def __init__(self, events: list[str]) -> None:
        super().__init__(None, events)
        self.medication_started = asyncio.Event()
        self.release_medication = asyncio.Event()

    async def medication_current_census(self, alias, *, cutoff):
        self.events.append("medication-http")
        self.medication_alias = alias
        self.medication_cutoff = cutoff
        self.medication_started.set()
        await self.release_medication.wait()
        raise AssertionError("cancelled medication request continued")


@pytest.mark.asyncio
async def test_active_task_cancellation_shields_interruption(monkeypatch):
    events: list[str] = []
    repository = _Repository(events)
    client = _BlockingClient(events)
    synchronization_task = asyncio.create_task(
        _run(monkeypatch, repository, _SourceDatabase(events), client)
    )
    await client.medication_started.wait()

    synchronization_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await synchronization_task

    assert isinstance(repository.interrupted_with, asyncio.CancelledError)
    assert "interrupt" in events
    assert events.count("client-exit") == 1


@pytest.mark.parametrize(
    "database_error",
    [
        asyncpg.SerializationError("retry transaction"),
        asyncpg.DeadlockDetectedError("retry transaction"),
        asyncpg.ConnectionDoesNotExistError("retry connection"),
        sqlalchemy_error.TimeoutError("retry pool"),
        sqlalchemy_error.OperationalError("SELECT 1", {}, Exception("retry")),
    ],
)
def test_transient_database_failures_are_resumable(database_error):
    assert sync_module._is_resumable(database_error) is True


def test_nontransient_database_failure_is_terminal():
    database_error = sqlalchemy_error.IntegrityError(
        "INSERT",
        {},
        Exception("constraint"),
    )

    assert sync_module._is_resumable(database_error) is False


def test_completion_fence_rejects_invalid_contracts():
    with pytest.raises(ValueError, match="completion fence"):
        AliasCompletionFence(0, None)
    with pytest.raises(ValueError, match="acquisition mode"):
        AliasCompletionFence(1, "reuse")


@pytest.mark.asyncio
async def test_source_recheck_rejects_nonbinding_value():
    with pytest.raises(Exception, match="source configuration"):
        await require_source_unchanged(object(), database=object())


def test_enabled_binding_rejects_nonconfiguration_value():
    with pytest.raises(ValueError, match="source binding"):
        EnabledSourceBinding("source-alpha", object(), "a" * 64)


@pytest.mark.asyncio
async def test_source_loader_rejects_invalid_identity_before_database_access():
    with pytest.raises(Exception, match="source configuration"):
        await load_enabled_source(" ", database=object())


def test_planner_rejects_forged_prior_and_search_plan():
    binding = _binding()
    coverage_plan = plan_coverage_census(
        binding,
        _coverage_census((_one_alias_plan(),)),
        CUTOFF,
    )
    work = coverage_plan.work_items[0]
    census = _medication_census("SYNTH-A", (_medication_resource(),))
    alias_plan = plan_alias_census(binding, work, census, CUTOFF, None)
    forged_prior = PriorAliasState(
        "source-beta",
        work.plan.public_id,
        "ffa_" + "1" * 48,
        work.source_plan_identifier,
        "ffav_" + "2" * 48,
        alias_plan.expected_count,
        CUTOFF - dt.timedelta(days=1),
        {},
        alias_plan.membership_hash,
    )

    assert "aliases=1" in repr(coverage_plan)
    assert "mode='full'" in repr(alias_plan)
    with pytest.raises(RuntimeError, match="ownership"):
        plan_alias_census(binding, work, census, CUTOFF, forged_prior)
    with pytest.raises(RuntimeError, match="search plan"):
        plan_alias_census(
            binding,
            replace(work, search_contract_hash="b" * 64),
            census,
            CUTOFF,
            None,
        )


def _checkpoint_contracts():
    dataset = DatasetRef(
        "source-alpha",
        "ffd_" + "1" * 48,
        "synthetic-run",
        None,
        CUTOFF,
        "a" * 64,
        "none",
        "building",
    )
    alias = AliasRef(
        "source-alpha",
        "fhir_" + "a" * 26,
        "ffa_" + "2" * 48,
        "SYNTH-A",
    )
    checkpoint_by_field = {
        "source_id": dataset.source_id,
        "alias_id": alias.alias_id,
        "source_plan_identifier": alias.source_plan_identifier,
        "run_id": dataset.run_id,
        "dataset_id": dataset.dataset_id,
        "fence_token": 7,
        "cutoff_at": dataset.cutoff_at,
        "acquisition_mode": "full",
        "completed": False,
    }
    return dataset, alias, checkpoint_by_field


class _CheckpointDatabase:
    def __init__(self, checkpoint_by_field) -> None:
        self.checkpoint_by_field = checkpoint_by_field
        self.statement = ""
        self.params_by_name = None

    async def first(self, statement, **params_by_name):
        self.statement = statement
        self.params_by_name = params_by_name
        return self.checkpoint_by_field


@pytest.mark.asyncio
async def test_next_checkpoint_fence_handles_absent_exact_and_invalid_rows():
    dataset, alias, checkpoint_by_field = _checkpoint_contracts()
    absent_database = _CheckpointDatabase(None)
    exact_database = _CheckpointDatabase(checkpoint_by_field)
    absent = await next_checkpoint_fence(
        absent_database,
        dataset.source_id,
        dataset,
        alias,
    )
    exact = await next_checkpoint_fence(
        exact_database,
        dataset.source_id,
        dataset,
        alias,
    )
    assert absent == AliasCompletionFence(1, None)
    assert exact == AliasCompletionFence(8, "full")
    assert exact_database.params_by_name == {
        "source_id": dataset.source_id,
        "alias_id": alias.alias_id,
        "run_id": dataset.run_id,
    }
    assert all(
        f":{parameter_name}" in exact_database.statement
        for parameter_name in exact_database.params_by_name
    )

    with pytest.raises(RuntimeError, match="inconsistent"):
        await next_checkpoint_fence(
            _CheckpointDatabase({**checkpoint_by_field, "completed": True}),
            dataset.source_id,
            dataset,
            alias,
        )
    with pytest.raises(RuntimeError, match="fence"):
        await next_checkpoint_fence(
            _CheckpointDatabase({**checkpoint_by_field, "fence_token": 0}),
            dataset.source_id,
            dataset,
            alias,
        )


@pytest.mark.asyncio
async def test_incomplete_full_restart_forces_full_materialization(monkeypatch):
    medication = parse_medication_knowledge(_medication_resource())
    prior_hash = membership_hash(
        {medication.upstream_medication_id: medication_variant_hash(medication)}
    )
    events: list[str] = []
    repository = _Repository(
        events,
        current=_published_snapshot(membership_hash_value=prior_hash),
        next_fence_token=2,
    )

    synchronization_result, _client = await _run(
        monkeypatch,
        repository,
        _SourceDatabase(events),
    )

    assert synchronization_result.full_aliases == 1
    assert synchronization_result.reused_aliases == 0
    assert "put-full" in events and "put-reuse" not in events


@pytest.mark.asyncio
async def test_invalid_client_metric_fails_candidate(monkeypatch):
    events: list[str] = []
    repository = _Repository(events)
    client = _Client(None, events)
    client.throttle_count = -1

    with pytest.raises(RuntimeError, match="metrics"):
        await _run(monkeypatch, repository, _SourceDatabase(events), client)

    assert isinstance(repository.failed_with, RuntimeError)


@pytest.mark.asyncio
async def test_verified_replay_rechecks_source_before_verification(monkeypatch):
    events: list[str] = []
    repository = _Repository(events, dataset_status="verified")

    with pytest.raises(Exception, match="changed"):
        await _run(
            monkeypatch,
            repository,
            _SourceDatabase(events, drift_on_read=3),
        )

    assert "verify" not in events
    assert repository.failed_with is not None


def _candidate_dataset() -> DatasetRef:
    return DatasetRef(
        "source-alpha",
        "ffd_" + "1" * 48,
        "synthetic-run",
        None,
        CUTOFF,
        "a" * 64,
        "none",
        "building",
    )


class _CoverageRepository:
    def __init__(self, write_result: CoveragePlanWriteResult) -> None:
        self.write_result = write_result

    async def put_coverage_plan(self, **_values):
        return self.write_result


@pytest.mark.asyncio
async def test_coverage_write_defenses_reject_mismatched_repository_results():
    coverage_plan = plan_coverage_census(
        _binding(),
        _coverage_census((_one_alias_plan(),)),
        CUTOFF,
    )
    dataset = _candidate_dataset()
    plan = coverage_plan.plans[0]
    alias = AliasRef(
        dataset.source_id,
        plan.public_id,
        "ffa_" + "2" * 48,
        plan.source_plan_identifiers[0],
    )
    wrong_dataset = replace(dataset, dataset_id="ffd_" + "9" * 48)
    with pytest.raises(RuntimeError, match="changed the dataset"):
        await sync_module._persist_coverage_plans(
            _CoverageRepository(CoveragePlanWriteResult(wrong_dataset, "v1", (alias,))),
            dataset,
            coverage_plan,
        )
    with pytest.raises(RuntimeError, match="aliases"):
        await sync_module._persist_coverage_plans(
            _CoverageRepository(CoveragePlanWriteResult(dataset, "v1", ())),
            dataset,
            coverage_plan,
        )
    with pytest.raises(RuntimeError, match="work is incomplete"):
        await sync_module._persist_coverage_plans(
            _CoverageRepository(CoveragePlanWriteResult(dataset, "v1", (alias,))),
            dataset,
            replace(coverage_plan, work_items=()),
        )


@pytest.mark.asyncio
async def test_alias_write_defenses_reject_missing_prior_and_wrong_result():
    coverage_plan = plan_coverage_census(
        _binding(),
        _coverage_census((_one_alias_plan(),)),
        CUTOFF,
    )
    dataset = _candidate_dataset()
    work = coverage_plan.work_items[0]
    alias = AliasRef(
        dataset.source_id,
        work.plan.public_id,
        "ffa_" + "2" * 48,
        work.source_plan_identifier,
    )
    work_item = sync_module._AliasWorkItem(work, alias)
    alias_plan = AliasCensusPlan((), 0, membership_hash({}), "reuse")
    with pytest.raises(RuntimeError, match="no predecessor"):
        await sync_module._write_alias_plan(
            object(), dataset, work_item, alias_plan, None, 1
        )

    wrong_result = AliasVersionResult(
        dataset.source_id,
        dataset.dataset_id,
        alias.alias_id,
        "ffav_" + "3" * 48,
        0,
        "b" * 64,
        "full",
    )
    with pytest.raises(RuntimeError, match="write result"):
        sync_module._require_alias_result(
            wrong_result,
            dataset,
            alias,
            replace(alias_plan, mode="full"),
        )


@pytest.mark.asyncio
async def test_lifecycle_shield_drains_after_repeated_cancellation():
    lifecycle_started = asyncio.Event()
    release_lifecycle = asyncio.Event()

    async def lifecycle_update():
        lifecycle_started.set()
        await release_lifecycle.wait()

    shield_task = asyncio.create_task(
        sync_module._shield_lifecycle(lifecycle_update())
    )
    await lifecycle_started.wait()
    shield_task.cancel()
    await asyncio.sleep(0)
    release_lifecycle.set()
    await shield_task
