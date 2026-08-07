# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Disposable PostgreSQL proof for the dormant formulary repository."""

from __future__ import annotations

import asyncio
import datetime as dt
import uuid

import pytest
from sqlalchemy.ext.asyncio import create_async_engine

from db.models import db
from process.formulary_fhir.repository import AliasRef, AliasVersionWrite
from process.formulary_fhir.repository import CheckpointWrite
from process.formulary_fhir.repository import FHIRFormularyRepository
from process.formulary_fhir.repository import PriorAliasState
from process.formulary_fhir.repository_shared import table_name
from process.formulary_fhir.types import CoveragePlanRecord, MedicationRecord
from tests.test_formulary_fhir_storage_postgres import _connect
from tests.test_formulary_fhir_storage_postgres import _database_url
from tests.test_formulary_fhir_storage_postgres import _drop_schema
from tests.test_formulary_fhir_storage_postgres import _load_migration
from tests.test_formulary_fhir_storage_postgres import _quoted
from tests.test_formulary_fhir_storage_postgres import _run_migration_action


ALIAS_SIZE = 1_001
SOURCE_A = "source-a"
SOURCE_B = "source-b"
PLAN_A = "fhir_" + "a" * 26
PLAN_B = "fhir_" + "b" * 26
PLAN_IDENTIFIER = "SYNTHETIC-PLAN"
CUTOFF = dt.datetime(2026, 8, 7, 12, tzinfo=dt.UTC)


class _RollbackProof(Exception):
    """Roll back all repository mutations after assertions complete."""


def _configure_database(monkeypatch, database_url, schema_name: str) -> None:
    monkeypatch.setenv("HLTHPRT_DB_DRIVER", "asyncpg")
    monkeypatch.setenv("HLTHPRT_DB_HOST", str(database_url.host))
    monkeypatch.setenv("HLTHPRT_DB_PORT", str(database_url.port or 5432))
    monkeypatch.setenv("HLTHPRT_DB_USER", str(database_url.username))
    monkeypatch.setenv("HLTHPRT_DB_PASSWORD", str(database_url.password or ""))
    monkeypatch.setenv("HLTHPRT_DB_DATABASE", str(database_url.database))
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", schema_name)
    monkeypatch.setenv("DB_SCHEMA", schema_name)


async def _seed_sources(connection, schema_name: str) -> None:
    schema = _quoted(schema_name)
    await connection.execute(
        f"""INSERT INTO {schema}.fhir_formulary_source
            (source_id, canonical_base, display_name)
        VALUES
            ('source-a', 'https://a.example.invalid/fhir', 'Synthetic A'),
            ('source-b', 'https://b.example.invalid/fhir', 'Synthetic B')"""
    )


async def _prepare_repository_schema(
    monkeypatch,
    database_url,
    schema_name: str,
    engine,
) -> None:
    _configure_database(monkeypatch, database_url, schema_name)
    async with engine.begin() as engine_connection:
        await engine_connection.exec_driver_sql(
            f"CREATE SCHEMA {_quoted(schema_name)}"
        )
    await _run_migration_action(engine, _load_migration(), "upgrade")
    connection = await _connect(database_url)
    try:
        await _seed_sources(connection, schema_name)
    finally:
        await connection.close()
    await db.disconnect()


def _coverage_plan(source_id: str) -> CoveragePlanRecord:
    is_source_a = source_id == SOURCE_A
    public_id = PLAN_A if is_source_a else PLAN_B
    list_id = "list-a" if is_source_a else "list-b"
    content_hash = ("a" if is_source_a else "b") * 64
    return CoveragePlanRecord(
        upstream_list_id=list_id,
        public_id=public_id,
        canonical_identity=f"https://{source_id}.example.invalid/fhir/List/{list_id}",
        upstream_version_id="1",
        upstream_last_updated=CUTOFF,
        status="current",
        title="Synthetic coverage plan",
        name="Synthetic plan",
        upstream_date=CUTOFF,
        period_start=None,
        period_end=None,
        source_plan_identifiers=(PLAN_IDENTIFIER,),
        raw_identifiers=(),
        raw_extensions=(),
        content_hash=content_hash,
    )


def _medication(index: int) -> MedicationRecord:
    alternatives = ("MedicationKnowledge/med-1",) if index == 0 else ()
    return MedicationRecord(
        upstream_medication_id=f"med-{index}",
        upstream_version_id="1",
        upstream_last_updated=CUTOFF,
        status="active",
        drug_name=f"Synthetic medication {index}",
        rxnorm_id=str(index),
        ndc11=None,
        codings=(),
        raw_extensions=(),
        source_plan_identifiers=(PLAN_IDENTIFIER,),
        drug_tier="preferred",
        prior_authorization=False,
        step_therapy=False,
        quantity_limit=False,
        alternative_references=alternatives,
        content_hash=f"{index:064x}",
    )


async def _assert_forged_checkpoint_absent(
    repository: FHIRFormularyRepository,
    dataset,
    alias: AliasRef,
) -> None:
    forged_alias = AliasRef(
        SOURCE_A,
        PLAN_B,
        alias.alias_id,
        alias.source_plan_identifier,
    )
    assert (
        await repository.completed_alias_checkpoint(
            dataset=dataset,
            alias=forged_alias,
        )
        is None
    )


async def _build_seed(
    repository: FHIRFormularyRepository,
    source_id: str,
    *,
    medication_count: int,
):
    dataset = await repository.begin_dataset(
        run_id=f"run-{source_id}-seed",
        cutoff_at=CUTOFF,
        acquisition_contract_hash=("c" if source_id == SOURCE_A else "d") * 64,
        intent="seed",
    )
    coverage = await repository.put_coverage_plan(
        dataset=dataset,
        plan=_coverage_plan(source_id),
    )
    alias = coverage.aliases[0]
    if source_id == SOURCE_A:
        await repository.save_checkpoint(
            CheckpointWrite(
                dataset=dataset,
                alias=alias,
                fence_token=1,
                acquisition_mode="full",
                expected_count=medication_count,
                processed_count=min(500, medication_count),
                membership_hash=None,
                completed=False,
            )
        )
        fence_token = 2
    else:
        fence_token = 1
    alias_result = await repository.put_alias_version(
        AliasVersionWrite(
            dataset=dataset,
            alias=alias,
            expected_count=medication_count,
            medications=tuple(_medication(index) for index in range(medication_count)),
            fence_token=fence_token,
        )
    )
    completed = await repository.completed_alias_checkpoint(
        dataset=dataset,
        alias=alias,
    )
    assert completed is not None
    assert completed.alias_version_id == alias_result.alias_version_id
    if source_id == SOURCE_A:
        await _assert_forged_checkpoint_absent(
            repository,
            dataset,
            alias,
        )
    verification = await repository.verify_dataset(dataset=dataset)
    publication = await repository.publish_verified_seed(dataset=dataset)
    assert verification.medication_membership_count == medication_count
    assert publication.generation == 1
    return dataset, alias, alias_result


async def _reuse_generation(
    repository: FHIRFormularyRepository,
    run_id: str,
    prior,
    *,
    publish: bool,
):
    dataset = await repository.begin_dataset(
        run_id=run_id,
        cutoff_at=CUTOFF + dt.timedelta(days=1),
        acquisition_contract_hash="e" * 64,
        intent="requested",
    )
    coverage = await repository.put_coverage_plan(
        dataset=dataset,
        plan=_coverage_plan(SOURCE_A),
    )
    alias = coverage.aliases[0]
    result = await repository.link_reused_alias(
        dataset=dataset,
        alias=alias,
        prior=prior,
        fence_token=1,
    )
    await repository.verify_dataset(dataset=dataset)
    publication = (
        await repository.publish_dataset(dataset=dataset) if publish else None
    )
    return dataset, result, publication


async def _assert_graph_and_isolation(alias_version_id: str) -> None:
    medication_count = await db.scalar(
        f"SELECT count(*) FROM {table_name('fhir_formulary_medication')} "
        "WHERE source_id = :source_id;",
        source_id=SOURCE_A,
    )
    membership_count = await db.scalar(
        f"SELECT count(*) FROM {table_name('fhir_formulary_alias_membership')} "
        "WHERE source_id = :source_id AND alias_version_id = :alias_version_id;",
        source_id=SOURCE_A,
        alias_version_id=alias_version_id,
    )
    source_b_memberships = await db.scalar(
        f"SELECT count(*) FROM {table_name('fhir_formulary_alias_membership')} "
        "WHERE source_id = :source_id;",
        source_id=SOURCE_B,
    )
    alternative_count = await db.scalar(
        f"SELECT count(*) FROM {table_name('fhir_formulary_alternative')} "
        "WHERE alias_version_id = :alias_version_id;",
        alias_version_id=alias_version_id,
    )
    assert medication_count == ALIAS_SIZE + 1
    assert membership_count == ALIAS_SIZE
    assert source_b_memberships == 1
    assert alternative_count == 1


async def _sibling_prior(repository: FHIRFormularyRepository) -> PriorAliasState:
    sibling_dataset = await repository.begin_dataset(
        run_id="run-source-a-sibling",
        cutoff_at=CUTOFF + dt.timedelta(days=2),
        acquisition_contract_hash="f" * 64,
        intent="requested",
    )
    coverage = await repository.put_coverage_plan(
        dataset=sibling_dataset,
        plan=_coverage_plan(SOURCE_A),
    )
    alias = coverage.aliases[0]
    sibling_result = await repository.put_alias_version(
        AliasVersionWrite(
            dataset=sibling_dataset,
            alias=alias,
            expected_count=1,
            medications=(_medication(ALIAS_SIZE + 1),),
            fence_token=1,
        )
    )
    return PriorAliasState(
        SOURCE_A,
        alias.public_id,
        alias.alias_id,
        alias.source_plan_identifier,
        sibling_result.alias_version_id,
        sibling_result.membership_count,
        sibling_dataset.cutoff_at,
        {},
        sibling_result.membership_hash,
    )


async def _assert_sibling_reuse_rejected(
    repository: FHIRFormularyRepository,
) -> None:
    sibling_prior = await _sibling_prior(repository)
    candidate = await repository.begin_dataset(
        run_id="run-source-a-forged-reuse",
        cutoff_at=CUTOFF + dt.timedelta(days=2),
        acquisition_contract_hash="1" * 64,
        intent="requested",
    )
    coverage = await repository.put_coverage_plan(
        dataset=candidate,
        plan=_coverage_plan(SOURCE_A),
    )
    with pytest.raises(RuntimeError, match="predecessor alias"):
        await repository.link_reused_alias(
            dataset=candidate,
            alias=coverage.aliases[0],
            prior=sibling_prior,
            fence_token=1,
        )


async def _run_repository_proof() -> None:
    repository_a = FHIRFormularyRepository(source_id=SOURCE_A)
    repository_b = FHIRFormularyRepository(source_id=SOURCE_B)
    _seed_a, _alias_a, alias_result = await _build_seed(
        repository_a,
        SOURCE_A,
        medication_count=ALIAS_SIZE,
    )
    await _build_seed(repository_b, SOURCE_B, medication_count=1)
    snapshot = await repository_a.current_snapshot()
    assert snapshot.dataset is not None
    prior = next(iter(snapshot.aliases.values()))
    loaded_prior = await repository_a.load_prior_alias_state(prior)
    assert len(loaded_prior.variants_by_medication_id) == ALIAS_SIZE
    _dataset_two, reuse_result, publication_two = await _reuse_generation(
        repository_a,
        "run-source-a-second",
        prior,
        publish=True,
    )
    assert reuse_result.alias_version_id == alias_result.alias_version_id
    assert publication_two is not None and publication_two.generation == 2
    current_snapshot = await repository_a.current_snapshot()
    current_prior = next(iter(current_snapshot.aliases.values()))
    await _assert_sibling_reuse_rejected(repository_a)
    stale_dataset, _result, _publication = await _reuse_generation(
        repository_a,
        "run-source-a-stale",
        current_prior,
        publish=False,
    )
    _winner_dataset, _winner_result, winner_publication = await _reuse_generation(
        repository_a,
        "run-source-a-winner",
        current_prior,
        publish=True,
    )
    assert winner_publication is not None and winner_publication.generation == 3
    with pytest.raises(RuntimeError, match="predecessor is stale"):
        await repository_a.publish_dataset(dataset=stale_dataset)
    await _assert_graph_and_isolation(alias_result.alias_version_id)


async def _assert_competing_publication_cas(
    repository: FHIRFormularyRepository,
    prior: PriorAliasState,
) -> None:
    candidates = []
    for suffix in ("one", "two"):
        candidate, _reuse_result, _publication = await _reuse_generation(
            repository,
            f"run-source-a-candidate-{suffix}",
            prior,
            publish=False,
        )
        candidates.append(candidate)
    outcomes = await asyncio.gather(
        *(repository.publish_dataset(dataset=candidate) for candidate in candidates),
        return_exceptions=True,
    )
    publications = [
        outcome for outcome in outcomes if not isinstance(outcome, BaseException)
    ]
    errors = [
        outcome for outcome in outcomes if isinstance(outcome, BaseException)
    ]
    assert len(publications) == len(errors) == 1
    assert isinstance(errors[0], RuntimeError)
    assert "predecessor is stale" in str(errors[0])
    winner = publications[0]
    datasets_by_id = {candidate.dataset_id: candidate for candidate in candidates}
    repeated = await repository.publish_dataset(
        dataset=datasets_by_id[winner.dataset_id]
    )
    assert repeated == winner
    generation = await db.scalar(
        f"SELECT generation FROM {table_name('fhir_formulary_current')} "
        "WHERE source_id = :source_id;",
        source_id=SOURCE_A,
    )
    assert generation == 2


@pytest.mark.asyncio
async def test_repository_postgres_exact_graph_publication_and_rollback(monkeypatch):
    database_url = _database_url()
    schema_name = f"fhir_formulary_test_{uuid.uuid4().hex}"
    engine = create_async_engine(
        database_url.set(drivername="postgresql+asyncpg")
    )
    try:
        await _prepare_repository_schema(
            monkeypatch,
            database_url,
            schema_name,
            engine,
        )
        with pytest.raises(_RollbackProof):
            async with db.transaction():
                await _run_repository_proof()
                raise _RollbackProof
        retained_datasets = await db.scalar(
            f"SELECT count(*) FROM {table_name('fhir_formulary_dataset')};"
        )
        assert retained_datasets == 0
    finally:
        await db.disconnect()
        await _drop_schema(engine, schema_name)
        await engine.dispose()


@pytest.mark.asyncio
async def test_repository_postgres_serializes_competing_publications(monkeypatch):
    """Prove source-lock serialization, stale rejection, and idempotence."""

    database_url = _database_url()
    schema_name = f"fhir_formulary_test_{uuid.uuid4().hex}"
    engine = create_async_engine(
        database_url.set(drivername="postgresql+asyncpg")
    )
    try:
        await _prepare_repository_schema(
            monkeypatch,
            database_url,
            schema_name,
            engine,
        )
        repository = FHIRFormularyRepository(source_id=SOURCE_A)
        await _build_seed(repository, SOURCE_A, medication_count=1)
        snapshot = await repository.current_snapshot()
        prior = next(iter(snapshot.aliases.values()))
        await _assert_competing_publication_cas(repository, prior)
    finally:
        await db.disconnect()
        await _drop_schema(engine, schema_name)
        await engine.dispose()
