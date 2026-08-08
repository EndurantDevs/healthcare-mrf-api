# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Real PostgreSQL proof for admitted formulary publication."""

from __future__ import annotations

import asyncio
import datetime as dt
import importlib.util
from pathlib import Path
from typing import Any
import uuid

import pytest
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import create_async_engine

from db.models import db
from process.formulary_fhir.repository import AliasVersionWrite
from process.formulary_fhir.repository import DatasetRef
from process.formulary_fhir.repository import FHIRFormularyRepository
from process.formulary_fhir.repository_admission import admit_verified_twins
from process.formulary_fhir.repository_admission import TwinAdmissionError
from process.formulary_fhir.repository_shared import table_name
from process.formulary_fhir.reviewed_source import register_reviewed_source
from tests.test_formulary_fhir_repository_postgres import _configure_database
from tests.test_formulary_fhir_repository_postgres import _coverage_plan
from tests.test_formulary_fhir_repository_postgres import _medication
from tests.test_formulary_fhir_storage_postgres import _database_url
from tests.test_formulary_fhir_storage_postgres import _drop_schema
from tests.test_formulary_fhir_storage_postgres import _load_migration
from tests.test_formulary_fhir_storage_postgres import _quoted
from tests.test_formulary_fhir_storage_postgres import _run_migration_action


ROOT = Path(__file__).resolve().parents[1]
ATTEMPT_PATH = ROOT / "alembic" / "versions" / (
    "20260808110000_fhir_formulary_twin_attempt.py"
)
ADMISSION_PATH = ROOT / "alembic" / "versions" / (
    "20260808120000_fhir_formulary_twin_admission.py"
)
PUBLICATION_PATH = ROOT / "alembic" / "versions" / (
    "20260808130000_fhir_formulary_publication_guards.py"
)
PAIR_CUTOFF = dt.datetime(2026, 8, 8, 12, tzinfo=dt.UTC)
CONTRACT_HASH = "9" * 64


def _load_task_migration(path: Path, module_name: str) -> Any:
    module_spec = importlib.util.spec_from_file_location(module_name, path)
    assert module_spec is not None and module_spec.loader is not None
    migration = importlib.util.module_from_spec(module_spec)
    module_spec.loader.exec_module(migration)
    return migration


async def _prepare_schema(monkeypatch, database_url, schema_name, engine) -> None:
    _configure_database(monkeypatch, database_url, schema_name)
    async with engine.begin() as engine_connection:
        await engine_connection.exec_driver_sql(
            f"CREATE SCHEMA {_quoted(schema_name)}"
        )
    await _run_migration_action(engine, _load_migration(), "upgrade")
    for migration_path, module_name in (
        (ATTEMPT_PATH, "fhir_twin_repository_attempt"),
        (ADMISSION_PATH, "fhir_twin_repository_admission"),
        (PUBLICATION_PATH, "fhir_twin_repository_publication"),
    ):
        migration = _load_task_migration(migration_path, module_name)
        await _run_migration_action(engine, migration, "upgrade")
    await db.disconnect()


async def _verified_full_dataset(
    repository: FHIRFormularyRepository,
    source_id: str,
    run_id: str,
    cutoff_at: dt.datetime,
    intent: str,
    *,
    medication_count: int = 2,
) -> DatasetRef:
    dataset = await repository.begin_dataset(
        run_id=run_id,
        cutoff_at=cutoff_at,
        acquisition_contract_hash=CONTRACT_HASH,
        intent=intent,
    )
    coverage = await repository.put_coverage_plan(
        dataset=dataset,
        plan=_coverage_plan(source_id),
    )
    alias = coverage.aliases[0]
    await repository.put_alias_version(
        AliasVersionWrite(
            dataset=dataset,
            alias=alias,
            expected_count=medication_count,
            medications=tuple(
                _medication(index) for index in range(medication_count)
            ),
            fence_token=1,
        )
    )
    await repository.verify_dataset(dataset=dataset)
    verified = await repository.begin_dataset(
        run_id=run_id,
        cutoff_at=cutoff_at,
        acquisition_contract_hash=CONTRACT_HASH,
        intent=intent,
    )
    assert verified.status == "verified"
    return verified


async def _admitted_pair(
    repository: FHIRFormularyRepository,
    binding,
    label: str,
    cutoff_at: dt.datetime,
) -> tuple[DatasetRef, DatasetRef]:
    baseline = await _verified_full_dataset(
        repository,
        binding.source_id,
        f"twin-{label}-baseline",
        cutoff_at,
        "none",
    )
    candidate = await _verified_full_dataset(
        repository,
        binding.source_id,
        f"twin-{label}-candidate",
        cutoff_at,
        "requested",
    )
    admission = await admit_verified_twins(
        database=db,
        binding=binding,
        baseline=baseline,
        candidate=candidate,
    )
    assert admission.baseline_dataset_id == baseline.dataset_id
    assert admission.candidate_dataset_id == candidate.dataset_id
    return baseline, candidate


async def _assert_mismatch_consumes_roots(
    repository: FHIRFormularyRepository,
    binding,
) -> None:
    baseline = await _verified_full_dataset(
        repository,
        binding.source_id,
        "twin-mismatch-baseline",
        PAIR_CUTOFF,
        "none",
    )
    candidate = await _verified_full_dataset(
        repository,
        binding.source_id,
        "twin-mismatch-candidate",
        PAIR_CUTOFF,
        "requested",
        medication_count=3,
    )
    with pytest.raises(TwinAdmissionError) as mismatch:
        await admit_verified_twins(
            database=db,
            binding=binding,
            baseline=baseline,
            candidate=candidate,
        )
    assert mismatch.value.code == "mismatch"
    attempt_by_field = dict(
        (
            await db.first(
                f"SELECT matched FROM "
                f"{table_name('fhir_formulary_twin_attempt')} "
                "WHERE baseline_dataset_id = :baseline_dataset_id "
                "AND candidate_dataset_id = :candidate_dataset_id;",
                baseline_dataset_id=baseline.dataset_id,
                candidate_dataset_id=candidate.dataset_id,
            )
        )._mapping
    )
    assert attempt_by_field == {"matched": False}
    replacement = await _verified_full_dataset(
        repository,
        binding.source_id,
        "twin-mismatch-replacement",
        PAIR_CUTOFF,
        "requested",
    )
    with pytest.raises(TwinAdmissionError) as reused:
        await admit_verified_twins(
            database=db,
            binding=binding,
            baseline=baseline,
            candidate=replacement,
        )
    assert reused.value.code == "attempt"


async def _assert_source_drift_rejected_by_database(
    source_id: str,
) -> None:
    with pytest.raises(DBAPIError, match="fhir_formulary_current_source_immutable"):
        await db.status(
            f"UPDATE {table_name('fhir_formulary_source')} SET "
            "runtime_config_json = runtime_config_json || "
            "CAST(:drift AS jsonb) WHERE source_id = :source_id;",
            drift='{"unexpected":true}',
            source_id=source_id,
        )


async def _assert_alternative_drift_rejected_by_database() -> None:
    with pytest.raises(DBAPIError, match="fhir_formulary_cow_immutable"):
        await db.status(
            f"UPDATE {table_name('fhir_formulary_alternative')} SET "
            "evidence_json = CAST(:evidence AS jsonb);",
            evidence='{"unexpected":true}',
        )


async def _assert_competing_admissions_serialize(
    repository: FHIRFormularyRepository,
    binding,
) -> None:
    next_cutoff = PAIR_CUTOFF + dt.timedelta(days=1)
    _baseline_one, candidate_one = await _admitted_pair(
        repository,
        binding,
        "competing-one",
        next_cutoff,
    )
    _baseline_two, candidate_two = await _admitted_pair(
        repository,
        binding,
        "competing-two",
        next_cutoff,
    )
    outcomes = await asyncio.gather(
        repository.publish_dataset(dataset=candidate_one),
        repository.publish_dataset(dataset=candidate_two),
        return_exceptions=True,
    )
    publications = [
        outcome for outcome in outcomes if not isinstance(outcome, BaseException)
    ]
    failures = [
        outcome for outcome in outcomes if isinstance(outcome, BaseException)
    ]
    assert len(publications) == len(failures) == 1
    assert "stale" in str(failures[0]).lower()
    winner = publications[0]
    winner_by_id = {
        candidate_one.dataset_id: candidate_one,
        candidate_two.dataset_id: candidate_two,
    }
    assert await repository.publish_dataset(
        dataset=winner_by_id[winner.dataset_id]
    ) == winner
    assert winner.generation == 2


@pytest.mark.asyncio
async def test_twin_repository_postgres_publication_integrity(monkeypatch):
    database_url = _database_url()
    schema_name = f"fhir_formulary_test_{uuid.uuid4().hex}"
    engine = create_async_engine(
        database_url.set(drivername="postgresql+asyncpg")
    )
    try:
        await _prepare_schema(monkeypatch, database_url, schema_name, engine)
        binding = await register_reviewed_source()
        repository = FHIRFormularyRepository(source_id=binding.source_id)

        unadmitted = await _verified_full_dataset(
            repository,
            binding.source_id,
            "unadmitted-candidate",
            PAIR_CUTOFF,
            "requested",
        )
        with pytest.raises(TwinAdmissionError):
            await repository.publish_dataset(dataset=unadmitted)

        await _assert_mismatch_consumes_roots(repository, binding)

        _baseline, candidate = await _admitted_pair(
            repository,
            binding,
            "first",
            PAIR_CUTOFF,
        )
        await _assert_source_drift_rejected_by_database(binding.source_id)
        await _assert_alternative_drift_rejected_by_database()
        publication = await repository.publish_dataset(dataset=candidate)
        assert publication.generation == 1
        assert await repository.publish_dataset(dataset=candidate) == publication

        await _assert_competing_admissions_serialize(repository, binding)
        assert await db.scalar(
            f"SELECT count(*) FROM {table_name('fhir_formulary_twin_attempt')};"
        ) == 4
        assert await db.scalar(
            f"SELECT count(*) FROM {table_name('fhir_formulary_twin_admission')};"
        ) == 3
    finally:
        await db.disconnect()
        await _drop_schema(engine, schema_name)
        await engine.dispose()
