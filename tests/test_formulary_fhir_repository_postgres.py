# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import datetime as dt
import os
import re

import pytest
from sqlalchemy.engine import make_url

from db.models import db
from process.formulary_fhir.repository import AliasVersionWrite
from process.formulary_fhir.repository import FHIRFormularyRepository
from process.formulary_fhir.repository_shared import table_name
from process.formulary_fhir.types import CoveragePlanRecord, MedicationRecord

OPT_IN_DSN_ENV = "HLTHPRT_FORMULARY_FHIR_POSTGRES_DSN"
DISPOSABLE_DATABASE_PATTERN = re.compile(
    r"^ptg2_v3_lifecycle_test_[a-z0-9][a-z0-9_]{7,}$"
)
ALIAS_SIZE = 1_001


class _RollbackProofTransaction(Exception):
    """End the integration proof without retaining its synthetic rows."""


def _database_url():
    dsn = os.getenv(OPT_IN_DSN_ENV)
    if not dsn:
        pytest.skip(f"set {OPT_IN_DSN_ENV} to run PostgreSQL batch proof")
    database_url = make_url(dsn)
    if not database_url.drivername.startswith("postgresql"):
        pytest.fail(f"{OPT_IN_DSN_ENV} must use PostgreSQL")
    database_name = str(database_url.database or "")
    if not DISPOSABLE_DATABASE_PATTERN.fullmatch(database_name):
        pytest.fail(f"refusing non-disposable PostgreSQL database {database_name!r}")
    if not database_url.host or not database_url.username:
        pytest.fail(f"{OPT_IN_DSN_ENV} must include an explicit host and user")
    return database_url


def _configure_database(monkeypatch, database_url) -> None:
    monkeypatch.setenv("HLTHPRT_DB_DRIVER", "asyncpg")
    monkeypatch.setenv("HLTHPRT_DB_HOST", str(database_url.host))
    monkeypatch.setenv("HLTHPRT_DB_PORT", str(database_url.port or 5432))
    monkeypatch.setenv("HLTHPRT_DB_USER", str(database_url.username))
    monkeypatch.setenv("HLTHPRT_DB_PASSWORD", str(database_url.password or ""))
    monkeypatch.setenv("HLTHPRT_DB_DATABASE", str(database_url.database))
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "mrf")
    monkeypatch.setenv("DB_SCHEMA", "mrf")


def _medication(index: int) -> MedicationRecord:
    alternatives = ("MedicationKnowledge/synthetic-1",) if index == 0 else ()
    return MedicationRecord(
        upstream_medication_id=f"MI-synthetic-{index}",
        upstream_version_id="1",
        upstream_last_updated="2026-08-06T12:00:00Z",
        status="active",
        drug_name=f"Synthetic medication {index}",
        rxnorm_id=str(index),
        ndc11=None,
        codings=(),
        raw_extensions=(),
        source_plan_identifiers=("SYNTHETIC-PLAN",),
        drug_tier="preferred",
        prior_authorization=False,
        step_therapy=False,
        quantity_limit=False,
        alternative_references=alternatives,
        content_hash=f"{index:064x}",
    )


def _coverage_plan() -> CoveragePlanRecord:
    return CoveragePlanRecord(
        upstream_list_id="synthetic-list",
        public_id="fhir_aaaaaaaaaaaaaaaaaaaaaaaaaa",
        canonical_identity="https://example.test/fhir/List/synthetic-list",
        upstream_version_id="1",
        upstream_last_updated="2026-08-06T12:00:00Z",
        status="current",
        title="Synthetic coverage plan",
        name="Synthetic plan",
        upstream_date="2026-08-06T12:00:00Z",
        period_start=None,
        period_end=None,
        source_plan_identifiers=("SYNTHETIC-PLAN",),
        raw_identifiers=(),
        raw_extensions=(),
        content_hash="c" * 64,
    )


async def _assert_persisted_alias(
    dataset_id: str,
    alias_version_id: str,
) -> None:
    medication_count = await db.scalar(
        f"SELECT COUNT(*) FROM {table_name('fhir_formulary_medication')} "
        "WHERE upstream_medication_id LIKE 'MI-synthetic-%';"
    )
    membership_count = await db.scalar(
        f"SELECT COUNT(*) FROM "
        f"{table_name('fhir_formulary_alias_membership')} "
        "WHERE alias_version_id = :alias_version_id;",
        alias_version_id=alias_version_id,
    )
    assert medication_count == ALIAS_SIZE
    assert membership_count == ALIAS_SIZE
    alternative = await db.first(
        f"SELECT raw_reference, corrected_reference, "
        "resolved_medication_id, resolved FROM "
        f"{table_name('fhir_formulary_alternative')} "
        "WHERE alias_version_id = :alias_version_id;",
        alias_version_id=alias_version_id,
    )
    assert alternative is not None
    assert alternative.raw_reference == "MedicationKnowledge/synthetic-1"
    assert alternative.corrected_reference == ("MedicationKnowledge/MI-synthetic-1")
    assert alternative.resolved_medication_id == "MI-synthetic-1"
    assert alternative.resolved is True
    current_dataset_id = await db.scalar(
        f"SELECT dataset_id FROM {table_name('fhir_formulary_current')} "
        "WHERE source_id = 'fhir-formulary-primary';"
    )
    assert current_dataset_id == dataset_id


async def _run_batch_proof() -> None:
    cutoff = dt.datetime(2026, 8, 6, 12, tzinfo=dt.UTC)
    formulary_repository = FHIRFormularyRepository()
    async with db.transaction():
        dataset_id = await formulary_repository.begin_dataset(
            run_id="synthetic-postgres-seed",
            cutoff_at=cutoff,
            publish_requested=False,
            seed_eligible=True,
        )
        aliases_by_identifier = await formulary_repository.put_coverage_plan(
            dataset_id=dataset_id,
            plan=_coverage_plan(),
        )
        alias_version_id = await formulary_repository.put_alias_version(
            AliasVersionWrite(
                dataset_id=dataset_id,
                alias_id=aliases_by_identifier["SYNTHETIC-PLAN"],
                expected_count=ALIAS_SIZE,
                cutoff_at=cutoff,
                medications=tuple(_medication(index) for index in range(ALIAS_SIZE)),
                acquisition_mode="full",
                apply_california_rule=True,
            )
        )
        proof_by_field = await formulary_repository.verify_dataset(dataset_id)
        generation = await formulary_repository.publish_verified_seed(dataset_id)
        assert proof_by_field["list_count"] == 1
        assert proof_by_field["alias_count"] == 1
        assert proof_by_field["medication_membership_count"] == ALIAS_SIZE
        assert generation == 1
        await _assert_persisted_alias(dataset_id, alias_version_id)
        raise _RollbackProofTransaction


@pytest.mark.asyncio
async def test_postgres_batches_large_alias_and_preserves_alternative_evidence(
    monkeypatch,
):
    """Cross batching, verification, and publication through real asyncpg."""

    database_url = _database_url()
    _configure_database(monkeypatch, database_url)
    await db.disconnect()
    try:
        with pytest.raises(_RollbackProofTransaction):
            await _run_batch_proof()
    finally:
        await db.disconnect()
