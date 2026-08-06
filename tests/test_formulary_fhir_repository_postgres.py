# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import datetime as dt
import os
import re

import pytest
from sqlalchemy.engine import make_url

from db.models import db
from process.formulary_fhir import repository_batch
from process.formulary_fhir.repository_shared import SOURCE_ID, table_name
from process.formulary_fhir.types import MedicationRecord

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


async def _insert_alias_version() -> None:
    await db.status(
        f"INSERT INTO {table_name('fhir_formulary_coverage_plan')} ("
        "public_id, source_id, upstream_list_id, canonical_identity) VALUES ("
        ":public_id, :source_id, :upstream_list_id, :canonical_identity);",
        public_id="fhir_aaaaaaaaaaaaaaaaaaaaaaaaaa",
        source_id=SOURCE_ID,
        upstream_list_id="synthetic-list",
        canonical_identity="https://example.test/fhir/List/synthetic-list",
    )
    await db.status(
        f"INSERT INTO {table_name('fhir_formulary_drug_plan_alias')} ("
        "alias_id, public_id, source_plan_identifier) VALUES ("
        ":alias_id, :public_id, :source_plan_identifier);",
        alias_id="synthetic-alias",
        public_id="fhir_aaaaaaaaaaaaaaaaaaaaaaaaaa",
        source_plan_identifier="SYNTHETIC-PLAN",
    )
    await db.status(
        f"INSERT INTO {table_name('fhir_formulary_drug_plan_alias_version')} ("
        "alias_version_id, alias_id, expected_count, membership_count, "
        "membership_hash, cutoff_at, acquisition_mode, summary_json) VALUES ("
        ":alias_version_id, :alias_id, :expected_count, :membership_count, "
        ":membership_hash, :cutoff_at, 'full', '{}'::jsonb);",
        alias_version_id="synthetic-alias-version",
        alias_id="synthetic-alias",
        expected_count=ALIAS_SIZE,
        membership_count=ALIAS_SIZE,
        membership_hash="a" * 64,
        cutoff_at=dt.datetime(2026, 8, 6, 12, tzinfo=dt.UTC),
    )


def _alias_rows():
    medications = tuple(_medication(index) for index in range(ALIAS_SIZE))
    medications_by_id = {
        medication.upstream_medication_id: medication for medication in medications
    }
    variants_by_id = {
        medication.upstream_medication_id: f"{index + ALIAS_SIZE:064x}"
        for index, medication in enumerate(medications)
    }
    return medications_by_id, variants_by_id


async def _assert_persisted_alias() -> None:
    medication_count = await db.scalar(
        f"SELECT COUNT(*) FROM {table_name('fhir_formulary_medication')} "
        "WHERE upstream_medication_id LIKE 'MI-synthetic-%';"
    )
    membership_count = await db.scalar(
        f"SELECT COUNT(*) FROM "
        f"{table_name('fhir_formulary_alias_membership')} "
        "WHERE alias_version_id = :alias_version_id;",
        alias_version_id="synthetic-alias-version",
    )
    assert medication_count == ALIAS_SIZE
    assert membership_count == ALIAS_SIZE
    alternative = await db.first(
        f"SELECT raw_reference, corrected_reference, "
        "resolved_medication_id, resolved FROM "
        f"{table_name('fhir_formulary_alternative')} "
        "WHERE alias_version_id = :alias_version_id;",
        alias_version_id="synthetic-alias-version",
    )
    assert alternative is not None
    assert alternative.raw_reference == "MedicationKnowledge/synthetic-1"
    assert alternative.corrected_reference == ("MedicationKnowledge/MI-synthetic-1")
    assert alternative.resolved_medication_id == "MI-synthetic-1"
    assert alternative.resolved is True


async def _run_batch_proof() -> None:
    medications_by_id, variants_by_id = _alias_rows()
    async with db.transaction():
        await _insert_alias_version()
        await repository_batch.insert_changed_alias_rows(
            "synthetic-alias-version",
            medications_by_id,
            variants_by_id,
            apply_california_rule=True,
        )
        await _assert_persisted_alias()
        raise _RollbackProofTransaction


@pytest.mark.asyncio
async def test_postgres_batches_large_alias_and_preserves_alternative_evidence(
    monkeypatch,
):
    """Cross the production SQLAlchemy and asyncpg bind boundary."""

    database_url = _database_url()
    _configure_database(monkeypatch, database_url)
    await db.disconnect()
    try:
        with pytest.raises(_RollbackProofTransaction):
            await _run_batch_proof()
    finally:
        await db.disconnect()
