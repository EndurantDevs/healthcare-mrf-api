# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Support for disposable PostgreSQL twin-admission migration proofs."""

from __future__ import annotations

import importlib.util
import json
import os
from pathlib import Path
import re
from typing import Any

import asyncpg
from alembic.migration import MigrationContext
from alembic.operations import Operations
import pytest
import sqlalchemy as sa
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import AsyncEngine


ROOT = Path(__file__).resolve().parents[1]
VERSIONS = ROOT / "alembic" / "versions"
FOUNDATION_PATH = VERSIONS / "20260807110000_fhir_formulary_storage_foundation.py"
ATTEMPT_PATH = VERSIONS / "20260808110000_fhir_formulary_twin_attempt.py"
ADMISSION_PATH = VERSIONS / "20260808120000_fhir_formulary_twin_admission.py"
GUARDS_PATH = VERSIONS / "20260808130000_fhir_formulary_publication_guards.py"
POSTGRES_DSN_ENV = "HLTHPRT_FHIR_FORMULARY_MIGRATION_POSTGRES_DSN"
DISPOSABLE_DATABASE_RE = re.compile(
    r"(?:^test(?:[_-]|$)|(?:^|[_-])test(?:[_-]|$))",
    re.IGNORECASE,
)
DISPOSABLE_SCHEMA_RE = re.compile(r"^fhir_twin_test_[0-9a-f]{32}$")
HASHES = {
    "source": "a" * 64,
    "contract": "b" * 64,
    "coverage": "c" * 64,
    "membership": "d" * 64,
    "alternative": "e" * 64,
    "evidence": "f" * 64,
    "different_evidence": "1" * 64,
}
_DATASET_COLUMNS = """(dataset_id, source_id, run_id, previous_dataset_id, cutoff_at,
 status, publish_requested, seed_eligible, list_count, alias_count, medication_count,
 coverage_hash, membership_hash, summary_json, verified_at, published_at, failed_at)"""
_TWIN_DATASET_VALUES = """
 ('seed-a', 'source-a', 'run-seed-a', NULL, '2026-08-07Z', 'published', false,
  true, 1, 1, 1, $1, $2, $3, '2026-08-07T00:01Z', '2026-08-07T00:03Z', NULL),
 ('baseline-a', 'source-a', 'run-baseline-a', 'seed-a', '2026-08-08Z', 'verified',
  false, false, 2, 3, 4, $1, $2, $3, '2026-08-08T00:01Z', NULL, NULL),
 ('candidate-a', 'source-a', 'run-candidate-a', 'seed-a', '2026-08-08Z', 'verified',
  true, false, 2, 3, 4, $1, $2, $3, '2026-08-08T00:02Z', NULL, NULL),
 ('baseline-mismatch', 'source-a', 'run-baseline-mismatch', 'seed-a', '2026-08-08Z',
  'verified', false, false, 2, 3, 4, $1, $2, $3, '2026-08-08T00:01Z', NULL, NULL),
 ('candidate-mismatch', 'source-a', 'run-candidate-mismatch', 'seed-a', '2026-08-08Z',
  'verified', true, false, 2, 3, 5, $1, $2, $3, '2026-08-08T00:02Z', NULL, NULL),
 ('graph-building', 'source-a', 'run-graph-building', 'seed-a', '2026-08-08Z',
  'building', false, false, 0, 0, 0, NULL, NULL, $3, NULL, NULL, NULL),
 ('graph-verified', 'source-a', 'run-graph-verified', 'seed-a', '2026-08-08Z',
  'verified', false, false, 1, 1, 1, $1, $2, $3, '2026-08-08T00:01Z', NULL, NULL)"""
_POINTER_DATASET_VALUES = """
 ('none-current', 'source-none', 'run-none-current', NULL, '2026-08-08Z', 'verified',
  false, false, 1, 1, 1, $1, $2, $3, '2026-08-08T00:01Z', NULL, NULL),
 ('building-current', 'source-building', 'run-building-current', NULL, '2026-08-08Z',
  'building', false, true, 1, 1, 1, $1, $2, $3, NULL, NULL, NULL),
 ('failed-current', 'source-failed', 'run-failed-current', NULL, '2026-08-08Z',
  'failed', true, false, 1, 1, 1, $1, $2, $3, NULL, NULL, '2026-08-08T00:02Z'),
 ('ordinary-current', 'source-ordinary', 'run-ordinary-current', NULL, '2026-08-08Z',
  'verified', true, false, 1, 1, 1, $1, $2, $3, '2026-08-08T00:01Z', NULL, NULL),
 ('zero-seed', 'source-zero-seed', 'run-zero-seed', NULL, '2026-08-08Z', 'verified',
  false, true, 0, 0, 0, NULL, NULL, $3, '2026-08-08T00:01Z', NULL, NULL),
 ('live-seed', 'source-live-seed', 'run-live-seed', NULL, '2026-08-08Z', 'verified',
  false, true, 1, 1, 1, $1, $2, $3, '2026-08-08T00:01Z', NULL, NULL),
 ('real-seed', 'source-real-seed', 'run-real-seed', NULL, '2026-08-08Z', 'verified',
  false, true, 1, 1, 1, $1, $2, $3, '2026-08-08T00:01Z', NULL, NULL),
 ('real-published-seed', 'source-real-published', 'run-real-published', NULL,
  '2026-08-08Z', 'published', false, true, 1, 1, 1, $1, $2, $3,
  '2026-08-08T00:01Z', '2026-08-08T00:03Z', NULL)"""


def load_migration(path: Path, module_name: str) -> Any:
    module_spec = importlib.util.spec_from_file_location(module_name, path)
    assert module_spec is not None and module_spec.loader is not None
    migration = importlib.util.module_from_spec(module_spec)
    module_spec.loader.exec_module(migration)
    return migration


def database_url() -> sa.URL:
    raw_dsn = os.getenv(POSTGRES_DSN_ENV)
    if not raw_dsn:
        pytest.skip(f"set {POSTGRES_DSN_ENV} for the PostgreSQL proof")
    url = make_url(raw_dsn)
    database_name = str(url.database or "")
    if (
        not url.drivername.startswith("postgresql")
        or not DISPOSABLE_DATABASE_RE.search(database_name)
        or not url.host
        or not url.username
    ):
        pytest.fail(
            f"{POSTGRES_DSN_ENV} must identify an explicit PostgreSQL test "
            "database; only a generated disposable schema is modified"
        )
    return url


def quoted(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


async def connect(url: sa.URL) -> asyncpg.Connection:
    return await asyncpg.connect(
        host=str(url.host),
        port=int(url.port or 5432),
        user=str(url.username),
        password=str(url.password or ""),
        database=str(url.database),
    )


async def run_migration(engine: AsyncEngine, migration: Any, action: str) -> None:
    async with engine.connect() as async_connection:

        def run_action(sync_connection) -> None:
            context = MigrationContext.configure(sync_connection)
            migration.op = Operations(context)
            with context.begin_transaction():
                getattr(migration, action)()

        await async_connection.run_sync(run_action)


async def drop_schema(engine: AsyncEngine, schema_name: str) -> None:
    if not DISPOSABLE_SCHEMA_RE.fullmatch(schema_name):
        raise RuntimeError(f"refusing to drop schema {schema_name!r}")
    async with engine.begin() as connection:
        await connection.exec_driver_sql(
            f"DROP SCHEMA IF EXISTS {quoted(schema_name)} CASCADE"
        )


async def assert_sqlstate(
    connection: asyncpg.Connection,
    expected_sqlstate: str | set[str],
    statement: str,
) -> None:
    with pytest.raises(asyncpg.PostgresError) as error:
        async with connection.transaction():
            await connection.execute(statement)
    allowed = {expected_sqlstate} if type(expected_sqlstate) is str else expected_sqlstate
    assert error.value.sqlstate in allowed


async def assert_invalid_pointer_writes(
    connection: asyncpg.Connection,
    schema_name: str,
) -> None:
    """Reject invalid intents plus pointer and published-seed destruction."""

    schema = quoted(schema_name)
    invalid_pointers = (
        ("source-none", "none-current"),
        ("source-building", "building-current"),
        ("source-failed", "failed-current"),
        ("source-ordinary", "ordinary-current"),
        ("source-zero-seed", "zero-seed"),
    )
    for source_id, dataset_id in invalid_pointers:
        await assert_sqlstate(
            connection,
            "55000",
            f"INSERT INTO {schema}.fhir_formulary_current "
            f"(source_id, dataset_id) VALUES ('{source_id}', '{dataset_id}')",
        )
    for statement in (
        f"UPDATE {schema}.fhir_formulary_source SET display_name = 'drift' "
        "WHERE source_id = 'source-real-seed'",
        f"DELETE FROM {schema}.fhir_formulary_current WHERE source_id = 'source-a'",
        f"TRUNCATE TABLE {schema}.fhir_formulary_current",
        f"UPDATE {schema}.fhir_formulary_dataset SET status = 'failed' "
        "WHERE dataset_id = 'seed-a'",
        f"DELETE FROM {schema}.fhir_formulary_dataset WHERE dataset_id = 'live-seed'",
    ):
        await assert_sqlstate(connection, "55000", statement)


async def set_source_enabled(
    connection: asyncpg.Connection,
    schema_name: str,
    source_id: str,
    enabled: bool,
) -> None:
    """Toggle only the source field allowed after proof is frozen."""

    await connection.execute(
        f"UPDATE {quoted(schema_name)}.fhir_formulary_source SET enabled = $1, "
        "updated_at = transaction_timestamp() WHERE source_id = $2",
        enabled,
        source_id,
    )


async def _seed_sources(connection: asyncpg.Connection, schema: str) -> None:
    source_ids = (
        "source-a",
        "source-none",
        "source-building",
        "source-failed",
        "source-ordinary",
        "source-zero-seed",
        "source-live-seed",
        "source-real-seed",
        "source-real-published",
    )
    await connection.executemany(
        f"INSERT INTO {schema}.fhir_formulary_source "
        "(source_id, canonical_base, display_name, metadata_json) "
        "VALUES ($1, $2, $3, $4)",
        [
            (
                source_id,
                f"https://{source_id}.example.invalid/fhir",
                source_id,
                json.dumps(
                    {}
                    if source_id.startswith("source-real-")
                    else {"synthetic": True}
                ),
            )
            for source_id in source_ids
        ],
    )


async def seed_datasets(connection: asyncpg.Connection, schema_name: str) -> None:
    """Seed exact twin, pointer, and source fixtures."""

    schema = quoted(schema_name)
    await _seed_sources(connection, schema)
    parameters = (
        HASHES["coverage"],
        HASHES["membership"],
        json.dumps({"acquisition_contract_hash": HASHES["contract"]}),
    )
    for values_sql in (_TWIN_DATASET_VALUES, _POINTER_DATASET_VALUES):
        await connection.execute(
            f"INSERT INTO {schema}.fhir_formulary_dataset {_DATASET_COLUMNS} "
            f"VALUES {values_sql}",
            *parameters,
        )
    await connection.execute(
        f"INSERT INTO {schema}.fhir_formulary_current "
        "(source_id, dataset_id, generation, published_at) VALUES "
        "('source-a', 'seed-a', 1, '2026-08-07T00:03Z')"
    )


async def seed_content_graph(connection: asyncpg.Connection, schema_name: str) -> None:
    """Seed building content and two intentionally incomplete verified aliases."""

    schema = quoted(schema_name)
    public_id = "fhir_" + "a" * 26
    await connection.execute(
        f"""INSERT INTO {schema}.fhir_formulary_coverage_plan
            (public_id, source_id, upstream_list_id, canonical_identity)
            VALUES ('{public_id}', 'source-a', 'list-one', 'plan-one');
        INSERT INTO {schema}.fhir_formulary_coverage_plan_version
            (coverage_version_id, public_id, content_hash)
            VALUES ('coverage-v1', '{public_id}', '{HASHES["coverage"]}');
        INSERT INTO {schema}.fhir_formulary_dataset_coverage_plan
            (source_id, dataset_id, public_id, coverage_version_id)
            VALUES ('source-a', 'graph-building', '{public_id}', 'coverage-v1');
        INSERT INTO {schema}.fhir_formulary_drug_plan_alias
            (alias_id, source_id, public_id, source_plan_identifier)
            VALUES
              ('build-alias', 'source-a', '{public_id}', 'build-plan'),
              ('late-member-alias', 'source-a', '{public_id}', 'late-member-plan'),
              ('late-alt-alias', 'source-a', '{public_id}', 'late-alt-plan'),
              ('late-owner-alias', 'source-a', '{public_id}', 'late-owner-plan');
        INSERT INTO {schema}.fhir_formulary_drug_plan_alias_version
            (alias_version_id, source_id, alias_id, expected_count,
             membership_count, membership_hash, cutoff_at, acquisition_mode)
            VALUES
              ('build-av', 'source-a', 'build-alias', 1, 1,
               '{HASHES["membership"]}', '2026-08-08Z', 'full'),
              ('late-member-av', 'source-a', 'late-member-alias', 1, 1,
               '{HASHES["membership"]}', '2026-08-08Z', 'full'),
              ('late-alt-av', 'source-a', 'late-alt-alias', 1, 1,
               '{HASHES["membership"]}', '2026-08-08Z', 'full'),
              ('late-owner-av', 'source-a', 'late-owner-alias', 0, 0,
               '{HASHES["alternative"]}', '2026-08-08Z', 'full');
        INSERT INTO {schema}.fhir_formulary_dataset_alias
            (source_id, dataset_id, alias_id, alias_version_id)
            VALUES
              ('source-a', 'graph-building', 'build-alias', 'build-av'),
              ('source-a', 'graph-verified', 'late-member-alias', 'late-member-av'),
              ('source-a', 'graph-verified', 'late-alt-alias', 'late-alt-av');
        INSERT INTO {schema}.fhir_formulary_medication
            (medication_version_id, source_id, upstream_medication_id,
             codings_json, content_hash)
            VALUES ('med-v1', 'source-a', 'med-one', '[]', '{HASHES["coverage"]}');
        INSERT INTO {schema}.fhir_formulary_alias_membership
            (source_id, alias_version_id, upstream_medication_id,
             medication_version_id, variant_hash)
            VALUES
              ('source-a', 'build-av', 'med-one', 'med-v1', '{HASHES["membership"]}'),
              ('source-a', 'late-alt-av', 'med-one', 'med-v1', '{HASHES["membership"]}');
        INSERT INTO {schema}.fhir_formulary_alternative
            (alias_version_id, upstream_medication_id, raw_reference)
            VALUES ('build-av', 'med-one', 'self');
        INSERT INTO {schema}.fhir_formulary_checkpoint
            (source_id, alias_id, source_plan_identifier, run_id, dataset_id,
             fence_token, cutoff_at, acquisition_mode, expected_count, processed_count)
            VALUES ('source-a', 'build-alias', 'build-plan', 'run-graph-building',
             'graph-building', 1, '2026-08-08Z', 'full', 1, 0);"""
    )


def attempt_insert(
    schema_name: str,
    baseline_id: str,
    baseline_run_id: str,
    candidate_id: str,
    candidate_run_id: str,
    *,
    matched: bool,
) -> str:
    schema = quoted(schema_name)
    candidate_hash = HASHES["evidence"] if matched else HASHES["different_evidence"]
    return f"""INSERT INTO {schema}.fhir_formulary_twin_attempt
        (source_id, baseline_dataset_id, baseline_run_id,
         candidate_dataset_id, candidate_run_id, cutoff_at,
         source_configuration_hash, acquisition_contract_hash,
         baseline_evidence_hash, candidate_evidence_hash, matched, attempted_at)
    VALUES ('source-a', '{baseline_id}', '{baseline_run_id}',
            '{candidate_id}', '{candidate_run_id}', '2026-08-08Z',
            '{HASHES["source"]}', '{HASHES["contract"]}',
            '{HASHES["evidence"]}', '{candidate_hash}', {str(matched).lower()},
            '2026-08-08T00:04Z')"""


def admission_insert(
    schema_name: str,
    baseline_id: str,
    baseline_run_id: str,
    candidate_id: str,
    candidate_run_id: str,
    medication_count: int,
) -> str:
    schema = quoted(schema_name)
    return f"""INSERT INTO {schema}.fhir_formulary_twin_admission
        (source_id, baseline_dataset_id, baseline_run_id,
         candidate_dataset_id, candidate_run_id, predecessor_dataset_id,
         cutoff_at, source_configuration_hash, acquisition_contract_hash,
         list_count, alias_count, medication_count, coverage_hash,
         membership_hash, alternative_count, alternative_hash,
         baseline_verified_at, candidate_verified_at, admitted_at)
    VALUES ('source-a', '{baseline_id}', '{baseline_run_id}',
            '{candidate_id}', '{candidate_run_id}', 'seed-a', '2026-08-08Z',
            '{HASHES["source"]}', '{HASHES["contract"]}', 2, 3,
            {medication_count}, '{HASHES["coverage"]}', '{HASHES["membership"]}',
            1, '{HASHES["alternative"]}', '2026-08-08T00:01Z',
            '2026-08-08T00:02Z', '2026-08-08T00:05Z')"""
