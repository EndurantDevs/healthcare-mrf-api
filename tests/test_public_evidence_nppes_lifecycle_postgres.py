# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""PostgreSQL parity proof for snapshot-effective NPPES lifecycle dates."""

from __future__ import annotations

from pathlib import Path

import asyncpg
import pytest

from public_evidence import nppes_registry_row_projection as row_projection
from process.nppes_public_evidence_catalog import assert_nppes_admission_catalog
from process.nppes_public_evidence_writer_contract import (
    NppesPublicEvidenceWriterError,
)
from tests.public_evidence_npi_enumeration_postgres_support import (
    npi_enumeration_schema,
)
from tests.public_evidence_nppes_admission_postgres_support import (
    admit_replay,
    load_admission_migration,
    load_lifecycle_migration,
    nppes_admission_schema,
    prepared_replay,
)
from tests.public_evidence_nppes_registry_support import (
    equal_day_reactivated_type_1_row,
    future_deactivation_type_1_row,
    future_last_update_type_1_row,
    orphan_reactivated_type_2_row,
)
from tests.public_evidence_storage_postgres_support import (
    connect,
    run_migration_action,
)


ADMISSION_TABLE = "public_evidence_nppes_registry_admission"
MEMBER_TABLE = "public_evidence_nppes_registry_member"
COMMON_TABLE = "public_evidence_record"
TYPED_TABLE = "public_evidence_npi_enumeration"
ADMISSION_TRIGGER = "public_evidence_nppes_registry_admission_integrity_guard"
OLD_VALIDATOR = "validate_public_evidence_nppes_registry_admission"
NEW_VALIDATOR = (
    "validate_public_evidence_nppes_registry_admission_lifecycle_v2"
)
OBSERVED_LIFECYCLE_ROWS = (
    equal_day_reactivated_type_1_row(),
    orphan_reactivated_type_2_row(),
    future_deactivation_type_1_row(),
    future_last_update_type_1_row(),
    (
        "1003001314",
        "1",
        "05/23/2005",
        "07/12/2026",
        "06/20/2026",
        "06/15/2026",
    ),
    (
        "1000000004",
        "2",
        "05/23/2005",
        "07/13/2026",
        "06/20/2026",
        "07/13/2026",
    ),
)
EXPECTED_LIFECYCLE_ROWS = (
    (1, "2026-07-12", "2026-06-15", "2026-06-15", "active", "2026-06-15"),
    (2, "2026-07-12", None, "2026-06-15", "active", "2026-06-15"),
    (3, "2026-07-12", "2026-07-13", None, "active", "2005-05-23"),
    (4, "2026-07-13", None, None, "active", "2005-05-23"),
    (5, "2026-07-12", "2026-06-20", "2026-06-15", "deactivated", "2026-06-20"),
    (6, "2026-07-13", "2026-06-20", "2026-07-13", "deactivated", "2026-06-20"),
)


@pytest.mark.parametrize(
    ("runtime_schema", "legacy_schema", "expected_schema"),
    (
        (None, None, "mrf"),
        ("runtime_test", None, "runtime_test"),
        (None, "legacy_test", "legacy_test"),
        ("shared_test", "shared_test", "shared_test"),
    ),
)
def test_lifecycle_migration_schema_resolution_is_exact(
    monkeypatch: pytest.MonkeyPatch,
    runtime_schema: str | None,
    legacy_schema: str | None,
    expected_schema: str,
) -> None:
    for variable_name, schema_value in (
        ("HLTHPRT_DB_SCHEMA", runtime_schema),
        ("DB_SCHEMA", legacy_schema),
    ):
        if schema_value is None:
            monkeypatch.delenv(variable_name, raising=False)
        else:
            monkeypatch.setenv(variable_name, schema_value)
    migration = load_lifecycle_migration()
    assert migration.revision == "20260809020000_nppes_lifecycle_date_tolerance"
    assert migration.down_revision == (
        "20260809010000_provider_directory_effective_endpoint_identity"
    )
    assert migration._schema() == expected_schema


def test_lifecycle_migration_rejects_conflicting_schema_configuration(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "runtime_test")
    monkeypatch.setenv("DB_SCHEMA", "legacy_test")
    migration = load_lifecycle_migration()
    with pytest.raises(RuntimeError):
        migration._schema()


def _accept_future_enumeration_for_sql_boundary(
    _snapshot,
    _source_dates,
    _entity_type,
) -> tuple[str, str, None]:
    return "active", "2026-06-15T00:00:00Z", None


async def _admission_trigger_record(
    connection: asyncpg.Connection,
    schema: str,
) -> asyncpg.Record:
    record = await connection.fetchrow(
        "SELECT trigger_record.oid AS trigger_oid, procedure.oid AS function_oid, "
        "procedure.proname AS function_name, procedure.prosecdef, "
        "procedure.proconfig, trigger_record.tgenabled::text, "
        "trigger_record.tgdeferrable, trigger_record.tginitdeferred "
        "FROM pg_trigger AS trigger_record JOIN pg_class AS relation "
        "ON relation.oid=trigger_record.tgrelid JOIN pg_namespace AS namespace "
        "ON namespace.oid=relation.relnamespace JOIN pg_proc AS procedure "
        "ON procedure.oid=trigger_record.tgfoid WHERE namespace.nspname=$1 "
        "AND relation.relname=$2 AND trigger_record.tgname=$3",
        schema,
        ADMISSION_TABLE,
        ADMISSION_TRIGGER,
    )
    assert record is not None
    return record


@pytest.mark.asyncio
async def test_lifecycle_migration_preserves_the_sealed_trigger_identity() -> None:
    async with npi_enumeration_schema() as (engine, url, schema, _migration):
        admission = load_admission_migration()
        lifecycle = load_lifecycle_migration()
        admission._schema = lambda: schema
        lifecycle._schema = lambda: schema
        await run_migration_action(engine, admission, "upgrade")
        connection = await connect(url)
        try:
            before = await _admission_trigger_record(connection, schema)
            assert before["function_name"] == OLD_VALIDATOR
            with pytest.raises(NppesPublicEvidenceWriterError):
                await assert_nppes_admission_catalog(connection, schema)

            await run_migration_action(engine, lifecycle, "upgrade")
            after = await _admission_trigger_record(connection, schema)
            assert after["trigger_oid"] == before["trigger_oid"]
            assert after["function_oid"] == before["function_oid"]
            assert after["function_name"] == NEW_VALIDATOR
            assert after["prosecdef"] is True
            assert after["proconfig"] == ["search_path=pg_catalog"]
            assert after["tgenabled"] == "A"
            assert after["tgdeferrable"] is True
            assert after["tginitdeferred"] is True
            assert not await connection.fetchval(
                "SELECT has_function_privilege('public', $1, 'EXECUTE')",
                f"{schema}.{NEW_VALIDATOR}()",
            )
            await assert_nppes_admission_catalog(connection, schema)

            await run_migration_action(engine, lifecycle, "downgrade")
            restored = await _admission_trigger_record(connection, schema)
            assert restored["trigger_oid"] == before["trigger_oid"]
            assert restored["function_oid"] == before["function_oid"]
            assert restored["function_name"] == OLD_VALIDATOR
        finally:
            await connection.close()


@pytest.mark.asyncio
async def test_lifecycle_v2_rejects_future_enumeration_before_seal(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        row_projection,
        "_temporal_projection",
        _accept_future_enumeration_for_sql_boundary,
    )
    future_enumeration_row = (
        "1003000100",
        "1",
        "07/13/2026",
        "07/13/2026",
        "",
        "06/15/2026",
    )
    replay = await prepared_replay(tmp_path, (future_enumeration_row,))
    async with nppes_admission_schema() as (_engine, url, schema, _migration):
        connection = await connect(url)
        try:
            with pytest.raises(NppesPublicEvidenceWriterError):
                await admit_replay(connection, schema, replay)
            for table_name in (
                ADMISSION_TABLE,
                "public_evidence_nppes_registry_admission_seal",
                MEMBER_TABLE,
            ):
                assert await connection.fetchval(
                    f'SELECT count(*) FROM "{schema}"."{table_name}"'
                ) == 0
        finally:
            await connection.close()


@pytest.mark.asyncio
async def test_lifecycle_v2_admits_and_seals_observed_as_of_shapes(
    tmp_path: Path,
) -> None:
    replay = await prepared_replay(tmp_path, OBSERVED_LIFECYCLE_ROWS)
    async with nppes_admission_schema() as (_engine, url, schema, _migration):
        connection = await connect(url)
        try:
            admission_receipt = await admit_replay(connection, schema, replay)
            assert admission_receipt.write_state == "inserted"
            assert await connection.fetchval(
                f'SELECT count(*) FROM "{schema}".'
                '"public_evidence_nppes_registry_admission_seal"'
            ) == 1
            lifecycle_rows = await connection.fetch(
                "SELECT member.source_row_ordinal, "
                "to_char(member.last_update_date, 'YYYY-MM-DD') AS last_update, "
                "to_char(member.npi_deactivation_date, 'YYYY-MM-DD') AS deactivation, "
                "to_char(member.npi_reactivation_date, 'YYYY-MM-DD') AS reactivation, "
                "typed.enumeration_state, "
                "to_char(common.effective_start_at AT TIME ZONE 'UTC', "
                "'YYYY-MM-DD') AS effective_start "
                f'FROM "{schema}"."{MEMBER_TABLE}" AS member '
                f'JOIN "{schema}"."{COMMON_TABLE}" AS common '
                "ON common.evidence_ref=member.evidence_ref "
                "AND common.nppes_admission_ref=member.admission_ref "
                f'JOIN "{schema}"."{TYPED_TABLE}" AS typed '
                "ON typed.evidence_ref=member.evidence_ref "
                "AND typed.nppes_admission_ref=member.admission_ref "
                "ORDER BY member.source_row_ordinal"
            )
            assert tuple(
                tuple(lifecycle_row.values())
                for lifecycle_row in lifecycle_rows
            ) == EXPECTED_LIFECYCLE_ROWS
        finally:
            await connection.close()
