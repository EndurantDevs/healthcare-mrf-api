# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Taxonomy authority starts empty and advances only at actual writer boundaries."""

from pathlib import Path

import pytest
from alembic.script import ScriptDirectory
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError

from tests.test_reference_source_generation_postgres import (
    _observe,
    _read_authority,
    _run_migration,
    _source_family,
)

_MIGRATION = Path(__file__).resolve().parents[1] / "alembic/versions/20261006010000_nucc_reference_result_generation.py"


def test_taxonomy_migration_extends_one_linear_history():
    scripts = ScriptDirectory(str(_MIGRATION.parent.parent))
    assert len(scripts.get_heads()) == 1
    assert scripts.get_revision("20261006010000_nucc_reference_result_generation").down_revision == (
        "20261007000000_custom_import_rejection_anti_joins"
    )


async def _install_taxonomy(connection, schema):
    await connection.execute(text(f'CREATE TABLE "{schema}".nucc_taxonomy (id bigint, marker text)'))
    await connection.execute(text(f"INSERT INTO \"{schema}\".nucc_taxonomy VALUES (1,'legacy')"))
    await _run_migration(connection, _MIGRATION, "upgrade")


@pytest.mark.asyncio
async def test_taxonomy_seed_preserves_prior_rows_and_does_not_attest_legacy_data(monkeypatch):
    async with _source_family(monkeypatch) as (engine, schema, sessions):
        table = f'"{schema}".reference_family_result_generation'
        async with engine.begin() as connection:
            before = (await connection.execute(text(f"SELECT * FROM {table} ORDER BY importer_id"))).all()
            await _install_taxonomy(connection, schema)
            after = (
                await connection.execute(text(f"SELECT * FROM {table} WHERE importer_id<>'nucc' ORDER BY importer_id"))
            ).all()
            assert after == before
            seed = await _read_authority(connection, schema, "nucc")
            assert seed.local_generation == 0 and seed.serving_generation is None and seed.relation_oids is None
            assert (
                await connection.scalar(text(f"SELECT source_revision_tracked FROM {table} WHERE importer_id='nucc'"))
                is False
            )
        with pytest.raises(RuntimeError, match="tracking is unavailable"):
            async with sessions.begin() as session:
                await _observe(session, schema, "nucc")
        async with engine.begin() as connection:
            await _run_migration(connection, _MIGRATION, "downgrade")
            assert (await connection.execute(text(f"SELECT * FROM {table} ORDER BY importer_id"))).all() == before
            assert await connection.scalar(text(f'SELECT marker FROM "{schema}".nucc_taxonomy')) == "legacy"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "statement",
    (
        "INSERT INTO {table} VALUES (2,'inserted')",
        "UPDATE {table} SET marker='updated'",
        "DELETE FROM {table}",
        "TRUNCATE {table}",
    ),
)
async def test_taxonomy_source_boundary_installs_statement_guards_and_preserves_evidence(monkeypatch, statement):
    async with _source_family(monkeypatch) as (engine, schema, sessions):
        async with engine.begin() as connection:
            await _install_taxonomy(connection, schema)
        async with sessions.begin() as session:
            first = await _observe(session, schema, "nucc", bootstrap=True)
            assert first.local_generation == 1 and len(first.relation_oids) == 1
        async with engine.begin() as connection:
            guard_type = await connection.scalar(
                text(
                    "SELECT tgtype FROM pg_catalog.pg_trigger WHERE "
                    f"tgrelid='\"{schema}\".nucc_taxonomy'::regclass AND NOT tgisinternal"
                )
            )
            assert guard_type == 60  # One AFTER STATEMENT boundary, not a per-row trigger.
            await connection.execute(text(statement.format(table=f'"{schema}".nucc_taxonomy')))
        async with sessions.begin() as session:
            updated = await _observe(session, schema, "nucc")
            assert updated.local_generation == first.local_generation + 1
            assert updated.relation_oids == first.relation_oids
            with pytest.raises(RuntimeError, match="evidence prevents downgrade"):
                await _run_migration(await session.connection(), _MIGRATION, "downgrade")


@pytest.mark.asyncio
async def test_taxonomy_generation_rejects_a_multi_relation_identity(monkeypatch):
    async with _source_family(monkeypatch) as (engine, schema, _sessions):
        async with engine.begin() as connection:
            await _install_taxonomy(connection, schema)
        with pytest.raises(DBAPIError) as rejected:
            async with engine.begin() as connection:
                await connection.execute(
                    text(
                        f'UPDATE "{schema}".reference_family_result_generation SET origin_lineage_id=local_lineage_id, '
                        "origin_generation=1,published_at=transaction_timestamp(),relation_oids=ARRAY[1,2] "
                        "WHERE importer_id='nucc'"
                    )
                )
        assert rejected.value.orig.sqlstate == "23514"
