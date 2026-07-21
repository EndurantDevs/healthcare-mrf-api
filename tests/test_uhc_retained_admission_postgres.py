# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

from dataclasses import replace
from pathlib import Path

import asyncpg
import pytest
import sqlalchemy as sa
from sqlalchemy.ext.asyncio import create_async_engine

import process.uhc_retained_source_registry as source_registry
from process.uhc_retained_registry_contract import UHCSourceBindingMismatch
from process.uhc_retained_source_registry import register_verified_source
from tests.uhc_retained_postgres_test_support import (
    ADOPTION_SCHEMA,
    EXPECTED_ADMISSION_TABLES,
    FRESH_SCHEMA,
    admission_database,
    _admission_table_oids,
    _asyncpg_connection,
    _binding,
    _database_url,
    _digest,
    _drop_schema,
    _insert_catalog_file,
    _install_catalog_contract,
    _load_migration,
    _native_binary,
    _native_source,
    _proof_counts,
    _reset_schema,
    _upgrade_admission,
    _write_source,
)


async def _register_shared_layouts(
    connection,
    *,
    source_path,
    tmp_path,
    artifact_sha256,
    artifact_byte_count,
    first_binding,
    second_binding,
):
    source_four = await _native_source(
        source_path,
        tmp_path,
        artifact_sha256,
        artifact_byte_count,
        4,
    )
    await register_verified_source(
        connection,
        binding=first_binding,
        source=source_four,
    )
    for binding in (first_binding, second_binding):
        await register_verified_source(
            connection,
            binding=binding,
            source=await _native_source(
                source_path,
                tmp_path,
                artifact_sha256,
                artifact_byte_count,
                4,
            ),
        )
    await register_verified_source(
        connection,
        binding=first_binding,
        source=await _native_source(
            source_path,
            tmp_path,
            artifact_sha256,
            artifact_byte_count,
            8,
        ),
    )
    return source_four


async def _assert_catalog_and_reference_constraints(
    connection,
    *,
    source_path,
    tmp_path,
    artifact_sha256,
    artifact_byte_count,
    first_binding,
    source_four,
) -> None:
    wrong_entry = replace(
        first_binding,
        catalog_entry_sha256=_digest("wrong-entry"),
    )
    with pytest.raises(UHCSourceBindingMismatch, match="catalog file identity"):
        await register_verified_source(
            connection,
            binding=wrong_entry,
            source=await _native_source(
                source_path,
                tmp_path,
                artifact_sha256,
                artifact_byte_count,
                4,
            ),
        )
    with pytest.raises(asyncpg.CheckViolationError):
        await connection.execute(
            f'''INSERT INTO "{FRESH_SCHEMA}".
                "provider_directory_uhc_artifact_reference" (
                    content_sha256, artifact_kind, contract_version,
                    layout_artifact_sha256, range_count,
                    catalog_set_sha256, source_file_id,
                    storage_uri, created_at, retain_until, released_at
                ) VALUES ($1, 'manifest', 2, $2, 257, $3, $4, $5,
                          now(), NULL, NULL)''',
            _digest("excessive-reference"),
            artifact_sha256,
            first_binding.catalog_set_sha256,
            first_binding.source_file_id,
            Path(source_four.raw_artifact.manifest_path).as_uri(),
        )


async def _assert_raw_tamper_rejected(
    connection,
    binding,
    *,
    source_path,
    tmp_path,
    artifact_sha256,
    artifact_byte_count,
) -> None:
    await connection.execute(
        f'''UPDATE "{FRESH_SCHEMA}".provider_directory_uhc_raw_artifact
               SET byte_count=byte_count + 1
             WHERE artifact_sha256=$1''',
        artifact_sha256,
    )
    with pytest.raises(UHCSourceBindingMismatch, match="raw artifact"):
        await register_verified_source(
            connection,
            binding=binding,
            source=await _native_source(
                source_path,
                tmp_path,
                artifact_sha256,
                artifact_byte_count,
                4,
            ),
        )
    await connection.execute(
        f'''UPDATE "{FRESH_SCHEMA}".provider_directory_uhc_raw_artifact
               SET byte_count=$2
             WHERE artifact_sha256=$1''',
        artifact_sha256,
        artifact_byte_count,
    )


async def _assert_released_reference_rejected(
    connection,
    binding,
    *,
    source_path,
    tmp_path,
    artifact_sha256,
    artifact_byte_count,
) -> None:
    await connection.execute(
        f'''UPDATE "{FRESH_SCHEMA}".
                provider_directory_uhc_artifact_reference
               SET released_at=now()
             WHERE catalog_set_sha256=$1 AND source_file_id=$2
               AND artifact_kind='raw' AND contract_version=0
               AND range_count=0''',
        binding.catalog_set_sha256,
        binding.source_file_id,
    )
    with pytest.raises(UHCSourceBindingMismatch, match="artifact reference"):
        await register_verified_source(
            connection,
            binding=binding,
            source=await _native_source(
                source_path,
                tmp_path,
                artifact_sha256,
                artifact_byte_count,
                4,
            ),
        )
    released_at = await connection.fetchval(
        f'''SELECT released_at FROM "{FRESH_SCHEMA}".
                provider_directory_uhc_artifact_reference
            WHERE catalog_set_sha256=$1 AND source_file_id=$2
              AND artifact_kind='raw' AND contract_version=0
              AND range_count=0''',
        binding.catalog_set_sha256,
        binding.source_file_id,
    )
    assert released_at is not None


@pytest.mark.asyncio
async def test_admission_migration_fresh_and_rejects_existing_relations(monkeypatch):
    database_url = _database_url()
    engine = create_async_engine(database_url.set(drivername="postgresql+asyncpg"))
    migration = _load_migration()
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", FRESH_SCHEMA)
    try:
        await _reset_schema(engine, FRESH_SCHEMA)
        await _install_catalog_contract(engine, FRESH_SCHEMA)
        await _upgrade_admission(engine, migration)
        fresh_oids = await _admission_table_oids(engine, FRESH_SCHEMA)
        assert set(fresh_oids) == EXPECTED_ADMISSION_TABLES

        monkeypatch.setenv("HLTHPRT_DB_SCHEMA", ADOPTION_SCHEMA)
        await _reset_schema(engine, ADOPTION_SCHEMA)
        await _install_catalog_contract(engine, ADOPTION_SCHEMA)
        await _upgrade_admission(engine, migration)
        before_adoption_oids = await _admission_table_oids(engine, ADOPTION_SCHEMA)
        with pytest.raises(sa.exc.ProgrammingError):
            await _upgrade_admission(engine, migration)
        after_adoption_oids = await _admission_table_oids(engine, ADOPTION_SCHEMA)
        assert before_adoption_oids == after_adoption_oids
        assert set(after_adoption_oids) == EXPECTED_ADMISSION_TABLES
    finally:
        await _drop_schema(engine, FRESH_SCHEMA)
        await _drop_schema(engine, ADOPTION_SCHEMA)
        await engine.dispose()


@pytest.mark.asyncio
async def test_native_registry_multi_layout_cross_catalog_and_constraints(
    tmp_path,
    monkeypatch,
):
    async with admission_database(tmp_path, monkeypatch) as (connection, _database_url):
        source_path, artifact_sha256, artifact_byte_count = _write_source(
            tmp_path,
            "provider",
        )
        first_binding = _binding(
            artifact_sha256,
            artifact_byte_count,
            catalog_label="catalog-one",
        )
        second_binding = replace(
            first_binding,
            catalog_set_sha256=_digest("catalog-two"),
        )
        await _insert_catalog_file(connection, FRESH_SCHEMA, first_binding)
        await _insert_catalog_file(connection, FRESH_SCHEMA, second_binding)
        source_four = await _register_shared_layouts(
            connection,
            source_path=source_path,
            tmp_path=tmp_path,
            artifact_sha256=artifact_sha256,
            artifact_byte_count=artifact_byte_count,
            first_binding=first_binding,
            second_binding=second_binding,
        )
        assert await _proof_counts(connection, FRESH_SCHEMA) == {
            "raw_count": 1,
            "layout_count": 2,
            "binding_count": 2,
            "range_count": 12,
            "reference_count": 5,
        }
        await _assert_catalog_and_reference_constraints(
            connection,
            source_path=source_path,
            tmp_path=tmp_path,
            artifact_sha256=artifact_sha256,
            artifact_byte_count=artifact_byte_count,
            first_binding=first_binding,
            source_four=source_four,
        )


@pytest.mark.asyncio
async def test_native_registry_rolls_back_every_row_after_database_failure(
    tmp_path,
    monkeypatch,
):
    async with admission_database(tmp_path, monkeypatch) as (connection, _database_url):
        source_path, artifact_sha256, artifact_byte_count = _write_source(
            tmp_path,
            "rollback-provider",
        )
        binding = _binding(
            artifact_sha256,
            artifact_byte_count,
            catalog_label="rollback-catalog",
        )
        await _insert_catalog_file(connection, FRESH_SCHEMA, binding)
        await connection.execute(
            f'''CREATE FUNCTION "{FRESH_SCHEMA}".fail_uhc_range_insert()
                RETURNS trigger LANGUAGE plpgsql AS $$
                BEGIN
                    RAISE EXCEPTION 'forced UHC range failure';
                END;
                $$'''
        )
        await connection.execute(
            f'''CREATE TRIGGER fail_uhc_range_insert
                BEFORE INSERT ON "{FRESH_SCHEMA}".
                    provider_directory_uhc_raw_range
                FOR EACH ROW EXECUTE FUNCTION
                    "{FRESH_SCHEMA}".fail_uhc_range_insert()'''
        )
        verified_source = await _native_source(
            source_path,
            tmp_path,
            artifact_sha256,
            artifact_byte_count,
            4,
        )
        with pytest.raises(asyncpg.PostgresError, match="forced UHC range failure"):
            await register_verified_source(
                connection,
                binding=binding,
                source=verified_source,
            )
        assert await _proof_counts(connection, FRESH_SCHEMA) == {
            "raw_count": 0,
            "layout_count": 0,
            "binding_count": 0,
            "range_count": 0,
            "reference_count": 0,
        }


@pytest.mark.asyncio
async def test_native_registry_rejects_tamper_and_released_reference(
    tmp_path,
    monkeypatch,
):
    async with admission_database(tmp_path, monkeypatch) as (connection, _database_url):
        source_path, artifact_sha256, artifact_byte_count = _write_source(
            tmp_path,
            "tamper-provider",
        )
        binding = _binding(
            artifact_sha256,
            artifact_byte_count,
            catalog_label="tamper-catalog",
        )
        await _insert_catalog_file(connection, FRESH_SCHEMA, binding)
        await register_verified_source(
            connection,
            binding=binding,
            source=await _native_source(
                source_path,
                tmp_path,
                artifact_sha256,
                artifact_byte_count,
                4,
            ),
        )
        proof_argument_map = {
            "source_path": source_path,
            "tmp_path": tmp_path,
            "artifact_sha256": artifact_sha256,
            "artifact_byte_count": artifact_byte_count,
        }
        await _assert_raw_tamper_rejected(
            connection,
            binding,
            **proof_argument_map,
        )
        await _assert_released_reference_rejected(
            connection,
            binding,
            **proof_argument_map,
        )
