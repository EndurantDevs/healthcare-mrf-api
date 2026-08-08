"""Disposable PostgreSQL proof for JSON-null preclaim parity."""

from __future__ import annotations

import pytest

from tests.test_ptg_wave_recovery_storage_postgres import (
    _dsn,
    _evidence,
    _insert_successor,
    _insert_supersession,
    _install_migration,
    _quote,
    asyncpg,
)


@pytest.mark.asyncio
async def test_json_null_patch_accepts_sql_null_run_error(monkeypatch):
    dsn = _dsn()
    schema = "wave_recovery_sql_null_error"
    quoted = _quote(schema)
    connection = await asyncpg.connect(dsn)
    try:
        await _install_migration(connection, monkeypatch, schema)
        await connection.execute(
            f"UPDATE {quoted}.import_run SET error = NULL "
            "WHERE run_id = 'run-1'"
        )
        wave_id = "sql-null-successor"
        evidence, canonical = _evidence(wave_id)
        cohort_by_field = {
            "schema_version": "healthporta.ptg-import-wave-attestation.v3",
            "wave_id": wave_id,
            "supersession": evidence,
        }

        async with connection.transaction():
            await _insert_supersession(
                connection,
                schema,
                wave_id,
                evidence,
                canonical,
            )
            await _insert_successor(
                connection,
                schema,
                wave_id,
                "admitted",
                cohort_by_field,
            )

        assert await connection.fetchval(
            f"SELECT count(*) FROM {quoted}.ptg_import_wave_supersession "
            "WHERE successor_wave_id = $1",
            wave_id,
        ) == 1
    finally:
        await connection.execute(f"DROP SCHEMA IF EXISTS {quoted} CASCADE")
        await connection.close()


@pytest.mark.asyncio
async def test_json_null_patch_rejects_representative_non_null_errors(
    monkeypatch,
):
    dsn = _dsn()
    schema = "wave_recovery_non_null_error"
    quoted = _quote(schema)
    connection = await asyncpg.connect(dsn)
    try:
        await _install_migration(connection, monkeypatch, schema)
        for index, error_json in enumerate(('{}', '"synthetic"', 'false')):
            await connection.execute(
                f"UPDATE {quoted}.import_run SET error = $1::jsonb "
                "WHERE run_id = 'run-1'",
                error_json,
            )
            wave_id = f"blocked-successor-{index}"
            evidence, canonical = _evidence(wave_id)
            cohort_by_field = {
                "schema_version": "healthporta.ptg-import-wave-attestation.v3",
                "wave_id": wave_id,
                "supersession": evidence,
            }

            with pytest.raises(asyncpg.PostgresError, match="PRECLAIM_REQUIRED"):
                async with connection.transaction():
                    await _insert_supersession(
                        connection,
                        schema,
                        wave_id,
                        evidence,
                        canonical,
                    )
                    await _insert_successor(
                        connection,
                        schema,
                        wave_id,
                        "admitted",
                        cohort_by_field,
                    )
            assert await connection.fetchval(
                f"SELECT count(*) FROM {quoted}.ptg_import_wave_supersession"
            ) == 0
            assert await connection.fetchval(
                f"SELECT count(*) FROM {quoted}.ptg_import_wave "
                "WHERE wave_id = $1",
                wave_id,
            ) == 0
    finally:
        await connection.execute(f"DROP SCHEMA IF EXISTS {quoted} CASCADE")
        await connection.close()
