# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import importlib
import uuid

import pytest
from sqlalchemy.exc import OperationalError

from db.connection import Database


address_canon = importlib.import_module("process.ext.address_canon")


async def _require_test_database(database):
    try:
        database_name = str(await database.scalar("SELECT current_database();") or "")
    except (OSError, OperationalError):
        pytest.skip("archive swap rollback test needs disposable Postgres")
    if "test" not in database_name.lower():
        pytest.skip("archive swap rollback test needs a test database")


async def _create_swap_fixture(database, schema):
    statement_list = [
        f'CREATE TABLE "{schema}".address_archive (checksum bigint);',
        f'CREATE TABLE "{schema}".address_archive_v2 (address_key text);',
        f'CREATE TABLE "{schema}".address_archive_legacy (marker text);',
        f'CREATE TABLE "{schema}".address_checksum_map '
        '(checksum bigint, address_key text);',
        f'CREATE TABLE "{schema}".address_checksum_collision '
        '(checksum bigint, address_key text);',
        f'INSERT INTO "{schema}".address_archive VALUES (10);',
        f'INSERT INTO "{schema}".address_archive_v2 VALUES (\'key-1\');',
        f'INSERT INTO "{schema}".address_archive_legacy VALUES (\'keep\');',
        f'INSERT INTO "{schema}".address_checksum_map VALUES (10, \'key-1\');',
    ]
    for statement in statement_list:
        await database.status(statement)


@pytest.mark.asyncio
async def test_archive_swap_rolls_back_post_rename_verification_failure(monkeypatch):
    database = Database()
    schema = f"address_swap_{uuid.uuid4().hex[:12]}"
    is_schema_created = False
    try:
        await database.connect()
        await _require_test_database(database)
        await database.status(f'CREATE SCHEMA "{schema}";')
        is_schema_created = True
        await _create_swap_fixture(database, schema)
        original_fingerprint = address_canon._legacy_archive_fingerprint

        async def changed_post_swap_fingerprint(session, table):
            fingerprint_by_field = await original_fingerprint(session, table)
            if table.endswith('"address_archive_legacy"'):
                fingerprint_by_field["checksum_sum"] = 11
            return fingerprint_by_field

        monkeypatch.setattr(address_canon, "db", database)
        monkeypatch.setattr(
            address_canon,
            "_legacy_archive_fingerprint",
            changed_post_swap_fingerprint,
        )

        with pytest.raises(RuntimeError, match="fingerprint changed"):
            await address_canon.swap_archive_v2_to_current(
                schema=schema,
                allow_replace_backup=True,
            )

        assert await database.scalar(
            f'SELECT count(*) FROM "{schema}".address_archive;'
        ) == 1
        assert await database.scalar(
            f'SELECT count(*) FROM "{schema}".address_archive_v2;'
        ) == 1
        assert await database.scalar(
            f'SELECT count(*) FROM "{schema}".address_archive_legacy '
            "WHERE marker = 'keep';"
        ) == 1
    finally:
        if is_schema_created:
            await database.status(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE;')
        await database.disconnect()
