# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import asyncio
import hashlib
import os
import uuid
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock

import asyncpg
import pytest
from sqlalchemy.engine import make_url

from process import tennessee_profile_binding as binding
from process import tennessee_profile_registry as registry
from process.massachusetts_profile_acquisition import encoded_json, write_new_json
from tests.test_tennessee_profile_binding import _candidate


class RegistryCursor:
    def __init__(self, candidates):
        self.candidates = candidates

    async def cursor(self, _query, *, prefetch):
        assert prefetch == 500
        for candidate in self.candidates:
            yield candidate


@pytest.mark.parametrize("failure", ["count", "row_size", "total_size", "order", "cancel"])
async def test_incomplete_or_unbounded_cursor_is_rejected(monkeypatch, failure):
    candidates = [_candidate(), _candidate(taxonomy_occurrence_checksum=18)]
    expected, progress = 2, AsyncMock()
    if failure == "count":
        expected = 3
    elif failure == "row_size":
        monkeypatch.setattr(registry, "MAX_ROW_BYTES", 1)
    elif failure == "total_size":
        monkeypatch.setattr(binding, "MAX_SNAPSHOT_BYTES", 1024 * 1024)
    elif failure == "order":
        candidates.reverse()
    else:
        progress.side_effect = asyncio.CancelledError
    with pytest.raises(asyncio.CancelledError if failure == "cancel" else ValueError):
        await registry._registry_rows(RegistryCursor(candidates), "synthetic", expected, progress)


@asynccontextmanager
async def _database(monkeypatch):
    database_dsn = os.getenv("HLTHPRT_PROVIDER_DIRECTORY_PROFILE_POSTGRES_DSN")
    if not database_dsn:
        pytest.skip("set the profile PostgreSQL DSN for native TN snapshot tests")
    url = make_url(database_dsn).set(drivername="postgresql")
    connection = await asyncpg.connect(url.render_as_string(hide_password=False))
    schema = "tn_registry_" + uuid.uuid4().hex
    is_created = False
    try:
        assert "test" in (await connection.fetchval("SELECT current_database()")).lower()
        await connection.execute(f"CREATE SCHEMA {schema}")
        is_created = True
        await connection.execute(f"""CREATE TABLE {schema}.npi (
            npi bigint, entity_type_code integer, provider_first_name text, provider_middle_name text,
            provider_last_name text, provider_name_suffix_text text);
            CREATE TABLE {schema}.npi_taxonomy (
            npi bigint, checksum bigint, provider_license_number text, provider_license_number_state_code text,
            healthcare_provider_taxonomy_code text, healthcare_provider_primary_taxonomy_switch text);
            CREATE TABLE {schema}.nucc_taxonomy (code text, grouping text);
            INSERT INTO {schema}.npi VALUES (1000000004, 1, 'Alex', 'Morgan', 'Example', NULL);
            INSERT INTO {schema}.nucc_taxonomy VALUES ('207R00000X', 'Allopathic & Osteopathic Physicians');
            INSERT INTO {schema}.npi_taxonomy VALUES
            (1000000004,17,'123','TN','207R00000X','Y'), (1000000004,17,'123','TN','207R00000X','Y'),
            (1000000004,18,NULL,'TN','UNKNOWN','N'), (1000000020,20,'123','TN','207R00000X','Y'),
            (1000000004,21,'123','FL','207R00000X','Y')""")
        monkeypatch.setattr(registry, "db", SimpleNamespace(engine=SimpleNamespace(url=url)))
        yield connection, schema
    finally:
        try:
            if is_created:
                await connection.execute(f"DROP SCHEMA {schema} CASCADE")
                assert await connection.fetchval("SELECT to_regnamespace($1::text)", schema) is None
        finally:
            await connection.close()
            assert connection.is_closed()


async def test_native_snapshot_keeps_all_occurrences_and_locks_relations(monkeypatch, tmp_path):
    async with _database(monkeypatch) as (writer, schema):
        observed_connections = []
        open_connection = registry._open_connection

        async def tracked_open():
            connection = await open_connection()
            observed_connections.append(connection)
            return connection

        monkeypatch.setattr(registry, "_open_connection", tracked_open)

        async def progress(done, total):
            if done == 0:
                return
            assert done == total == 4
            await writer.execute(
                f"INSERT INTO {schema}.npi_taxonomy VALUES (1000000004,22,'999','TN','207R00000X','Y')"
            )
            await writer.execute("SET lock_timeout='50ms'")
            with pytest.raises(asyncpg.LockNotAvailableError):
                await writer.execute(f"ALTER TABLE {schema}.npi RENAME TO replacement")

        snapshot = await registry.capture_registry_snapshot(schema, progress)
        assert len(observed_connections) == 1 and observed_connections[0].is_closed()
        candidates = snapshot["registry_rows"]
        assert snapshot["row_count"] == snapshot["expected_source_row_count"] == 4
        assert candidates[0] == candidates[1]
        assert candidates[2]["license_number"] is None and candidates[2]["joined_taxonomy_code"] is None
        assert candidates[3]["joined_npi"] is None
        assert await writer.fetchval(f"SELECT count(*) FROM {schema}.npi_taxonomy") == 6
        content = encoded_json(candidates)
        assert len(content) == snapshot["registry_rows_bytes"]
        assert hashlib.sha256(content).hexdigest() == snapshot["registry_rows_sha256"]
        path = tmp_path / "snapshot.json"
        write_new_json(path, snapshot)
        pin = hashlib.sha256(encoded_json(snapshot)).hexdigest()
        assert binding.read_registry_snapshot(path, snapshot_sha256=pin, source_schema=schema) == snapshot
        with pytest.raises(ValueError, match="snapshot_scope_invalid"):
            binding.read_registry_snapshot(path, snapshot_sha256=pin)


async def test_native_capture_cancellation_closes_its_connection(monkeypatch):
    async with _database(monkeypatch) as (_, schema):
        connection = await registry._open_connection()
        monkeypatch.setattr(registry, "_open_connection", AsyncMock(return_value=connection))

        async def progress(done, _total):
            if done:
                raise asyncio.CancelledError

        with pytest.raises(asyncio.CancelledError):
            await registry.capture_registry_snapshot(schema, progress)
        assert connection.is_closed()
