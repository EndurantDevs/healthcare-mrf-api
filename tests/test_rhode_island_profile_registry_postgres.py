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

from process import rhode_island_profile_registry as registry
from process.massachusetts_profile_acquisition import encoded_json, write_new_json
from tests.test_rhode_island_profile_registry import profile


@asynccontextmanager
async def registry_database(monkeypatch):
    database_dsn = os.getenv("HLTHPRT_PROVIDER_DIRECTORY_PROFILE_POSTGRES_DSN")
    if not database_dsn:
        pytest.skip("set the profile PostgreSQL DSN for native RI snapshot tests")
    url = make_url(database_dsn).set(drivername="postgresql")
    connection = await asyncpg.connect(url.render_as_string(hide_password=False))
    schema = "ri_registry_" + uuid.uuid4().hex
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
            INSERT INTO {schema}.npi VALUES (1003000126,1,'Alex','','Example',NULL),
                (1003000134,1,'Alex','','Example',NULL), (1003000142,2,NULL,NULL,NULL,NULL);
            INSERT INTO {schema}.nucc_taxonomy VALUES ('207R00000X','Allopathic & Osteopathic Physicians'),
                ('OTHER','Other Providers');
            INSERT INTO {schema}.npi_taxonomy VALUES
            (1003000126,17,'MD00001','RI','207R00000X','Y'), (1003000126,17,'MD00001','RI','207R00000X','Y'),
            (1003000126,18,NULL,'RI','UNKNOWN','N'), (1003000126,19,'DO00001','RI','207R00000X','N'),
            (1003000126,20,'00001','RI','207R00000X','N'), (1003000126,21,'MD00001','FL','207R00000X','N'),
            (1003000134,22,'MD00001','RI','207R00000X','Y'), (1003000142,23,'MD00003','RI','OTHER','Y'),
            (1000000020,24,'MD00002','RI','207R00000X','Y')""")
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


def assert_snapshot_occurrences(captured):
    candidates = captured["registry_rows"]
    assert captured["row_count"] == captured["expected_source_row_count"] == 8
    exact_md_candidates = [candidate for candidate in candidates if candidate["license_number"] == "MD00001"]
    assert len(exact_md_candidates) == 3 and exact_md_candidates[0] == exact_md_candidates[1]
    assert {candidate["npi"] for candidate in exact_md_candidates} == {1003000126, 1003000134}
    assert any(
        candidate["license_number"] is None and candidate["joined_taxonomy_code"] is None for candidate in candidates
    )
    assert any(candidate["license_number"] == "MD00002" and candidate["joined_npi"] is None for candidate in candidates)
    assert any(candidate["entity_type_code"] == 2 for candidate in candidates)
    assert {candidate["license_number"] for candidate in candidates if candidate["npi"] == 1003000126} == {
        "MD00001",
        "DO00001",
        "00001",
        None,
    }
    content = encoded_json(candidates)
    assert len(content) == captured["registry_rows_bytes"]
    assert hashlib.sha256(content).hexdigest() == captured["registry_rows_sha256"]


async def test_native_snapshot_preserves_occurrences_and_stable_relations(monkeypatch, tmp_path):
    async with registry_database(monkeypatch) as (writer, schema):
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
            assert done == total == 8
            await writer.execute(
                f"INSERT INTO {schema}.npi_taxonomy VALUES (1003000126,25,'MD00999','RI','207R00000X','Y')"
            )
            await writer.execute("SET lock_timeout='50ms'")
            with pytest.raises(asyncpg.LockNotAvailableError):
                await writer.execute(f"ALTER TABLE {schema}.npi RENAME TO replacement")

        captured = await registry.capture_registry_snapshot(schema, progress)
        assert len(observed_connections) == 1 and observed_connections[0].is_closed()
        assert_snapshot_occurrences(captured)
        assert await writer.fetchval(f"SELECT count(*) FROM {schema}.npi_taxonomy") == 10
        path = tmp_path / "snapshot.json"
        write_new_json(path, captured)
        pin = hashlib.sha256(path.read_bytes()).hexdigest()
        assert registry.read_registry_snapshot(path, snapshot_sha256=pin, source_schema=schema) == captured
        with pytest.raises(ValueError, match="snapshot_scope_invalid"):
            registry.read_registry_snapshot(path, snapshot_sha256=pin)
        bound_profiles = list(
            registry.bind_snapshot_profiles(
                [profile(), profile("DO00001"), profile("MD00002")],
                snapshot_path=path,
                snapshot_sha256=pin,
                source_schema=schema,
            )
        )
        assert [source_record["match_status"] for source_record, _ in bound_profiles] == [
            "ambiguous",
            "deterministic",
            "identity_conflict",
        ]
        assert all(
            not source_record["match_evidence"]["registry_binding"]["registry_completeness_verified"]
            for source_record, _ in bound_profiles
        )


@pytest.mark.parametrize("failure", ["cancel", "count_limit", "cursor_failure", "timeout"])
async def test_native_failure_closes_owned_connection(monkeypatch, failure):
    async with registry_database(monkeypatch) as (_, schema):
        connection = await registry._open_connection()
        monkeypatch.setattr(registry, "_open_connection", AsyncMock(return_value=connection))
        progress = AsyncMock()
        expected_exception, reason = ValueError, "count_invalid"
        if failure == "count_limit":
            monkeypatch.setattr(registry, "MAX_REGISTRY_ROWS", 1)
        elif failure == "cursor_failure":
            monkeypatch.setattr(
                registry, "_registry_rows", AsyncMock(side_effect=RuntimeError("synthetic_cursor_failure"))
            )
            expected_exception, reason = RuntimeError, "synthetic_cursor_failure"
        elif failure == "timeout":
            monkeypatch.setattr(registry, "_registry_rows", AsyncMock(side_effect=TimeoutError()))
            expected_exception, reason = TimeoutError, None
        else:

            async def cancel_after_capture(done, _total):
                if done:
                    raise asyncio.CancelledError

            progress = cancel_after_capture
            expected_exception, reason = asyncio.CancelledError, None
        with pytest.raises(expected_exception, match=reason):
            await registry.capture_registry_snapshot(schema, progress)
        assert connection.is_closed()
