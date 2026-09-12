# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Optional PostgreSQL proof of complete registry capture in an owned database."""

import asyncio
import hashlib
import os
import uuid
from contextlib import asynccontextmanager
from types import SimpleNamespace

import asyncpg
import pytest
from sqlalchemy.engine import make_url

from process import new_york_profile_registry as registry
from tests.test_new_york_profile_binding import _candidate


@asynccontextmanager
async def _database(monkeypatch):
    database_dsn = os.getenv("HLTHPRT_PROVIDER_DIRECTORY_PROFILE_POSTGRES_DSN")
    if not database_dsn:
        pytest.skip("set the profile PostgreSQL DSN for native registry tests")
    admin_url = make_url(database_dsn).set(drivername="postgresql")
    database_name = f"ny_profile_registry_test_{uuid.uuid4().hex}"
    database_url = admin_url.set(database=database_name)
    admin = await asyncpg.connect(admin_url.render_as_string(hide_password=False), timeout=10)
    connections, capture_connections = [], []
    is_creation_attempted = False
    original_open = registry._open_connection
    try:
        assert await admin.fetchval("SELECT count(*) FROM pg_database WHERE datname=$1", database_name) == 0
        is_creation_attempted = True
        await admin.execute(f'CREATE DATABASE "{database_name}" TEMPLATE template0')
        writer = await asyncpg.connect(database_url.render_as_string(hide_password=False), timeout=10)
        connections.append(writer)
        assert await writer.fetchval("SELECT current_database()") == database_name
        monkeypatch.setattr(registry, "db", SimpleNamespace(engine=SimpleNamespace(url=database_url)))

        async def track_connection():
            connection = await original_open()
            connections.append(connection)
            capture_connections.append(connection)
            assert await connection.fetchval("SELECT current_database()") == database_name
            return connection

        monkeypatch.setattr(registry, "_open_connection", track_connection)
        yield writer, capture_connections
    finally:
        try:
            await _close_connections(connections)
            if is_creation_attempted:
                await admin.execute(f'DROP DATABASE IF EXISTS "{database_name}"')
                assert await admin.fetchval("SELECT count(*) FROM pg_database WHERE datname=$1", database_name) == 0
                assert (
                    await admin.fetchval("SELECT count(*) FROM pg_stat_activity WHERE datname=$1", database_name) == 0
                )
        finally:
            await admin.close(timeout=5)


async def _close_connections(connections):
    try:
        await asyncio.gather(*(connection.close(timeout=5) for connection in connections), return_exceptions=True)
    finally:
        for connection in connections:
            if not connection.is_closed():
                connection.terminate()


def _expected_rows():
    candidates = [_candidate(taxonomy_occurrence_checksum=index) for index in range(1, 502)]
    candidates.append(_candidate(taxonomy_occurrence_checksum=1))
    candidates.extend(
        [
            _candidate(
                taxonomy_occurrence_checksum=502, taxonomy="UNMAPPED", joined_taxonomy_code=None, taxonomy_grouping=None
            ),
            _candidate(
                taxonomy_occurrence_checksum=503,
                taxonomy="164W00000X",
                joined_taxonomy_code="164W00000X",
                taxonomy_grouping="Nursing Service Providers",
            ),
            _candidate(taxonomy_occurrence_checksum=504, license_number="000123"),
            _candidate(taxonomy_occurrence_checksum=505, license_number=" 654321 "),
            _candidate(taxonomy_occurrence_checksum=506, license_number=None),
            _candidate(taxonomy_occurrence_checksum=507, license_number=""),
            _candidate(
                taxonomy_occurrence_checksum=508,
                license_number="333333",
                taxonomy="164W00000X",
                joined_taxonomy_code="164W00000X",
                taxonomy_grouping="Nursing Service Providers",
            ),
            _candidate(npi=1000000012, joined_npi=1000000012, entity_type_code=2),
            _candidate(npi=1000000020, joined_npi=None, entity_type_code=None, first_name=None, last_name=None),
        ]
    )
    return sorted(candidates, key=lambda candidate: (candidate["npi"], candidate["taxonomy_occurrence_checksum"]))


async def _seed_registry(connection, candidates):
    await connection.execute("""CREATE SCHEMA mrf;
        CREATE TABLE mrf.npi (npi bigint, entity_type_code integer, provider_first_name text,
            provider_middle_name text, provider_last_name text, provider_name_suffix_text text);
        CREATE TABLE mrf.nucc_taxonomy (code text, grouping text);
        CREATE TABLE mrf.npi_taxonomy (npi bigint, checksum bigint, provider_license_number text,
            provider_license_number_state_code text, healthcare_provider_taxonomy_code text,
            healthcare_provider_primary_taxonomy_switch text);
        INSERT INTO mrf.npi VALUES (1000000004,1,'Alex',NULL,'Example',NULL),
                                  (1000000012,2,'Alex',NULL,'Example',NULL);
        INSERT INTO mrf.nucc_taxonomy VALUES ('207R00000X','Allopathic & Osteopathic Physicians'),
                                            ('164W00000X','Nursing Service Providers');
    """)
    await connection.executemany(
        "INSERT INTO mrf.npi_taxonomy VALUES ($1,$2,$3,$4,$5,$6)",
        [
            (
                candidate["npi"],
                candidate["taxonomy_occurrence_checksum"],
                candidate["license_number"],
                candidate["license_state"],
                candidate["taxonomy"],
                candidate["primary_taxonomy_switch"],
            )
            for candidate in candidates
        ],
    )
    await connection.execute("INSERT INTO mrf.npi_taxonomy VALUES (1000000004,9000,'999999','CA','207R00000X','Y')")


def _check_capture(captured, candidates, tmp_path):
    assert captured["registry_rows"] == candidates
    assert captured["row_count"] == captured["expected_source_row_count"] == len(candidates)
    assert captured["snapshot"]["read_only"] == "on" and captured["snapshot"]["isolation"] == "repeatable read"
    assert captured["all_rows_received"] and captured["connection_closed"] and captured["status"] == "passed"
    content = registry.encoded_json(candidates)
    assert captured["registry_rows_bytes"] == len(content)
    assert captured["registry_rows_sha256"] == hashlib.sha256(content).hexdigest()
    snapshot_bytes = registry.encoded_json(captured)
    snapshot_path = tmp_path.resolve() / "snapshot.json"
    snapshot_path.write_bytes(snapshot_bytes)
    cohort = registry.build_acquisition_cohort(
        snapshot_path, snapshot_sha256=hashlib.sha256(snapshot_bytes).hexdigest()
    )
    assert [root["license_number"] for root in cohort["roots"]] == ["000123", "654321"]
    for root in cohort["roots"]:
        assert root["registry_occurrence_indexes"] == [
            index for index, candidate in enumerate(candidates) if candidate["license_number"] == root["license_number"]
        ]
    assert [root["registry_only_precondition"] for root in cohort["roots"]] == [
        "single_npi_source_identity_unverified",
        "registry_occurrence_or_name_conflict",
    ]
    assert cohort["summary"]["selected_conflicting_occurrence_count"] == 4
    assert cohort["summary"]["excluded_no_physician_root_count"] == 1
    assert cohort["summary"]["unsupported_license_row_count"] == 3
    assert not cohort["state_census"] and cohort["source_identity"] == "unverified"


async def test_native_capture_keeps_complete_snapshot_and_rejects_join_multiplication(monkeypatch, tmp_path):
    candidates = _expected_rows()
    async with _database(monkeypatch) as (writer, capture_connections):
        await _seed_registry(writer, candidates)
        progress_calls = []

        async def progress(current, total):
            progress_calls.append((current, total))
            if current == 500:
                with pytest.raises(asyncpg.ReadOnlySQLTransactionError):
                    async with capture_connections[-1].transaction():
                        await capture_connections[-1].execute("UPDATE mrf.npi SET entity_type_code=2 WHERE false")
                await writer.execute(
                    "INSERT INTO mrf.npi_taxonomy VALUES (1000000004,9001,'555555','NY','207R00000X','Y')"
                )

        async with asyncio.timeout(30):
            captured = await registry.capture_registry_snapshot(progress)
            _check_capture(captured, candidates, tmp_path)
            assert progress_calls == [(0, 0), (500, len(candidates)), (len(candidates), len(candidates))]
            assert len(capture_connections) == 1 and capture_connections[0].is_closed()
            assert await writer.fetchval(registry.COUNT_QUERY) == len(candidates) + 1
            await writer.execute("INSERT INTO mrf.npi SELECT * FROM mrf.npi WHERE npi=1000000004")
            with pytest.raises(ValueError, match="new_york_registry_count_changed"):
                await registry.capture_registry_snapshot()
            assert len(capture_connections) == 2 and all(connection.is_closed() for connection in capture_connections)
