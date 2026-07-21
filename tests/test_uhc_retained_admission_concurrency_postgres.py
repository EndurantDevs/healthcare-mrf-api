# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Concurrency and plan-shape PostgreSQL proofs for retained UHC admission."""

import asyncio
import hashlib
import json

import pytest
from sqlalchemy.ext.asyncio import create_async_engine

import process.uhc_retained_registry_store as registry_store
import process.uhc_retained_source_registry as source_registry
from process.uhc_retained_source_registry import register_verified_source
from tests.uhc_retained_postgres_test_support import (
    FRESH_SCHEMA,
    admission_database,
    _asyncpg_connection,
    _binding,
    _database_url,
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


async def _wait_for_advisory_lock(connection, backend_pid: int) -> None:
    for _attempt in range(100):
        lock_wait = await connection.fetchrow(
            """SELECT wait_event_type, wait_event
                 FROM pg_stat_activity
                WHERE pid=$1""",
            backend_pid,
        )
        if lock_wait is not None and dict(lock_wait) == {
            "wait_event_type": "Lock",
            "wait_event": "advisory",
        }:
            return
        await asyncio.sleep(0.01)
    pytest.fail("second admission never reached the PostgreSQL advisory wait")


@pytest.mark.asyncio
async def test_native_registry_serializes_concurrent_same_raw_admission(
    tmp_path,
    monkeypatch,
):
    async with admission_database(tmp_path, monkeypatch) as (
        first_connection,
        database_url,
    ):
        second_connection = await _asyncpg_connection(database_url)
        try:
            source_path, artifact_sha256, artifact_byte_count = _write_source(
                tmp_path,
                "concurrent-provider",
            )
            binding = _binding(
                artifact_sha256,
                artifact_byte_count,
                catalog_label="concurrent-catalog",
            )
            await _insert_catalog_file(first_connection, FRESH_SCHEMA, binding)
            verified_source = await _native_source(
                source_path,
                tmp_path,
                artifact_sha256,
                artifact_byte_count,
                4,
            )
            lock_transaction = first_connection.transaction()
            await lock_transaction.start()
            await first_connection.execute(
                "SELECT pg_advisory_xact_lock($1::bigint)",
                registry_store._advisory_lock_key(artifact_sha256),
            )
            second_backend_pid = await second_connection.fetchval(
                "SELECT pg_backend_pid()"
            )
            registration_task = asyncio.create_task(
                register_verified_source(
                    second_connection,
                    binding=binding,
                    source=verified_source,
                ),
            )
            await _wait_for_advisory_lock(first_connection, second_backend_pid)
            assert not registration_task.done()
            await lock_transaction.commit()
            await asyncio.wait_for(registration_task, timeout=5)
            assert await _proof_counts(second_connection, FRESH_SCHEMA) == {
                "raw_count": 1,
                "layout_count": 1,
                "binding_count": 1,
                "range_count": 4,
                "reference_count": 2,
            }
        finally:
            await second_connection.close()


@pytest.mark.asyncio
async def test_plan_reference_uses_same_native_and_database_contract(
    tmp_path,
    monkeypatch,
):
    async with admission_database(tmp_path, monkeypatch) as (connection, _database_url):
        plan_records = [
            {"PlanID": f"P{ordinal:03}", "PlanName": f"Plan {ordinal}"}
            for ordinal in range(24)
        ]
        encoded = json.dumps(plan_records, separators=(",", ":")).encode("ascii")
        source_path = tmp_path / "JSON_PLANS_WY.json"
        source_path.write_bytes(encoded)
        artifact_sha256 = hashlib.sha256(encoded).hexdigest()
        binding = _binding(
            artifact_sha256,
            len(encoded),
            catalog_label="plan-catalog",
            collection_kind="plan_reference",
        )
        await _insert_catalog_file(connection, FRESH_SCHEMA, binding)
        await register_verified_source(
            connection,
            binding=binding,
            source=await _native_source(
                source_path,
                tmp_path,
                artifact_sha256,
                len(encoded),
                4,
            ),
        )
        proof_record = await connection.fetchrow(
            f'''SELECT binding.collection_kind, layout.record_count,
                       count(raw_range.*) AS range_count
                  FROM "{FRESH_SCHEMA}".provider_directory_uhc_source_binding
                       AS binding
                  JOIN "{FRESH_SCHEMA}".provider_directory_uhc_raw_layout
                       AS layout USING (artifact_sha256)
                  JOIN "{FRESH_SCHEMA}".provider_directory_uhc_raw_range
                       AS raw_range USING (
                           artifact_sha256, contract_version, range_count
                       )
                 GROUP BY binding.collection_kind, layout.record_count'''
        )
        assert dict(proof_record) == {
            "collection_kind": "plan_reference",
            "record_count": 24,
            "range_count": 4,
        }
