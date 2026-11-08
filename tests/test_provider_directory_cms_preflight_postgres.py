# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Real single-use ledger, exact paired exclusion and replay for the CMS producer."""

from contextlib import asynccontextmanager
from unittest.mock import AsyncMock

import pytest

from db.connection import Database
from process import provider_directory_cms_capacity_contract as contract
from process import provider_directory_cms_preflight as producer
from process import provider_directory_profile_capacity_preflight_contract as preflight
from tests.cms_npd_admission_postgres_support import _database_url, _run_migrations
from tests.provider_directory_cms_capacity_test_support import cms_request
from tests.test_cms_capacity_preflight_receipt_migration import _ledger, _migrate
from tests.test_provider_directory_cms_preflight import _inputs, _request, fhir
from tests.test_provider_directory_profile_capacity_attestation import VALIDATION_TIME, _trust
from tests.test_provider_directory_profile_capacity_preflight_postgres import _receipt_values


@asynccontextmanager
async def _fixture(monkeypatch):
    """Keep all test writes in one owned schema on the explicitly configured UUID test database."""
    url = _database_url()
    for key, setting_value in {
        "DRIVER": "asyncpg",
        "HOST": url.host,
        "PORT": str(url.port),
        "USER": url.username,
        "PASSWORD": url.password or "",
        "DATABASE": url.database,
    }.items():
        monkeypatch.setenv("HLTHPRT_DB_" + key, setting_value)
    monkeypatch.setenv("HLTHPRT_IMPORT_NODE_ID", "dev-node")
    async with _ledger(monkeypatch) as (engine, schema):
        await _migrate(engine, schema, "upgrade")
        async with engine.begin() as connection:
            await connection.run_sync(_run_migrations, ("20261001100000",))
        database = Database()
        await database.connect()
        try:
            monkeypatch.setattr(fhir, "db", database)
            for relation, columns in (
                (
                    fhir._unscoped_qt(schema, fhir.ImportRun.__tablename__),
                    "run_id text, importer text, status text, params jsonb",
                ),
                (fhir._provider_directory_profile_checkpoint_ref(schema), "state text, owner_run_id text"),
                (
                    fhir._unscoped_qt(schema, fhir.ProviderDirectoryProfileCapacityLeaseConsumption.__tablename__),
                    "run_id text, expires_at timestamptz",
                ),
                (fhir._provider_directory_profile_serving_generation_ref(schema), "singleton_key text"),
            ):
                await database.status(f"CREATE TABLE {relation} ({columns})")
            yield database, schema
        finally:
            await database.disconnect()


async def _insert(database, schema, values):
    columns = ",".join(values)
    binds = ",".join("CAST(:receipt_json AS jsonb)" if name == "receipt_json" else ":" + name for name in values)
    await database.status(
        f"INSERT INTO {fhir._profile_capacity_preflight_receipt_ref(schema)} ({columns}) VALUES ({binds})", **values
    )


async def _seed_pair(database, schema, profile_lease):
    guard = profile_lease.signing_preflight_guard
    request = preflight.validated_capacity_preflight_request(guard["healthcare_request"])
    receipt = guard["healthcare_receipt"]
    values = contract.capacity_preflight_receipt_row_values(
        request, receipt, issued_at=preflight._utc_timestamp(receipt["issued_at"])
    )
    await _insert(database, schema, values)


def _native_stubs(monkeypatch, inputs, profile_lease):
    """This test exercises real ledger SQL; full native proof checks have separate fixtures."""
    monkeypatch.setattr(producer, "_assert_current_inputs", AsyncMock())
    monkeypatch.setattr(fhir, "_profile_capacity_preflight_clock", AsyncMock(return_value=VALIDATION_TIME))
    monkeypatch.setattr(producer.capacity_runtime, "configured_capacity_lease_trust", _trust)
    layout = profile_lease.signing_preflight_guard["healthcare_receipt"]["preflight_receipt_storage"]
    monkeypatch.setattr(fhir, "_profile_capacity_preflight_receipt_layout", AsyncMock(return_value=layout))


async def _rows(database, schema):
    values = await database.all(
        f"SELECT * FROM {fhir._profile_capacity_preflight_receipt_ref(schema)} ORDER BY receipt_sha256"
    )
    return [dict(row._mapping) for row in values]


@pytest.mark.asyncio
async def test_native_producer_preserves_pair_replays_exactly_and_rejects_competitor(monkeypatch):
    async with _fixture(monkeypatch) as (database, schema):
        request = _request()
        inputs, profile_lease = _inputs(request)
        _native_stubs(monkeypatch, inputs, profile_lease)
        await _seed_pair(database, schema, profile_lease)
        initial_pair = (await _rows(database, schema))[0]
        with pytest.raises(fhir.ProviderDirectoryArtifactBuildStale, match="not_quiescent"):
            await fhir._profile_capacity_quiescence(
                schema, observed_at=VALIDATION_TIME, request_sha256=request.request_sha256
            )
        receipt = await producer._issue_receipt(fhir, request, inputs, profile_lease)
        receipt_rows = await _rows(database, schema)
        assert len(receipt_rows) == 2
        assert (
            next(receipt_row for receipt_row in receipt_rows if receipt_row["receipt_sha256"] == profile_lease.nonce)
            == initial_pair
        )
        assert (
            next(
                receipt_row
                for receipt_row in receipt_rows
                if receipt_row["receipt_sha256"] == receipt["receipt_sha256"]
            )["contract_id"]
            == contract.CMS_PREFLIGHT_CONTRACT
        )
        assert await producer._issue_receipt(fhir, request, inputs, profile_lease) == receipt
        assert await _rows(database, schema) == receipt_rows
        await _insert(
            database,
            schema,
            _receipt_values(
                "unrelated-admission", issued_at=profile_lease.issued_at, expires_at=profile_lease.expires_at
            ),
        )
        before_conflict = await _rows(database, schema)
        with pytest.raises(RuntimeError, match="not_quiescent"):
            await producer._issue_receipt(fhir, request, inputs, profile_lease)
        assert await _rows(database, schema) == before_conflict


@pytest.mark.asyncio
async def test_native_producer_refuses_consumed_pair_without_new_receipt(monkeypatch):
    async with _fixture(monkeypatch) as (database, schema):
        request = _request()
        inputs, profile_lease = _inputs(request)
        _native_stubs(monkeypatch, inputs, profile_lease)
        await _seed_pair(database, schema, profile_lease)
        await database.status(
            f"UPDATE {fhir._profile_capacity_preflight_receipt_ref(schema)} "
            "SET consumed_at=:now,consumed_run_id=:run,consumed_attestation_id=:attestation WHERE receipt_sha256=:receipt",
            now=VALIDATION_TIME,
            run="run_" + "a" * 32,
            attestation=profile_lease.attestation_id,
            receipt=profile_lease.nonce,
        )
        before = await _rows(database, schema)
        with pytest.raises(fhir.ProviderDirectoryArtifactBuildStale, match="receipt_expired"):
            await producer._issue_receipt(fhir, request, inputs, profile_lease)
        assert await _rows(database, schema) == before


@pytest.mark.asyncio
async def test_native_producer_refuses_reusing_profile_nonce(monkeypatch):
    async with _fixture(monkeypatch) as (database, schema):
        request = preflight.validated_capacity_preflight_request(cms_request())
        inputs, profile_lease = _inputs(request)
        _native_stubs(monkeypatch, inputs, profile_lease)
        await _seed_pair(database, schema, profile_lease)
        before = await _rows(database, schema)
        with pytest.raises(fhir.ProviderDirectoryArtifactBuildStale, match="nonce_reused"):
            await producer._issue_receipt(fhir, request, inputs, profile_lease)
        assert await _rows(database, schema) == before
