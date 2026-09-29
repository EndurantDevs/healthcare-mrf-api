# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native MVCC and read-only boundary on an explicitly owned disposable database."""

import asyncio
import datetime
import json
from contextlib import asynccontextmanager
from types import SimpleNamespace
from uuid import uuid4

import pytest
from sqlalchemy import MetaData, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from db.connection import Database
from db.models.system import (
    ImportRun,
    ProviderDirectoryProfileCapacityLeaseConsumption,
    ProviderDirectoryProfileCapacityPreflightReceipt,
)
from process import provider_directory_capacity_reservation_snapshot as snapshot
from tests.cms_npd_admission_postgres_support import _database_url
from tests.test_provider_directory_capacity_reservation_snapshot import RUN_ID, _consumption, _preflight, _run
from tests.test_provider_directory_profile_capacity_attestation import _signed_envelope


@asynccontextmanager
async def _fixture(monkeypatch, session_type=AsyncSession):
    engine = create_async_engine(_database_url())
    schema = "capacity_snapshot_" + uuid4().hex
    monkeypatch.setenv("HLTHPRT_IMPORT_NODE_ID", "dev-node")
    database = Database(
        engine=engine, session_factory=async_sessionmaker(engine, class_=session_type, expire_on_commit=False)
    )
    metadata = MetaData()
    tables_by_name = {
        model.__tablename__: model.__table__.to_metadata(metadata, schema=schema)
        for model in (
            ImportRun,
            ProviderDirectoryProfileCapacityLeaseConsumption,
            ProviderDirectoryProfileCapacityPreflightReceipt,
        )
    }
    quote = engine.dialect.identifier_preparer.quote_identifier
    qualify = lambda table_schema, name: quote(table_schema) + "." + quote(name)
    fhir = SimpleNamespace(
        db=database,
        _schema=lambda: schema,
        _unscoped_qt=qualify,
        _profile_capacity_preflight_receipt_ref=lambda value: qualify(
            value, ProviderDirectoryProfileCapacityPreflightReceipt.__tablename__
        ),
        _provider_directory_profile_checkpoint_ref=lambda value: qualify(
            value, "provider_directory_profile_build_checkpoint"
        ),
    )
    try:
        async with engine.begin() as connection:
            await connection.execute(text(f"CREATE SCHEMA {quote(schema)}"))
            await connection.run_sync(metadata.create_all)
            await connection.execute(
                text(f"""CREATE TABLE {fhir._provider_directory_profile_checkpoint_ref(schema)} (
                build_id text,owner_run_id text,state text,evidence_stage text,profile_stage text,
                evidence_stage_oid bigint,profile_stage_oid bigint,affected_npi_stage text,affected_npi_stage_oid bigint
            )""")
            )
        yield fhir, engine, tables_by_name
    finally:
        try:
            async with engine.begin() as connection:
                await connection.execute(text(f"DROP SCHEMA IF EXISTS {quote(schema)} CASCADE"))
        finally:
            await engine.dispose()


def _pinned_snapshot_session(pinned, resume, settings):
    """Pause after pinning MVCC and record the effective backend read settings."""

    class SnapshotSession(AsyncSession):
        async def execute(self, statement, *args, **kwargs):
            result = await super().execute(statement, *args, **kwargs)
            if "pg_current_snapshot()" in str(statement):
                settings.append(
                    (
                        await super().execute(
                            text("""SELECT
                    current_setting('transaction_isolation'),current_setting('transaction_read_only'),
                    current_setting('temp_file_limit'),current_setting('statement_timeout'),
                    current_setting('max_parallel_workers_per_gather')
                """)
                        )
                    ).one()
                )
                if not pinned.is_set():
                    pinned.set()
                    await asyncio.wait_for(resume.wait(), timeout=10)
            return result

    return SnapshotSession


@pytest.mark.asyncio
async def test_native_snapshot_cannot_mix_pending_and_consumed_transactions(monkeypatch):
    """A concurrent consume must appear wholly before or wholly after the retained snapshot."""
    pinned, resume = asyncio.Event(), asyncio.Event()
    settings = []
    async with _fixture(monkeypatch, _pinned_snapshot_session(pinned, resume, settings)) as (fhir, engine, tables):
        envelope = _signed_envelope()
        receipt = _preflight(envelope)
        receipt["receipt_json"] = json.loads(receipt["receipt_json"])
        receipt_table = tables[ProviderDirectoryProfileCapacityPreflightReceipt.__tablename__]
        async with engine.begin() as connection:
            await connection.execute(receipt_table.insert().values(**receipt))
        task = asyncio.create_task(snapshot.capacity_reservation_snapshot(fhir))
        try:
            await asyncio.wait_for(pinned.wait(), timeout=10)
            consumption = _consumption(envelope)
            async with engine.begin() as connection:
                await connection.execute(tables[ImportRun.__tablename__].insert().values(**_run(envelope)))
                await connection.execute(
                    tables[ProviderDirectoryProfileCapacityLeaseConsumption.__tablename__]
                    .insert()
                    .values(**consumption)
                )
                await connection.execute(
                    receipt_table.update().values(
                        consumed_at=consumption["accepted_at"],
                        consumed_run_id=RUN_ID,
                        consumed_attestation_id=consumption["attestation_id"],
                    )
                )
            resume.set()
            before = await asyncio.wait_for(task, timeout=10)
        finally:
            resume.set()
            if not task.done():
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
        after = await snapshot.capacity_reservation_snapshot(fhir)
        assert before["reservations"] == before["owners"] == []
        assert before["preflight_receipts"][0]["record"]["consumed_at"] is None
        assert len(after["reservations"]) == len(after["owners"]) == 1
        assert after["preflight_receipts"][0]["record"]["consumed_at"] == consumption["accepted_at"].isoformat()
        assert after["reservations"][0]["envelope"] == envelope
        assert after["database_binding"]["database_name"] == engine.url.database
        assert all(tuple(setting_row) == ("repeatable read", "on", "64MB", "30s", "0") for setting_row in settings)
        assert before["capacity_complete"] is after["capacity_complete"] is False


@pytest.mark.asyncio
async def test_native_stage_oid_observation_and_nested_transaction_refusal(monkeypatch):
    async with _fixture(monkeypatch) as (fhir, engine, _):
        schema = fhir._schema()
        original = fhir._unscoped_qt(schema, "original_stage")
        async with engine.begin() as connection:
            await connection.execute(text(f"CREATE TABLE {original} (value int)"))
            oid = (
                await connection.execute(text("SELECT to_regclass(:name)::oid::bigint"), {"name": original})
            ).scalar_one()
            await connection.execute(
                text(f"""INSERT INTO {fhir._provider_directory_profile_checkpoint_ref(schema)}
                (build_id,owner_run_id,state,evidence_stage,evidence_stage_oid)
                VALUES ('build',:run,'failed','original_stage',:oid)
            """),
                {"run": RUN_ID, "oid": oid},
            )
            await connection.execute(text(f"ALTER TABLE {original} RENAME TO published_relation"))
        result = await snapshot.capacity_reservation_snapshot(fhir)
        stage = result["profile_stage_observations"][0]
        assert stage["expected_oid"] == oid and stage["current_name_oid"] is None
        assert stage["expected_oid_current_name"] == "published_relation"
        assert result["release_proof_available"] is False
        assert datetime.datetime.fromisoformat(result["observed_at"]).tzinfo is not None
        async with fhir.db.transaction():
            with pytest.raises(RuntimeError, match="fresh_transaction_required"):
                await snapshot.capacity_reservation_snapshot(fhir)
