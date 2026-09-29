# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Small real Doctors tables under the existing disposable common-receipt fixture."""

import asyncio
import importlib
from contextlib import asynccontextmanager
from datetime import datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker
from sqlalchemy.schema import CreateTable

from api import provider_profile_snapshot as snapshot
from db.connection import Database
from process import cms_doctors_preparation as preparation
from process import reference_family_result_generation as generation
from tests import test_provider_directory_cms_serving_receipt_postgres as receipt_fixture

native = importlib.import_module("process.cms_doctors")


async def _insert_marker(session, model, marker):
    """Populate the real address, education, and physical group row shapes."""
    fields_by_table = {
        "doctor_clinician_address": {"address_checksum": 1, "city": marker},
        "cms_doctor_education": {
            "education_key": "assertion",
            "medical_school": marker,
            "generation_id": "a" * 64,
            "source_json": {},
            "imported_at": datetime(2026, 1, 1),
        },
        "cms_doctor_group_site": {
            "row_number": 1,
            "org_pac_id": marker,
            "generation_id": "a" * 64,
            "source_json": {},
            "observed_at": datetime(2026, 1, 1),
        },
    }
    await session.execute(model.__table__.insert().values(npi=1000000004, **fields_by_table[model.__main_table__]))


async def stage_family(database, schema):
    """Create three populated, unlogged stage tables without their serving indexes."""
    import_date = uuid4().hex
    async with database.transaction() as session:
        for model in preparation._models():
            stage = native.make_class(model, import_date)
            await session.execute(CreateTable(stage.__table__))
            await session.execute(text(f'ALTER TABLE "{schema}"."{stage.__tablename__}" SET UNLOGGED'))
            await _insert_marker(session, stage, "prepared")
    return {
        "import_date": import_date,
        "context": {"run": True, "education_stage_owned": True, "group_site_stage_owned": True},
    }


@asynccontextmanager
async def doctors_database(monkeypatch, *, cms_active=True):
    """Use real model tables and authority SQL with only completed-source validation stubbed."""
    async with receipt_fixture._database(monkeypatch, install_receipt=False) as (engine, schema):
        database = Database(engine=engine, session_factory=async_sessionmaker(engine, expire_on_commit=False))
        for module in (
            native,
            importlib.import_module("process.cms_doctors_education"),
            importlib.import_module("process.cms_doctors_groups"),
        ):
            monkeypatch.setattr(module, "db", database)
        monkeypatch.setattr(native, "ensure_database", AsyncMock())
        monkeypatch.setattr(native, "_prepare_cms_doctors_sources", AsyncMock(side_effect=lambda *_args: {"rows": 1}))
        async with database.transaction() as session:
            for model in preparation._models():
                monkeypatch.setattr(model.__table__, "schema", schema)
                await session.execute(text(f'DROP TABLE "{schema}"."{model.__tablename__}"'))
                await session.execute(CreateTable(model.__table__))
                await _insert_marker(session, model, "incumbent")
            connection = await session.connection()
            await connection.run_sync(lambda sync: receipt_fixture._apply(sync, "20260930100000"))
            await connection.run_sync(lambda sync: receipt_fixture._apply(sync, "20260930130000"))
        initial = await receipt_fixture._publish_initial(engine, schema) if cms_active else None
        if not cms_active:
            async with database.transaction() as session:
                await generation.publish_local_reference_family_generation(
                    session, importer_id="cms-doctors", schema_name=schema
                )
        yield SimpleNamespace(engine=engine, schema=schema, database=database, initial=initial)


async def authority(fixture):
    """Read the exact current native authority through a separate completed transaction."""
    async with fixture.database.transaction() as session:
        return await generation.read_reference_family_result_generation_authority(
            session, importer_id="cms-doctors", schema_name=fixture.schema
        )


async def doctors_snapshot(fixture, *, entered=None, release=None):
    """Read all three markers under the actual response lock and authority checks."""
    async with snapshot.provider_profile_read_snapshot(fixture.database, fixture.schema) as session:
        markers = tuple(
            [
                await session.scalar(text(f'SELECT {column} FROM "{fixture.schema}".{table}'))
                for table, column in (
                    ("doctor_clinician_address", "city"),
                    ("cms_doctor_education", "medical_school"),
                    ("cms_doctor_group_site", "org_pac_id"),
                )
            ]
        )
        incumbent = session.info["provider_profile_native_authorities"]["cms-doctors"]
        if entered is not None:
            entered.set()
            await release.wait()
        return markers, incumbent


async def pending_publisher_locks(fixture, publisher):
    """Observe a real queued writer, or the publisher's completed fail-fast attempts."""
    for _poll in range(200):
        async with fixture.engine.connect() as observer:
            pending_locks = (
                await observer.execute(
                    text(
                        "SELECT locks.pid, pg_blocking_pids(locks.pid) FROM pg_locks AS locks "
                        "JOIN pg_class AS relation ON relation.oid=locks.relation "
                        "JOIN pg_namespace AS namespace ON namespace.oid=relation.relnamespace "
                        "WHERE namespace.nspname=:schema AND mode='AccessExclusiveLock' AND NOT granted"
                    ),
                    {"schema": fixture.schema},
                )
            ).all()
        if pending_locks or publisher.done():
            return pending_locks
        await asyncio.sleep(0.01)
    raise AssertionError("publisher neither completed nor reached a relation lock")


async def stage_oids(fixture, ctx):
    """Observe retained stage identities without confusing them with published authority."""
    async with fixture.database.transaction() as session:
        return await preparation._stage_inventory(session, fixture.schema, ctx["import_date"])


async def append_common_receipt(session, fixture, prior):
    """Advance scalar dependent stand-ins and the real receipt around the native Doctors apply."""
    await receipt_fixture._advance_native(session, fixture.schema)
    await session.execute(
        text(f'UPDATE "{fixture.schema}".provider_directory_profile_serving_generation SET generation_id=:generation'),
        {"generation": "pdprofile_" + uuid4().hex},
    )
    payload = await receipt_fixture._payload(session, fixture.schema, prior)
    receipt_id = await receipt_fixture.receipts.append_serving_receipt(session, fixture.schema, payload)
    return {"receipt_id": receipt_id, "payload": payload}
