# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Source-local address coverage rejects omissions and later relevant edits."""

import asyncio
from unittest.mock import Mock
from uuid import uuid4

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.pool import NullPool

from db.connection import Database
from process import mrf_address_publication as address
from process import mrf_publication_receipt as receipt
from process import reference_family_result_generation as generation
from process.mrf_address_publication import lock_publication_family
from tests.reference_family_generation_fixture import install_source_generation_guards
from tests.test_mrf_publication_receipt_postgres import _MIGRATION
from tests.test_reference_family_result_generation_postgres import (
    _MRF_MIGRATION_PATH,
    _REFERENCE_MIGRATION_PATH,
    _database_url,
    _run_migration,
)


async def _assert_pinned_publication_blocks_writes(engine, sessions, schema):
    """Ensure receipt capture holds write locks on all covered relations."""

    async with sessions() as pinned, pinned.begin():
        await lock_publication_family(pinned, schema, receipt.qualified)
        await receipt.require_completed_publication(pinned, schema)
        for table_name in ("mrf_address", "mrf_address_evidence", "address_archive_v2"):
            with pytest.raises(DBAPIError) as blocked:
                async with engine.begin() as writer:
                    await writer.execute(text("SET LOCAL lock_timeout='100ms'"))
                    await writer.execute(text(f"UPDATE \"{schema}\".{table_name} SET value='racing'"))
            assert blocked.value.orig.sqlstate == "55P03"


async def _assert_relevant_address_changes_require_republication(database, schema, admit, key):
    """Reject changed, invalid, or missing MRF canonical-address contributions."""

    for table_name in ("mrf_address", "mrf_address_evidence", "address_archive_v2"):
        async with database.transaction() as connection:
            await connection.execute(
                text(f"UPDATE \"{schema}\".{table_name} SET value='changed' WHERE address_key=:key"), {"key": key}
            )
        is_tracked_source = table_name != "address_archive_v2"
        with pytest.raises(RuntimeError, match="generation differs" if is_tracked_source else "content differs"):
            await admit()
        async with database.transaction() as connection:
            await connection.execute(
                text(f"UPDATE \"{schema}\".{table_name} SET value='original' WHERE address_key=:key"), {"key": key}
            )
        if is_tracked_source:
            # Restoring bytes does not erase a committed source revision.
            with pytest.raises(RuntimeError, match="generation differs"):
                await admit()
            await _complete_address_publication(database, schema)
        await admit()
    for assignment in ("source_bits=0", "merged_into=address_key"):
        async with database.transaction() as connection:
            await connection.execute(
                text(f'UPDATE "{schema}".address_archive_v2 SET {assignment} WHERE address_key=:key'), {"key": key}
            )
        with pytest.raises(RuntimeError, match="coverage is incomplete"):
            await admit()
        async with database.transaction() as connection:
            await connection.execute(
                text(
                    f'UPDATE "{schema}".address_archive_v2 SET source_bits=16, merged_into=NULL WHERE address_key=:key'
                ),
                {"key": key},
            )


async def _assert_missing_address_rejected(database, schema, admit):
    """Reject a serving MRF address relation with an uncovered null key."""

    async with database.transaction() as connection:
        await connection.execute(text(f'UPDATE "{schema}".mrf_address_evidence SET address_key=NULL'))
    with pytest.raises(RuntimeError, match="generation differs"):
        await admit()
    await _complete_address_publication(database, schema)
    with pytest.raises(RuntimeError, match="coverage is incomplete"):
        await admit()


async def _require_completed_address_publication(sessions, schema):
    """Return the completed receipt while holding its canonical-address locks."""

    async with sessions() as session, session.begin():
        await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"))
        await lock_publication_family(session, schema, receipt.qualified)
        return await receipt.require_completed_publication(session, schema)


async def _complete_address_publication(database, schema):
    """Publish and complete one synthetic MRF receipt."""

    attempt = await receipt.begin_publication(schema, "synthetic")
    async with database.transaction() as session:
        await lock_publication_family(session, schema, receipt.qualified)
        authority = await generation.publish_local_reference_family_generation(
            session,
            importer_id="mrf",
            schema_name=schema,
        )
        inputs = await receipt.capture_summary_inputs(session, schema)
        await receipt.complete_publication(session, schema, attempt, authority, inputs, True)


async def _create_address_publication_schema(engine, schema, key):
    """Create the synthetic tables and receipt migrations for the address test."""

    async with engine.begin() as connection:
        await connection.execute(text(f'CREATE SCHEMA "{schema}"'))
        for name in dict.fromkeys(name for tables in generation.RELATION_NAMES_BY_IMPORTER.values() for name in tables):
            await connection.execute(text(f'CREATE TABLE "{schema}"."{name}" (value text, address_key uuid)'))
        await connection.execute(text(f'CREATE TABLE "{schema}".plan_search_summary (value text)'))
        await connection.execute(
            text(f'''CREATE TABLE "{schema}".address_archive_v2 (
            address_key uuid PRIMARY KEY, merged_into uuid, source_bits integer, value text)''')
        )
        for name in ("mrf_address", "mrf_address_evidence"):
            await connection.execute(text(f"INSERT INTO \"{schema}\".{name} VALUES ('original', :key)"), {"key": key})
        await _run_migration(connection, _REFERENCE_MIGRATION_PATH, "upgrade")
        await _run_migration(connection, _MRF_MIGRATION_PATH, "upgrade")
        await _run_migration(connection, _MIGRATION, "upgrade")
        await install_source_generation_guards(connection, schema)


@pytest.mark.asyncio
async def test_address_coverage_and_content_are_bound_to_completion(monkeypatch):
    """Bind completion to precisely covered canonical-address content and locks."""
    schema = "mrf_address_receipt_" + uuid4().hex
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", schema)
    monkeypatch.delenv("DB_SCHEMA", raising=False)
    monkeypatch.setenv("HLTHPRT_ADDRESS_ARCHIVE_TABLE", "address_archive_v2")
    engine = create_async_engine(_database_url(), poolclass=NullPool)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    database = Database(engine=engine, session_factory=sessions)
    monkeypatch.setattr(receipt, "db", database)
    key = uuid4()

    try:
        await _create_address_publication_schema(engine, schema, key)
        await _complete_address_publication(database, schema)
        with pytest.raises(RuntimeError, match="coverage is incomplete"):
            await _require_completed_address_publication(sessions, schema)
        async with engine.begin() as connection:
            await connection.execute(
                text(f"INSERT INTO \"{schema}\".address_archive_v2 VALUES (:key, NULL, 16, 'original')"), {"key": key}
            )
        with pytest.raises(RuntimeError, match="content differs"):
            await _require_completed_address_publication(sessions, schema)
        await _complete_address_publication(database, schema)
        await _require_completed_address_publication(sessions, schema)
        await _assert_pinned_publication_blocks_writes(engine, sessions, schema)
        # An unrelated canonical row is outside this publication's contribution.
        async with engine.begin() as connection:
            await connection.execute(
                text(f"INSERT INTO \"{schema}\".address_archive_v2 VALUES (:key, NULL, 1, 'unrelated')"),
                {"key": uuid4()},
            )
        await _require_completed_address_publication(sessions, schema)
        admit = lambda: _require_completed_address_publication(sessions, schema)
        await _assert_relevant_address_changes_require_republication(database, schema, admit, key)
        await _assert_missing_address_rejected(database, schema, admit)
    finally:
        async with engine.begin() as connection:
            await connection.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
        await engine.dispose()


@pytest.mark.asyncio
async def test_native_accounting_retains_invalid_raw_rows_and_refuses_every_keyable_gap(monkeypatch):
    """Retain invalid rows while refusing every missing canonical target and interrupted capture."""
    schema = "mrf_raw_accounting_" + uuid4().hex
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", schema)
    monkeypatch.delenv("DB_SCHEMA", raising=False)
    monkeypatch.setenv("HLTHPRT_ADDRESS_ARCHIVE_TABLE", "address_archive_v2")
    engine = create_async_engine(_database_url(), poolclass=NullPool)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    database = Database(engine=engine, session_factory=sessions)
    monkeypatch.setattr(receipt, "db", database)
    key = uuid4()
    try:
        await _create_address_publication_schema(engine, schema, key)
        await _seed_raw_accounting_exceptions(engine, schema, key)
        await _complete_address_publication(database, schema)
        with pytest.raises(RuntimeError, match="coverage is incomplete"):
            await _require_completed_address_publication(sessions, schema)
        await _assert_accounted_publication(sessions, schema)
        await _assert_accounting_cancellation(monkeypatch, sessions, schema)
        await _assert_keyable_nulls_refused(sessions, schema)
        await _assert_nonnull_gaps_refused(sessions, schema)
    finally:
        async with engine.begin() as connection:
            await connection.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
        await engine.dispose()


async def _seed_raw_accounting_exceptions(engine, schema, key):
    async with engine.begin() as connection:
        await connection.execute(
            text(f"INSERT INTO \"{schema}\".address_archive_v2 VALUES (:key,NULL,16,'original')"), {"key": key}
        )
        for name in address.REFERENCES:
            table = receipt.qualified(schema, name)
            for column in ("first_line", "second_line", "city_name", "state_name", "postal_code", "country_code"):
                await connection.execute(text(f'ALTER TABLE {table} ADD COLUMN "{column}" text'))
            await connection.execute(
                text(
                    f"UPDATE {table} SET first_line='10 Main St',city_name='Austin',state_name='TX',postal_code='78701',country_code='US'"
                )
            )
            await connection.execute(
                text(
                    f"INSERT INTO {table} (value,first_line,city_name,state_name,postal_code,country_code) VALUES "
                    "('invalid-state','10 Main St','Austin','invalid','78701','US'),"
                    "('invalid-zip','10 Main St','Austin','TX','invalid','US')"
                )
            )
        await connection.execute(
            text(f'CREATE TABLE "{schema}".mrf_canonical_address AS TABLE "{schema}".address_archive_v2')
        )


async def _assert_accounted_publication(sessions, schema):
    async with sessions() as session, session.begin():
        await lock_publication_family(session, schema, receipt.qualified)
        completed = await receipt.require_completed_publication(session, schema, native_set=True)
        coverage = completed["native_address_coverage"]
        for name in address.REFERENCES:
            assert coverage["tables"][name] == {
                "rows": 3,
                "covered": 1,
                "unkeyable_null": 2,
                "keyable_null": 0,
                "uncovered_nonnull": 0,
            }
            assert completed["address_content"]["tables"][name]["uncovered"] == 2
            assert len(completed["address_content"]["tables"][name]["sha256"]) == 64
        await address.validate_canonical_closure(session, "mrf", schema, receipt.qualified, address_coverage=coverage)
        with pytest.raises(RuntimeError, match="closure is incomplete"):
            await address.validate_canonical_closure(session, "mrf", schema, receipt.qualified)


async def _assert_accounting_cancellation(monkeypatch, sessions, schema):
    with monkeypatch.context() as interrupted:
        interrupted.setattr(address, "address_key_v1", Mock(side_effect=asyncio.CancelledError()))
        async with sessions() as session:
            with pytest.raises(asyncio.CancelledError):
                async with session.begin():
                    await address.capture_address_content(session, schema, receipt.qualified, native_set=True)
            assert not session.in_transaction()
            assert await session.scalar(text("SELECT 1")) == 1


async def _assert_keyable_nulls_refused(sessions, schema):
    for name in address.REFERENCES:
        for assignment in ("state_name='TX'", "first_line=NULL,state_name='TX'"):
            async with sessions() as session, session.begin():
                await session.execute(text(f"UPDATE \"{schema}\".{name} SET {assignment} WHERE value='invalid-state'"))
                observed = await address.capture_address_content(session, schema, receipt.qualified, native_set=True)
                assert observed["tables"][name]["keyable_null"] == 1
                with pytest.raises(RuntimeError, match="coverage is incomplete"):
                    address.require_address_coverage(observed)
                await session.rollback()


async def _assert_nonnull_gaps_refused(sessions, schema):
    for assignment in ("source_bits=0", "merged_into=address_key", "address_key=:other"):
        async with sessions() as session, session.begin():
            await session.execute(text(f'UPDATE "{schema}".address_archive_v2 SET {assignment}'), {"other": uuid4()})
            observed = await address.capture_address_content(session, schema, receipt.qualified, native_set=True)
            assert all(counts["uncovered_nonnull"] == 1 for counts in observed["tables"].values())
            with pytest.raises(RuntimeError, match="coverage is incomplete"):
                address.require_address_coverage(observed)
            await session.rollback()
