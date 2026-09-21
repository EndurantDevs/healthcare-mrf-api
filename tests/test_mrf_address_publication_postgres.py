# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Source-local address coverage rejects omissions and later relevant edits."""

from uuid import uuid4

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.pool import NullPool

from db.connection import Database
from process import mrf_publication_receipt as receipt
from process import reference_family_result_generation as generation
from process.mrf_address_publication import lock_publication_family
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


async def _assert_relevant_address_changes_require_republication(engine, schema, admit, key):
    """Reject changed, invalid, or missing MRF canonical-address contributions."""

    for table_name in ("mrf_address", "mrf_address_evidence", "address_archive_v2"):
        async with engine.begin() as connection:
            await connection.execute(
                text(f"UPDATE \"{schema}\".{table_name} SET value='changed' WHERE address_key=:key"), {"key": key}
            )
        with pytest.raises(RuntimeError, match="content differs"):
            await admit()
        async with engine.begin() as connection:
            await connection.execute(
                text(f"UPDATE \"{schema}\".{table_name} SET value='original' WHERE address_key=:key"), {"key": key}
            )
        await admit()
    for assignment in ("source_bits=0", "merged_into=address_key"):
        async with engine.begin() as connection:
            await connection.execute(
                text(f'UPDATE "{schema}".address_archive_v2 SET {assignment} WHERE address_key=:key'), {"key": key}
            )
        with pytest.raises(RuntimeError, match="coverage is incomplete"):
            await admit()
        async with engine.begin() as connection:
            await connection.execute(
                text(
                    f'UPDATE "{schema}".address_archive_v2 SET source_bits=16, merged_into=NULL WHERE address_key=:key'
                ),
                {"key": key},
            )


async def _assert_missing_address_rejected(engine, schema, admit):
    """Reject a serving MRF address relation with an uncovered null key."""

    async with engine.begin() as connection:
        await connection.execute(text(f'UPDATE "{schema}".mrf_address_evidence SET address_key=NULL'))
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
        await _assert_relevant_address_changes_require_republication(engine, schema, admit, key)
        await _assert_missing_address_rejected(engine, schema, admit)
    finally:
        async with engine.begin() as connection:
            await connection.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
        await engine.dispose()
