# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Shared final-schema DDL for reference-family archive fixtures."""

import hashlib
import importlib.util
from contextlib import asynccontextmanager
from pathlib import Path
from uuid import uuid4

from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import create_async_engine

_MIGRATION_PATH = Path(__file__).resolve().parents[1] / "alembic/versions/20260929000000_cms_doctor_group_site.py"
_SPEC = importlib.util.spec_from_file_location("reference_family_generation_fixture_migration", _MIGRATION_PATH)
assert _SPEC is not None and _SPEC.loader is not None
_MIGRATION = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(_MIGRATION)


def generation_shape_check() -> str:
    previous, counts = _MIGRATION._shape_support()
    return previous._shape({**counts, "cms-doctors": 3})


async def install_source_generation_guards(connection, schema: str, *, named=True) -> None:
    """Apply current revision protection without changing historical fixture chains."""
    path = _MIGRATION_PATH.with_name("20260929040000_reference_source_generation_guard.py")
    spec = importlib.util.spec_from_file_location("reference_source_guard_fixture_migration", path)
    assert spec is not None and spec.loader is not None
    migration = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(migration)
    migration._schema = lambda: schema

    def apply(sync_connection):
        migration.op = Operations(MigrationContext.configure(sync_connection))
        migration.upgrade()

    await connection.run_sync(apply)
    if named:
        await install_named_generation(connection, schema)


async def install_named_generation(connection, schema: str) -> None:
    """Upgrade a fixture table created in its historical shape to named publication columns."""
    has_named_columns = await connection.scalar(
        text(
            "SELECT EXISTS(SELECT FROM information_schema.columns WHERE table_schema=:schema "
            "AND table_name='reference_family_result_generation' AND column_name='serving_schema')"
        ),
        {"schema": schema},
    )
    if has_named_columns:
        return
    path = _MIGRATION_PATH.with_name("20261114180000_reference_family_named_generation.py")
    spec = importlib.util.spec_from_file_location("reference_named_generation_fixture_migration", path)
    assert spec is not None and spec.loader is not None
    migration = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(migration)
    migration._schema = lambda: schema

    def apply(sync_connection):
        migration.op = Operations(MigrationContext.configure(sync_connection))
        migration.upgrade()

    await connection.run_sync(apply)


async def install_source_generation_guards_from_dsn(dsn: str, schema: str) -> None:
    """Migrate committed asyncpg fixtures through a real Alembic connection."""
    engine = create_async_engine(make_url(dsn).set(drivername="postgresql+asyncpg"))
    try:
        async with engine.begin() as connection:
            await install_source_generation_guards(connection, schema)
    finally:
        await engine.dispose()


class ReferenceSourceCustody:
    """Provision native fixture grants and retain complete named snapshot contents."""

    def __init__(self, owner, readers):
        from process import reference_family_archive as archive

        self.owner, self.readers = owner, readers
        self.precreated, self.closed, self.schemas = {}, {}, set()
        self.source_copy = archive.ReferenceFamilySourceCopy(archive.native_copy_projection, 1_000_000, 30)

    async def precreate(self, session, ownership):
        from process import reference_family_archive as archive

        assert session.in_transaction()
        await archive.verify_reference_family_stage_ownership(session, ownership)
        self.schemas.add(ownership.schema_name)
        assert ownership.dataset_id not in self.precreated
        for table, _ in ownership.relation_oids:
            assert not await session.scalar(text(f'SELECT EXISTS(SELECT 1 FROM "{ownership.schema_name}"."{table}")'))
            assert not await session.scalar(
                text("SELECT EXISTS(SELECT 1 FROM pg_index WHERE indrelid=to_regclass(:name))"),
                {"name": f'"{ownership.schema_name}"."{table}"'},
            )
        self.precreated[ownership.dataset_id] = (None, ownership)
        await session.execute(text(f'ALTER SCHEMA "{ownership.schema_name}" OWNER TO "{self.owner}"'))
        await session.execute(text(f'REVOKE ALL ON SCHEMA "{ownership.schema_name}" FROM PUBLIC'))
        for table, _ in ownership.relation_oids:
            await session.execute(text(f'ALTER TABLE "{ownership.schema_name}"."{table}" OWNER TO "{self.owner}"'))
            await session.execute(text(f'REVOKE ALL ON "{ownership.schema_name}"."{table}" FROM PUBLIC'))
        for reader in self.readers:
            await session.execute(text(f'GRANT USAGE ON SCHEMA "{ownership.schema_name}" TO "{reader}"'))
            await session.execute(text(f'GRANT SELECT ON ALL TABLES IN SCHEMA "{ownership.schema_name}" TO "{reader}"'))
            await session.execute(
                text(f'GRANT SELECT ON ALL SEQUENCES IN SCHEMA "{ownership.schema_name}" TO "{reader}"')
            )

    async def retain(self, session, prepared):
        _, ownership = self.precreated[prepared.ownership.dataset_id]
        assert session.in_transaction()
        assert self._declaration(prepared.ownership) == self._declaration(ownership)
        assert ownership.dataset_id not in self.closed
        self.closed[ownership.dataset_id] = await self._receipt(session, ownership)

    async def verify(self, session, prepared):
        from process import reference_family_archive as archive

        await archive.verify_reference_family_stage_ownership(session, prepared.ownership)
        if prepared.ownership.dataset_id not in self.closed:
            await self.retain(session, prepared)
        assert await self._receipt(session, prepared.ownership) == self.closed[prepared.ownership.dataset_id]

    async def retire(self, session, ownership):
        """Retire exact captured heaps before a restore can reuse the UUID namespace."""
        from process import reference_family_archive as archive

        assert self._declaration(self.precreated[ownership.dataset_id][1]) == self._declaration(ownership)
        await archive.cleanup_reference_family_stage(session, ownership)
        self.schemas.remove(ownership.schema_name)

    @staticmethod
    def _declaration(ownership):
        return (
            ownership.importer_id,
            ownership.dataset_id,
            ownership.schema_name,
            ownership.relation_names,
            tuple((name, table, column) for name, _, table, column in ownership.sequence_oids),
        )

    async def _receipt(self, session, ownership):
        """Retain logical columns, complete rows and sequence state without physical identity."""
        table_contents = []
        names = (*ownership.relation_names, *(("mrf_canonical_address",) if ownership.auxiliary_oid else ()))
        for name in names:
            qualified = f'"{ownership.schema_name}"."{name}"'
            columns = tuple(
                await session.execute(
                    text(
                        "SELECT attname::text,format_type(atttypid,atttypmod) FROM pg_attribute "
                        "WHERE attrelid=to_regclass(:name) AND attnum>0 AND NOT attisdropped ORDER BY attnum"
                    ),
                    {"name": qualified},
                )
            )
            digest, count = hashlib.sha256(), 0
            serialized_contents = await session.stream(
                text(
                    f'SELECT row_to_json(payload)::text FROM {qualified} payload ORDER BY row_to_json(payload)::text COLLATE "C"'
                )
            )
            try:
                async for serialized_record in serialized_contents:
                    digest.update(serialized_record[0].encode("utf8"))
                    digest.update(b"\n")
                    count += 1
            finally:
                await serialized_contents.close()
            table_contents.append((name, tuple(map(tuple, columns)), count, digest.hexdigest()))
        sequences = []
        for name, _, table, column in ownership.sequence_oids:
            state = (
                await session.execute(text(f'SELECT last_value,is_called FROM "{ownership.schema_name}"."{name}"'))
            ).one()
            sequences.append((name, table, column, tuple(state)))
        return self._declaration(ownership), tuple(table_contents), tuple(sequences)


@asynccontextmanager
async def native_reference_source(sessions, *, readers=(), owner=None):
    """Use a distinct NOLOGIN fixture role; grant Readers only SELECT access."""
    is_created = owner is None
    owner = owner or "reference_clone_owner_" + uuid4().hex
    custody = ReferenceSourceCustody(owner, readers)
    try:
        async with sessions.begin() as session:
            if is_created:
                await session.execute(
                    text(f'CREATE ROLE "{owner}" NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT')
                )
        yield custody
    finally:
        async with sessions.begin() as session:
            for schema in sorted(custody.schemas):
                await session.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
            if is_created and await session.scalar(
                text("SELECT 1 FROM pg_roles WHERE rolname=:owner"), {"owner": owner}
            ):
                await session.execute(text(f'DROP ROLE "{owner}"'))
