# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Shared final-schema DDL for reference-family archive fixtures."""

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


async def install_source_generation_guards(connection, schema: str) -> None:
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


async def install_source_generation_guards_from_dsn(dsn: str, schema: str) -> None:
    """Migrate committed asyncpg fixtures through a real Alembic connection."""
    engine = create_async_engine(make_url(dsn).set(drivername="postgresql+asyncpg"))
    try:
        async with engine.begin() as connection:
            await install_source_generation_guards(connection, schema)
    finally:
        await engine.dispose()


class ReferenceSourceCustody:
    """Bind actual empty heaps and their closed catalog to the clone transaction."""

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
        for table, oid in ownership.relation_oids:
            assert await session.scalar(
                text("SELECT relowner=current_user::regrole::oid FROM pg_class WHERE oid=:oid"), {"oid": oid}
            )
            assert not await session.scalar(text(f'SELECT EXISTS(SELECT 1 FROM "{ownership.schema_name}"."{table}")'))
            assert not await session.scalar(
                text("SELECT EXISTS(SELECT 1 FROM pg_index WHERE indrelid=:oid)"), {"oid": oid}
            )
        self.precreated[ownership.dataset_id] = (session.get_transaction(), ownership)
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
        transaction, ownership = self.precreated[prepared.ownership.dataset_id]
        assert session.get_transaction() is transaction and transaction.is_active
        assert prepared.ownership == ownership
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

        assert self.precreated[ownership.dataset_id][1] == ownership
        await archive.cleanup_reference_family_stage(session, ownership)
        self.schemas.remove(ownership.schema_name)

    async def _receipt(self, session, ownership):
        oids = [oid for _, oid in ownership.relation_oids] + [oid for _, oid, _, _ in ownership.sequence_oids]
        catalog_records = (
            await session.execute(
                text(
                    "SELECT oid,relowner,xmin::text,ctid::text,relacl::text FROM pg_class "
                    "WHERE oid=ANY(CAST(:oids AS oid[])) OR oid IN "
                    "(SELECT indexrelid FROM pg_index WHERE indrelid=ANY(CAST(:oids AS oid[]))) ORDER BY oid"
                ),
                {"oids": oids},
            )
        ).all()
        owner_oid = await session.scalar(text("SELECT CAST(:owner AS regrole)::oid"), {"owner": self.owner})
        assert len(catalog_records) > len(oids) and all(
            catalog_record.relowner == owner_oid for catalog_record in catalog_records
        )
        assert await session.scalar(
            text(
                "SELECT NOT rolcanlogin AND NOT rolsuper AND NOT rolcreatedb AND NOT rolcreaterole "
                "AND NOT rolbypassrls FROM pg_roles WHERE oid=:oid"
            ),
            {"oid": owner_oid},
        )
        assert not await session.scalar(
            text(
                "SELECT EXISTS(SELECT 1 FROM pg_trigger WHERE tgrelid=ANY(CAST(:oids AS oid[]))) "
                "OR EXISTS(SELECT 1 FROM pg_constraint WHERE conrelid=ANY(CAST(:oids AS oid[])) AND contype='f')"
            ),
            {"oids": oids},
        )
        for reader in self.readers:
            assert not await session.scalar(
                text(
                    "SELECT pg_has_role(:reader,:owner,'MEMBER') OR has_schema_privilege(:reader,CAST(:schema AS oid),'CREATE') "
                    "OR EXISTS(SELECT 1 FROM unnest(CAST(:oids AS oid[])) r(oid) "
                    "WHERE has_table_privilege(:reader,r.oid,'INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER,MAINTAIN') "
                    "OR has_any_column_privilege(:reader,r.oid,'INSERT,UPDATE,REFERENCES'))"
                ),
                {
                    "reader": reader,
                    "owner": self.owner,
                    "schema": ownership.schema_oid,
                    "oids": [oid for _, oid in ownership.relation_oids],
                },
            )
            for _, oid, _, _ in ownership.sequence_oids:
                assert not await session.scalar(
                    text("SELECT has_sequence_privilege(:reader,CAST(:oid AS oid),'USAGE,UPDATE')"),
                    {"reader": reader, "oid": oid},
                )
        namespace = (
            await session.execute(
                text("SELECT nspowner,nspacl::text,xmin::text,ctid::text FROM pg_namespace WHERE oid=:oid"),
                {"oid": ownership.schema_oid},
            )
        ).one()
        assert namespace.nspowner == owner_oid
        return tuple(map(tuple, catalog_records)), tuple(namespace)


@asynccontextmanager
async def native_reference_source(sessions, *, readers=()):
    """Use a distinct registered NOLOGIN clone owner; never grant payload writes to Readers."""
    owner = "reference_clone_owner_" + uuid4().hex
    custody = ReferenceSourceCustody(owner, readers)
    try:
        async with sessions.begin() as session:
            await session.execute(text(f'CREATE ROLE "{owner}" NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT'))
        yield custody
    finally:
        async with sessions.begin() as session:
            for schema in sorted(custody.schemas):
                actual = await session.scalar(
                    text("SELECT nspowner FROM pg_namespace WHERE nspname=:schema"), {"schema": schema}
                )
                if actual is not None:
                    from process import reference_family_archive as archive

                    ownership = next(
                        value[1] for value in custody.precreated.values() if value[1].schema_name == schema
                    )
                    await archive.verify_reference_family_stage_ownership(session, ownership)
                    expected = await session.scalar(text("SELECT CAST(:owner AS regrole)::oid"), {"owner": owner})
                    assert actual == expected
                    await session.execute(text(f'DROP SCHEMA "{schema}" CASCADE'))
            if await session.scalar(text("SELECT 1 FROM pg_roles WHERE rolname=:owner"), {"owner": owner}):
                await session.execute(text(f'DROP ROLE "{owner}"'))
