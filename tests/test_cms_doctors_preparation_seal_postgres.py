# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native ACL/catalog custody survives publication and retained predecessors."""

from contextlib import asynccontextmanager
from uuid import uuid4

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError

from process import cms_doctors_preparation as preparation
from process import reference_family_archive as archive
from tests.cms_doctors_component_postgres_support import ordinary_doctors_reader
from tests.cms_doctors_preparation_postgres_support import doctors_database, stage_family
from tests.cms_doctors_preparation_postgres_support import doctors_source_template as doctors_source_template


@pytest.mark.asyncio
@pytest.mark.parametrize("table_index", range(3))
@pytest.mark.parametrize(
    "statement",
    [
        "INSERT INTO {relation} DEFAULT VALUES",
        "UPDATE {relation} SET npi=npi",
        "DELETE FROM {relation} WHERE FALSE",
        "TRUNCATE {relation}",
    ],
)
async def test_each_finalized_table_denies_unprivileged_data_writes(monkeypatch, table_index, statement):
    """Read-write Reader transactions are denied by ACLs, with no payload hook."""
    async with doctors_database(monkeypatch, cms_active=False) as fixture:
        ctx = await stage_family(fixture.database, fixture.schema)
        async with preparation.prepare_cms_doctors_generation(ctx) as prepared:
            _target, stage, oid = prepared.stage_oids[table_index]
            relation = f'"{fixture.schema}"."{stage}"'
            async with ordinary_doctors_reader(fixture.database) as session:
                with pytest.raises(DBAPIError) as denied:
                    await session.execute(text(statement.format(relation=relation)))
                assert denied.value.orig.sqlstate == "42501"
            assert await fixture.database.scalar("SELECT to_regclass(:name)::oid::bigint", name=relation) == oid
            assert await fixture.database.scalar(f"SELECT count(*) FROM {relation}") == 1
            assert not await fixture.database.scalar(
                "SELECT EXISTS(SELECT FROM pg_trigger WHERE tgrelid=:oid AND NOT tgisinternal)", oid=oid
            )


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["update", "column", "truncate", "owner", "index", "column_shape", "rls", "rule"])
async def test_changed_native_custody_rejects_apply(monkeypatch, change):
    """Changing effective writer authority or native storage invalidates the seal."""
    async with doctors_database(monkeypatch, cms_active=False) as fixture:
        ctx = await stage_family(fixture.database, fixture.schema)
        async with preparation.prepare_cms_doctors_generation(ctx) as prepared:
            async with _changed_native_custody(fixture, prepared, change):
                await _assert_custody_rejected(fixture, prepared, change)
            assert (
                await fixture.database.scalar(f'SELECT city FROM "{fixture.schema}".doctor_clinician_address')
                == "incumbent"
            )
            async with fixture.database.transaction() as session:
                await preparation.assert_prepared_cms_doctors_seal(session, prepared)


@asynccontextmanager
async def _changed_native_custody(fixture, prepared, change):
    relation = f'"{fixture.schema}"."{prepared.stage_oids[0][1]}"'
    reader, owner = fixture.component.reader, fixture.component.owner
    async with fixture.engine.connect() as admin:
        actor = await admin.scalar(text("SELECT session_user"))
        await admin.rollback()
        alter, restore = {
            "update": (f'GRANT UPDATE ON {relation} TO "{reader}"', f'REVOKE UPDATE ON {relation} FROM "{reader}"'),
            "column": (
                f'GRANT INSERT(city) ON {relation} TO "{reader}"',
                f'REVOKE INSERT(city) ON {relation} FROM "{reader}"',
            ),
            "truncate": (
                f'GRANT TRUNCATE ON {relation} TO "{reader}"',
                f'REVOKE TRUNCATE ON {relation} FROM "{reader}"',
            ),
            "owner": (f'ALTER TABLE {relation} OWNER TO "{actor}"', f'ALTER TABLE {relation} OWNER TO "{owner}"'),
            "index": (
                f"CREATE INDEX extra_custody_index ON {relation} (city)",
                f'DROP INDEX "{fixture.schema}".extra_custody_index',
            ),
            "column_shape": (
                f"ALTER TABLE {relation} ADD COLUMN extra text",
                f"ALTER TABLE {relation} DROP COLUMN extra",
            ),
            "rls": (
                f"ALTER TABLE {relation} ENABLE ROW LEVEL SECURITY",
                f"ALTER TABLE {relation} DISABLE ROW LEVEL SECURITY",
            ),
            "rule": (
                f"CREATE RULE changed_custody AS ON DELETE TO {relation} DO INSTEAD NOTHING",
                f"DROP RULE changed_custody ON {relation}",
            ),
        }[change]
        await admin.execute(text(alter))
        await admin.commit()
        try:
            yield
        finally:
            await admin.execute(text(restore))
            await admin.commit()


async def _assert_custody_rejected(fixture, prepared, change):
    if change == "owner":
        with pytest.raises(DBAPIError) as denied:
            async with fixture.database.transaction():
                await preparation.apply_prepared_cms_doctors_generation(prepared)
        assert denied.value.orig.sqlstate == "42501"
    else:
        with pytest.raises(RuntimeError, match="ordinary mutation|cms_doctors_immutable_"):
            async with fixture.database.transaction():
                await preparation.apply_prepared_cms_doctors_generation(prepared)


@pytest.mark.asyncio
@pytest.mark.parametrize("table_index", range(3))
async def test_post_seal_rewrite_of_any_family_heap_rejects_publication(monkeypatch, table_index):
    """The source proof includes all three final filenodes, not only the address used by geometry."""
    async with doctors_database(monkeypatch, cms_active=False) as fixture:
        ctx = await stage_family(fixture.database, fixture.schema)
        async with preparation.prepare_cms_doctors_generation(ctx) as prepared:
            _target, stage, oid = prepared.stage_oids[table_index]
            relation = f'"{fixture.schema}"."{stage}"'
            await fixture.database.status(f"ALTER TABLE {relation} SET UNLOGGED")
            await fixture.database.status(f"ALTER TABLE {relation} SET LOGGED")
            assert await fixture.database.scalar("SELECT to_regclass(:name)::oid::bigint", name=relation) == oid
            with pytest.raises(RuntimeError, match="physical_seal_changed"):
                async with fixture.database.transaction():
                    await preparation.apply_prepared_cms_doctors_generation(prepared)


@pytest.mark.asyncio
async def test_future_import_has_mutable_new_heaps_and_retained_generations_stay_sealed(monkeypatch):
    """Ordinary source builders can populate new tables while published and previous tables stay read-only."""
    async with doctors_database(monkeypatch, cms_active=False) as fixture:
        for expected_generation in (2, 3):
            ctx = await stage_family(fixture.database, fixture.schema)
            async with preparation.prepare_cms_doctors_generation(ctx) as prepared:
                async with fixture.database.transaction():
                    result = await preparation.apply_prepared_cms_doctors_generation(prepared)
                    assert result.local_generation == expected_generation
                await _assert_named_family_sealed(fixture, prepared, "")
                if expected_generation == 3:
                    await _assert_named_family_sealed(fixture, prepared, "_old")
        assert not await fixture.database.scalar(
            "SELECT EXISTS(SELECT FROM pg_trigger WHERE tgrelid=ANY(CAST(:oids AS oid[])) AND NOT tgisinternal)",
            oids=[oid for _, _, oid in prepared.stage_oids],
        )


async def _assert_named_family_sealed(fixture, prepared, suffix):
    """Every surviving heap remains closed to ordinary write authority after rename."""
    for target, _stage, _oid in prepared.stage_oids:
        async with ordinary_doctors_reader(fixture.database) as session:
            with pytest.raises(DBAPIError) as denied:
                await session.execute(text(f'DELETE FROM "{fixture.schema}"."{target}{suffix}" WHERE FALSE'))
            assert denied.value.orig.sqlstate == "42501"


@pytest.mark.asyncio
async def test_archive_restore_creates_new_mutable_heaps_from_sealed_source(monkeypatch):
    """The native restore builds fresh model tables; it never mutates a sealed generation."""
    async with doctors_database(monkeypatch, cms_active=False) as fixture:
        ctx = await stage_family(fixture.database, fixture.schema)
        async with preparation.prepare_cms_doctors_generation(ctx) as prepared:
            async with fixture.database.transaction():
                await preparation.apply_prepared_cms_doctors_generation(prepared)
            await _assert_fresh_restore(fixture, prepared)


async def _assert_fresh_restore(fixture, prepared):
    """Create and clean the exact native UUID stage through its existing ownership lifecycle."""
    ownership = None
    try:
        async with fixture.database.session_factory() as session, session.begin():
            ownership = await archive.precreate_reference_family_restore(
                session, importer_id="cms-doctors", dataset_id=uuid4()
            )
            for target, _stage, _oid in prepared.stage_oids:
                relation = f'"{ownership.schema_name}"."{target}"'
                await session.execute(text(f'INSERT INTO {relation} SELECT * FROM "{fixture.schema}"."{target}"'))
                assert (await session.execute(text(f"UPDATE {relation} SET npi=npi"))).rowcount == 1
            await archive.complete_reference_family_restore(session, ownership)
    finally:
        if ownership is not None:
            async with fixture.database.session_factory() as session, session.begin():
                await archive.cleanup_reference_family_stage(session, ownership)
                assert (
                    await session.scalar(text("SELECT to_regnamespace(:schema)"), {"schema": ownership.schema_name})
                    is None
                )


@pytest.mark.asyncio
async def test_current_incumbent_is_closed_before_successor_preparation(monkeypatch):
    """The current operator cut closes incumbent ACLs without adding statement hooks."""
    async with doctors_database(monkeypatch, cms_active=False) as fixture:
        for table in ("doctor_clinician_address", "cms_doctor_education", "cms_doctor_group_site"):
            relation = f'"{fixture.schema}"."{table}"'
            async with ordinary_doctors_reader(fixture.database) as session:
                with pytest.raises(DBAPIError) as denied:
                    await session.execute(text(f"DELETE FROM {relation} WHERE FALSE"))
                assert denied.value.orig.sqlstate == "42501"
            assert not await fixture.database.scalar(
                "SELECT EXISTS(SELECT FROM pg_trigger WHERE tgrelid=to_regclass(:relation) AND NOT tgisinternal)",
                relation=relation,
            )
