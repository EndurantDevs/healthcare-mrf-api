# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Real post-finalization write seals survive native publication and retained predecessors."""

from uuid import uuid4

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError

from process import cms_doctors_preparation as preparation
from process import reference_family_archive as archive
from tests.cms_doctors_preparation_postgres_support import doctors_database, stage_family
from tests.test_provider_directory_cms_serving_receipt_postgres import _apply


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
async def test_each_finalized_table_rejects_all_data_writes_in_replica_sessions(monkeypatch, table_index, statement):
    """The seal protects rows through its ALWAYS statement trigger, even for zero affected rows."""
    async with doctors_database(monkeypatch, cms_active=False) as fixture:
        ctx = await stage_family(fixture.database, fixture.schema)
        async with preparation.prepare_cms_doctors_generation(ctx) as prepared:
            _target, stage, oid = prepared.stage_oids[table_index]
            relation = f'"{fixture.schema}"."{stage}"'
            with pytest.raises(DBAPIError, match="cms_doctors_prepared_read_only"):
                async with fixture.database.transaction() as session:
                    await session.execute(text("SET LOCAL session_replication_role='replica'"))
                    await session.execute(text(statement.format(relation=relation)))
            assert await fixture.database.scalar("SELECT to_regclass(:name)::oid::bigint", name=relation) == oid
            assert await fixture.database.scalar(f"SELECT count(*) FROM {relation}") == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["ordinary", "after", "events", "args", "when", "body", "security", "settings"])
async def test_changed_seal_contract_rejects_native_apply(monkeypatch, change):
    """A named trigger alone cannot stand in for the exact immutable native guard."""
    async with doctors_database(monkeypatch, cms_active=False) as fixture:
        ctx = await stage_family(fixture.database, fixture.schema)
        async with preparation.prepare_cms_doctors_generation(ctx) as prepared:
            async with fixture.database.transaction() as session:
                await _tamper_seal(session, prepared, change)
            with pytest.raises(RuntimeError, match="seal.*changed"):
                async with fixture.database.transaction():
                    await preparation.apply_prepared_cms_doctors_generation(prepared)
            assert (
                await fixture.database.scalar(f'SELECT city FROM "{fixture.schema}".doctor_clinician_address')
                == "incumbent"
            )


async def _tamper_seal(session, prepared, change):
    """Mutate one owned fixture guard without affecting any shared schema."""
    _target, stage, _oid = prepared.stage_oids[0]
    relation = f'"{prepared.schema}"."{stage}"'
    function = f'"{prepared.schema}".cms_doctors_prepared_immutable()'
    trigger = preparation._SEAL_TRIGGER
    if change == "body":
        await session.execute(
            text(
                f"CREATE OR REPLACE FUNCTION {function} RETURNS trigger LANGUAGE plpgsql AS $$BEGIN RETURN NULL; END;$$"
            )
        )
    elif change in {"security", "settings"}:
        clause = "SECURITY DEFINER" if change == "security" else "RESET ALL"
        await session.execute(text(f"ALTER FUNCTION {function} {clause}"))
    elif change == "ordinary":
        await session.execute(text(f"ALTER TABLE {relation} ENABLE TRIGGER {trigger}"))
    else:
        await session.execute(text(f"DROP TRIGGER {trigger} ON {relation}"))
        timing = "AFTER" if change == "after" else "BEFORE"
        events = "INSERT" if change == "events" else "INSERT OR UPDATE OR DELETE OR TRUNCATE"
        arguments = "'extra'" if change == "args" else ""
        condition = "WHEN (true)" if change == "when" else ""
        await session.execute(
            text(
                f"CREATE TRIGGER {trigger} {timing} {events} ON {relation} FOR EACH STATEMENT {condition} "
                f"EXECUTE FUNCTION {function[:-1]}{arguments})"
            )
        )
        await session.execute(text(f"ALTER TABLE {relation} ENABLE ALWAYS TRIGGER {trigger}"))


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
        with pytest.raises(DBAPIError, match="depend"):
            async with fixture.database.engine.begin() as connection:
                await connection.run_sync(lambda sync: _apply(sync, "20260930130000", "downgrade"))


async def _assert_named_family_sealed(fixture, prepared, suffix):
    """Every surviving heap carries the trigger across the native table rename."""
    for target, _stage, _oid in prepared.stage_oids:
        with pytest.raises(DBAPIError, match="cms_doctors_prepared_read_only"):
            await fixture.database.status(f'DELETE FROM "{fixture.schema}"."{target}{suffix}" WHERE FALSE')


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
        async with fixture.database.transaction() as session:
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
            async with fixture.database.transaction() as session:
                await archive.cleanup_reference_family_stage(session, ownership)
                assert (
                    await session.scalar(text("SELECT to_regnamespace(:schema)"), {"schema": ownership.schema_name})
                    is None
                )


@pytest.mark.asyncio
async def test_migration_does_not_retroactively_seal_incumbent_tables(monkeypatch):
    """An old generation retains its existing mutation contract until replaced."""
    async with doctors_database(monkeypatch, cms_active=False) as fixture:
        assert (
            await fixture.database.status(f"UPDATE \"{fixture.schema}\".doctor_clinician_address SET city='historical'")
            == 1
        )
        async with fixture.database.engine.begin() as connection:
            await connection.run_sync(lambda sync: _apply(sync, "20260930130000", "downgrade"))
        assert (
            await fixture.database.scalar(
                "SELECT to_regprocedure(:name)", name=f'"{fixture.schema}".cms_doctors_prepared_immutable()'
            )
            is None
        )
