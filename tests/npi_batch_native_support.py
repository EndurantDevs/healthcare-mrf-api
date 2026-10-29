# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native isolated writer parity and cleanup in the registered archive lane."""

import asyncio
from types import SimpleNamespace

import pytest
from sqlalchemy import MetaData, text

from db.connection import Database
from db.models import NPIAddress, NPIData, NPIDataOtherIdentifier, NPIDataTaxonomy, NPIDataTaxonomyGroup
from process import npi_isolated_batches as batches


async def _prepared(runtime):
    from tests.npi_owned_publication_postgres_support import _builder_output

    output, _owner_oid, _builder_oid = await _builder_output(runtime)
    models_by_kind = {}
    for kind, model, rewrite in (
        ("npi_obj_list", NPIData, True),
        ("npi_taxonomy_list", NPIDataTaxonomy, True),
        ("npi_other_id_list", NPIDataOtherIdentifier, False),
        ("npi_taxonomy_group_list", NPIDataTaxonomyGroup, True),
        ("npi_address_list", NPIAddress, True),
    ):
        models_by_kind[kind] = (
            SimpleNamespace(
                __table__=model.__table__.to_metadata(MetaData(), schema=output.schema_name),
                __tablename__=model.__main_table__,
                __my_index_elements__=model.__my_index_elements__,
            ),
            rewrite,
        )
    database = Database()
    database._database_override = runtime.builder_url.database
    return output.schema_name, database, models_by_kind


async def _nothing():
    return None


async def _identity(session):
    return tuple(
        (
            await session.execute(
                text("SELECT pg_backend_pid(),pg_current_xact_id()::text,session_user::text,current_user::text")
            )
        ).one()
    )


async def _assert_temp_removed(session, prefix):
    assert (
        await session.scalar(
            text(
                "SELECT count(*) FROM pg_catalog.pg_class WHERE relnamespace=pg_my_temp_schema() "
                "AND left(relname,length(:prefix))=:prefix"
            ),
            {"prefix": prefix},
        )
        == 0
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "kind,table,field",
    [
        ("npi_taxonomy_list", "npi_taxonomy", "healthcare_provider_taxonomy_code"),
        ("npi_other_id_list", "npi_other_identifier", "other_provider_identifier"),
        ("npi_taxonomy_group_list", "npi_taxonomy_group", "healthcare_provider_taxonomy_group"),
    ],
)
async def test_native_window_keeps_checksum_collision_parity(native_runtime, kind, table, field):
    runtime = native_runtime
    schema, database, models_by_kind = await _prepared(runtime)
    async with runtime.builders() as session, session.begin(), database.bind_existing_session(session):
        before = await _identity(session)
        assert before[2:] == (runtime.builder, runtime.builder)
        async with batches.bind(database, models_by_kind.__getitem__, _nothing) as writer:
            await writer.add({kind: [{"npi": 1000000001, "checksum": 7, field: "first"}]})
            await writer.add(
                {
                    kind: [
                        {"npi": 1000000002, "checksum": 7, field: "last"},
                        {"npi": 1000000001, "checksum": 8, field: "other"},
                    ]
                }
            )
        rows = (
            await session.execute(text(f'SELECT npi,checksum,"{field}" FROM "{schema}"."{table}" ORDER BY checksum'))
        ).all()
        assert [tuple(row) for row in rows] == [(1000000002, 7, "last"), (1000000001, 8, "other")]
        assert await _identity(session) == before
        await _assert_temp_removed(session, writer.prefix)


@pytest.mark.asyncio
async def test_native_windows_keep_rewrite_and_identifier_winners(native_runtime):
    schema, database, models_by_kind = await _prepared(native_runtime)
    async with native_runtime.builders() as session, session.begin(), database.bind_existing_session(session):
        async with batches.bind(database, models_by_kind.__getitem__, _nothing) as writer:
            for label in ("first", "within"):
                await writer.add(
                    {
                        "npi_obj_list": [
                            {"npi": 1000000001, "provider_first_name": label, "do_business_as": [label, "two"]}
                        ],
                        "npi_other_id_list": [{"npi": 1000000001, "checksum": 3, "other_provider_identifier": label}],
                    }
                )
            await writer.finish_window()
            await writer.add(
                {
                    "npi_obj_list": [{"npi": 1000000001, "provider_first_name": "later", "do_business_as": ["later"]}],
                    "npi_other_id_list": [{"npi": 1000000001, "checksum": 3, "other_provider_identifier": "later"}],
                }
            )
        observed = await session.scalar(
            text(
                f"SELECT jsonb_build_object('name',provider_first_name,'aliases',do_business_as,'identifier',"
                f'(SELECT other_provider_identifier FROM "{schema}".npi_other_identifier)) FROM "{schema}".npi'
            )
        )
        assert observed == {"name": "later", "aliases": ["later"], "identifier": "within"}
        assert await session.scalar(text(f'SELECT count(*) FROM "{schema}".npi')) == 1
        assert await session.scalar(text(f'SELECT count(*) FROM "{schema}".npi_other_identifier')) == 1
        await _assert_temp_removed(session, writer.prefix)


@pytest.mark.asyncio
async def test_native_borrowed_copy_retains_outer_rollback(native_runtime):
    schema, database, models_by_kind = await _prepared(native_runtime)
    async with native_runtime.builders() as session:
        async with session.begin() as outer:
            async with database.bind_existing_session(session):
                connection = await session.connection()
                raw = await connection.get_raw_connection()
                driver = raw.driver_connection
                before_pid = driver.get_server_pid()
                assert not driver.is_in_transaction()
                async with batches.bind(database, models_by_kind.__getitem__, _nothing) as writer:
                    await writer.add({"npi_obj_list": [{"npi": 1000000001, "provider_first_name": "private"}]})
                identity = await _identity(session)
                assert identity[0] == before_pid
                assert identity[2:] == (native_runtime.builder, native_runtime.builder)
                assert session.get_transaction() is outer and outer.is_active
                assert await session.scalar(text(f'SELECT count(*) FROM "{schema}".npi')) == 1
                await _assert_temp_removed(session, writer.prefix)
            await outer.rollback()
    async with native_runtime.builders() as session, session.begin():
        assert await session.scalar(text(f'SELECT count(*) FROM "{schema}".npi')) == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("error_type", [ValueError, asyncio.CancelledError])
async def test_native_failed_window_preserves_caller_transaction(native_runtime, error_type):
    schema, database, models_by_kind = await _prepared(native_runtime)
    async with native_runtime.builders() as session, session.begin(), database.bind_existing_session(session):
        await session.execute(text(f'INSERT INTO "{schema}".npi(npi) VALUES(1000000001)'))
        before = await _identity(session)

        async def fail_after_insert():
            if await session.scalar(text(f'SELECT EXISTS(SELECT FROM "{schema}".npi WHERE npi=1000000002)')):
                raise error_type("synthetic window failure")

        with pytest.raises(error_type):
            async with batches.bind(database, models_by_kind.__getitem__, fail_after_insert) as writer:
                await writer.add({"npi_obj_list": [{"npi": 1000000002}]})
        assert await _identity(session) == before
        assert (await session.execute(text(f'SELECT npi FROM "{schema}".npi'))).scalars().all() == [1000000001]
        await _assert_temp_removed(session, writer.prefix)


@pytest.mark.asyncio
async def test_native_repeated_cancel_drains_cleanup(native_runtime):
    schema, database, models_by_kind = await _prepared(native_runtime)
    async with native_runtime.builders() as session, session.begin(), database.bind_existing_session(session):
        before = await _identity(session)
        writer = batches.NativeWriteWindow(database, models_by_kind.__getitem__, _nothing)
        await writer.add({"npi_obj_list": [{"npi": 1000000001}]})
        driver, entered, release = writer.connection, asyncio.Event(), asyncio.Event()

        class DelayedRollback:
            def __getattr__(self, name):
                return getattr(driver, name)

            async def execute(self, statement, *arguments):
                if statement.startswith("ROLLBACK TO SAVEPOINT"):
                    entered.set()
                    await release.wait()
                return await driver.execute(statement, *arguments)

        writer.connection = DelayedRollback()
        closing = asyncio.create_task(writer.close(ValueError("synthetic failure")))
        try:
            await asyncio.wait_for(entered.wait(), timeout=5)
            closing.cancel()
            await asyncio.sleep(0)
            closing.cancel()
            release.set()
            with pytest.raises(asyncio.CancelledError):
                await closing
            assert await _identity(session) == before
            assert await session.scalar(text(f'SELECT count(*) FROM "{schema}".npi')) == 0
            await _assert_temp_removed(session, writer.prefix)
        finally:
            release.set()
            if not closing.done():
                await closing


@pytest.mark.asyncio
async def test_native_weekly_delete_keeps_family_type_scope(native_runtime):
    schema, database, models_by_kind = await _prepared(native_runtime)
    async with native_runtime.builders() as session, session.begin(), database.bind_existing_session(session):
        async with batches.bind(database, models_by_kind.__getitem__, _nothing) as writer:
            await writer.add(
                {
                    "npi_obj_list": [{"npi": 1000000001}, {"npi": 1000000002}],
                    "npi_other_id_list": [{"npi": 1000000001, "checksum": 9, "other_provider_identifier": "kept"}],
                    "npi_address_list": [
                        {"npi": npi, "type": kind, "checksum": index}
                        for npi in (1000000001, 1000000002)
                        for index, kind in enumerate(("primary", "mail", "secondary"))
                    ],
                }
            )
            await writer.finish_window()

            async def keys():
                for npi in ("1000000001", "1000000001", "1000000003", ""):
                    yield {"NPI": npi}

            count = await writer.delete_selected(
                keys(),
                ((models_by_kind["npi_obj_list"][0], ()), (models_by_kind["npi_address_list"][0], ("primary", "mail"))),
            )
            assert count == 2
        assert (await session.execute(text(f'SELECT npi FROM "{schema}".npi'))).scalars().all() == [1000000002]
        remaining = (
            await session.execute(text(f'SELECT npi,type FROM "{schema}".npi_address ORDER BY npi,type'))
        ).all()
        assert [tuple(address_record) for address_record in remaining] == [
            (1000000001, "secondary"),
            (1000000002, "mail"),
            (1000000002, "primary"),
            (1000000002, "secondary"),
        ]
        assert (
            await session.scalar(text(f'SELECT other_provider_identifier FROM "{schema}".npi_other_identifier'))
            == "kept"
        )
        await _assert_temp_removed(session, writer.prefix)
