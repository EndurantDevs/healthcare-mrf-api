# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Landing safety cases enrolled through the existing native bulk entrypoint."""

import pytest
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy import func, select, text
from sqlalchemy.exc import DBAPIError

from db.models.custom_import import CustomImportBuildAttempt, CustomImportBuildStream, CustomImportPack
from process.custom_import import build_source as source
from process.custom_import import source_finalize_sql as finalizer
from process.custom_import.storage_layout import snapshot_models, snapshot_schema
from tests.custom_import_postgres_support import isolated_publication_case
from tests.test_custom_import_bulk_snapshot_writers_postgres import _authorize, _bootstrap
from tests.test_custom_import_unlogged_landing import _migration


def _install(connection, schema):
    migration = _migration()
    migration._schema = lambda: schema
    migration.op = Operations(MigrationContext.configure(connection))
    migration.upgrade()


async def _catalog(connection, schema):
    signatures = [item[0] for item in _migration()._definitions(schema)]
    rows = await connection.execute(
        text(
            "SELECT p.oid,p.prosrc,to_jsonb(p)-'prosrc' metadata FROM pg_proc p "
            "JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname=:schema "
            "AND p.oid IN (SELECT to_regprocedure(format('%I.%s',:schema,signature)) "
            "FROM unnest(CAST(:signatures AS text[])) signature) ORDER BY p.oid"
        ),
        dict(schema=schema, signatures=signatures),
    )
    return {row.oid: dict(body=row.prosrc, metadata=row.metadata) for row in rows}


async def _persistence(session, family_id):
    return await session.scalar(
        text("SELECT relpersistence::text FROM pg_class WHERE oid=to_regclass(:name)"),
        dict(name=f"{snapshot_schema(family_id)}.source_bulk_landing"),
    )


async def _assert_source_state(case, context, family_id, *, count):
    async with case.sessions() as session:
        build = await session.get(CustomImportBuildAttempt, context.build_id)
        cursor = await session.get(CustomImportBuildStream, (context.build_id, context.stream_slot))
        assert build.source_occurrence_count == cursor.next_source_ordinal == cursor.next_pack_ordinal == count
        pack_model = snapshot_models(family_id)[CustomImportPack]
        assert await session.scalar(select(func.count()).select_from(pack_model)) == count
        namespace = snapshot_schema(family_id)
        assert await session.scalar(text(f'SELECT count(*) FROM "{namespace}".source_bulk_landing')) == 0
        assert await session.scalar(text(f'SELECT count(*) FROM "{case.schema_name}".source_bulk_completion')) == count


async def test_logged_family_survives_migration_with_all_function_metadata_exact():
    async with isolated_publication_case() as case:
        context, page, family_id = await _bootstrap(case)
        async with case.engine.begin() as connection:
            before = await _catalog(connection, case.schema_name)
            await connection.run_sync(_install, case.schema_name)
            after = await _catalog(connection, case.schema_name)
            assert len(before) == len(after) == 3
            assert before.keys() == after.keys()
            assert all(before[oid]["metadata"] == after[oid]["metadata"] for oid in before)
            assert all(before[oid]["body"] != after[oid]["body"] for oid in before)
            await connection.run_sync(_install, case.schema_name)
            assert await _catalog(connection, case.schema_name) == after
        async with case.sessions() as session:
            assert await _persistence(session, family_id) == "p"
        assert await source._store_single_page(case.sessions, context, page) == 1
        await source._compare_committed_page(case.sessions, context, page)
        assert (await source.stage_segmented_source(case.sessions, context.request)).source_occurrence_count == 1


async def test_new_landing_rolls_back_promoted_batch_and_replays_committed_prefix(monkeypatch):
    async with isolated_publication_case() as case:
        async with case.engine.begin() as connection:
            await connection.run_sync(_install, case.schema_name)
        context, page, family_id = await _bootstrap(case)
        async with case.sessions() as session:
            assert await _persistence(session, family_id) == "u"
        original = finalizer.finalize_source_batch

        async def interrupted(session, batch_id, parts):
            assert await original(session, batch_id, parts) == 1
            raise RuntimeError("synthetic interruption before commit")

        with monkeypatch.context() as patch:
            patch.setattr(finalizer, "finalize_source_batch", interrupted)
            with pytest.raises(RuntimeError, match="before commit"):
                await source._store_single_page(case.sessions, context, page)
        await _assert_source_state(case, context, family_id, count=0)
        assert await source._store_single_page(case.sessions, context, page) == 1
        await _assert_source_state(case, context, family_id, count=1)
        # A lost response after commit must replay the retained prefix exactly.
        await source._compare_committed_page(case.sessions, context, page)
        outcome = await source.stage_segmented_source(case.sessions, context.request)
        assert outcome.source_occurrence_count == 1 and outcome.phase == "graph"
        assert await source.stage_segmented_source(case.sessions, context.request) == outcome


async def test_unlogged_serving_leaf_is_still_rejected_and_rolls_back():
    async with isolated_publication_case() as case:
        async with case.engine.begin() as connection:
            await connection.run_sync(_install, case.schema_name)
        context, page, family_id = await _bootstrap(case)
        namespace = snapshot_schema(family_id)
        with pytest.raises(DBAPIError, match="custom_import_snapshot_relation_mismatch"):
            async with case.engine.begin() as connection:
                await connection.execute(text(f'ALTER TABLE "{namespace}".custom_import_pack SET UNLOGGED'))
                await connection.execute(
                    text(f'SELECT * FROM "{case.schema_name}".resolve_custom_import_snapshot_relations(:family)'),
                    dict(family=family_id),
                )
        async with source._page_session(case.sessions, context.request, context.build_id) as (session, _):
            batch_id, records = await _authorize(session, context, page)
            await source._copy_source_landing(session, records)
            assert (
                await source._call(session, "source_set_finalize", (("uuid", batch_id), ("integer[]", [])))
            ).scalar_one() == 1
        await _assert_source_state(case, context, family_id, count=1)
