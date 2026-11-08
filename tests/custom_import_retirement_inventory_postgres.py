# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Native bounded preparation, connection custody and frozen-reference coverage."""

from contextlib import asynccontextmanager

import pytest
from sqlalchemy import event, exists, select, text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncSession

from db.models.custom_import_storage import (
    CustomImportSnapshotFamily,
    CustomImportSnapshotRetirement,
)
from process.custom_import import retirement
from process.custom_import import retirement_inventory as inventory
from tests.custom_import_postgres_support import isolated_publication_case
from tests.test_custom_import_snapshot_storage_postgres import (
    _bounded,
    _external_fixture_check,
    _remember_retirement,
    _replaced_fence,
    _sealed_current_snapshot,
)


@asynccontextmanager
async def _inventory_case():
    async with isolated_publication_case() as case:
        _, target = await _replaced_fence(case)
        graph, _, kept = await _sealed_current_snapshot(case)
        async with case.engine.connect() as connection:
            try:
                yield case, connection, target, kept, graph
            finally:
                if connection.in_transaction():
                    await connection.rollback()
                await _cleanup_inventory(connection)


async def _cleanup_inventory(connection):
    async with AsyncSession(bind=connection) as session, session.begin():
        await inventory.cleanup_frozen_reference_inventory(session)
    assert await connection.scalar(text(f"SELECT to_regclass('pg_temp.{inventory._TABLE}')")) is None
    assert inventory._KEY not in connection.info and inventory._OWNED_KEY not in connection.info
    await connection.rollback()


@asynccontextmanager
async def _prepared(connection):
    async with AsyncSession(bind=connection, expire_on_commit=False) as session, session.begin():
        await _bounded(session)
        receipt = await inventory.prepare_frozen_reference_inventory(session)
        yield session, receipt


async def _prove(session, target):
    retained = (
        await session.scalars(
            select(CustomImportSnapshotFamily).where(
                ~exists().where(
                    CustomImportSnapshotRetirement.family_id == CustomImportSnapshotFamily.family_id,
                )
            )
        )
    ).all()
    family = next(row for row in retained if row.family_id == target)
    return await inventory.can_prove_frozen_reference_inventory(session, family, retained)


async def _home(case, target, ranges, kind=2):
    async with case.sessions() as session, session.begin():
        await session.execute(
            text(
                f'INSERT INTO "{case.schema_name}".custom_import_revision_home '
                "(family_id,revision_kind,revision_ids) VALUES (:family,:kind,CAST(CAST(:ranges AS text) AS int8multirange))"
            ),
            {"family": target, "kind": kind, "ranges": ranges},
        )


async def _members(case, kept, graph, ids):
    async with case.sessions() as session, session.begin():
        await session.execute(
            text(
                f'INSERT INTO "{retirement.snapshot_schema(kept)}".custom_import_family_child '
                "(family_revision_id,dataset_id,schema_revision_id,root_record_id,collection_slot,child_revision_id) "
                "SELECT 777,:dataset,:schema,999,1,id FROM unnest(CAST(:ids AS bigint[])) child(id)"
            ),
            {"dataset": graph.dataset_id, "schema": graph.schema_revision_id, "ids": ids},
        )


async def test_inventory_pages_stay_server_side_and_reuse_only_in_live_transaction(monkeypatch):
    async with _inventory_case() as (case, connection, target, kept, graph):
        await _home(case, target, "{[900,902),[904,906)}")
        await _members(case, kept, graph, [1, 2, 3, 902, 903])
        monkeypatch.setattr(retirement, "REFERENCE_PAGE_ROWS", 2)
        statements = []

        def record(_connection, _cursor, statement, _parameters, _context, _many):
            statements.append(statement)

        event.listen(connection.sync_connection, "before_cursor_execute", record)
        try:
            async with _prepared(connection) as (session, receipt):
                assert receipt["pages"] == 3 and receipt["rows"] == 3
                identity = connection.info[inventory._KEY].identity
                assert await _prove(session, target) is True
                assert await _prove(session, target) is True
                assert connection.info[inventory._KEY].identity == identity
        finally:
            event.remove(connection.sync_connection, "before_cursor_execute", record)
        pages = [statement for statement in statements if "WITH page AS MATERIALIZED" in statement]
        assert pages and all("range_agg(" in statement and "INSERT INTO pg_temp." in statement for statement in pages)
        assert all("OFFSET" not in statement for statement in pages)
        assert not any("LOCK TABLE" in statement and "SHARE ROW EXCLUSIVE" in statement for statement in statements)
        await _home(case, target, "{[902,903)}")
        async with _prepared(connection) as (session, _receipt):
            with pytest.raises(retirement.SnapshotRetirementRefused, match="revision reference"):
                await _prove(session, target)


@pytest.mark.parametrize("reference", [9000000, 9223372036854775807])
async def test_inventory_detects_wrong_parent_and_maximum_bigint_references(monkeypatch, reference):
    async with _inventory_case() as (case, connection, target, kept, graph):
        upper = "" if reference == 9223372036854775807 else str(reference + 1)
        await _home(case, target, "{[" + str(reference) + "," + upper + ")}")
        await _members(case, kept, graph, [1, 2, reference])
        monkeypatch.setattr(retirement, "REFERENCE_PAGE_ROWS", 2)
        async with _prepared(connection) as (session, _receipt):
            with pytest.raises(retirement.SnapshotRetirementRefused, match="revision reference"):
                await _prove(session, target)


@pytest.mark.parametrize("kind,revision_kind", [("root", 1), ("child", 2)])
async def test_inventory_preserves_origin_revision_kind_and_null_references(kind, revision_kind):
    async with _inventory_case() as (case, connection, target, kept, graph):
        await _home(case, target, "{[9000000,9000001)}", kind=revision_kind)
        async with case.sessions() as session, session.begin():
            await session.execute(
                text(
                    f'INSERT INTO "{retirement.snapshot_schema(kept)}".legacy_copy_origin '
                    "(kind,revision_id,family_revision_id,root_record_id,root_revision_id,collection_slot,"
                    "base_generation_id,base_family_revision_id,base_root_revision_id,base_child_revision_id) "
                    "VALUES (:kind,9000000,777,999,:root,:collection,:generation,888,201,:base_child)"
                ),
                {
                    "kind": kind,
                    "root": 9000000 if kind == "root" else 200,
                    "collection": None if kind == "root" else 1,
                    "generation": graph.first_generation_id,
                    "base_child": None if kind == "root" else 202,
                },
            )
        async with _prepared(connection) as (session, _receipt):
            with pytest.raises(retirement.SnapshotRetirementRefused, match="revision reference"):
                await _prove(session, target)


@pytest.mark.parametrize("change", ["truncate", "new_family", "missing_index", "temporary_rows"])
async def test_inventory_refuses_changed_frozen_or_temporary_evidence(change):
    async with _inventory_case() as (case, connection, target, kept, graph):
        await _members(case, kept, graph, [1, 2])
        async with _prepared(connection) as (session, _receipt):
            if change == "temporary_rows":
                await session.execute(text(f"DELETE FROM pg_temp.{inventory._TABLE}"))
                with pytest.raises(retirement.SnapshotRetirementRefused, match="page accounting"):
                    await _prove(session, target)
                return
        if change == "new_family":
            await _sealed_current_snapshot(case)
        else:
            async with case.sessions() as session, session.begin():
                namespace = retirement.snapshot_schema(kept)
                statement = (
                    f'TRUNCATE "{namespace}".custom_import_family_child'
                    if change == "truncate"
                    else f'ALTER TABLE "{namespace}".custom_import_build_occurrence '
                    "DROP CONSTRAINT custom_import_build_occurrence_root_revision_id_key"
                )
                await session.execute(text(statement))
        async with AsyncSession(bind=connection) as session, session.begin():
            await _bounded(session)
            assert await _prove(session, target) is False


async def _refuse_other_connection(case, connection, target):
    async with case.engine.connect() as other:
        other.info[inventory._KEY] = connection.info[inventory._KEY]
        try:
            async with AsyncSession(bind=other) as session, session.begin():
                await _bounded(session)
                with pytest.raises(retirement.SnapshotRetirementRefused, match="connection or temporary relation"):
                    await _prove(session, target)
        finally:
            other.info.pop(inventory._KEY, None)


async def test_inventory_is_bound_to_its_actual_connection_and_backend():
    async with _inventory_case() as (case, connection, target, _kept, _graph):
        async with AsyncSession(bind=connection) as session, session.begin():
            await _bounded(session)
            assert await _prove(session, target) is False
        async with _prepared(connection):
            await _refuse_other_connection(case, connection, target)


async def test_inventory_does_not_certify_interrupted_preparation(monkeypatch):
    async with _inventory_case() as (_case, connection, target, _kept, _graph):

        async def interrupted(*_args):
            raise RuntimeError("synthetic inventory interruption")

        with monkeypatch.context() as patch:
            patch.setattr(inventory, "_copy_reference_pages", interrupted)
            with pytest.raises(RuntimeError, match="synthetic inventory interruption"):
                async with _prepared(connection):
                    pytest.fail("interrupted preparation returned")
        assert inventory._KEY not in connection.info
        async with AsyncSession(bind=connection) as session, session.begin():
            await _bounded(session)
            with pytest.raises(retirement.SnapshotRetirementRefused, match="incomplete"):
                await _prove(session, target)
        async with _prepared(connection) as (session, _receipt):
            assert await _prove(session, target) is True


async def test_inventory_excludes_only_the_target_and_completed_retired_sources():
    async with _inventory_case() as (case, connection, target, kept, graph):
        await _home(case, kept, "{[9000000,9000001)}")
        await _members(case, kept, graph, [9000000])
        async with _prepared(connection) as (session, _receipt):
            assert await _prove(session, kept) is True
            await _remember_retirement(session, kept)
            row = await session.get(CustomImportSnapshotFamily, kept)
            await retirement._physical_drop(session, '"' + case.schema_name + '"', row)
            assert await _prove(session, target) is True


@pytest.mark.parametrize("operation", ["insert", "update"])
async def test_inventory_excludes_owner_writes_and_discards_cross_commit_evidence(operation):
    async with _inventory_case() as (database_case, connection, target_family_id, kept, graph):
        await _home(database_case, target_family_id, "{[9000000,9000001)}")
        await _members(database_case, kept, graph, [1])
        relation = f'"{retirement.snapshot_schema(kept)}".custom_import_family_child'
        statement = (
            f"INSERT INTO {relation} "
            "(family_revision_id,dataset_id,schema_revision_id,root_record_id,collection_slot,child_revision_id) "
            "VALUES (777,:dataset,:schema,999,1,9000000)"
            if operation == "insert"
            else f"UPDATE {relation} SET child_revision_id=9000000 WHERE child_revision_id=1"
        )
        bind_by_name = {"dataset": graph.dataset_id, "schema": graph.schema_revision_id}
        async with _prepared(connection) as (session, _receipt):
            assert await _prove(session, target_family_id) is True
            with pytest.raises(DBAPIError) as refused:
                async with database_case.sessions() as writer, writer.begin():
                    await writer.execute(text("SET LOCAL lock_timeout='100ms'"))
                    await writer.execute(text(statement), bind_by_name)
            assert getattr(refused.value.orig, "sqlstate", None) == "55P03"
            async with database_case.sessions() as reader:
                assert await reader.scalar(text(f"SELECT count(*) FROM {relation}")) == 1
        async with database_case.sessions() as writer, writer.begin():
            await writer.execute(text(statement), bind_by_name)
        async with AsyncSession(bind=connection) as session, session.begin():
            await _bounded(session)
            assert await _prove(session, target_family_id) is False
            with pytest.raises(retirement.SnapshotRetirementRefused, match="revision reference"):
                await retirement.retire_snapshot_family(
                    session,
                    target_family_id,
                    engine_id="synthetic",
                    assert_external_unreferenced=_external_fixture_check,
                )


async def _sparse_families(case, kept, references, *, gaps=False):
    count = len(references)
    buckets = [1 if index < count // 2 else 9 for index in range(count)]
    positions = [1, 1009, 999999, 7, 2**40, 2**63 - 1] if gaps else list(range(1, count + 1))
    async with case.sessions() as session, session.begin():
        await session.execute(
            text(
                f'INSERT INTO "{retirement.snapshot_schema(kept)}".custom_import_build_family '
                "(build_id,root_record_id,root_key_sha256,selection_kind,source_root_occurrence_id,"
                "last_child_collection_slot,last_child_key_sha256,last_input_child_revision_id,attached_child_count) "
                "SELECT source_bucket,source_position,decode(repeat('11',32),'hex'),'source',source_position,"
                "CASE WHEN source_reference IS NOT NULL THEN 1 END,"
                "CASE WHEN source_reference IS NOT NULL THEN decode(repeat('22',32),'hex') END,source_reference,0 "
                "FROM unnest(CAST(:references AS bigint[]),CAST(:buckets AS bigint[]),CAST(:positions AS bigint[])) "
                "source_rows(source_reference,source_bucket,source_position)"
            ),
            {"references": references, "buckets": buckets, "positions": positions},
        )


async def _assert_null_reference_write_excluded(case, kept):
    relation = f'"{retirement.snapshot_schema(kept)}".custom_import_build_family'
    with pytest.raises(DBAPIError) as refused:
        async with case.sessions() as writer, writer.begin():
            await writer.execute(text("SET LOCAL lock_timeout='100ms'"))
            await writer.execute(
                text(
                    f"UPDATE {relation} SET last_child_collection_slot=1,"
                    "last_child_key_sha256=decode(repeat('22',32),'hex'),last_input_child_revision_id=9000000 "
                    "WHERE build_id=1 AND root_record_id=1"
                )
            )
    assert getattr(refused.value.orig, "sqlstate", None) == "55P03"


async def _assert_inventory_shape(case, connection, target, kept, shape, references):
    relevant = sum(value is not None for value in references)
    async with _prepared(connection) as (session, receipt):
        coverage = connection.info[inventory._KEY].coverage[kept, "custom_import_build_family"][1]
        assert coverage == ((relevant + 1) // 2, len(references), relevant), shape
        assert receipt["source_rows"] == len(references), shape
        assert receipt["relevant_rows"] == relevant, shape
        assert receipt["pages"] == receipt["rows"] == (relevant + 1) // 2, shape
        assert await _prove(session, target) is True, shape
        if shape == "all_null":
            await _assert_null_reference_write_excluded(case, kept)


async def test_inventory_counts_complete_sources_and_only_pages_relevant_rows(monkeypatch):
    references_by_shape = {
        "empty": [],
        "all_null": [None] * 7,
        "sparse": [None, 11, None, 17, None, 23, None],
        "dense": [11, 13, 17, 19, 23],
        "composite_gaps": [11, None, None, 17, None, 23],
    }
    async with _inventory_case() as (case, connection, target, kept, _graph):
        await _home(case, target, "{[9000000,9000001)}")
        monkeypatch.setattr(retirement, "REFERENCE_PAGE_ROWS", 2)
        for shape, references in references_by_shape.items():
            await _sparse_families(case, kept, references, gaps=shape == "composite_gaps")
            await _assert_inventory_shape(case, connection, target, kept, shape, references)
            await _cleanup_inventory(connection)
            async with case.sessions() as session, session.begin():
                await session.execute(
                    text(f'DELETE FROM "{retirement.snapshot_schema(kept)}".custom_import_build_family')
                )


async def test_inventory_does_not_reuse_evidence_after_unknown_commit_outcome():
    async with _inventory_case() as (_case, connection, target, _kept, _graph):
        with pytest.raises(ConnectionError, match="commit response"):
            async with _prepared(connection) as (session, _receipt):
                assert await _prove(session, target) is True
            # Simulate a lost response after the real transaction commits.
            raise ConnectionError("synthetic commit response lost")
        async with AsyncSession(bind=connection) as session, session.begin():
            await _bounded(session)
            assert await _prove(session, target) is False
