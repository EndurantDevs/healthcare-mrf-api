# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Retained set writers preserve revision homes across exact committed retries."""

from __future__ import annotations

from contextlib import asynccontextmanager
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import pytest
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError

from process.custom_import import build_graph as graph
from process.custom_import import build_graph_sets as sets
from process.custom_import import build_source as source
from process.custom_import.definition import CustomImportDefinition
from process.custom_import.storage_layout import snapshot_schema
from tests import custom_import_grouped_child_support as material_fixture
from tests import test_custom_import_read_core_postgres as read_fixture
from tests import test_custom_import_runner_postgres as runner_fixture
from tests import test_custom_import_snapshot_reads_postgres as base_fixture
from tests.custom_import_postgres_support import _migration, isolated_publication_case, seed_running_generation
from tests.test_custom_import_build_output_postgres import _request_for
from tests.test_custom_import_snapshot_storage_postgres import _call as snapshot_call

_ROOT = Path(__file__).resolve().parents[1]
_CHAIN = (
    "20261005040000_custom_import_bulk_snapshot_writers",
    "20261005050000_custom_import_legacy_snapshot_writers",
    "20261005060000_custom_import_snapshot_finality",
    "20261005070000_custom_import_materialization_storage",
    "20261005080000_custom_import_writer_cutover",
)


def _definition():
    document = material_fixture.definition_document()
    for stream in document["streams"]:
        stream.update(format="parquet", compression="none")
    return CustomImportDefinition.from_mapping(document)


def _install(connection, schema):
    for name in _CHAIN:
        migration = _migration(_ROOT / "alembic/versions" / f"{name}.py", f"retained_home_{name}")
        migration._schema = lambda: schema
        migration.op = Operations(MigrationContext.configure(connection))
        migration.upgrade()


async def _sealed_base(session, case, is_snapshot):
    seed = await runner_fixture._seed_identity(session, uuid4().hex, _definition())
    identity = read_fixture._publication_graph(seed)
    attempt = await seed_running_generation(
        session,
        identity,
        suffix="retained_home_base",
        base_generation_id=None,
        root_count=len(base_fixture._families()),
        family_count=len(base_fixture._families()),
    )
    await base_fixture._seed_material(session, seed, attempt)
    if is_snapshot:
        family_id = await snapshot_call(session, case, attempt, "create_custom_import_snapshot_family")
        await base_fixture._copy_snapshot_material(session, case, attempt, family_id)
    await base_fixture._activate_read_generation(session, identity, attempt)
    return seed, attempt


@asynccontextmanager
async def _case(is_snapshot):
    async with isolated_publication_case(migration_through="20261005030000") as case:
        async with case.sessions() as session, session.begin():
            seed, base = await _sealed_base(session, case, is_snapshot)
        async with case.engine.begin() as connection:
            await connection.run_sync(_install, case.schema_name)
        request = await _request_for(
            case,
            {stream.stream_id: [[]] for stream in seed.definition.source_streams},
            seed=seed,
            base=base.generation_id,
            version=1,
            page_rows=128,
            definition=seed.definition,
        )
        build_id, registry, prepared = await _retained_input(case, request)
        yield SimpleNamespace(case=case, request=request, build_id=build_id, registry=registry, prepared=prepared)


async def _retained_input(case, request):
    staged = await source.stage_segmented_source(case.sessions, request)
    assert staged.phase == "graph" and staged.source_occurrence_count == 0
    await source._prepare_snapshot_indexes(case.sessions, request, staged.build_id, "graph")
    while True:
        async with source._page_session(case.sessions, request, staged.build_id) as (session, build):
            if build.plan_complete_at is not None:
                registry = await graph.load_registry(session, request)
                break
            await graph._call(
                session,
                "plan_custom_import_build_family_page",
                (staged.build_id, build.plan_page_sequence),
                ("phase", "plan_stage", "page_sequence", "rows_processed", "plan_complete"),
            )
    async with graph._session(case.sessions) as session:
        prepared = await session.run_sync(
            lambda sync: graph._next_family_inputs(sync, request, registry, staged.build_id, 0)
        )
    selected = next(family for family in prepared if family.child_count > 0)
    assert selected.plan.selection_kind == "retained"
    return staged.build_id, registry, selected


async def _write_page(fixture, name, arguments):
    async with source._page_session(fixture.case.sessions, fixture.request, fixture.build_id) as (session, _):
        return (await graph._source_call(session, name, arguments)).all()


async def _durable_state(fixture):
    async with source._page_session(fixture.case.sessions, fixture.request, fixture.build_id) as (session, _):
        family_id = await source._resolve_build_snapshot(session, fixture.build_id)
        control = f'"{fixture.case.schema_name}"'
        namespace = f'"{snapshot_schema(family_id)}"'
        return await session.scalar(
            text(
                f"SELECT jsonb_build_object('build',to_jsonb(b),'streams',"
                f"(SELECT jsonb_agg(to_jsonb(s) ORDER BY s.stream_slot) FROM {control}.custom_import_build_stream s "
                "WHERE s.build_id=b.build_id),'homes',"
                f"(SELECT jsonb_agg(to_jsonb(h) ORDER BY h.revision_kind,h.first_revision_id) "
                f"FROM {control}.custom_import_revision_home h WHERE h.family_id=:family),'packs',"
                f"(SELECT count(*) FROM {namespace}.custom_import_pack),'occurrences',"
                f"(SELECT count(*) FROM {namespace}.custom_import_build_occurrence)) "
                f"FROM {control}.custom_import_build_attempt b WHERE b.build_id=:build"
            ),
            dict(family=family_id, build=fixture.build_id),
        )


async def _children_page(fixture, root_receipts):
    started = sets._root_receipts(fixture.request, fixture.registry, (fixture.prepared,), root_receipts)
    async with graph._session(fixture.case.sessions) as session:
        groups, name, arguments = await session.run_sync(
            lambda sync: sets._read_child_batch(
                sync, fixture.request, fixture.registry, (fixture.prepared,), started, "retained", 64, 64
            )
        )
    assert len(groups) == 1 and groups[0].children
    return (name, arguments), groups


async def _verify_homes(fixture, kind, base_ids):
    async with source._page_session(fixture.case.sessions, fixture.request, fixture.build_id) as (session, _):
        family_id = await source._resolve_build_snapshot(session, fixture.build_id)
        namespace = f'"{snapshot_schema(family_id)}"'
        label = "root" if kind == 1 else "child"
        copied_ids = tuple(
            await session.scalars(text(f"SELECT {label}_revision_id FROM {namespace}.custom_import_{label}_revision"))
        )
        assert copied_ids and set(copied_ids).isdisjoint(base_ids)
        bindings_by_name = dict(
            roots=list(copied_ids) if kind == 1 else [], children=list(copied_ids) if kind == 2 else []
        )
        homes = (
            await session.execute(
                text(
                    f'SELECT revision_kind,revision_id,family_id FROM "{fixture.case.schema_name}".'
                    "lookup_custom_import_revision_home(CAST(:roots AS bigint[]),CAST(:children AS bigint[])) "
                    "ORDER BY revision_id"
                ),
                bindings_by_name,
            )
        ).all()
        assert homes == [(kind, revision_id, family_id) for revision_id in sorted(copied_ids)]
        count = await session.scalar(
            text(
                f'SELECT count(*) FROM "{fixture.case.schema_name}".custom_import_revision_home '
                "WHERE family_id=:family AND revision_kind=:kind"
            ),
            dict(family=family_id, kind=kind),
        )
        assert count == 1


async def _reject_missing_or_stale(fixture, name, arguments, kind):
    for mutation in ("missing_home", "stale_relation"):
        async with source._page_session(fixture.case.sessions, fixture.request, fixture.build_id) as (session, _):
            family_id = await source._resolve_build_snapshot(session, fixture.build_id)
            with pytest.raises(DBAPIError, match="retry_home_mismatch|snapshot_"):
                async with session.begin_nested():
                    await _mutate_home_binding(session, fixture.case, family_id, kind, mutation)
                    await graph._source_call(session, name, arguments)


async def _mutate_home_binding(session, case, family_id, kind, mutation):
    if mutation == "missing_home":
        await session.execute(
            text(
                f'DELETE FROM "{case.schema_name}".custom_import_revision_home '
                "WHERE family_id=:family AND revision_kind=:kind"
            ),
            dict(family=family_id, kind=kind),
        )
        return
    label = "root" if kind == 1 else "child"
    await session.execute(
        text(
            f'ALTER TABLE "{snapshot_schema(family_id)}".custom_import_{label}_revision '
            f"RENAME TO stale_{label}_revision"
        )
    )


@pytest.mark.parametrize("is_snapshot", (False, True), ids=("canonical_base", "snapshot_base"))
async def test_native_retained_homes_are_fresh_idempotent_and_fail_closed(is_snapshot):
    async with _case(is_snapshot) as fixture:
        root_name, root_arguments = sets._root_arguments(fixture.request, fixture.registry, (fixture.prepared,))
        root_receipts = await _write_page(fixture, root_name, root_arguments)
        await _verify_homes(fixture, 1, (fixture.prepared.root.root_revision_id,))
        root_state = await _durable_state(fixture)
        assert await _write_page(fixture, root_name, root_arguments) == root_receipts
        await _reject_missing_or_stale(fixture, root_name, root_arguments, 1)
        assert await _durable_state(fixture) == root_state
        (child_name, child_arguments), groups = await _children_page(fixture, root_receipts)
        child_receipts = await _write_page(fixture, child_name, child_arguments)
        sets._child_receipts(groups, child_receipts)
        await _verify_homes(fixture, 2, tuple(child.child_revision_id for child in groups[0].children))
        child_state = await _durable_state(fixture)
        assert await _write_page(fixture, child_name, child_arguments) == child_receipts
        await _reject_missing_or_stale(fixture, child_name, child_arguments, 2)
        assert await _durable_state(fixture) == child_state
