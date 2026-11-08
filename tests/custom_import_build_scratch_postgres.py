# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Native scratch cleanup, unchanged reads, retained base and continuation proof."""

import datetime as dt
import uuid
from dataclasses import replace
from pathlib import Path

import pytest
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy import func, select, text
from sqlalchemy.exc import DBAPIError

from db.models.custom_import import CustomImportGeneration
from db.models.custom_import_build_scratch import CustomImportBuildScratchCleanup
from db.models.custom_import_storage import CustomImportSnapshotFamily
from process.custom_import import (
    build_graph,
    build_output,
    build_source,
    publication,
    read_core,
)
from process.custom_import import build_scratch as cleanup
from process.custom_import.runner_types import LeaseAuthorityLost
from tests import test_custom_import_build_output_postgres as fixture
from tests import test_custom_import_manual_acceptance_postgres as manual
from tests.custom_import_postgres_support import _migration
from tests.test_custom_import_build_source_postgres import _source_case
from tests.test_custom_import_continuation_postgres import _transfer


def install_cleanup_migration(connection, schema):
    versions = (
        ("20261102030000_custom_import_build_scratch", "custom_import_build_scratch_cleanup"),
        ("20261102040000_custom_import_post_seal_audit", "custom_import_post_seal_audit"),
    )
    for version, table in versions:
        identity = f'"{schema}".{table}'
        if connection.scalar(text("SELECT to_regclass(:relation)"), {"relation": identity}) is None:
            path = Path(__file__).resolve().parents[1] / "alembic/versions" / (version + ".py")
            migration = _migration(path, "scratch_" + version)
            migration._schema = lambda: schema
            migration.op = Operations(MigrationContext.configure(connection))
            migration.upgrade()


def install_current_verification(connection, schema):
    present = connection.scalar(
        text("""SELECT EXISTS(SELECT 1 FROM pg_attribute
        WHERE attrelid=to_regclass(:relation) AND attname='manual_acceptance' AND NOT attisdropped)"""),
        {"relation": f'"{schema}".custom_import_build_verification'},
    )
    for name in (
        ()
        if present
        else (
            "20261020150000_custom_import_staged_verification",
            "20261020180000_custom_import_verification_arms",
            "20261020190000_custom_import_verification_root_ranges",
            "20261020200000_custom_import_manual_acceptance",
        )
    ):
        path = Path(__file__).resolve().parents[1] / "alembic/versions" / (name + ".py")
        migration = _migration(path, "scratch_" + name)
        migration._schema = lambda: schema
        migration.op = Operations(MigrationContext.configure(connection))
        migration.upgrade()
    from tests.custom_import_post_seal_audit_postgres import (
        _family_reference_migration,
        _refresh_aggregate,
        _refresh_family_reference,
    )

    identity = f'"{schema}".verify_custom_import_build_structure(bigint)'
    installed = connection.scalar(
        text("SELECT prosrc FROM pg_proc WHERE oid=to_regprocedure(:identity)"), {"identity": identity}
    )
    if installed != _family_reference_migration()._bodies(schema)[1]:
        _refresh_aggregate(connection, schema)
    _refresh_family_reference(connection, schema)


async def _family(case, request):
    async with case.sessions() as session:
        return await session.scalar(
            select(CustomImportSnapshotFamily.family_id).where(
                CustomImportSnapshotFamily.execution_id == request.execution_id,
                CustomImportSnapshotFamily.producing_fence == request.fence,
            )
        )


async def _bounds(session):
    await session.execute(text("SET LOCAL statement_timeout='5000ms'"))
    await session.execute(text("SET LOCAL lock_timeout='100ms'"))


async def _trim(case, family_id, *, engine_id="synthetic_engine"):
    async with case.sessions() as session, session.begin():
        await _bounds(session)
        return await cleanup.trim_build_scratch(session, family_id, engine_id=engine_id)


async def _physical(case, family_id):
    namespace = f"ci_snapshot_{family_id}"
    async with case.sessions() as session:
        rows = (
            await session.execute(
                text("""
            SELECT c.relname,c.oid,c.relfilenode,pg_total_relation_size(c.oid) bytes,
                   array_agg(a.attname::text ORDER BY a.attnum) columns
            FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
            JOIN pg_attribute a ON a.attrelid=c.oid AND a.attnum>0 AND NOT a.attisdropped
            WHERE n.nspname=:namespace AND c.relkind='r' GROUP BY c.oid ORDER BY c.relname
        """),
                {"namespace": namespace},
            )
        ).all()
        count_by_name = {
            name: await session.scalar(text(f'SELECT count(*) FROM "{namespace}"."{name}"'))
            for name in cleanup.SCRATCH_RELATIONS
        }
        return {row.relname: (row.oid, tuple(row.columns)) for row in rows}, count_by_name


async def _indexes(case, family_id):
    async with case.sessions() as session:
        rows = (
            await session.execute(
                text("""
            SELECT c.relname,c.oid FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
            WHERE n.nspname=:namespace AND c.relkind='i'
        """),
                {"namespace": f"ci_snapshot_{family_id}"},
            )
        ).all()
        return dict(rows)


async def _read(case, request, generation_id):
    target = read_core.PinnedReadTarget(
        request.dataset_id, generation_id, request.definition_revision_id, request.schema_revision_id, "default"
    )
    async with case.sessions() as session:
        return await fixture._service().search(
            session,
            authorization=read_core.ExtensionReadAuthorization("synthetic-scratch-read"),
            request=read_core.SearchRequest(target=target, page_size=20),
        )


async def _continue_trimmed_base(case, request, sealed, before_read, monkeypatch):
    # A trimmed serving base remains sufficient for fresh SOURCE, retained GRAPH and transferred authority.
    next_request = await fixture._request_for(
        case, {"providers": [[]], "rates": [[]]}, seed=request, base=sealed.generation_id, version=1, page_rows=16
    )
    async with case.sessions() as session:
        now = await session.scalar(select(func.clock_timestamp()))
    next_request = replace(next_request, build_deadline_at=now + dt.timedelta(seconds=3))
    staged = await build_source.stage_segmented_source(case.sessions, next_request)
    assert staged.phase == "graph"

    def pause_after_plan(*_arguments, **_options):
        raise ConnectionError("synthetic admitted plan checkpoint")

    with monkeypatch.context() as patch:
        patch.setattr(build_graph, "_next_family_inputs", pause_after_plan)
        with pytest.raises(ConnectionError, match="admitted plan checkpoint"):
            await build_graph.build_graph(case.sessions, next_request, staged.build_id)
    continued = await _transfer(case, next_request, staged.build_id)
    await build_graph.build_graph(case.sessions, continued, staged.build_id)
    next_output = await build_output.build_output(case.sessions, continued, staged.build_id)
    assert next_output.no_change is not None
    assert next_output.seal.effective_output_sha256 == sealed.seal.effective_output_sha256
    with pytest.raises(LeaseAuthorityLost, match="continuation authority"):
        await build_output.build_output(case.sessions, continued, staged.build_id)
    assert await _read(case, request, sealed.generation_id) == before_read


async def _assert_interrupted_cleanup(case, family_id, physical_before, counts_before, indexes_before, monkeypatch):
    """An interrupted truncate restores rows, indexes and the cleanup marker."""

    async def interrupted(session, qualified):
        await original_truncate(session, qualified)
        raise ConnectionError("synthetic cleanup acknowledgement")

    original_truncate = cleanup._truncate
    with monkeypatch.context() as patch:
        patch.setattr(cleanup, "_truncate", interrupted)
        with pytest.raises(ConnectionError, match="cleanup acknowledgement"):
            await _trim(case, family_id)
    assert await _physical(case, family_id) == (physical_before, counts_before)
    assert await _indexes(case, family_id) == indexes_before
    async with case.sessions() as session:
        assert await session.get(CustomImportBuildScratchCleanup, family_id) is None


async def test_cleanup_is_atomic_pin_safe_and_preserves_retained_base_continuation(monkeypatch):
    """Pins and interrupted cleanup preserve readers and retained continuation."""
    async with _source_case() as case:
        async with case.engine.begin() as connection:
            await connection.run_sync(install_cleanup_migration, case.schema_name)
            await connection.run_sync(install_current_verification, case.schema_name)
        request = await fixture._request_for(case, fixture._records(3, 14), page_rows=16)
        build_id, sealed = await fixture._complete(case, request)
        await fixture._activate(case, request, sealed.generation_id)
        family_id = await _family(case, request)
        physical_before, counts_before = await _physical(case, family_id)
        indexes_before = await _indexes(case, family_id)
        assert all(counts_before[name] > 0 for name in cleanup.SCRATCH_RELATIONS[:-1])
        assert counts_before["source_bulk_landing"] == 0
        before_read = await _read(case, request, sealed.generation_id)
        before_material = await fixture._assert_sealed_materialization(case, request, sealed)

        await _assert_interrupted_cleanup(case, family_id, physical_before, counts_before, indexes_before, monkeypatch)

        # The actual generation resolver pins every registered leaf, including scratch.
        async with case.sessions() as reader, reader.begin():
            generation = await reader.get(CustomImportGeneration, sealed.generation_id)
            await publication._validated_generation_seal(reader, generation)
            with pytest.raises(DBAPIError) as locked:
                await _trim(case, family_id)
            assert getattr(locked.value.orig, "sqlstate", None) == "55P03"
        assert await _physical(case, family_id) == (physical_before, counts_before)
        assert await _indexes(case, family_id) == indexes_before

        receipt = await _trim(case, family_id)
        assert not receipt.replayed and receipt.retained_bytes > receipt.remaining_bytes
        physical_after, counts_after = await _physical(case, family_id)
        assert physical_after == physical_before and set(counts_after.values()) == {0}
        indexes_after = await _indexes(case, family_id)
        from process.custom_import.storage_layout import _PHASE_INDEX_NAMES

        phase_names = frozenset().union(*_PHASE_INDEX_NAMES.values())
        assert indexes_after == {name: oid for name, oid in indexes_before.items() if name not in phase_names}
        assert await _read(case, request, sealed.generation_id) == before_read
        assert await fixture._assert_sealed_materialization(case, request, sealed) == before_material
        assert (await build_output.build_output(case.sessions, request, build_id)).seal.replayed
        assert (await _trim(case, family_id)).replayed
        with pytest.raises(cleanup.BuildScratchCleanupRefused, match="replay identity"):
            await _trim(case, family_id, engine_id="another_engine")

        async with case.sessions() as session:
            marker = await session.get(CustomImportBuildScratchCleanup, family_id)
            assert marker.aggregate_evidence["build_counters"]["source_occurrence_count"] > 0
            assert marker.aggregate_evidence["structural_counts"]["family_count"] == 3
            assert len(marker.aggregate_evidence["relations"]) == 17

        await _continue_trimmed_base(case, request, sealed, before_read, monkeypatch)


async def test_cleanup_refuses_phase_index_rebound_to_a_serving_table():
    async with _source_case() as case:
        async with case.engine.begin() as connection:
            await connection.run_sync(install_cleanup_migration, case.schema_name)
            await connection.run_sync(install_current_verification, case.schema_name)
        request = await fixture._request_for(case, fixture._records(1, 2))
        await fixture._complete(case, request)
        family_id = await _family(case, request)
        before = await _physical(case, family_id)
        indexes_before = await _indexes(case, family_id)
        async with case.sessions() as session, session.begin():
            await _bounds(session)
            with pytest.raises(cleanup.BuildScratchCleanupRefused, match="phase index custody"):
                async with session.begin_nested():
                    await session.execute(
                        text(f'DROP INDEX "ci_snapshot_{family_id}".custom_import_build_raw_parent_idx')
                    )
                    await session.execute(
                        text(
                            f'CREATE INDEX custom_import_build_raw_parent_idx ON "ci_snapshot_{family_id}".custom_import_root_scalar(root_revision_id)'
                        )
                    )
                    await cleanup.trim_build_scratch(session, family_id, engine_id="synthetic_engine")
        assert await _physical(case, family_id) == before
        assert await _indexes(case, family_id) == indexes_before


async def test_cleanup_refuses_unverified_and_manual_accepted_scratch(monkeypatch):
    async with _source_case() as case:
        async with case.engine.begin() as connection:
            await connection.run_sync(install_cleanup_migration, case.schema_name)
        request, build_id, generation_id = await manual._ready(case, monkeypatch)
        family_id = await _family(case, request)
        before = await _physical(case, family_id)
        with pytest.raises(cleanup.BuildScratchCleanupRefused, match="completed producing build"):
            await _trim(case, family_id)
        assert await _physical(case, family_id) == before
        assert await manual._step(case, request, build_id) == "scanning"
        async with case.sessions() as session, session.begin():
            await manual._accept(session, case, build_id, request=request)
        assert await manual._step(case, request, build_id) == "complete"
        sealed = await build_output.build_output(case.sessions, request, build_id)
        assert sealed.generation_id == generation_id
        before = await _physical(case, family_id)
        with pytest.raises(cleanup.BuildScratchCleanupRefused, match="full automated"):
            await _trim(case, family_id)
        assert await _physical(case, family_id) == before
        async with case.sessions() as session:
            assert await session.get(CustomImportBuildScratchCleanup, family_id) is None


async def test_cleanup_owner_custody_and_changed_registry_refusal():
    async with _source_case() as case:
        async with case.engine.begin() as connection:
            await connection.run_sync(install_cleanup_migration, case.schema_name)
            await connection.run_sync(install_current_verification, case.schema_name)
        request = await fixture._request_for(case, fixture._records(1, 2))
        await fixture._complete(case, request)
        family_id = await _family(case, request)
        before = await _physical(case, family_id)
        role = "scratch_fixture_" + uuid.uuid4().hex[:12]
        async with case.engine.begin() as connection:
            await connection.execute(text(f'CREATE ROLE "{role}" NOLOGIN'))
        try:
            async with case.sessions() as session, session.begin():
                await _bounds(session)
                await session.execute(text(f'SET LOCAL ROLE "{role}"'))
                with pytest.raises((cleanup.BuildScratchCleanupRefused, DBAPIError)):
                    await cleanup.trim_build_scratch(session, family_id, engine_id="synthetic_engine")
        finally:
            async with case.engine.begin() as connection:
                await connection.execute(text(f'DROP ROLE "{role}"'))
        assert await _physical(case, family_id) == before
        async with case.sessions() as session, session.begin():
            await _bounds(session)
            with pytest.raises(DBAPIError):
                async with session.begin_nested():
                    await session.execute(
                        text(
                            f'ALTER TABLE "ci_snapshot_{family_id}".custom_import_build_family ADD COLUMN synthetic_extra integer'
                        )
                    )
                    await cleanup.trim_build_scratch(session, family_id, engine_id="synthetic_engine")
        assert await _physical(case, family_id) == before
        async with case.sessions() as session:
            relation = f'"{case.schema_name}".custom_import_build_scratch_cleanup'
            assert not await session.scalar(
                text(
                    "SELECT EXISTS(SELECT 1 FROM pg_constraint WHERE conrelid=to_regclass(:relation) AND contype='f')"
                ),
                {"relation": relation},
            )
            assert not await session.scalar(
                text("SELECT EXISTS(SELECT 1 FROM pg_trigger WHERE tgrelid=to_regclass(:relation))"),
                {"relation": relation},
            )
            assert not await session.scalar(
                text("""SELECT EXISTS(SELECT 1 FROM pg_class c,
                LATERAL aclexplode(coalesce(c.relacl,acldefault('r',c.relowner))) acl
                WHERE c.oid=to_regclass(:relation) AND acl.grantee<>c.relowner)"""),
                {"relation": relation},
            )
