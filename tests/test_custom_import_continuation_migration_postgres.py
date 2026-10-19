# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Upgrade registered old writers without modifying retained build storage."""

from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy import select, text

from db.models.custom_import import CustomImportBuildAttempt
from db.models.custom_import_storage import CustomImportSnapshotFamily
from process.custom_import import build_graph, build_output
from process.custom_import.build_source import stage_segmented_source
from tests.custom_import_postgres_support import isolated_publication_case
from tests.test_custom_import_build_output_postgres import _activate, _complete, _records, _request_for
from tests.test_custom_import_continuation_migration import _migration


async def _retained_metadata(session, schema):
    snapshots = (
        await session.execute(
            select(
                CustomImportSnapshotFamily.family_id,
                CustomImportSnapshotFamily.producing_fence,
                CustomImportSnapshotFamily.producing_token_sha256,
                CustomImportSnapshotFamily.generation_id,
                CustomImportSnapshotFamily.frozen_at,
                CustomImportSnapshotFamily.landing_table_oid,
            ).order_by(CustomImportSnapshotFamily.family_id)
        )
    ).all()
    build = (
        await session.execute(
            select(
                CustomImportBuildAttempt.build_id,
                CustomImportBuildAttempt.producing_fence,
                CustomImportBuildAttempt.producing_token_sha256,
                CustomImportBuildAttempt.build_deadline_at,
                CustomImportBuildAttempt.source_occurrence_count,
                CustomImportBuildAttempt.completed_family_count,
                CustomImportBuildAttempt.generation_id,
            ).order_by(CustomImportBuildAttempt.build_id)
        )
    ).all()
    objects = (
        await session.execute(
            text(
                f'SELECT family_id,relation_slot,table_oid FROM "{schema}".custom_import_snapshot_relation ORDER BY family_id,relation_slot'
            )
        )
    ).all()
    return snapshots, build, objects


async def test_existing_base_and_candidate_writers_refresh_without_rebinding_retained_rows():
    async with isolated_publication_case(migration_through="20261010000000") as case:
        original = await _request_for(case, _records(2, 3))
        _, reference = await _complete(case, original)
        await _activate(case, original, reference.generation_id)
        request = await _request_for(
            case,
            {"providers": [[]], "rates": [[]]},
            seed=original,
            base=reference.generation_id,
            version=1,
        )
        staged = await stage_segmented_source(case.sessions, request)
        async with case.sessions() as session:
            before = await _retained_metadata(session, case.schema_name)

        def refresh(connection):
            migration = _migration()
            migration._schema = lambda: case.schema_name
            migration.op = Operations(MigrationContext.configure(connection))
            # Historical fixtures already carry current nullable model columns;
            # this is the exact function-replacement part of upgrade().
            connection.execute(
                text(f'LOCK TABLE "{case.schema_name}".custom_import_snapshot_family IN SHARE ROW EXCLUSIVE MODE')
            )
            migration._authority_changes(case.schema_name)

        async with case.engine.begin() as connection:
            await connection.run_sync(refresh)
        async with case.sessions() as session:
            assert await _retained_metadata(session, case.schema_name) == before
            assert (
                await session.scalar(
                    text(f"""
                SELECT count(*) FROM pg_class relation,
                    LATERAL aclexplode(coalesce(relation.relacl,acldefault(
                        CASE WHEN relation.relkind='S' THEN 'S'::"char" ELSE 'r'::"char" END,relation.relowner))) acl
                WHERE relation.oid IN (
                    '"{case.schema_name}".custom_import_build_continuation'::regclass,
                    '"{case.schema_name}".custom_import_build_continuation_continuation_id_seq'::regclass)
                    AND acl.grantee<>relation.relowner
            """)
                )
                == 0
            )
        await build_graph.build_graph(case.sessions, request, staged.build_id)
        sealed = await build_output.build_output(case.sessions, request, staged.build_id)
        assert sealed.seal.effective_output_sha256 == reference.seal.effective_output_sha256
        assert sealed.no_change is not None
