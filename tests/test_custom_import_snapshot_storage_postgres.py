# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Native snapshot registry lifecycle, replay and access-boundary regressions."""

from contextlib import asynccontextmanager
from pathlib import Path

import pytest
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy import text, update
from sqlalchemy.exc import DBAPIError

from db.models.custom_import import CustomImportCurrentGeneration, CustomImportExecution, CustomImportGenerationSeal
from db.models.custom_import_storage import CustomImportSnapshotFamily
from process.custom_import.publication import seal_generation
from process.custom_import.storage_layout import snapshot_schema
from tests.custom_import_postgres_support import (
    _migration,
    isolated_publication_case,
    lease_digest,
    seed_publication_graph,
    seed_running_generation,
)
from tests.test_custom_import_bounded_build_postgres import _writer_role


def _install_snapshot_registry(connection, schema_name):
    """Install the actual additive migration, preserving every existing guard."""

    path = Path(__file__).resolve().parents[1] / "alembic/versions/20261005030000_custom_import_snapshot_storage.py"
    migration = _migration(path, "snapshot_registry_native_migration")
    migration._schema = lambda: schema_name
    migration.op = Operations(MigrationContext.configure(connection))
    migration.upgrade()


@asynccontextmanager
async def _snapshot_case():
    """Clean only exact namespaces recorded by this isolated registry."""

    async with (
        isolated_publication_case(migration_through="20261005020000") as case,
        _writer_role(case) as role,
    ):
        async with case.engine.begin() as connection:
            await connection.run_sync(_install_snapshot_registry, case.schema_name)
        yield case, role


async def _attempt(case):
    async with case.sessions() as session, session.begin():
        graph = await seed_publication_graph(session)
        attempt = await seed_running_generation(session, graph, suffix="snapshot", base_generation_id=None)
    return graph, attempt


async def _call(session, case, attempt, function_name, *, generation_id=None):
    """Exercise the real fenced entry point with one bounded statement."""

    await session.execute(text("SET LOCAL statement_timeout='2000ms'"))
    parameters_by_name = dict(execution=attempt.execution_id, fence=attempt.fence, token=lease_digest(attempt.token))
    arguments = ":execution,:fence,:token"
    if generation_id is not None:
        parameters_by_name["generation"] = generation_id
        arguments += ",:generation"
    return await session.scalar(text(f'SELECT "{case.schema_name}".{function_name}({arguments})'), parameters_by_name)


async def _create_snapshot(case, attempt):
    async with case.sessions() as session, session.begin():
        return await _call(session, case, attempt, "create_custom_import_snapshot_family")


async def test_native_snapshot_create_bind_freeze_and_replay_preserve_unpublished_state():
    async with _snapshot_case() as (case, _role):
        _graph, attempt = await _attempt(case)
        family_id = await _create_snapshot(case, attempt)
        assert await _create_snapshot(case, attempt) == family_id
        with pytest.raises(DBAPIError, match="custom_import_snapshot_generation_required"):
            async with case.sessions() as session, session.begin():
                await _call(session, case, attempt, "freeze_custom_import_snapshot_family")
        async with case.sessions() as session, session.begin():
            assert (
                await _call(
                    session,
                    case,
                    attempt,
                    "bind_custom_import_snapshot_generation",
                    generation_id=attempt.generation_id,
                )
                == family_id
            )
            assert await _call(session, case, attempt, "freeze_custom_import_snapshot_family") == family_id
        async with case.sessions() as session, session.begin():
            snapshot = await session.get(CustomImportSnapshotFamily, family_id)
            frozen_at = snapshot.frozen_at
            assert snapshot.generation_id == attempt.generation_id and frozen_at is not None
            assert await _call(session, case, attempt, "freeze_custom_import_snapshot_family") == family_id
            await session.refresh(snapshot)
            assert snapshot.frozen_at == frozen_at
            assert (await session.get(CustomImportExecution, attempt.execution_id)).state == "running"
            assert await session.get(CustomImportGenerationSeal, attempt.generation_id) is None
            assert await session.get(CustomImportCurrentGeneration, snapshot.dataset_id) is None


async def _read_binding(session, case, graph, generation_id, **changed_scope):
    """Resolve four pinned read IDs through the actual protected SQL entry point."""

    parameters_by_name = dict(
        generation=generation_id,
        dataset=graph.dataset_id,
        definition=graph.definition_revision_id,
        schema=graph.schema_revision_id,
    )
    parameters_by_name.update(changed_scope)
    return await session.scalar(
        text(
            f'SELECT "{case.schema_name}".resolve_custom_import_generation_snapshot'
            "(:generation,:dataset,:definition,:schema)"
        ),
        parameters_by_name,
    )


async def _seal_empty_fixture(session, graph, attempt):
    """Use real finality for an empty synthetic generation, not forged seal rows."""

    return await seal_generation(
        session,
        dataset_id=graph.dataset_id,
        generation_id=attempt.generation_id,
        lease_fence=attempt.fence,
        lease_token=attempt.token,
    )


async def _finality_binding(session, case, graph, attempt, **changed_scope):
    """Call the exact frozen binding before any publication seal exists."""

    await session.execute(text("SET LOCAL statement_timeout='2000ms'"))
    parameters_by_name = dict(
        generation=attempt.generation_id,
        dataset=graph.dataset_id,
        definition=graph.definition_revision_id,
        schema=graph.schema_revision_id,
        execution=attempt.execution_id,
        capture=graph.capture_bundle_id,
        fence=attempt.fence,
        token=lease_digest(attempt.token),
    )
    parameters_by_name.update(changed_scope)
    return await session.scalar(
        text(
            f'SELECT "{case.schema_name}".lock_custom_import_snapshot_finality'
            "(:generation,:dataset,:definition,:schema,:execution,:capture,:fence,:token)"
        ),
        parameters_by_name,
    )


async def test_native_finality_binding_requires_frozen_exact_producer_without_prior_seal():
    """Finality cannot validate mutable output or substitute a sibling producer."""

    async with _snapshot_case() as (case, _role):
        graph, attempt = await _attempt(case)
        family_id = await _create_snapshot(case, attempt)
        async with case.sessions() as session, session.begin():
            await _call(
                session, case, attempt, "bind_custom_import_snapshot_generation", generation_id=attempt.generation_id
            )
        with pytest.raises(DBAPIError, match="custom_import_snapshot_finality_binding_mismatch"):
            async with case.sessions() as session, session.begin():
                await _finality_binding(session, case, graph, attempt)
        async with case.sessions() as session, session.begin():
            await _call(session, case, attempt, "freeze_custom_import_snapshot_family")
            assert await _finality_binding(session, case, graph, attempt) == family_id
            assert await session.get(CustomImportGenerationSeal, attempt.generation_id) is None
            namespace = snapshot_schema(family_id)
            assert (
                await session.scalar(
                    text(
                        "SELECT count(*) FROM pg_locks l JOIN pg_class c ON c.oid=l.relation "
                        "JOIN pg_namespace n ON n.oid=c.relnamespace WHERE l.pid=pg_backend_pid() "
                        "AND l.mode='AccessShareLock' AND l.granted AND n.nspname=:namespace AND c.relkind='r'"
                    ),
                    dict(namespace=namespace),
                )
                == 15
            )
        for changed_scope in (
            dict(dataset=graph.dataset_id + 100),
            dict(definition=graph.definition_revision_id + 100),
            dict(schema=graph.schema_revision_id + 100),
            dict(capture=graph.capture_bundle_id + 100),
            dict(generation=graph.first_generation_id),
        ):
            with pytest.raises(DBAPIError, match="custom_import_snapshot_finality_binding_mismatch"):
                async with case.sessions() as session, session.begin():
                    await _finality_binding(session, case, graph, attempt, **changed_scope)


async def test_native_read_binding_legacy_fallback_never_masks_a_registered_attempt():
    """A registered-but-unbound producer must fail, not expose canonical records."""

    async with _snapshot_case() as (case, _role):
        graph, attempt = await _attempt(case)
        async with case.sessions() as session, session.begin():
            assert await _read_binding(session, case, graph, graph.first_generation_id) is None
        with pytest.raises(DBAPIError, match="custom_import_snapshot_read_generation_mismatch"):
            async with case.sessions() as session, session.begin():
                await _read_binding(session, case, graph, graph.first_generation_id, dataset=graph.dataset_id + 100)
        with pytest.raises(DBAPIError, match="custom_import_snapshot_read_generation_mismatch"):
            async with case.sessions() as session, session.begin():
                await _read_binding(session, case, graph, attempt.generation_id)
        family_id = await _create_snapshot(case, attempt)
        with pytest.raises(DBAPIError, match="custom_import_snapshot_finality_binding_mismatch"):
            async with case.sessions() as session, session.begin():
                await _seal_empty_fixture(session, graph, attempt)
        async with case.sessions() as session, session.begin():
            await _call(
                session, case, attempt, "bind_custom_import_snapshot_generation", generation_id=attempt.generation_id
            )
            await _call(session, case, attempt, "freeze_custom_import_snapshot_family")
            await _seal_empty_fixture(session, graph, attempt)
        with pytest.raises(DBAPIError, match="custom_import_snapshot_read_binding_mismatch"):
            async with case.sessions() as session, session.begin():
                await session.execute(
                    update(CustomImportSnapshotFamily)
                    .where(CustomImportSnapshotFamily.family_id == family_id)
                    .values(generation_id=None)
                )
                await _read_binding(session, case, graph, attempt.generation_id)
        async with case.sessions() as session, session.begin():
            assert await _read_binding(session, case, graph, attempt.generation_id) == family_id


async def _sealed_snapshot(case):
    """Create, bind, freeze and seal one empty fixture using real entry points."""

    graph, attempt = await _attempt(case)
    family_id = await _create_snapshot(case, attempt)
    async with case.sessions() as session, session.begin():
        await _call(
            session, case, attempt, "bind_custom_import_snapshot_generation", generation_id=attempt.generation_id
        )
        await _call(session, case, attempt, "freeze_custom_import_snapshot_family")
        await _seal_empty_fixture(session, graph, attempt)
    return graph, attempt, family_id


async def test_native_frozen_read_binding_pins_leaf_oids_until_request_completion():
    """The protected resolver holds all fifteen exact leaf OIDs against DDL."""

    async with _snapshot_case() as (case, role):
        graph, attempt, family_id = await _sealed_snapshot(case)
        namespace = snapshot_schema(family_id)
        async with case.sessions() as session, session.begin():
            await session.execute(
                text(
                    f'GRANT EXECUTE ON FUNCTION "{case.schema_name}".'
                    f'resolve_custom_import_generation_snapshot(bigint,bigint,bigint,bigint) TO "{role}"'
                )
            )
            relation_oids = (
                (
                    await session.execute(
                        text(
                            f'SELECT table_oid FROM "{case.schema_name}".custom_import_snapshot_relation WHERE family_id=:family'
                        ),
                        dict(family=family_id),
                    )
                )
                .scalars()
                .all()
            )
        async with case.sessions() as reader, reader.begin():
            await reader.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"))
            await reader.execute(text(f'SET LOCAL ROLE "{role}"'))
            assert await _read_binding(reader, case, graph, attempt.generation_id) == family_id
            assert (
                await reader.scalar(
                    text(
                        "SELECT count(*) FROM pg_locks WHERE pid=pg_backend_pid() "
                        "AND locktype='relation' AND mode='AccessShareLock' AND granted "
                        "AND relation=ANY(CAST(:oids AS oid[]))"
                    ),
                    dict(oids=relation_oids),
                )
                == 15
            )
            with pytest.raises(DBAPIError, match="lock timeout"):
                async with case.engine.begin() as writer:
                    await writer.execute(text("SET LOCAL lock_timeout='150ms'"))
                    await writer.execute(
                        text(f'LOCK TABLE "{namespace}".custom_import_root_record IN ACCESS EXCLUSIVE MODE')
                    )
            assert await reader.scalar(text(f'SELECT count(*) FROM "{namespace}".custom_import_root_record')) == 0


async def test_native_write_binding_requires_registration_and_pins_open_leaves():
    """Only a registered live attempt can hold all fifteen write targets."""

    async with _snapshot_case() as (case, _role):
        _graph, attempt = await _attempt(case)
        with pytest.raises(DBAPIError, match="custom_import_snapshot_write_binding_mismatch"):
            async with case.sessions() as session, session.begin():
                await _call(session, case, attempt, "lock_custom_import_writable_snapshot")
        family_id = await _create_snapshot(case, attempt)
        async with case.sessions() as writer, writer.begin():
            assert await _call(writer, case, attempt, "lock_custom_import_writable_snapshot") == family_id
            relation_oids = (
                (
                    await writer.execute(
                        text(
                            f'SELECT table_oid FROM "{case.schema_name}".custom_import_snapshot_relation '
                            "WHERE family_id=:family"
                        ),
                        dict(family=family_id),
                    )
                )
                .scalars()
                .all()
            )
            assert (
                await writer.scalar(
                    text(
                        "SELECT count(*) FROM pg_locks WHERE pid=pg_backend_pid() "
                        "AND locktype='relation' AND mode='RowExclusiveLock' AND granted "
                        "AND relation=ANY(CAST(:oids AS oid[]))"
                    ),
                    dict(oids=relation_oids),
                )
                == 15
            )


async def test_native_freeze_waits_for_writer_and_rejects_later_writes():
    """Freeze shares the attempt mutex and closes writes without trigger toggles."""

    async with _snapshot_case() as (case, _role):
        _graph, attempt = await _attempt(case)
        family_id = await _create_snapshot(case, attempt)
        async with case.sessions() as session, session.begin():
            await _call(
                session, case, attempt, "bind_custom_import_snapshot_generation", generation_id=attempt.generation_id
            )
        async with case.sessions() as writer, writer.begin():
            assert await _call(writer, case, attempt, "lock_custom_import_writable_snapshot") == family_id
            with pytest.raises(DBAPIError, match="lock timeout"):
                async with case.sessions() as freezer, freezer.begin():
                    await freezer.execute(text("SET LOCAL lock_timeout='150ms'"))
                    await _call(freezer, case, attempt, "freeze_custom_import_snapshot_family")
        async with case.sessions() as session, session.begin():
            assert await _call(session, case, attempt, "freeze_custom_import_snapshot_family") == family_id
        with pytest.raises(DBAPIError, match="custom_import_snapshot_writes_closed"):
            async with case.sessions() as session, session.begin():
                await _call(session, case, attempt, "lock_custom_import_writable_snapshot")


async def test_native_read_binding_denies_unfrozen_producer_and_layout_drift():
    """Corrupt fixture states fail closed and their transaction rolls back."""

    async with _snapshot_case() as (case, _role):
        graph, attempt, family_id = await _sealed_snapshot(case)
        namespace = snapshot_schema(family_id)
        for changed_values in (dict(frozen_at=None), dict(producing_token_sha256=b"x" * 32)):
            with pytest.raises(DBAPIError, match="custom_import_snapshot_read_binding_mismatch"):
                async with case.sessions() as session, session.begin():
                    await session.execute(
                        update(CustomImportSnapshotFamily)
                        .where(CustomImportSnapshotFamily.family_id == family_id)
                        .values(**changed_values)
                    )
                    await _read_binding(session, case, graph, attempt.generation_id)
        with pytest.raises(DBAPIError, match="custom_import_snapshot_relation_mismatch"):
            async with case.sessions() as session, session.begin():
                await session.execute(
                    text(f'ALTER TABLE "{namespace}".custom_import_root_record ADD COLUMN synthetic_read_drift integer')
                )
                await _read_binding(session, case, graph, attempt.generation_id)
        async with case.sessions() as session, session.begin():
            assert await _read_binding(session, case, graph, attempt.generation_id) == family_id


async def test_native_snapshot_wrong_producer_and_failed_creation_leave_no_partial_family():
    async with _snapshot_case() as (case, _role):
        graph, attempt = await _attempt(case)
        with pytest.raises(DBAPIError, match="custom_import_snapshot_attempt_lost"):
            async with case.sessions() as session, session.begin():
                await session.execute(text("SET LOCAL statement_timeout='2000ms'"))
                await session.execute(
                    text(f'SELECT "{case.schema_name}".create_custom_import_snapshot_family(:execution,99,:token)'),
                    dict(execution=attempt.execution_id, token=lease_digest(attempt.token)),
                )
        with pytest.raises(RuntimeError, match="synthetic rollback"):
            async with case.sessions() as session, session.begin():
                family_id = await _call(session, case, attempt, "create_custom_import_snapshot_family")
                raise RuntimeError("synthetic rollback")
        async with case.sessions() as session:
            assert await session.get(CustomImportSnapshotFamily, family_id) is None
            assert (
                await session.scalar(
                    text("SELECT to_regnamespace(:namespace)"), dict(namespace=snapshot_schema(family_id))
                )
                is None
            )
        family_id = await _create_snapshot(case, attempt)
        with pytest.raises(DBAPIError, match="custom_import_snapshot_generation_mismatch"):
            async with case.sessions() as session, session.begin():
                await _call(
                    session,
                    case,
                    attempt,
                    "bind_custom_import_snapshot_generation",
                    generation_id=graph.first_generation_id,
                )
        async with case.sessions() as session:
            assert (await session.get(CustomImportSnapshotFamily, family_id)).generation_id is None


async def test_native_snapshot_exact_oids_shape_and_read_only_grants():
    async with _snapshot_case() as (case, role):
        _graph, attempt = await _attempt(case)
        family_id = await _create_snapshot(case, attempt)
        namespace = snapshot_schema(family_id)
        async with case.sessions() as session, session.begin():
            registered_relations = (
                await session.execute(
                    text(f'SELECT * FROM "{case.schema_name}".resolve_custom_import_snapshot_relations(:family)'),
                    dict(family=family_id),
                )
            ).all()
            assert len(registered_relations) == len({relation.table_oid for relation in registered_relations}) == 15
            assert (
                await session.scalar(
                    text(
                        "SELECT count(*) FROM pg_constraint c JOIN pg_namespace n ON n.oid=c.connamespace WHERE n.nspname=:namespace AND c.contype='f'"
                    ),
                    dict(namespace=namespace),
                )
                == 0
            )
            await session.execute(text(f'SET LOCAL ROLE "{role}"'))
            assert await session.scalar(text(f'SELECT count(*) FROM "{namespace}".custom_import_root_record')) == 0
        for statement in (
            f'TRUNCATE "{namespace}".custom_import_root_record',
            f'SELECT * FROM "{case.schema_name}".custom_import_snapshot_family',
            f"SELECT \"{case.schema_name}\".create_custom_import_snapshot_family(1,1,decode(repeat('00',32),'hex'))",
            f'SELECT "{case.schema_name}".resolve_custom_import_generation_snapshot(1,1,1,1)',
            f"SELECT \"{case.schema_name}\".lock_custom_import_writable_snapshot(1,1,decode(repeat('00',32),'hex'))",
            f"SELECT \"{case.schema_name}\".lock_custom_import_snapshot_finality(1,1,1,1,1,1,1,decode(repeat('00',32),'hex'))",
        ):
            with pytest.raises(DBAPIError, match="permission denied"):
                async with case.sessions() as session, session.begin():
                    await session.execute(text(f'SET LOCAL ROLE "{role}"'))
                    await session.execute(text(statement))
        with pytest.raises(DBAPIError, match="custom_import_snapshot_relation_mismatch"):
            async with case.sessions() as session, session.begin():
                await session.execute(
                    text(f'ALTER TABLE "{namespace}".custom_import_root_record ADD COLUMN synthetic_drift integer')
                )
                await session.execute(
                    text(f'SELECT * FROM "{case.schema_name}".resolve_custom_import_snapshot_relations(:family)'),
                    dict(family=family_id),
                )
        assert await _create_snapshot(case, attempt) == family_id


async def _fixture_landing(case, attempt, family_id, role):
    """Register a minimal owner fixture, not proof of the SOURCE factory/codec."""

    namespace = snapshot_schema(family_id)
    async with case.sessions() as session, session.begin():
        await session.execute(
            text(
                f'CREATE TABLE "{namespace}".source_bulk_landing '
                "(landing_ordinal bigint PRIMARY KEY,payload text NOT NULL)"
            )
        )
        await session.execute(
            text(
                f'UPDATE "{case.schema_name}".custom_import_snapshot_family '
                f"SET landing_table_oid=c.oid::bigint,landing_table_owner=c.relowner::bigint, "
                f'landing_columns_sha256="{case.schema_name}".custom_import_snapshot_columns_sha256(c.oid::bigint) '
                "FROM pg_class c WHERE family_id=:family AND c.oid=to_regclass(:table)"
            ),
            dict(family=family_id, table=f"{namespace}.source_bulk_landing"),
        )
        await session.execute(
            text(f'GRANT INSERT(landing_ordinal,payload) ON "{namespace}".source_bulk_landing TO "{role}"')
        )
        await _call(
            session, case, attempt, "bind_custom_import_snapshot_generation", generation_id=attempt.generation_id
        )
    return f'"{namespace}".source_bulk_landing'


async def test_native_auxiliary_landing_is_pinned_and_column_writes_close_at_freeze():
    """Freeze denies unfinished input and retires actual column-level grants."""

    async with _snapshot_case() as (case, role):
        _graph, attempt = await _attempt(case)
        family_id = await _create_snapshot(case, attempt)
        table = await _fixture_landing(case, attempt, family_id, role)
        async with case.sessions() as writer, writer.begin():
            await _call(writer, case, attempt, "lock_custom_import_writable_snapshot")
            with pytest.raises(DBAPIError, match="lock timeout"):
                async with case.sessions() as ddl, ddl.begin():
                    await ddl.execute(text("SET LOCAL lock_timeout='100ms'"))
                    await ddl.execute(text(f"ALTER TABLE {table} ADD COLUMN synthetic_drift integer"))
            await writer.execute(text(f'SET LOCAL ROLE "{role}"'))
            await writer.execute(text(f"INSERT INTO {table}(landing_ordinal,payload) VALUES(1,'synthetic')"))
        with pytest.raises(DBAPIError, match="custom_import_snapshot_unfinished_landing"):
            async with case.sessions() as session, session.begin():
                await _call(session, case, attempt, "freeze_custom_import_snapshot_family")
        async with case.sessions() as session, session.begin():
            assert (await session.get(CustomImportSnapshotFamily, family_id)).frozen_at is None
            await session.execute(text(f"DELETE FROM {table}"))
            await _call(session, case, attempt, "freeze_custom_import_snapshot_family")
        for statement in (
            f"INSERT INTO {table}(landing_ordinal,payload) SELECT 2,'synthetic' WHERE false",
            f"UPDATE {table} SET payload='synthetic' WHERE false",
            f"DELETE FROM {table} WHERE false",
        ):
            with pytest.raises(DBAPIError, match="permission denied"):
                async with case.sessions() as session, session.begin():
                    await session.execute(text(f'SET LOCAL ROLE "{role}"'))
                    await session.execute(text(statement))
