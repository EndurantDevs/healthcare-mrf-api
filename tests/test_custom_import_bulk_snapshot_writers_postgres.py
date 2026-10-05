# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native SOURCE bootstrap, fixed dispatch, COPY and rollback regressions."""

from __future__ import annotations

import uuid
from contextlib import asynccontextmanager
from dataclasses import replace
from pathlib import Path

import pytest
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy import func, select, text
from sqlalchemy.exc import DBAPIError

from db.models.custom_import import CustomImportBuildAttempt, CustomImportBuildStream, CustomImportPack
from db.models.custom_import_storage import CustomImportSnapshotFamily
from process.custom_import import build_source as source
from process.custom_import.bulk_page_codec import encode_landing_batch
from process.custom_import.execution import lease_token_sha256
from process.custom_import.storage_layout import snapshot_models, snapshot_schema
from tests.custom_import_postgres_support import _migration, isolated_publication_case
from tests.test_custom_import_build_source_postgres import _retained_request, _root
from tests.test_custom_import_writer_cutover_postgres import _record_role


def _install(connection, schema):
    """Install complete SOURCE prerequisites after hostile default ACLs are set."""
    for filename in (
        "20261005040000_custom_import_bulk_snapshot_writers",
        "20261005050000_custom_import_legacy_snapshot_writers",
        "20261005060000_custom_import_snapshot_finality",
        "20261005070000_custom_import_materialization_storage",
    ):
        path = Path(__file__).resolve().parents[1] / "alembic/versions" / f"{filename}.py"
        migration = _migration(path, f"bulk_snapshot_{filename}")
        migration._schema = lambda: schema
        migration.op = Operations(MigrationContext.configure(connection))
        migration.upgrade()


async def _grant_default_rights(connection, case, role):
    for kind, privilege in (("FUNCTIONS", "EXECUTE"), ("TABLES", "ALL"), ("SCHEMAS", "ALL")):
        await connection.execute(text(f'ALTER DEFAULT PRIVILEGES GRANT {privilege} ON {kind} TO "{role}"'))
    await connection.execute(text(f'GRANT USAGE ON SCHEMA "{case.schema_name}" TO "{role}"'))
    await connection.execute(
        text(f'GRANT EXECUTE ON FUNCTION "{case.schema_name}".freeze_custom_import_build_source(bigint) TO "{role}"')
    )


async def _drop_fixture_role(connection, role):
    for kind in ("FUNCTIONS", "TABLES", "SCHEMAS"):
        await connection.execute(text(f'ALTER DEFAULT PRIVILEGES REVOKE ALL ON {kind} FROM "{role}"'))
    await connection.execute(text(f'DROP OWNED BY "{role}"'))
    await connection.execute(text(f'DROP ROLE "{role}"'))


@asynccontextmanager
async def _bulk_case(*, default_role=False):
    """Use actual prerequisite migrations and clean only registered test leaves."""
    async with isolated_publication_case(migration_through="20261005030000") as case:
        role = "cutover_" + uuid.uuid4().hex[:16] if default_role else None
        is_role_created = False
        try:
            if role:
                _record_role(role, case.schema_name, "planned")
                async with case.engine.begin() as connection:
                    await connection.execute(text(f'CREATE ROLE "{role}" NOLOGIN'))
                is_role_created = True
                async with case.engine.begin() as connection:
                    await _grant_default_rights(connection, case, role)
            async with case.engine.begin() as connection:
                await connection.run_sync(_install, case.schema_name)
            yield case, role
        finally:
            if is_role_created:
                async with case.engine.begin() as connection:
                    await _drop_fixture_role(connection, role)


async def _bootstrap(case):
    request = replace(
        await _retained_request(case, records_by_stream={"root_source": [[_root()]], "detail_source": [[]]}),
        page_row_limit=32,
    )
    build_id, registry = await source._begin_build(case.sessions, request)
    stream = request.definition.source_streams[0]
    context = source._StreamContext(request, registry, build_id, stream)
    page = source._SourcePage(1, 0, 0, (source._prepare_row(request, stream, _root()),))
    async with case.sessions() as session:
        family_id = await session.scalar(
            select(CustomImportSnapshotFamily.family_id).where(
                CustomImportSnapshotFamily.execution_id == request.execution_id
            )
        )
    assert type(family_id) is int and family_id > 0
    return context, page, family_id


async def _authorize(session, context, page):
    preview = encode_landing_batch(context, (page,), batch_id=uuid.UUID(int=0), first_pack_ordinal=0)
    batch_id = (
        await source._call(
            session,
            "source_bulk_authorize",
            (
                ("bigint", context.build_id),
                ("smallint", context.stream_slot),
                ("bigint", context.request.fence),
                ("bytea", lease_token_sha256(context.request.lease_token)),
                ("integer", len(preview.records)),
                ("bigint", preview.byte_count),
            ),
        )
    ).scalar_one()
    records = tuple((batch_id, position, *record[1:]) for position, record in enumerate(preview.records))
    return batch_id, records


async def test_native_source_bootstrap_copy_admission_and_committed_prefix_replay():
    async with _bulk_case() as (case, _role):
        context, page, family_id = await _bootstrap(case)
        assert await source._store_single_page(case.sessions, context, page) == 1
        await source._compare_committed_page(case.sessions, context, page)
        outcome = await source.stage_segmented_source(case.sessions, context.request)
        assert outcome.phase == "graph" and outcome.source_occurrence_count == 1
        assert await source.stage_segmented_source(case.sessions, context.request) == outcome
        models_by_name = {model.__tablename__: alias for model, alias in snapshot_models(family_id).items()}
        async with case.sessions() as session:
            assert await session.scalar(select(func.count()).select_from(CustomImportPack)) == 0
            for name in (
                "custom_import_pack",
                "custom_import_root_record",
                "custom_import_root_revision",
                "custom_import_build_occurrence",
            ):
                assert await session.scalar(select(func.count()).select_from(models_by_name[name])) == 1
            occurrence = (await session.scalars(select(models_by_name["custom_import_build_occurrence"]))).one()
            assert occurrence.source_ordinal == 0 and occurrence.rejection_id is None
            root = (await session.scalars(select(models_by_name["custom_import_root_revision"]))).one()
            assert bytes(root.payload_sha256) == page.records[0].payload_hash
            assert root.canonical_payload == page.records[0].payload
            snapshot = await session.get(CustomImportSnapshotFamily, family_id)
            assert snapshot.generation_id is None and snapshot.frozen_at is None
            assert (
                snapshot.landing_table_oid
                and snapshot.landing_table_owner
                and len(snapshot.landing_columns_sha256) == 32
            )
            assert (
                await session.scalar(text(f'SELECT count(*) FROM "{snapshot_schema(family_id)}".source_bulk_landing'))
                == 0
            )
            assert await session.scalar(text(f'SELECT count(*) FROM "{case.schema_name}".source_bulk_completion')) == 1


async def test_native_foreign_batch_landing_fails_complete_antijoin_and_rolls_back():
    async with _bulk_case() as (case, _role):
        context, page, family_id = await _bootstrap(case)
        with pytest.raises(DBAPIError, match="source_bulk_landing_owner_mismatch"):
            async with source._page_session(case.sessions, context.request, context.build_id) as (session, _build):
                batch_id, landing_rows = await _authorize(session, context, page)
                await source._copy_source_landing(session, landing_rows)
                connection = await session.connection()
                raw = await connection.get_raw_connection()
                orphan = (uuid.uuid4(), 0, *landing_rows[0][2:])
                await raw.driver_connection.copy_records_to_table(
                    "source_bulk_landing",
                    schema_name=snapshot_schema(family_id),
                    columns=source._SOURCE_COPY_COLUMNS,
                    records=(orphan,),
                )
                await source._call(session, "source_set_finalize", (("uuid", batch_id), ("integer[]", [])))
        async with case.sessions() as session:
            model = snapshot_models(family_id)[CustomImportPack]
            assert await session.scalar(select(func.count()).select_from(model)) == 0
            build = await session.get(CustomImportBuildAttempt, context.build_id)
            cursor = await session.get(CustomImportBuildStream, (context.build_id, context.stream_slot))
            assert build.source_occurrence_count == cursor.next_source_ordinal == cursor.next_pack_ordinal == 0
            assert (
                await session.scalar(text(f'SELECT count(*) FROM "{snapshot_schema(family_id)}".source_bulk_landing'))
                == 0
            )
            assert (
                await session.scalar(text(f'SELECT count(*) FROM "{case.schema_name}".source_bulk_authorization')) == 0
            )
        assert await source._store_single_page(case.sessions, context, page) == 1


async def _assert_landing_insert_closed(session, role, namespace):
    assert not await session.scalar(
        text("SELECT has_any_column_privilege(:role,:table,'INSERT')"),
        dict(role=role, table=f"{namespace}.source_bulk_landing"),
    )


async def test_native_default_acl_denial_existing_acl_preservation_and_transport_columns():
    async with _bulk_case(default_role=True) as (case, role):
        context, page, family_id = await _bootstrap(case)
        namespace = snapshot_schema(family_id)
        async with case.sessions() as session, session.begin():
            check = "SELECT has_function_privilege(:role,CAST(:signature AS text),'EXECUTE')"
            assert await session.scalar(
                text(check), dict(role=role, signature=f"{case.schema_name}.freeze_custom_import_build_source(bigint)")
            )
            for name in (
                "resolve_custom_import_build_snapshot(bigint)",
                "source_bulk_authorize(bigint,smallint,bigint,bytea,integer,bigint)",
            ):
                assert not await session.scalar(text(check), dict(role=role, signature=f"{case.schema_name}.{name}"))
            assert not await session.scalar(
                text("SELECT has_schema_privilege(:role,:namespace,'CREATE')"), dict(role=role, namespace=namespace)
            )
            assert not await session.scalar(
                text(
                    "SELECT bool_or(has_function_privilege(:role,p.oid,'EXECUTE')) FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname=:namespace"
                ),
                dict(role=role, namespace=namespace),
            )
            assert not await session.scalar(
                text(
                    "SELECT bool_or(has_table_privilege(:role,c.oid,'INSERT,UPDATE,DELETE,TRUNCATE')) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=:namespace AND c.relkind='r'"
                ),
                dict(role=role, namespace=namespace),
            )
            for signature in (
                "source_bulk_authorize(bigint,smallint,bigint,bytea,integer,bigint)",
                "resolve_custom_import_source_batch_snapshot(uuid)",
                "source_set_finalize(uuid,integer[])",
            ):
                await session.execute(text(f'GRANT EXECUTE ON FUNCTION "{case.schema_name}".{signature} TO "{role}"'))
        async with source._page_session(case.sessions, context.request, context.build_id) as (session, _build):
            await session.execute(text(f'SET LOCAL SESSION AUTHORIZATION "{role}"'))
            batch_id, landing_rows = await _authorize(session, context, page)
            grants = (
                await session.execute(
                    text(
                        "SELECT a.attname,has_column_privilege(:role,c.oid,a.attnum,'INSERT') FROM pg_attribute a "
                        "JOIN pg_class c ON c.oid=a.attrelid JOIN pg_namespace n ON n.oid=c.relnamespace "
                        "WHERE n.nspname=:namespace AND c.relname='source_bulk_landing' AND a.attnum>0 AND NOT a.attisdropped"
                    ),
                    dict(role=role, namespace=namespace),
                )
            ).all()
            assert {name for name, allowed in grants if allowed} == set(source._SOURCE_COPY_COLUMNS)
            await source._copy_source_landing(session, landing_rows)
            finalized = await source._call(session, "source_set_finalize", (("uuid", batch_id), ("integer[]", [])))
            assert finalized.scalar_one() == 1
            await _assert_landing_insert_closed(session, role, namespace)
            await session.execute(text("RESET SESSION AUTHORIZATION"))
        async with case.sessions() as session, session.begin():
            await _assert_landing_insert_closed(session, role, namespace)


@pytest.mark.parametrize("change", ["owner", "execute", "signature"])
async def test_native_tampered_private_function_is_not_recreated_or_called(change):
    async with _bulk_case(default_role=True) as (case, role):
        context, _page, family_id = await _bootstrap(case)
        identity = f'"{snapshot_schema(family_id)}".freeze_custom_import_build_source(bigint)'
        async with case.sessions() as session, session.begin():
            if change == "owner":
                await session.execute(text(f'ALTER FUNCTION {identity} OWNER TO "{role}"'))
            elif change == "execute":
                await session.execute(text(f'GRANT EXECUTE ON FUNCTION {identity} TO "{role}"'))
            else:
                await session.execute(text(f"DROP FUNCTION {identity}"))
        with pytest.raises(DBAPIError, match="custom_import_snapshot_writer_signature_mismatch"):
            async with source._page_session(case.sessions, context.request, context.build_id) as (session, _build):
                await source._call(session, "resolve_custom_import_build_snapshot", (("bigint", context.build_id),))
