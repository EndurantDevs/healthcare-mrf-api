# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native bulk writers, scalar-presence guards, COPY and rollback regressions."""

from __future__ import annotations

import json
import uuid
from contextlib import asynccontextmanager
from dataclasses import replace
from pathlib import Path

import pytest
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy import func, inspect, select, text, update
from sqlalchemy.exc import DBAPIError

from db.models.custom_import import (
    CustomImportBuildAttempt,
    CustomImportBuildFamily,
    CustomImportBuildStream,
    CustomImportChildRevision,
    CustomImportChildScalar,
    CustomImportFamilyChild,
    CustomImportField,
    CustomImportPack,
)
from db.models.custom_import_storage import CustomImportSnapshotFamily
from process.custom_import import build_graph as graph
from process.custom_import import build_output as output
from process.custom_import import build_source as source
from process.custom_import.bulk_page_codec import encode_landing_batch
from process.custom_import.execution import lease_token_sha256
from process.custom_import.storage_layout import snapshot_models, snapshot_schema
from tests.custom_import_postgres_support import _migration, isolated_publication_case
from tests.test_custom_import_build_output_postgres import _assert_legacy_parity, _complete, _records, _request_for
from tests.test_custom_import_build_source_postgres import _candidate_models, _retained_request, _root
from tests.test_custom_import_writer_cutover_postgres import _before_cutover, _record_role
from tests.test_custom_import_writer_cutover_postgres import _install as _install_cutover
from tests.test_custom_import_writer_cutover_postgres import _refresh as _refresh_rejections

_PATH = Path(__file__).resolve().parents[1] / "alembic/versions/20261009000000_custom_import_child_presence_decode.py"


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


def _refresh(connection, schema):
    migration = _migration(_PATH, "child_presence_decode_native")
    migration._schema = lambda: schema
    migration.op = Operations(MigrationContext.configure(connection))
    migration.upgrade()


def _presence_sql(migration, *, corrected):
    """Compare the actual old/new expected CTEs on identical synthetic rows."""
    expected = migration._NEW_EXPECTED if corrected else migration._OLD_EXPECTED
    for previous, replacement in (
        ("__CANDIDATE__.custom_import_child_revision", "child_rows"),
        ("__CONTROL__.custom_import_field", "field_rows"),
        ("b.dataset_id", "1::bigint"),
        ("b.schema_revision_id", "2::bigint"),
        ("p_child_ids", "CAST(:admitted AS bigint[])"),
    ):
        expected = expected.replace(previous, replacement)
    return text(
        """
        WITH child_rows(child_revision_id,collection_slot,canonical_payload) AS (
            SELECT * FROM unnest(ARRAY[11,12,13,14]::bigint[],ARRAY[1,2,3,1]::smallint[],
                ARRAY[CAST(:payload AS text),CAST(:other_payload AS text),
                    CAST(:other_payload AS text),CAST(:other_payload AS text)]::text[])
        ), field_rows(dataset_id,schema_revision_id,collection_slot,field_slot,field_name,projection_slot) AS (
            VALUES (1,2,1,1,'alpha',1),(1,2,1,2,'beta',2),(1,2,2,1,'alpha',0),
                (9,2,3,1,'alpha',1),(1,9,3,1,'alpha',1)
        ), """
        + expected.removeprefix("WITH ")
        + """,
        supplied AS (SELECT * FROM unnest(CAST(:scalar_ids AS bigint[]),CAST(:slots AS smallint[]),
            CAST(:states AS text[])) s(child_id,field_slot,value_state))
        SELECT NOT EXISTS((SELECT * FROM expected EXCEPT ALL SELECT * FROM supplied) UNION ALL
            (SELECT * FROM supplied EXCEPT ALL SELECT * FROM expected))
    """
    )


def _presence_cases():
    value = '{"field":"alpha","value":{"state":"value"}}'
    null = '{"field":"beta","value":{"state":"null"}}'
    missing = '{"field":"alpha","value":{"state":"missing"}}'
    return (
        (f'{{"fields":[{value},{null}]}}', [11, 11], [1, 2], ["value", "null"], True),
        (f'{{"fields":[{value},{null}]}}', [11], [1], ["value"], False),
        (f'{{"fields":[{value},{null}]}}', [11, 11, 99], [1, 2, 1], ["value", "null", "value"], False),
        (f'{{"fields":[{value}]}}', [11], [99], ["value"], False),
        (f'{{"fields":[{value},{value}]}}', [11], [1], ["value"], False),
        (f'{{"fields":[{value},{value}]}}', [11, 11], [1, 1], ["value", "value"], True),
        (f'{{"fields":[{value}]}}', [11, 11], [1, 1], ["value", "value"], False),
        (f'{{"fields":[{missing},{null}]}}', [11], [2], ["null"], True),
        (f'{{"fields":[{missing}]}}', [11], [1], ["missing"], False),
        ('{"fields":[{"field":"alpha","value":{}}]}', [], [], [], False),
        ('{"fields":[{"field":"alpha","value":{}}]}', [11], [1], [None], True),
        ('{"fields":[{"field":"unknown","value":{"state":"value"}}]}', [], [], [], True),
        ('{"fields":[]}', [], [], [], True),
        (r'{"fields":[{"field":"alpha","value":{"state":"value","value":"\u0000"}}]}', [11], [1], ["value"], True),
        (r'{"fields":[{"field":"alpha","value":{"state":"value","value":"\\u0000"}}]}', [11], [1], ["value"], True),
        ('{"fields":[{"field":"beta","field":"alpha","value":{"state":"value"}}]}', [11], [1], ["value"], True),
    )


async def _assert_presence_json_error(connection, statement, parameters_by_name, sqlstate):
    with pytest.raises(DBAPIError) as failure:
        async with connection.begin_nested():
            await connection.scalar(statement, parameters_by_name)
    assert failure.value.orig.sqlstate == sqlstate


async def _assert_presence_parse_errors(connection, migration):
    for payload, sqlstate in (("not-json", "22P02"), ('{"fields":{}}', "22023")):
        for corrected in (False, True):
            await _assert_presence_json_error(
                connection,
                _presence_sql(migration, corrected=corrected),
                dict(payload=payload, other_payload='{"fields":[]}', admitted=[11], scalar_ids=[], slots=[], states=[]),
                sqlstate,
            )


async def test_native_expected_multisets_match_including_zero_hot_and_unadmitted_payloads():
    """Retain both multiset directions and parse failures for the valid input domain."""
    migration = _migration(_PATH, "child_presence_query_native")
    async with isolated_publication_case() as case:
        async with case.engine.begin() as connection:
            for canonical_payload, scalar_ids, slots, states, accepted in _presence_cases():
                parameters_by_name = dict(
                    payload=canonical_payload,
                    other_payload='{"fields":[]}',
                    admitted=[11, 12, 13],
                    scalar_ids=scalar_ids,
                    slots=slots,
                    states=states,
                )
                verdicts = [
                    await connection.scalar(_presence_sql(migration, corrected=corrected), parameters_by_name)
                    for corrected in (False, True)
                ]
                assert verdicts == [accepted, accepted]
            for admitted in ([], [12, 13]):
                for corrected in (False, True):
                    assert await connection.scalar(
                        _presence_sql(migration, corrected=corrected),
                        dict(
                            payload='{"fields":[]}',
                            other_payload='{"fields":[]}',
                            admitted=admitted,
                            scalar_ids=[],
                            slots=[],
                            states=[],
                        ),
                    )
            await _assert_presence_parse_errors(connection, migration)


async def test_native_corrected_parse_scope_excludes_unadmitted_and_zero_hot_payloads():
    """Ignore only unadmitted or zero-hot malformed payloads, never admitted hot children."""
    migration = _migration(_PATH, "child_presence_parse_scope_native")
    statement = _presence_sql(migration, corrected=True)
    async with isolated_publication_case() as case:
        async with case.engine.begin() as connection:
            for canonical_payload, admitted in (
                ('{"fields":[]}', [11]),
                ('{"fields":[]}', [11, 12, 13]),
                ("not-json", []),
                ("not-json", [12, 13]),
            ):
                assert await connection.scalar(
                    statement,
                    dict(
                        payload=canonical_payload,
                        other_payload="not-json",
                        admitted=admitted,
                        scalar_ids=[],
                        slots=[],
                        states=[],
                    ),
                )
            for admitted in ([11], [14]):
                await _assert_presence_json_error(
                    connection,
                    statement,
                    dict(
                        payload="not-json",
                        other_payload="not-json",
                        admitted=admitted,
                        scalar_ids=[],
                        slots=[],
                        states=[],
                    ),
                    "22P02",
                )


async def _catalog(connection, schema):
    return {
        row.identity: (row.prosrc, row.metadata)
        for row in await connection.execute(
            text(f"""
            SELECT p.oid::regprocedure::text identity,p.prosrc,to_jsonb(p)-'prosrc' metadata
            FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace
            WHERE (n.nspname=:schema AND p.proname='install_custom_import_snapshot_writers')
                OR (p.proname='append_custom_import_build_source_families_page' AND n.nspname IN (
                    SELECT 'ci_snapshot_'||family_id::text FROM "{schema}".custom_import_snapshot_family
                    WHERE landing_table_oid IS NOT NULL))
            ORDER BY p.oid
        """),
            dict(schema=schema),
        )
    }


async def _assert_leaf_drift_rejected(connection, schema, leaf, expected_by_identity):
    for statement in (
        f"ALTER FUNCTION {leaf} SECURITY INVOKER",
        f"ALTER FUNCTION {leaf} SET search_path=public",
        f"ALTER FUNCTION {leaf} STRICT",
        f"ALTER FUNCTION {leaf} STABLE",
        f"ALTER FUNCTION {leaf} PARALLEL SAFE",
        f"ALTER FUNCTION {leaf} COST 101",
        f"ALTER FUNCTION {leaf} ROWS 1001",
        f"GRANT EXECUTE ON FUNCTION {leaf} TO PUBLIC",
        f"DROP FUNCTION {leaf}",
    ):
        with pytest.raises(RuntimeError, match="identity_mismatch"):
            async with connection.begin_nested():
                await connection.execute(text(statement))
                await connection.run_sync(_refresh, schema)
        assert await _catalog(connection, schema) == expected_by_identity


def _observe_partial_refresh(
    sync_connection, migration, schema, later_namespace, before_by_identity, after_by_identity, observed_refreshes
):
    """Observe earlier body changes before the known later-leaf failure."""
    migration._schema = lambda: schema
    migration.op = Operations(MigrationContext.configure(sync_connection))
    original_refresh = migration._refresh
    later_leaf = next(identity for identity in before_by_identity if identity.startswith(later_namespace + "."))

    def observe(*arguments):
        if arguments[1].startswith(f'"{later_namespace}".'):
            for identity in before_by_identity.keys() - {later_leaf}:
                body = sync_connection.execute(
                    text("SELECT prosrc FROM pg_proc WHERE oid=to_regprocedure(:identity)"),
                    dict(identity=identity),
                ).scalar_one()
                assert body == after_by_identity[identity][0] and body != before_by_identity[identity][0]
            observed_refreshes.append(True)
        return original_refresh(*arguments)

    migration._refresh = observe
    try:
        migration.upgrade()
    finally:
        migration._refresh = original_refresh


async def _assert_partial_refresh_rollback(
    connection, schema, storage_before, storage_query, before_by_identity, after_by_identity
):
    """Prove a later identity fault rolls actual earlier refreshes back to historical bodies."""
    migration = _migration(_PATH, "child_presence_partial_refresh_native")
    bulk = migration._bulk()
    namespaces = [f"ci_snapshot_{family.family_id}" for family in storage_before]
    historical_statements = [migration._installer(bulk, schema, corrected=False)] + [
        migration._leaf(bulk, schema, corrected=False, namespace=namespace) for namespace in namespaces
    ]
    for statement in historical_statements:
        await connection.execute(text(statement.replace("CREATE FUNCTION", "CREATE OR REPLACE FUNCTION", 1)))
    assert await _catalog(connection, schema) == before_by_identity
    source_queries = [
        text(f'SELECT to_jsonb(r) FROM "{namespace}".{relation} r ORDER BY {key}')
        for namespace in namespaces
        for relation, key in (
            ("custom_import_root_revision", "root_revision_id"),
            ("custom_import_child_revision", "child_revision_id"),
        )
    ]
    previous_source_rows = [(await connection.execute(query)).all() for query in source_queries]
    later_leaf = next(identity for identity in before_by_identity if identity.startswith(namespaces[-1] + "."))
    await connection.execute(text(f"ALTER FUNCTION {later_leaf} COST 101"))
    rollback_before = await _catalog(connection, schema)
    observed_refreshes = []
    with pytest.raises(RuntimeError, match="identity_mismatch"):
        async with connection.begin_nested():
            await connection.run_sync(
                _observe_partial_refresh,
                migration,
                schema,
                namespaces[-1],
                before_by_identity,
                after_by_identity,
                observed_refreshes,
            )
    assert observed_refreshes == [True]
    assert await _catalog(connection, schema) == rollback_before
    assert (await connection.execute(storage_query)).all() == storage_before
    assert [(await connection.execute(query)).all() for query in source_queries] == previous_source_rows
    await connection.execute(text(f"ALTER FUNCTION {later_leaf} COST 100"))
    assert await _catalog(connection, schema) == before_by_identity
    await connection.run_sync(_refresh, schema)
    assert await _catalog(connection, schema) == after_by_identity


async def test_native_refresh_reaches_frozen_writable_and_future_leaves_without_metadata_changes():
    """Refresh every registered leaf and future installer without altering protected metadata."""
    async with isolated_publication_case(migration_through="20261007000000") as case:
        frozen_request = await _request_for(case, _records(1, 1), page_rows=32)
        _, sealed = await _complete(case, frozen_request)
        writable_request = await _request_for(case, _records(1, 1), page_rows=32)
        await source.stage_segmented_source(case.sessions, writable_request)
        async with case.engine.begin() as connection:
            storage_query = text(f'SELECT * FROM "{case.schema_name}".custom_import_snapshot_family ORDER BY family_id')
            storage_before = (await connection.execute(storage_query)).all()
            assert {family.frozen_at is not None for family in storage_before} == {False, True}
            before_by_identity = await _catalog(connection, case.schema_name)
            assert len(before_by_identity) == 3
            await connection.run_sync(_refresh, case.schema_name)
            after_by_identity = await _catalog(connection, case.schema_name)
            assert before_by_identity.keys() == after_by_identity.keys()
            for identity, (body, metadata) in before_by_identity.items():
                assert after_by_identity[identity][1] == metadata and after_by_identity[identity][0] != body
                assert "decoded_fields AS MATERIALIZED" in after_by_identity[identity][0]
            await connection.run_sync(_refresh, case.schema_name)
            assert await _catalog(connection, case.schema_name) == after_by_identity
            assert (await connection.execute(storage_query)).all() == storage_before
            leaf = next(
                identity
                for identity in after_by_identity
                if "append_custom_import_build_source_families_page" in identity
            )
            await _assert_leaf_drift_rejected(connection, case.schema_name, leaf, after_by_identity)
            await _assert_partial_refresh_rollback(
                connection, case.schema_name, storage_before, storage_query, before_by_identity, after_by_identity
            )
        await _assert_legacy_parity(case, frozen_request, sealed)
        _, writable_sealed = await _complete(case, writable_request)
        await _assert_legacy_parity(case, writable_request, writable_sealed)
        future_request = await _request_for(case, _records(1, 1), page_rows=32)
        await source.stage_segmented_source(case.sessions, future_request)
        async with case.engine.begin() as connection:
            future = await _catalog(connection, case.schema_name)
            assert len(future) == 4
            assert all("decoded_fields AS MATERIALIZED" in body for body, _ in future.values())


class _PageCaptured(Exception):
    pass


async def _capture_child_arguments(case, request, build_id, monkeypatch, original):
    captured_arguments = []

    async def capture(session, name, arguments):
        if name == "append_custom_import_build_source_families_page":
            captured_arguments.append(arguments)
            raise _PageCaptured
        return await original(session, name, arguments)

    monkeypatch.setattr(graph, "_source_call", capture)
    with pytest.raises(_PageCaptured):
        await graph.build_graph(case.sessions, request, build_id)
    return captured_arguments[0]


async def _duplicate_child_payload(case, request, arguments):
    async with case.sessions() as session:
        models = await session.run_sync(_candidate_models, request)
        child_model = models[CustomImportChildRevision]
        child = (
            await session.scalars(select(child_model).where(child_model.child_revision_id == arguments[11][1][0]))
        ).one()
        payload = json.loads(child.canonical_payload)
        field_name = await session.scalar(
            select(CustomImportField.field_name).where(
                CustomImportField.dataset_id == request.dataset_id,
                CustomImportField.schema_revision_id == request.schema_revision_id,
                CustomImportField.collection_slot == child.collection_slot,
                CustomImportField.field_slot == arguments[13][1][0],
            )
        )
        payload["fields"].append(next(field for field in payload["fields"] if field["field"] == field_name))
        return models, json.dumps(payload)


async def _grant_child_dispatcher(case, worker, child_model, migration):
    bulk = migration._bulk()
    _, writer_arguments, _ = next(writer for writer in bulk._WRITERS if writer[0] == migration._WRITER)
    signature = f"{migration._WRITER}({bulk._argument_types(writer_arguments)})"
    async with case.engine.begin() as connection:
        await connection.execute(text(f'GRANT USAGE ON SCHEMA "{case.schema_name}" TO "{worker}"'))
        await connection.execute(text(f'GRANT EXECUTE ON FUNCTION "{case.schema_name}".{signature} TO "{worker}"'))
        leaf_namespace = inspect(child_model).selectable.schema
        leaf_identity = f'"{leaf_namespace}".{signature}'
        assert (
            await connection.scalar(text("SELECT to_regprocedure(:identity)::oid"), dict(identity=leaf_identity))
            is not None
        )
        assert not await connection.scalar(
            text("SELECT has_function_privilege(:role,:identity,'EXECUTE')"),
            dict(role=worker, identity=leaf_identity),
        )


def _mutated_child_arguments(arguments, mutation):
    changed_arguments = list(arguments)
    if mutation == "typed_before_json":
        changed_arguments[14] = (arguments[14][0], ("wrong", *arguments[14][1][1:]))
    elif mutation in {"omitted", "duplicate_supplied"}:
        for index in range(12, 22):
            values = arguments[index][1]
            changed_arguments[index] = (
                arguments[index][0],
                values[1:] if mutation == "omitted" else (*values, values[0]),
            )
    return tuple(changed_arguments)


async def _mutate_child_payload(session, child_model, child_id, mutation, duplicate_payload):
    if mutation in {"typed_before_json", "duplicate_payload", "malformed"}:
        await session.execute(
            update(child_model)
            .where(child_model.child_revision_id == child_id)
            .values(canonical_payload=duplicate_payload if mutation == "duplicate_payload" else "not-json")
        )


async def _assert_graph_empty(case, models):
    async with case.sessions() as session:
        for model in (CustomImportFamilyChild, CustomImportChildScalar):
            assert await session.scalar(select(func.count()).select_from(models[model])) == 0
        plan = (await session.scalars(select(models[CustomImportBuildFamily]))).one()
        assert plan.attached_child_count == 0 and plan.complete_at is None


async def _assert_role_rejections(case, request, build_id, models, arguments, duplicate_payload, role, original):
    """Check the unchanged error order and rolled-back output for one caller."""
    for mutation, message in (
        ("typed_before_json", "graph_children_scalar_mismatch"),
        ("omitted", "graph_children_scalar_presence"),
        ("duplicate_payload", "graph_children_scalar_presence"),
        ("duplicate_supplied", "graph_children_bounds"),
        ("malformed", "invalid input syntax for type json"),
    ):
        changed_arguments = _mutated_child_arguments(arguments, mutation)
        with pytest.raises(DBAPIError, match=message):
            async with graph._page_session(case.sessions, request, build_id) as (session, _build):
                await _mutate_child_payload(
                    session, models[CustomImportChildRevision], arguments[11][1][0], mutation, duplicate_payload
                )
                if role:
                    await session.execute(text(f'SET LOCAL ROLE "{role}"'))
                await original(session, "append_custom_import_build_source_families_page", changed_arguments)
        await _assert_graph_empty(case, models)


async def test_native_leaf_error_order_rollback_and_restricted_dispatcher_match(monkeypatch):
    """Exercise old and new guards as owner and restricted caller with exact rollback."""
    original = graph._source_call
    async with _before_cutover(roles=True) as (case, (_other_owner, worker)):
        async with case.engine.begin() as connection:
            await connection.run_sync(_install_cutover, case.schema_name)
            await connection.run_sync(_refresh_rejections, case.schema_name)
        request = await _request_for(case, _records(1, 1), page_rows=32)
        staged = await source.stage_segmented_source(case.sessions, request)
        arguments = await _capture_child_arguments(case, request, staged.build_id, monkeypatch, original)
        models, duplicate_payload = await _duplicate_child_payload(case, request, arguments)
        migration = _migration(_PATH, "child_presence_dispatch_native")
        await _grant_child_dispatcher(case, worker, models[CustomImportChildRevision], migration)
        for corrected in (False, True):
            if corrected:
                async with case.engine.begin() as connection:
                    await connection.run_sync(_refresh, case.schema_name)
            for role in (None, worker):
                await _assert_role_rejections(
                    case, request, staged.build_id, models, arguments, duplicate_payload, role, original
                )
        monkeypatch.setattr(graph, "_source_call", original)
        await graph.build_graph(case.sessions, request, staged.build_id)
        sealed = await output.build_output(case.sessions, request, staged.build_id)
        assert sealed.seal.family_count == sealed.seal.family_child_count == 1
        await _assert_legacy_parity(case, request, sealed)
