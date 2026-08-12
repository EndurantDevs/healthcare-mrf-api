# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Disposable PostgreSQL setup for admission-seal migration tests."""

from __future__ import annotations

import json

import pytest

from tests.test_provider_directory_endpoint_dataset_admission_seal_migration import (
    _SqlCapture,
)


asyncpg = pytest.importorskip("asyncpg")


async def _run_migration(migration, action: str, connection) -> None:
    capture = _SqlCapture()
    migration.op = capture
    getattr(migration, action)()
    for statement in capture.statements:
        await connection.execute(statement)


async def _install_legacy_dataset_surface(
    connection,
    schema: str,
    migration,
) -> None:
    table = f'"{schema}".provider_directory_endpoint_dataset'
    await _create_legacy_tables(connection, schema, table)
    await _create_legacy_guard_functions(connection, schema, migration)
    await _create_legacy_triggers(connection, schema, table, migration)
    await _create_legacy_support_functions(connection, schema)


async def _create_legacy_tables(connection, schema: str, table: str) -> None:
    await connection.execute(
        f"""
        CREATE TABLE {table} (
            dataset_id varchar(96) PRIMARY KEY,
            endpoint_id varchar(64),
            import_run_id varchar(64),
            acquisition_root_run_id varchar(64),
            previous_dataset_id varchar(96),
            dataset_hash varchar(64),
            status varchar(32) NOT NULL DEFAULT 'acquiring',
            is_current boolean NOT NULL DEFAULT false,
            resource_count bigint NOT NULL DEFAULT 0,
            created_at timestamp,
            validated_at timestamp,
            published_at timestamp,
            superseded_at timestamp,
            publication_metadata_json jsonb,
            artifact_selection_receipt_json jsonb,
            completion_proof_required_version integer,
            completion_proof_json jsonb,
            completion_proof_sha256 varchar(64)
        );
        CREATE TABLE "{schema}".legacy_guard_calls (
            guard_name text PRIMARY KEY,
            call_count bigint NOT NULL
        );
        """
    )


async def _create_legacy_guard_functions(connection, schema: str, migration) -> None:
    for function_name in sorted(
        {shape[2] for shape in migration._LEGACY_TRIGGER_SHAPES}
    ):
        await connection.execute(
            f"""
            CREATE FUNCTION "{schema}"."{function_name}"()
            RETURNS trigger LANGUAGE plpgsql AS $function$
            BEGIN
                INSERT INTO "{schema}".legacy_guard_calls (
                    guard_name, call_count
                ) VALUES ('{function_name}', 1)
                ON CONFLICT (guard_name) DO UPDATE
                    SET call_count =
                        "{schema}".legacy_guard_calls.call_count + 1;
                IF TG_OP = 'DELETE' THEN
                    RETURN OLD;
                END IF;
                RETURN NEW;
            END;
            $function$;
            """
        )


async def _create_legacy_triggers(
    connection,
    schema: str,
    table: str,
    migration,
) -> None:
    for name, event_clause, function_name, _type, is_constraint in (
        migration._LEGACY_TRIGGER_SHAPES
    ):
        constraint = "CONSTRAINT " if is_constraint else ""
        deferral = " DEFERRABLE INITIALLY DEFERRED" if is_constraint else ""
        await connection.execute(
            f"CREATE {constraint}TRIGGER \"{name}\" {event_clause} "
            f"ON {table}{deferral} FOR EACH ROW EXECUTE FUNCTION "
            f'"{schema}"."{function_name}"(); '
            f'ALTER TABLE {table} ENABLE ALWAYS TRIGGER "{name}";'
        )
    await connection.execute(
        f"ALTER TABLE {table} ADD CONSTRAINT "
        f'"{migration._REPLAY_CHECK}" CHECK (true)'
    )


async def _create_legacy_support_functions(connection, schema: str) -> None:
    await connection.execute(
        f"""
        CREATE FUNCTION "{schema}".provider_directory_subset_canonical_sha256(
            jsonb
        ) RETURNS text LANGUAGE sql IMMUTABLE AS
            'SELECT repeat(''a'', 64)';
        CREATE FUNCTION "{schema}".provider_directory_subset_replay_evidence_shape_valid(
            jsonb, text, jsonb, text
        ) RETURNS boolean LANGUAGE sql IMMUTABLE AS 'SELECT true';
        CREATE FUNCTION "{schema}".provider_directory_subset_coverage_shape_valid(
            jsonb, jsonb, text, text
        ) RETURNS boolean LANGUAGE sql IMMUTABLE AS 'SELECT true';
        """
    )


async def _assert_receipt_only_update_is_scoped(
    connection,
    schema: str,
) -> None:
    table = f'"{schema}".provider_directory_endpoint_dataset'
    await connection.execute(
        f"INSERT INTO {table} (dataset_id) VALUES ('dataset_receipt_only')"
    )
    await connection.execute(f'TRUNCATE "{schema}".legacy_guard_calls')
    await connection.execute(
        f"""
        UPDATE {table}
           SET publication_metadata_summary_json = '{{}}'::jsonb,
               publication_metadata_sha256 =
                   "{schema}".provider_directory_endpoint_dataset_admission_metadata_sha256(
                       '{{}}'::jsonb, 1::smallint, 'generic'::text,
                       repeat('a', 64), ARRAY[]::varchar[]
                   ),
               content_proof_admission_version = 1,
               content_proof_admission_kind = 'generic',
               content_proof_admission_sha256 = repeat('a', 64),
               content_proof_resource_types = ARRAY[]::varchar[]
         WHERE dataset_id = 'dataset_receipt_only'
        """
    )
    assert await connection.fetchval(
        f'SELECT COALESCE(sum(call_count), 0) FROM "{schema}".legacy_guard_calls'
    ) == 0
    await connection.execute(
        f"UPDATE {table} SET status = status "
        "WHERE dataset_id = 'dataset_receipt_only'"
    )
    assert await connection.fetchval(
        f'SELECT sum(call_count) FROM "{schema}".legacy_guard_calls'
    ) == 6
    await connection.execute(
        f"INSERT INTO {table} (dataset_id) VALUES ('dataset_replay_invalid')"
    )
    await _expect_error(
        connection,
        "pd_endpoint_dataset_subset_replay_evidence_check",
        f"""
        UPDATE {table}
           SET status = 'validated',
               dataset_hash = repeat('b', 64),
               completion_proof_required_version = 3,
               completion_proof_json = '{{}}'::jsonb,
               completion_proof_sha256 = repeat('c', 64),
               publication_metadata_json = '{{}}'::jsonb
         WHERE dataset_id = 'dataset_replay_invalid'
        """,
    )


async def _assert_legacy_surface_contract(
    connection,
    schema: str,
    migration,
    *,
    scoped: bool,
) -> None:
    table = f'"{schema}".provider_directory_endpoint_dataset'
    expected_attributes = await _legacy_trigger_attributes(
        connection,
        table,
        migration,
    )
    trigger_rows = await _legacy_trigger_rows(connection, table, migration)
    _assert_legacy_trigger_shapes(
        trigger_rows,
        migration,
        expected_attributes if scoped else "",
    )
    await _assert_replay_guard_shape(
        connection,
        table,
        migration,
        expected_attributes,
        scoped,
    )


async def _legacy_trigger_attributes(connection, table: str, migration) -> str:
    return await connection.fetchval(
        """
        SELECT pg_catalog.string_agg(
                   attribute.attnum::text,
                   ' ' ORDER BY requested.ordinality
               )
          FROM pg_catalog.unnest($1::text[])
               WITH ORDINALITY AS requested(column_name, ordinality)
          JOIN pg_catalog.pg_attribute AS attribute
            ON attribute.attrelid = $2::regclass
           AND attribute.attname = requested.column_name
        """,
        list(migration._PRE_M1_COLUMNS),
        table,
    )


async def _legacy_trigger_rows(connection, table: str, migration):
    return await connection.fetch(
        """
        SELECT trigger_row.tgname,
               trigger_row.tgtype,
               trigger_row.tgenabled,
               trigger_row.tgconstraint <> 0 AS is_constraint,
               trigger_row.tgdeferrable,
               trigger_row.tginitdeferred,
               trigger_row.tgattr::text AS attributes
          FROM pg_catalog.pg_trigger AS trigger_row
         WHERE trigger_row.tgrelid = $1::regclass
           AND trigger_row.tgname = ANY($2::text[])
         ORDER BY trigger_row.tgname
        """,
        table,
        [shape[0] for shape in migration._LEGACY_TRIGGER_SHAPES],
    )


def _assert_legacy_trigger_shapes(
    trigger_rows,
    migration,
    expected_attributes: str,
) -> None:
    assert len(trigger_rows) == len(migration._LEGACY_TRIGGER_SHAPES)
    shape_by_name = {shape[0]: shape for shape in migration._LEGACY_TRIGGER_SHAPES}
    for trigger_record in trigger_rows:
        shape = shape_by_name[trigger_record["tgname"]]
        assert trigger_record["tgtype"] == shape[3]
        assert trigger_record["tgenabled"] == b"A"
        assert trigger_record["is_constraint"] is shape[4]
        assert trigger_record["tgdeferrable"] is shape[4]
        assert trigger_record["tginitdeferred"] is shape[4]
        assert trigger_record["attributes"] == expected_attributes


async def _assert_replay_guard_shape(
    connection,
    table: str,
    migration,
    expected_attributes: str,
    scoped: bool,
) -> None:
    replay_check_exists = await connection.fetchval(
        "SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_constraint "
        "WHERE conrelid = $1::regclass AND conname = $2)",
        table,
        migration._REPLAY_CHECK,
    )
    replay_trigger = await connection.fetchrow(
        "SELECT tgenabled, tgattr::text AS attributes "
        "FROM pg_catalog.pg_trigger WHERE tgrelid = $1::regclass AND tgname = $2",
        table,
        migration._REPLAY_GUARD_TRIGGER,
    )
    assert replay_check_exists is (not scoped)
    if scoped:
        assert replay_trigger is not None
        assert replay_trigger["tgenabled"] == b"A"
        assert replay_trigger["attributes"] == expected_attributes
    else:
        assert replay_trigger is None


async def _expect_error(connection, marker: str, statement: str, *args) -> None:
    with pytest.raises(asyncpg.PostgresError, match=marker):
        await connection.execute(statement, *args)


def _digest_call(schema: str) -> str:
    return (
        f'"{schema}".'
        "provider_directory_endpoint_dataset_admission_metadata_sha256"
        "($1::jsonb, $2::smallint, $3::text, $4::text, $5::varchar[])"
    )


async def _insert_sealed(
    connection,
    schema: str,
    dataset_id: str,
    summary: dict,
    proof_sha256: str,
    resource_types: list[str],
) -> None:
    table = f'"{schema}".provider_directory_endpoint_dataset'
    digest = _digest_call(schema)
    await connection.execute(
        f"""
        INSERT INTO {table} (
            dataset_id,
            publication_metadata_json,
            publication_metadata_summary_json,
            publication_metadata_sha256,
            content_proof_admission_version,
            content_proof_admission_kind,
            content_proof_admission_sha256,
            content_proof_resource_types
        ) VALUES (
            $6,
            $1::json,
            $1::jsonb,
            {digest},
            $2,
            $3,
            $4,
            $5::varchar[]
        )
        """,
        json.dumps(summary, ensure_ascii=False),
        1,
        "generic",
        proof_sha256,
        resource_types,
        dataset_id,
    )
