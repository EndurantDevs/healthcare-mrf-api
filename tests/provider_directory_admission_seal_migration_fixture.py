# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Fixture and catalog helpers for the admission-seal migration proof."""

from __future__ import annotations

import importlib.util
import json
import re
from pathlib import Path

import pytest


asyncpg = pytest.importorskip("asyncpg")

ROOT = Path(__file__).resolve().parents[1]
MIGRATION_PATH = (
    ROOT
    / "alembic"
    / "versions"
    / "20260812010000_provider_directory_endpoint_dataset_admission_seal.py"
)
PROOF_MIGRATION_PATH = (
    ROOT
    / "alembic"
    / "versions"
    / "20260808190000_provider_directory_subset_completion_proof.py"
)
POSTGRES_DSN_ENV = "HLTHPRT_TIN_NPI_CONNECTOR_POSTGRES_DSN"
TEST_DATABASE_PATTERN = re.compile(r"(?:^|[_-])test(?:[_-]|$)", re.IGNORECASE)


class _SqlCapture:
    def __init__(self) -> None:
        self.statements: list[str] = []

    def execute(self, statement: str) -> None:
        self.statements.append(statement)


def _load(path: Path, name: str):
    module_spec = importlib.util.spec_from_file_location(name, path)
    assert module_spec is not None and module_spec.loader is not None
    module = importlib.util.module_from_spec(module_spec)
    module_spec.loader.exec_module(module)
    return module


def _capture(monkeypatch, action: str):
    migration = _load(MIGRATION_PATH, f"admission_seal_{action}_migration")
    capture = _SqlCapture()
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "admission_seal_contract")
    monkeypatch.delenv("DB_SCHEMA", raising=False)
    migration.op = capture
    getattr(migration, action)()
    normalized = " ".join(" ".join(sql.split()) for sql in capture.statements)
    return migration, capture.statements, normalized


async def _run_migration(migration, action: str, connection) -> None:
    capture = _SqlCapture()
    migration.op = capture
    getattr(migration, action)()
    for statement in capture.statements:
        await connection.execute(statement)


async def _create_legacy_dataset_tables(connection, schema: str) -> None:
    """Create the pre-M1 endpoint-dataset and trigger-counter tables."""

    table = f'"{schema}".provider_directory_endpoint_dataset'
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


async def _install_legacy_guard_functions(connection, schema: str, migration) -> None:
    """Install the pre-M1 trigger functions with observable call counters."""

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


async def _install_legacy_guard_triggers(connection, schema: str, migration) -> None:
    """Install the exact pre-M1 trigger shapes and replay constraint."""

    table = f'"{schema}".provider_directory_endpoint_dataset'
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


async def _install_legacy_canonical_functions(connection, schema: str) -> None:
    """Install the canonical helper functions required by predecessor guards."""

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


async def _install_legacy_dataset_surface(
    connection,
    schema: str,
    migration,
) -> None:
    """Install the exact pre-M1 table, trigger, and canonical surface."""

    await _create_legacy_dataset_tables(connection, schema)
    await _install_legacy_guard_functions(connection, schema, migration)
    await _install_legacy_guard_triggers(connection, schema, migration)
    await _install_legacy_canonical_functions(connection, schema)


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
    """Verify scoped upgrade triggers and restored downgrade constraints."""

    table = f'"{schema}".provider_directory_endpoint_dataset'
    expected_attributes = await connection.fetchval(
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
    trigger_rows = await connection.fetch(
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
    _assert_legacy_trigger_shapes(
        trigger_rows,
        migration,
        expected_attributes if scoped else "",
    )
    await _assert_replay_surface(
        connection,
        table,
        migration,
        expected_attributes,
        scoped=scoped,
    )


def _assert_legacy_trigger_shapes(
    trigger_rows,
    migration,
    expected_attributes: str,
) -> None:
    """Match every legacy trigger to its exact catalog shape."""

    assert len(trigger_rows) == len(migration._LEGACY_TRIGGER_SHAPES)
    shape_by_name = {
        shape[0]: shape for shape in migration._LEGACY_TRIGGER_SHAPES
    }
    for trigger_row in trigger_rows:
        shape = shape_by_name[trigger_row["tgname"]]
        assert trigger_row["tgtype"] == shape[3]
        assert trigger_row["tgenabled"] == b"A"
        assert trigger_row["is_constraint"] is shape[4]
        assert trigger_row["tgdeferrable"] is shape[4]
        assert trigger_row["tginitdeferred"] is shape[4]
        assert trigger_row["attributes"] == expected_attributes


async def _assert_replay_surface(
    connection,
    table: str,
    migration,
    expected_attributes: str,
    *,
    scoped: bool,
) -> None:
    """Verify the replay check-to-trigger swap in both migration directions."""

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
