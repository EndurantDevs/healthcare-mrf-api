# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Migration and PostgreSQL proofs for bounded endpoint-dataset receipts."""

from __future__ import annotations

import importlib.util
import json
import os
from pathlib import Path
import re
import uuid

import pytest

from process.provider_directory_fhir_subset_canonical import (
    canonical_payload_sha256,
)


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


def test_upgrade_is_nullable_bounded_and_application_trusted(monkeypatch) -> None:
    migration, statements, sql = _capture(monkeypatch, "upgrade")

    assert migration.revision == (
        "20260812010000_provider_directory_endpoint_dataset_admission_seal"
    )
    assert migration.down_revision == (
        "20260811140000_ptg_v12_provider_publication_merge"
    )
    assert len(statements) == 42
    for column_name in migration._SEAL_COLUMNS:
        assert f"ADD COLUMN {column_name}" in sql
    assert "ADD COLUMN publication_metadata_summary_json jsonb" in sql
    assert "ADD COLUMN content_proof_resource_types varchar(64)[]" in sql
    assert (
        "CREATE INDEX \"pd_endpoint_dataset_admission_source_ids_idx\""
        in sql
    )
    assert "USING gin ((publication_metadata_summary_json -> 'source_ids'))" in sql
    assert (
        "WHERE status = 'validated' AND is_current = false "
        "AND superseded_at IS NULL"
    ) in sql
    add_columns_statement = next(
        statement for statement in statements
        if "ADD COLUMN publication_metadata_summary_json" in statement
    )
    assert "NOT NULL" not in add_columns_statement
    assert "provider_directory_subset_payload_sha256" in sql
    assert "ptg_wave_canonical_json_ascii_v1" not in sql
    assert "provider-directory-admission-seal-v1" in sql
    assert all(
        f"'{field_name}'" in sql
        for field_name in (
            "contract",
            "metadata_summary",
            "admission_version",
            "admission_kind",
            "proof_sha256",
            "resource_types",
        )
    )
    assert (
        "metadata_summary jsonb, admission_version smallint, "
        "admission_kind text, proof_sha256 text, resource_types varchar[] "
        ") RETURNS varchar"
    ) in sql
    assert sql.count("SECURITY DEFINER SET search_path = pg_catalog") == 4
    assert sql.count("REVOKE ALL ON FUNCTION") == 4
    assert sql.count("ENABLE ALWAYS TRIGGER") == 10
    assert "BEFORE INSERT OR UPDATE OF" in sql
    assert "pd_endpoint_dataset_subset_replay_evidence_check" in sql
    assert "pd_endpoint_dataset_subset_replay_evidence_guard" in sql
    for trigger_name in (
        "tin_npi_connector_endpoint_dataset_guard",
        "provider_directory_reviewed_subset_activation_dataset_guard",
        "pd_subset_abandonment_dataset_guard",
        "pd_subset_abandonment_dataset_consistency_guard",
        "pd_subset_terminal_disposition_dataset_consistency_guard",
        "pd_trr_dataset_row",
    ):
        assert f'DROP TRIGGER "{trigger_name}"' in sql
    assert "BEFORE TRUNCATE" in sql
    assert "publication_metadata_json::jsonb" in sql
    assert "publication_metadata_json::text" not in (
        migration._guard_function_sql("admission_seal_contract")
    )
    assert "Application terminal validation is authoritative" in sql
    assert "not same-owner authentication" in sql


def test_downgrade_removes_only_the_m1_receipt_surface(monkeypatch) -> None:
    migration, statements, sql = _capture(monkeypatch, "downgrade")

    assert len(statements) == 32
    assert sql.count("DROP TRIGGER") == 10
    assert sql.count("DROP FUNCTION") == 4
    assert sql.count("DROP COLUMN") == len(migration._SEAL_COLUMNS)
    assert 'DROP INDEX "admission_seal_contract".' \
        '"pd_endpoint_dataset_admission_source_ids_idx"' in sql
    assert "provider_directory_endpoint_dataset_admission_downgrade_blocked" in sql
    assert "ADD CONSTRAINT \"pd_endpoint_dataset_subset_replay_evidence_check\"" in sql
    assert "UPDATE OF" not in " ".join(
        statement for statement in statements if statement.startswith("CREATE")
    )


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
    assert len(trigger_rows) == len(migration._LEGACY_TRIGGER_SHAPES)
    shape_by_name = {shape[0]: shape for shape in migration._LEGACY_TRIGGER_SHAPES}
    for row in trigger_rows:
        shape = shape_by_name[row["tgname"]]
        assert row["tgtype"] == shape[3]
        assert row["tgenabled"] == b"A"
        assert row["is_constraint"] is shape[4]
        assert row["tgdeferrable"] is shape[4]
        assert row["tginitdeferred"] is shape[4]
        assert row["attributes"] == (expected_attributes if scoped else "")
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


async def _assert_catalog_contract(connection, schema: str) -> None:
    trigger_rows = await connection.fetch(
        """
        SELECT trigger_row.tgname, trigger_row.tgenabled
          FROM pg_catalog.pg_trigger AS trigger_row
          JOIN pg_catalog.pg_class AS relation
            ON relation.oid = trigger_row.tgrelid
          JOIN pg_catalog.pg_namespace AS relation_namespace
            ON relation_namespace.oid = relation.relnamespace
         WHERE relation_namespace.nspname = $1
           AND relation.relname = 'provider_directory_endpoint_dataset'
           AND trigger_row.tgname LIKE
                   'provider_directory_endpoint_dataset_admission%'
           AND trigger_row.tgisinternal IS FALSE
         ORDER BY trigger_row.tgname
        """,
        schema,
    )
    assert [(row["tgname"], row["tgenabled"]) for row in trigger_rows] == [
        (
            "provider_directory_endpoint_dataset_admission_raw_guard",
            b"A",
        ),
        (
            "provider_directory_endpoint_dataset_admission_seal_guard",
            b"A",
        ),
        (
            "provider_directory_endpoint_dataset_admission_truncate_guard",
            b"A",
        ),
    ]
    for signature in (
        "provider_directory_endpoint_dataset_admission_metadata_sha256"
        "(jsonb,smallint,text,text,character varying[])",
        "guard_provider_directory_endpoint_dataset_admission_seal()",
        "guard_provider_directory_endpoint_dataset_admission_truncate()",
    ):
        assert not await connection.fetchval(
            "SELECT pg_catalog.has_function_privilege("
            "'public', pg_catalog.to_regprocedure($1), 'EXECUTE')",
            f'"{schema}".{signature}',
        )


async def _assert_invalid_write_paths(connection, schema: str) -> None:
    table = f'"{schema}".provider_directory_endpoint_dataset'
    await _expect_error(
        connection,
        "provider_directory_endpoint_dataset_admission_seal_partial",
        f"INSERT INTO {table} (dataset_id, content_proof_admission_version) "
        "VALUES ('dataset_values_partial', 1)",
    )
    with pytest.raises(
        asyncpg.PostgresError,
        match="provider_directory_endpoint_dataset_admission_seal_partial",
    ):
        await connection.copy_records_to_table(
            "provider_directory_endpoint_dataset",
            schema_name=schema,
            records=[("dataset_copy_partial", 1)],
            columns=["dataset_id", "content_proof_admission_version"],
        )
    await connection.execute(
        f"INSERT INTO {table} (dataset_id) VALUES ('dataset_upsert_partial')"
    )
    await _expect_error(
        connection,
        "provider_directory_endpoint_dataset_admission_seal_partial",
        f"""
        INSERT INTO {table} (dataset_id)
        VALUES ('dataset_upsert_partial')
        ON CONFLICT (dataset_id) DO UPDATE
        SET content_proof_admission_version = 1
        """,
    )
    await _expect_error(
        connection,
        "provider_directory_endpoint_dataset_admission_resources_invalid",
        f"""
        UPDATE {table}
           SET publication_metadata_summary_json = '{{}}'::jsonb,
               publication_metadata_sha256 =
                   "{schema}".provider_directory_endpoint_dataset_admission_metadata_sha256(
                       '{{}}'::jsonb, 1::smallint, 'generic'::text,
                       repeat('a', 64),
                       ARRAY['Organization', 'Location']::varchar[]
                   ),
               content_proof_admission_version = 1,
               content_proof_admission_kind = 'generic',
               content_proof_admission_sha256 = repeat('a', 64),
               content_proof_resource_types =
                   ARRAY['Organization', 'Location']::varchar[]
         WHERE dataset_id = 'dataset_upsert_partial'
        """,
    )
    oversized_resource = "é" * 33
    await _expect_error(
        connection,
        "provider_directory_endpoint_dataset_admission_resources_invalid",
        f"""
        UPDATE {table}
           SET publication_metadata_summary_json = '{{}}'::jsonb,
               publication_metadata_sha256 =
                   "{schema}".provider_directory_endpoint_dataset_admission_metadata_sha256(
                       '{{}}'::jsonb, 1::smallint, 'generic'::text,
                       repeat('a', 64),
                       ARRAY[$1]::varchar[]
                   ),
               content_proof_admission_version = 1,
               content_proof_admission_kind = 'generic',
               content_proof_admission_sha256 = repeat('a', 64),
               content_proof_resource_types = ARRAY[$1]::varchar[]
         WHERE dataset_id = 'dataset_upsert_partial'
        """,
        oversized_resource,
    )


async def _assert_invalid_complete_receipts(connection, schema: str) -> None:
    table = f'"{schema}".provider_directory_endpoint_dataset'
    invalid_receipts = (
        (
            "provider_directory_endpoint_dataset_admission_version_invalid",
            {},
            2,
            "generic",
            "a" * 64,
            ["Location"],
            None,
        ),
        (
            "provider_directory_endpoint_dataset_admission_kind_invalid",
            {},
            1,
            "other",
            "a" * 64,
            ["Location"],
            None,
        ),
        (
            "provider_directory_endpoint_dataset_admission_proof_sha256_invalid",
            {},
            1,
            "generic",
            "A" * 64,
            ["Location"],
            None,
        ),
        (
            "provider_directory_endpoint_dataset_admission_summary_invalid",
            [],
            1,
            "generic",
            "a" * 64,
            ["Location"],
            None,
        ),
        (
            "provider_directory_endpoint_dataset_admission_summary_unbounded",
            {"payload": "x" * (1024 * 1024)},
            1,
            "generic",
            "a" * 64,
            ["Location"],
            None,
        ),
        (
            "provider_directory_endpoint_dataset_admission_resources_invalid",
            {},
            1,
            "generic",
            "a" * 64,
            ["Location", "Location"],
            None,
        ),
        (
            "provider_directory_endpoint_dataset_admission_resources_invalid",
            {},
            1,
            "generic",
            "a" * 64,
            [f"Resource{index:02d}" for index in range(65)],
            None,
        ),
        (
            "provider_directory_endpoint_dataset_admission_metadata_sha256_invalid",
            {},
            1,
            "generic",
            "a" * 64,
            ["Location"],
            "0" * 64,
        ),
    )
    for index, (
        marker,
        summary,
        version,
        kind,
        proof_sha256,
        resource_types,
        digest_override,
    ) in enumerate(invalid_receipts):
        dataset_id = f"dataset_invalid_receipt_{index}"
        await connection.execute(
            f"INSERT INTO {table} (dataset_id) VALUES ($1)",
            dataset_id,
        )
        await _expect_error(
            connection,
            marker,
            f"""
            UPDATE {table}
               SET publication_metadata_json = $2::json,
                   publication_metadata_summary_json = $2::jsonb,
                   publication_metadata_sha256 = COALESCE(
                       $7::varchar,
                       "{schema}".provider_directory_endpoint_dataset_admission_metadata_sha256(
                           $2::jsonb,
                           $3::smallint,
                           $4::text,
                           $5::text,
                           $6::varchar[]
                       )
                   ),
                   content_proof_admission_version = $3::smallint,
                   content_proof_admission_kind = $4,
                   content_proof_admission_sha256 = $5,
                   content_proof_resource_types = $6::varchar[]
             WHERE dataset_id = $1
            """,
            dataset_id,
            json.dumps(summary),
            version,
            kind,
            proof_sha256,
            resource_types,
            digest_override,
        )


async def _assert_sealed_mutations(connection, schema: str) -> None:
    table = f'"{schema}".provider_directory_endpoint_dataset'
    await connection.execute(
        f"UPDATE {table} SET status = 'published' "
        "WHERE dataset_id = 'dataset_sealed'"
    )
    await _expect_error(
        connection,
        "provider_directory_endpoint_dataset_admission_raw_metadata_immutable",
        f"""
        UPDATE {table}
           SET publication_metadata_json = pg_catalog.jsonb_set(
                   publication_metadata_json::jsonb,
                   '{{reviewed}}',
                   'true'::jsonb,
                   true
               )::json
         WHERE dataset_id = 'dataset_sealed'
        """,
    )
    await _expect_error(
        connection,
        "provider_directory_endpoint_dataset_admission_receipt_immutable",
        f"""
        UPDATE {table}
           SET publication_metadata_summary_json =
                   publication_metadata_summary_json || '{{"reviewed":true}}',
               publication_metadata_sha256 =
                   "{schema}".provider_directory_endpoint_dataset_admission_metadata_sha256(
                       publication_metadata_summary_json || '{{"reviewed":true}}',
                       content_proof_admission_version,
                       content_proof_admission_kind,
                       content_proof_admission_sha256,
                       content_proof_resource_types
                   )
         WHERE dataset_id = 'dataset_sealed'
        """,
    )
    await connection.execute(
        f"""
        UPDATE {table}
           SET publication_metadata_summary_json = jsonb_set(
                   publication_metadata_summary_json,
                   '{{outcome_resource_counts_v1}}',
                   '{{"complete":true}}'::jsonb,
                   true
               ),
               publication_metadata_sha256 =
                   "{schema}".provider_directory_endpoint_dataset_admission_metadata_sha256(
                       jsonb_set(
                           publication_metadata_summary_json,
                           '{{outcome_resource_counts_v1}}',
                           '{{"complete":true}}'::jsonb,
                           true
                       ),
                       content_proof_admission_version,
                       content_proof_admission_kind,
                       content_proof_admission_sha256,
                       content_proof_resource_types
                   )
         WHERE dataset_id = 'dataset_sealed'
        """,
    )
    await _expect_error(
        connection,
        "provider_directory_endpoint_dataset_admission_raw_metadata_immutable",
        f"""
        UPDATE {table}
           SET publication_metadata_json = pg_catalog.jsonb_set(
                   publication_metadata_json::jsonb,
                   '{{reviewed}}',
                   'true'::jsonb,
                   true
               )::json,
               publication_metadata_summary_json =
                   publication_metadata_summary_json || '{{"raw":true}}',
               publication_metadata_sha256 =
                   "{schema}".provider_directory_endpoint_dataset_admission_metadata_sha256(
                       publication_metadata_summary_json || '{{"raw":true}}',
                       content_proof_admission_version,
                       content_proof_admission_kind,
                       content_proof_admission_sha256,
                       content_proof_resource_types
                   )
         WHERE dataset_id = 'dataset_sealed'
        """
    )
    await _expect_error(
        connection,
        "provider_directory_endpoint_dataset_admission_receipt_immutable",
        f"""
        UPDATE {table}
           SET content_proof_admission_sha256 = repeat('b', 64),
               publication_metadata_sha256 =
                   "{schema}".provider_directory_endpoint_dataset_admission_metadata_sha256(
                       publication_metadata_summary_json,
                       content_proof_admission_version,
                       content_proof_admission_kind,
                       repeat('b', 64),
                       content_proof_resource_types
                   )
         WHERE dataset_id = 'dataset_sealed'
        """,
    )
    await _expect_error(
        connection,
        "provider_directory_endpoint_dataset_admission_truncate_forbidden",
        f"TRUNCATE {table}",
    )


@pytest.mark.asyncio
async def test_upgrade_guard_and_downgrade_execute_on_disposable_postgres(
    monkeypatch,
) -> None:
    database_dsn = os.getenv(POSTGRES_DSN_ENV)
    if not database_dsn:
        pytest.skip(f"set {POSTGRES_DSN_ENV} for the PostgreSQL proof")
    connection = await asyncpg.connect(database_dsn)
    database_name = str(await connection.fetchval("SELECT current_database()"))
    if TEST_DATABASE_PATTERN.search(database_name) is None:
        await connection.close()
        pytest.fail(f"{POSTGRES_DSN_ENV} must target an explicit test database")
    schema = "admission_seal_" + uuid.uuid4().hex
    migration = _load(MIGRATION_PATH, "admission_seal_postgres_migration")
    proof_migration = _load(
        PROOF_MIGRATION_PATH,
        "admission_seal_prerequisite_postgres_migration",
    )
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", schema)
    monkeypatch.delenv("DB_SCHEMA", raising=False)
    table = f'"{schema}".provider_directory_endpoint_dataset'
    try:
        await connection.execute(f'CREATE SCHEMA "{schema}"')
        await _install_legacy_dataset_surface(
            connection,
            schema,
            migration,
        )
        await connection.execute(
            proof_migration._payload_canonical_json_function_sql(schema)
        )
        await connection.execute(
            proof_migration._payload_sha256_function_sql(schema)
        )
        await _run_migration(migration, "upgrade", connection)
        await _assert_catalog_contract(connection, schema)
        await _assert_legacy_surface_contract(
            connection,
            schema,
            migration,
            scoped=True,
        )
        await _assert_receipt_only_update_is_scoped(connection, schema)

        await connection.execute(
            f"INSERT INTO {table} (dataset_id) VALUES ('dataset_legacy')"
        )
        await _assert_invalid_write_paths(connection, schema)
        await _assert_invalid_complete_receipts(connection, schema)
        summary = {
            "endpoint": "synthetic",
            "large_integer": 10000000000000000000000000001,
            "negative_zero": -0.0,
            "unicode": "Příklad 🙂",
        }
        proof_sha256 = "a" * 64
        resource_types = ["Location", "Organization"]
        database_digest = await connection.fetchval(
            f"SELECT {_digest_call(schema)}",
            json.dumps(summary, ensure_ascii=False),
            1,
            "generic",
            proof_sha256,
            resource_types,
        )
        assert database_digest == canonical_payload_sha256(
            {
                "contract": "provider-directory-admission-seal-v1",
                "metadata_summary": summary,
                "admission_version": 1,
                "admission_kind": "generic",
                "proof_sha256": proof_sha256,
                "resource_types": resource_types,
            }
        )
        await _insert_sealed(
            connection,
            schema,
            "dataset_sealed",
            summary,
            proof_sha256,
            resource_types,
        )
        await _assert_sealed_mutations(connection, schema)

        with pytest.raises(
            asyncpg.PostgresError,
            match="provider_directory_endpoint_dataset_admission_downgrade_blocked",
        ):
            await _run_migration(migration, "downgrade", connection)
        await connection.execute(f"DELETE FROM {table}")
        await _run_migration(migration, "downgrade", connection)
        await _assert_legacy_surface_contract(
            connection,
            schema,
            migration,
            scoped=False,
        )
        assert await connection.fetchval(
            "SELECT pg_catalog.to_regprocedure($1) IS NULL",
            f'"{schema}".'
            "provider_directory_endpoint_dataset_admission_metadata_sha256"
            "(jsonb,smallint,text,text,character varying[])",
        )
        assert not await connection.fetchval(
            """
            SELECT EXISTS (
                SELECT 1
                  FROM information_schema.columns
                 WHERE table_schema = $1
                   AND table_name = 'provider_directory_endpoint_dataset'
                   AND column_name LIKE '%admission%'
            )
            """,
            schema,
        )
        await _run_migration(migration, "upgrade", connection)
        await _assert_catalog_contract(connection, schema)
    finally:
        await connection.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await connection.close()
