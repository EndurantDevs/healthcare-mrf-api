# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import importlib.util
import os
from pathlib import Path
import re
import subprocess
import sys
import uuid

import pytest
import sqlalchemy as sa
from alembic.config import Config
from alembic.script import ScriptDirectory
from sqlalchemy.dialects.postgresql import JSONB

from db.connection import Database
from db.models import (
    PTG2V3AuditOccurrence,
    PTG2V3Block,
    PTG2V3CandidateAuditAttestation,
    PTG2V3Code,
    PTG2V3GCCandidate,
    PTG2V3GraphOwner,
    PTG2V3LayoutFingerprint,
    PTG2V3NPIScope,
    PTG2V3PriceAttr,
    PTG2V3ProviderGroup,
    PTG2V3ProviderSet,
    PTG2V3SnapshotBlock,
    PTG2V3SnapshotBinding,
    PTG2V3SnapshotLayout,
    PTG2V3SnapshotScope,
    PTG2V3SnapshotSource,
    PTG2V3SourceAuditWitness,
    PTG2WitnessPart,
)
from db.models._legacy import (
    _move_address_key_column_to_end,
    _resolve_ptg2_database_schema,
)
from process.ptg_parts.ptg2_shared_gc import (
    require_migration_owned_tables,
)
from process.ptg_parts.ptg2_candidate_attestation import (
    candidate_attestation_digest,
)

from tests.ptg2_v3_shared_schema_assertions import (
    _OpRecorder,
    _assert_audit_attestation_types,
    _assert_block_provider_types,
    _assert_layout_audit_sql,
    _assert_layout_types,
    _assert_parent_columns,
    _assert_v3_check_constraint_names,
    _assert_v3_foreign_keys,
    _assert_v3_hash_partitions,
    _assert_v3_index_shapes,
    _assert_v3_migration_metadata,
    _assert_v3_migration_parent_tables,
    _assert_v3_migration_statement_order,
    _assert_v3_partition_intent,
    _assert_v3_primary_keys,
    _assert_v3_required_constraint_names,
    _assert_v3_required_migration_fragments,
    _assert_v3_sealed_mapping_index,
    _assert_v3_timezone_column_types,
    _assert_v3_unique_constraints,
    _constraints,
    _expected_v3_foreign_key_shapes,
    _foreign_key_shapes,
    _index_shapes,
    _load_migration,
    _normalized,
    _primary_key,
    _record_upgrade,
)
from tests.ptg2_v3_shared_schema_migrations import (
    FOLLOWUP_MIGRATION_PATH,
    HOLD_MIGRATION_PATH,
    MIGRATION_PATH,
)

@pytest.mark.parametrize(
    "migration_path",
    (MIGRATION_PATH, FOLLOWUP_MIGRATION_PATH),
)
def test_v3_migrations_reject_conflicting_database_schemas(
    monkeypatch,
    migration_path,
):
    migration = _load_migration(migration_path)
    monkeypatch.setenv("DB_SCHEMA", "alembic_schema")
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "runtime_schema")

    with pytest.raises(RuntimeError, match="must identify the same schema"):
        migration._schema()


@pytest.mark.parametrize(
    "migration_path",
    (MIGRATION_PATH, FOLLOWUP_MIGRATION_PATH),
)
def test_v3_migrations_support_legacy_schema_name(monkeypatch, migration_path):
    migration = _load_migration(migration_path)
    monkeypatch.setenv("DB_SCHEMA", "legacy_schema")
    monkeypatch.delenv("HLTHPRT_DB_SCHEMA", raising=False)

    assert migration._schema() == "legacy_schema"


def test_legacy_model_schema_helpers_reject_conflicts_and_ignore_missing_tables(
    monkeypatch,
):
    monkeypatch.setenv("DB_SCHEMA", "legacy_schema")
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "runtime_schema")

    with pytest.raises(RuntimeError, match="must identify the same schema"):
        _resolve_ptg2_database_schema()

    _move_address_key_column_to_end(object())


def test_v3_followup_migration_repairs_attestation_snapshot_index(monkeypatch):
    migration = _load_migration(FOLLOWUP_MIGRATION_PATH)
    recorder = _OpRecorder()
    monkeypatch.delenv("DB_SCHEMA", raising=False)
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "ptg_followup")
    monkeypatch.setattr(migration, "op", recorder)

    migration.upgrade()

    assert migration.revision == "20260714120000_ptg2_v3_schema_gc_consistency"
    assert migration.down_revision == "20260712120000_ptg2_v3_shared_schema"
    assert [_normalized(statement) for statement in recorder.executed] == [
        "CREATE INDEX IF NOT EXISTS "
        '"ptg2_v3_candidate_audit_attestation_snapshot_key_idx" '
        'ON "ptg_followup"."ptg2_v3_candidate_audit_attestation" '
        "(snapshot_key);"
    ]

    recorder.executed.clear()
    migration.downgrade()
    assert [_normalized(statement) for statement in recorder.executed] == [
        "DROP INDEX IF EXISTS "
        '"ptg_followup"."ptg2_v3_candidate_audit_attestation_snapshot_key_idx";'
    ]


def test_repository_has_single_alembic_head():
    root = Path(__file__).resolve().parents[1]
    config = Config(str(root / "alembic.ini"))

    assert ScriptDirectory.from_config(config).get_heads() == [
        "20260810040000_fhir_formulary_uhc_admission_receipt"
    ]


def test_candidate_audit_hold_migration_matches_runtime_digest():
    migration = _load_migration(HOLD_MIGRATION_PATH)
    report_digest = b"r" * 32

    assert migration.down_revision == (
        "20260728130000_provider_directory_content_proof_shards"
    )
    assert migration._attestation_digest(report_digest) == (
        candidate_attestation_digest(
            report_digest,
            "audit_and_activate",
        )
    )


def _fresh_v3_migration_statements(schema_name):
    original = _load_migration()
    followup = _load_migration(FOLLOWUP_MIGRATION_PATH)
    original_recorder = _OpRecorder()
    followup_recorder = _OpRecorder()
    original.op = original_recorder
    followup.op = followup_recorder
    original._schema = lambda: schema_name
    followup._schema = lambda: schema_name
    original.upgrade()
    followup.upgrade()
    return original_recorder.executed + followup_recorder.executed


async def _assert_v3_gc_schema_columns(database, schema_name):
    assert await database.scalar(
        """
        SELECT EXISTS (
            SELECT 1
              FROM pg_indexes
             WHERE schemaname = :schema_name
               AND tablename = 'ptg2_v3_candidate_audit_attestation'
               AND indexname =
                   'ptg2_v3_candidate_audit_attestation_snapshot_key_idx'
        )
        """,
        schema_name=schema_name,
    )
    assert await database.scalar(
        """
        SELECT COUNT(*)
          FROM information_schema.columns
         WHERE table_schema = :schema_name
           AND data_type = 'jsonb'
           AND (table_name, column_name) IN (
                ('ptg2_v3_snapshot_layout', 'layout_manifest'),
                ('ptg2_v3_candidate_audit_attestation', 'report')
           )
        """,
        schema_name=schema_name,
    ) == 2


async def _assert_v3_gc_foreign_keys(database, schema_name):
    fk_rows = await database.all(
        """
        SELECT constraint_name, delete_rule
          FROM information_schema.referential_constraints
         WHERE constraint_schema = :schema_name
           AND constraint_name IN (
                'ptg2_v3_snapshot_binding_snapshot_id_fkey',
                'ptg2_v3_snapshot_scope_snapshot_id_fkey',
                'ptg2_v3_candidate_audit_attestation_snapshot_id_fkey',
                'ptg2_v3_candidate_audit_attestation_snapshot_key_fkey'
           )
        """,
        schema_name=schema_name,
    )
    assert {str(row[0]): str(row[1]) for row in fk_rows} == {
        "ptg2_v3_snapshot_binding_snapshot_id_fkey": "CASCADE",
        "ptg2_v3_snapshot_scope_snapshot_id_fkey": "CASCADE",
        "ptg2_v3_candidate_audit_attestation_snapshot_id_fkey": "CASCADE",
        "ptg2_v3_candidate_audit_attestation_snapshot_key_fkey": "RESTRICT",
    }


@pytest.mark.asyncio
async def test_real_postgres_fresh_v3_migrations_have_gc_contract():
    """Verify real postgres fresh v3 migrations have gc contract."""
    if os.getenv("HLTHPRT_PTG2_SHARED_GC_POSTGRES_TEST") != "1":
        pytest.skip(
            "set HLTHPRT_PTG2_SHARED_GC_POSTGRES_TEST=1 for the isolated "
            "PostgreSQL test"
        )

    schema_name = f"ptg2_v3_schema_{uuid.uuid4().hex}"
    schema = f'"{schema_name}"'
    migration_statements = _fresh_v3_migration_statements(schema_name)

    database = Database()
    await database.connect()
    try:
        await database.execute_ddl(f"CREATE SCHEMA {schema}")
        for statement in migration_statements:
            await database.execute_ddl(statement)

        await require_migration_owned_tables(database, schema_name)
        await _assert_v3_gc_schema_columns(database, schema_name)
        await _assert_v3_gc_foreign_keys(database, schema_name)
    finally:
        try:
            await database.execute_ddl(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
        finally:
            await database.disconnect()


def _probe_db_schema_aliases(root: Path, env: dict[str, str]):
    script = """
from db.models import (
    PTG2Plan,
    PTG2Snapshot,
    PTG2SourceTraceSet,
    PTG2V3CandidateAuditAttestation,
    PTG2V3SnapshotBinding,
    PTG2V3SnapshotLayout,
    PTG2V3SnapshotSource,
)
models = (
    PTG2Plan,
    PTG2Snapshot,
    PTG2SourceTraceSet,
    PTG2V3CandidateAuditAttestation,
    PTG2V3SnapshotBinding,
    PTG2V3SnapshotLayout,
    PTG2V3SnapshotSource,
)
print("SCHEMAS=" + ",".join(model.__table__.schema for model in models))
print(
    "TARGETS="
    + ",".join(
        sorted(
            element.target_fullname
            for model in models
            for constraint in model.__table__.foreign_key_constraints
            for element in constraint.elements
        )
    )
)
"""
    return subprocess.run(
        [sys.executable, "-c", script],
        cwd=root,
        env=env,
        check=True,
        capture_output=True,
        text=True,
    )


def test_ptg2_models_support_db_schema_alias_without_cross_schema_fks():
    """Verify ptg2 models support db schema alias without cross schema fks."""
    root = Path(__file__).resolve().parents[1]
    env = os.environ.copy()
    env.pop("HLTHPRT_DB_SCHEMA", None)
    env["DB_SCHEMA"] = "ptg_legacy_schema"
    completed = _probe_db_schema_aliases(root, env)

    schema_line = next(
        line for line in completed.stdout.splitlines() if line.startswith("SCHEMAS=")
    )
    target_line = next(
        line for line in completed.stdout.splitlines() if line.startswith("TARGETS=")
    )
    assert set(schema_line.removeprefix("SCHEMAS=").split(",")) == {
        "ptg_legacy_schema"
    }
    assert all(
        schema_target.startswith("ptg_legacy_schema.")
        for schema_target in target_line.removeprefix("TARGETS=").split(",")
    )


def test_v3_shared_models_define_exact_parent_columns_and_types():
    """Ensure shared models expose the exact parent columns and SQL types."""

    _assert_parent_columns()
    _assert_layout_types()
    _assert_block_provider_types()
    _assert_audit_attestation_types()
    _assert_v3_timezone_column_types()


def test_v3_shared_models_define_keys_foreign_keys_and_uniqueness():
    """Ensure shared models define required keys, references, and uniqueness."""

    _assert_v3_primary_keys()
    _assert_v3_foreign_keys()
    _assert_v3_unique_constraints()


def test_v3_shared_models_define_checks_indexes_and_partition_intent():
    """Ensure shared models define required checks, indexes, and partitions."""

    _assert_v3_partition_intent()
    _assert_v3_index_shapes()
    _assert_v3_sealed_mapping_index()
    _assert_v3_check_constraint_names()


def test_v3_shared_migration_emits_tables_constraints_and_32_way_partitions(
    monkeypatch,
):
    """Ensure the migration emits every constraint and all 32 hash partitions."""

    migration, statements = _record_upgrade(monkeypatch, schema="ptg_shared")
    schema = '"ptg_shared"'

    _assert_v3_migration_metadata(migration)
    _assert_v3_migration_parent_tables(statements, schema)
    _assert_v3_migration_statement_order(statements, schema)
    _assert_layout_audit_sql(statements, schema)
    _assert_v3_hash_partitions(statements, schema)
    _assert_v3_required_migration_fragments(statements)
    _assert_v3_required_constraint_names(statements)


def test_v3_shared_migration_downgrade_is_dependency_safe(monkeypatch):
    migration = _load_migration()
    recorder = _OpRecorder()
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "ptg_shared")
    monkeypatch.setattr(migration, "op", recorder)

    migration.downgrade()

    assert [_normalized(statement) for statement in recorder.executed] == [
        'DROP TABLE IF EXISTS "ptg_shared"."ptg2_v3_gc_candidate";',
        'DROP TABLE IF EXISTS "ptg_shared"."ptg2_v3_candidate_audit_attestation";',
        'DROP TABLE IF EXISTS "ptg_shared"."ptg2_v3_audit_occurrence" CASCADE;',
        'DROP TABLE IF EXISTS "ptg_shared"."ptg2_v3_npi_scope" CASCADE;',
        'DROP TABLE IF EXISTS "ptg_shared"."ptg2_v3_price_attr";',
        'DROP TABLE IF EXISTS "ptg_shared"."ptg2_v3_provider_set";',
        'DROP TABLE IF EXISTS "ptg_shared"."ptg2_v3_provider_group";',
        'DROP TABLE IF EXISTS "ptg_shared"."ptg2_v3_code";',
        'DROP TABLE IF EXISTS "ptg_shared"."ptg2_v3_graph_owner";',
        'DROP TABLE IF EXISTS "ptg_shared"."ptg2_v3_snapshot_block";',
        'DROP TABLE IF EXISTS "ptg_shared"."ptg2_v3_snapshot_source";',
        'DROP TABLE IF EXISTS "ptg_shared"."ptg2_v3_snapshot_scope";',
        'DROP TABLE IF EXISTS "ptg_shared"."ptg2_v3_snapshot_binding";',
        'DROP TABLE IF EXISTS "ptg_shared"."ptg2_v3_layout_fingerprint";',
        'DROP TABLE IF EXISTS "ptg_shared"."ptg2_v3_block" CASCADE;',
        'DROP TABLE IF EXISTS "ptg_shared"."ptg2_v3_snapshot_layout";',
    ]
