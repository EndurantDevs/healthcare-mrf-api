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


MIGRATION_PATH = (
    Path(__file__).resolve().parents[1]
    / "alembic"
    / "versions"
    / "20260712120000_ptg2_v3_shared_schema.py"
)
FOLLOWUP_MIGRATION_PATH = (
    Path(__file__).resolve().parents[1]
    / "alembic"
    / "versions"
    / "20260714120000_ptg2_v3_schema_gc_consistency.py"
)


def _load_migration(path=MIGRATION_PATH):
    spec = importlib.util.spec_from_file_location(
        f"{path.stem}_migration",
        path,
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class _OpRecorder:
    def __init__(self):
        self.executed = []

    def execute(self, statement):
        self.executed.append(str(statement))


def _normalized(statement):
    return " ".join(str(statement).split())


def _primary_key(table):
    return tuple(column.name for column in table.primary_key.columns)


def _constraints(table, constraint_type):
    return {
        constraint.name: constraint
        for constraint in table.constraints
        if isinstance(constraint, constraint_type)
    }


def _foreign_key_shapes(table):
    return {
        constraint.name: (
            tuple(column.name for column in constraint.columns),
            tuple(element.target_fullname for element in constraint.elements),
            constraint.ondelete,
        )
        for constraint in table.foreign_key_constraints
    }


def _index_shapes(table):
    return {
        index.name: (
            tuple(expression.name for expression in index.expressions),
            tuple(index.dialect_options["postgresql"].get("include") or ()),
        )
        for index in table.indexes
    }


def _record_upgrade(monkeypatch, schema="mrf"):
    migration = _load_migration()
    recorder = _OpRecorder()
    monkeypatch.delenv("DB_SCHEMA", raising=False)
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", schema)
    monkeypatch.setattr(migration, "op", recorder)
    migration.upgrade()
    return migration, [_normalized(statement) for statement in recorder.executed]


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
        "20260721160000_provider_directory_retained_artifact_acquisition"
    ]


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

    database = Database()
    await database.connect()
    try:
        await database.execute_ddl(f"CREATE SCHEMA {schema}")
        for statement in original_recorder.executed:
            await database.execute_ddl(statement)
        for statement in followup_recorder.executed:
            await database.execute_ddl(statement)

        await require_migration_owned_tables(database, schema_name)
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
        assert {
            str(constraint_row[0]): str(constraint_row[1])
            for constraint_row in fk_rows
        } == {
            "ptg2_v3_snapshot_binding_snapshot_id_fkey": "CASCADE",
            "ptg2_v3_snapshot_scope_snapshot_id_fkey": "CASCADE",
            "ptg2_v3_candidate_audit_attestation_snapshot_id_fkey": "CASCADE",
            "ptg2_v3_candidate_audit_attestation_snapshot_key_fkey": "RESTRICT",
        }
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

    models = (
        PTG2V3SnapshotLayout,
        PTG2V3LayoutFingerprint,
        PTG2V3SnapshotBinding,
        PTG2V3SnapshotScope,
        PTG2V3SnapshotSource,
        PTG2V3Block,
        PTG2V3SnapshotBlock,
        PTG2V3GraphOwner,
        PTG2V3Code,
        PTG2V3ProviderGroup,
        PTG2V3ProviderSet,
        PTG2V3PriceAttr,
        PTG2V3NPIScope,
        PTG2V3AuditOccurrence,
        PTG2V3SourceAuditWitness,
        PTG2WitnessPart,
        PTG2V3CandidateAuditAttestation,
        PTG2V3GCCandidate,
    )
    expected_columns_by_table = {
        "ptg2_v3_snapshot_layout": (
            "snapshot_key",
            "storage_shard_id",
            "build_token",
            "generation",
            "state",
            "mapping_digest",
            "support_digest",
            "layout_manifest",
                "logical_byte_count",
                "created_at",
                "heartbeat_at",
                "lease_until",
                "published_at",
        ),
        "ptg2_v3_layout_fingerprint": (
            "semantic_fingerprint",
            "snapshot_key",
            "created_at",
        ),
        "ptg2_v3_snapshot_binding": (
            "snapshot_id",
            "snapshot_key",
            "created_at",
        ),
        "ptg2_v3_snapshot_scope": (
            "snapshot_id",
            "plan_id",
            "plan_market_type",
            "coverage_scope_id",
            "created_at",
        ),
        "ptg2_v3_snapshot_source": (
            "snapshot_id",
            "source_key",
            "source_type",
            "identity_kind",
            "identity_sha256",
            "raw_container_sha256",
            "logical_json_sha256",
            "logical_hash_deferred",
            "source_trace_set_hash",
        ),
        "ptg2_v3_block": (
            "block_hash",
            "format_version",
            "object_kind",
            "codec",
            "entry_count",
            "raw_byte_count",
            "stored_byte_count",
            "payload",
            "created_at",
        ),
        "ptg2_v3_snapshot_block": (
            "snapshot_key",
            "object_kind",
            "block_key",
            "fragment_no",
            "entry_count",
            "block_hash",
        ),
        "ptg2_v3_graph_owner": (
            "snapshot_key",
            "direction",
            "owner_key",
            "first_chunk",
            "member_offset",
            "member_count",
        ),
        "ptg2_v3_code": (
            "snapshot_key",
            "code_key",
            "code_global_id_128",
            "coverage_scope_id",
            "reported_code_system",
            "reported_code",
            "negotiation_arrangement",
            "billing_code_type_version",
            "source_name",
            "source_description",
            "rate_count",
        ),
        "ptg2_v3_provider_set": (
            "snapshot_key",
            "provider_set_key",
            "provider_set_global_id_128",
            "provider_count",
            "network_names",
        ),
        "ptg2_v3_provider_group": (
            "snapshot_key",
            "provider_group_key",
            "provider_group_global_id_128",
        ),
        "ptg2_v3_price_attr": (
            "snapshot_key",
            "attribute_kind",
            "attribute_key",
            "value",
        ),
        "ptg2_v3_npi_scope": (
            "snapshot_key",
            "npi",
        ),
        "ptg2_v3_audit_occurrence": (
            "snapshot_key",
            "occurrence_id",
            "code_key",
            "provider_set_key",
            "price_key",
            "source_key",
            "npi",
            "atom_ordinal",
            "atom_key",
        ),
        "ptg2_v3_source_audit_witness": (
            "snapshot_key",
            "contract",
            "selection_method",
            "source_set_digest",
            "sample_digest",
            "queryable_occurrence_population_count",
            "provider_population_count",
            "occurrence_witness_count",
            "provider_witness_count",
            "payload_sha256",
            "payload",
            "created_at",
        ),
        "ptg2_v3_source_audit_witness_part": (
            "snapshot_key",
            "part_number",
            "part_sha256",
            "payload",
            "created_at",
        ),
        "ptg2_v3_candidate_audit_attestation": (
            "snapshot_id",
            "snapshot_key",
            "source_key",
            "plan_id",
            "plan_market_type",
            "coverage_scope_id",
            "source_set_digest",
            "audit_sample_digest",
            "source_witness_digest",
            "contract",
            "tool_name",
            "tool_version",
            "report_digest",
            "report",
            "attested_at",
            "expires_at",
            "activated_at",
        ),
        "ptg2_v3_gc_candidate": (
            "block_hash",
            "eligible_at",
            "queued_at",
        ),
    }

    expected_schema = (
        os.getenv("HLTHPRT_DB_SCHEMA") or os.getenv("DB_SCHEMA") or "mrf"
    )
    assert {model.__tablename__ for model in models} == set(
        expected_columns_by_table
    )
    for model in models:
        table = model.__table__
        assert table.schema == expected_schema
        assert tuple(table.columns.keys()) == expected_columns_by_table[table.name]

    layout = PTG2V3SnapshotLayout.__table__
    assert isinstance(layout.c.snapshot_key.type, sa.BigInteger)
    assert layout.c.snapshot_key.identity is not None
    assert isinstance(layout.c.mapping_digest.type, sa.LargeBinary)
    assert isinstance(layout.c.support_digest.type, sa.LargeBinary)
    assert isinstance(layout.c.layout_manifest.type, JSONB)
    assert "snapshot_id" not in layout.c
    assert "semantic_fingerprint" not in layout.c
    assert layout.c.storage_shard_id.server_default.arg.text == "0"
    assert layout.c.logical_byte_count.server_default.arg.text == "0"

    fingerprint = PTG2V3LayoutFingerprint.__table__
    assert isinstance(fingerprint.c.semantic_fingerprint.type, sa.LargeBinary)
    assert isinstance(fingerprint.c.snapshot_key.type, sa.BigInteger)

    block = PTG2V3Block.__table__
    assert isinstance(block.c.block_hash.type, sa.LargeBinary)
    assert isinstance(block.c.payload.type, sa.LargeBinary)
    assert isinstance(block.c.entry_count.type, sa.BigInteger)
    assert isinstance(block.c.raw_byte_count.type, sa.BigInteger)
    assert isinstance(block.c.stored_byte_count.type, sa.BigInteger)
    assert isinstance(
        PTG2V3ProviderSet.__table__.c.provider_set_global_id_128.type, sa.LargeBinary
    )
    assert isinstance(PTG2V3ProviderSet.__table__.c.network_names.type, sa.ARRAY)
    assert isinstance(PTG2V3Code.__table__.c.code_global_id_128.type, sa.LargeBinary)
    assert isinstance(PTG2V3Code.__table__.c.coverage_scope_id.type, sa.LargeBinary)
    snapshot_scope = PTG2V3SnapshotScope.__table__
    assert isinstance(snapshot_scope.c.coverage_scope_id.type, sa.LargeBinary)
    assert snapshot_scope.c.plan_market_type.server_default.arg.text == "''"
    snapshot_source = PTG2V3SnapshotSource.__table__
    assert isinstance(snapshot_source.c.source_key.type, sa.Integer)
    assert isinstance(snapshot_source.c.logical_hash_deferred.type, sa.Boolean)
    assert isinstance(
        PTG2V3ProviderGroup.__table__.c.provider_group_global_id_128.type,
        sa.LargeBinary,
    )
    audit_occurrence = PTG2V3AuditOccurrence.__table__
    assert isinstance(audit_occurrence.c.occurrence_id.type, sa.LargeBinary)
    assert audit_occurrence.c.occurrence_id.type.length == 32
    assert isinstance(audit_occurrence.c.code_key.type, sa.Integer)
    assert isinstance(audit_occurrence.c.provider_set_key.type, sa.Integer)
    assert isinstance(audit_occurrence.c.source_key.type, sa.Integer)
    for column in (
        audit_occurrence.c.snapshot_key,
        audit_occurrence.c.price_key,
        audit_occurrence.c.npi,
        audit_occurrence.c.atom_ordinal,
        audit_occurrence.c.atom_key,
    ):
        assert isinstance(column.type, sa.BigInteger)
    attestation = PTG2V3CandidateAuditAttestation.__table__
    source_witness = PTG2V3SourceAuditWitness.__table__
    source_witness_part = PTG2WitnessPart.__table__
    for column in (
        attestation.c.coverage_scope_id,
        attestation.c.source_set_digest,
        attestation.c.audit_sample_digest,
        attestation.c.source_witness_digest,
        attestation.c.report_digest,
        source_witness.c.source_set_digest,
        source_witness.c.sample_digest,
        source_witness.c.payload_sha256,
        source_witness.c.payload,
        source_witness_part.c.part_sha256,
        source_witness_part.c.payload,
    ):
        assert isinstance(column.type, sa.LargeBinary)
    assert isinstance(attestation.c.report.type, JSONB)

    timezone_columns = (
        layout.c.created_at,
        layout.c.published_at,
        fingerprint.c.created_at,
        PTG2V3SnapshotBinding.__table__.c.created_at,
        snapshot_scope.c.created_at,
        block.c.created_at,
        PTG2V3GCCandidate.__table__.c.eligible_at,
        PTG2V3GCCandidate.__table__.c.queued_at,
        attestation.c.attested_at,
        attestation.c.expires_at,
        attestation.c.activated_at,
        source_witness.c.created_at,
        source_witness_part.c.created_at,
    )
    assert all(isinstance(column.type, sa.DateTime) for column in timezone_columns)
    assert all(column.type.timezone is True for column in timezone_columns)


def test_v3_shared_models_define_keys_foreign_keys_and_uniqueness():
    """Ensure shared models define required keys, references, and uniqueness."""

    expected_primary_keys_by_model = {
        PTG2V3SnapshotLayout: ("snapshot_key",),
        PTG2V3LayoutFingerprint: ("semantic_fingerprint",),
        PTG2V3SnapshotBinding: ("snapshot_id",),
        PTG2V3SnapshotScope: ("snapshot_id",),
        PTG2V3SnapshotSource: ("snapshot_id", "source_key"),
        PTG2V3Block: ("block_hash",),
        PTG2V3SnapshotBlock: (
            "snapshot_key",
            "object_kind",
            "block_key",
            "fragment_no",
        ),
        PTG2V3GraphOwner: ("snapshot_key", "direction", "owner_key"),
        PTG2V3Code: ("snapshot_key", "code_key"),
        PTG2V3ProviderGroup: ("snapshot_key", "provider_group_key"),
        PTG2V3ProviderSet: ("snapshot_key", "provider_set_key"),
        PTG2V3PriceAttr: (
            "snapshot_key",
            "attribute_kind",
            "attribute_key",
        ),
        PTG2V3NPIScope: ("snapshot_key", "npi"),
        PTG2V3AuditOccurrence: ("snapshot_key", "occurrence_id"),
        PTG2V3SourceAuditWitness: ("snapshot_key",),
        PTG2WitnessPart: ("snapshot_key", "part_number"),
        PTG2V3CandidateAuditAttestation: ("snapshot_id",),
        PTG2V3GCCandidate: ("block_hash",),
    }
    for model, expected_columns in expected_primary_keys_by_model.items():
        assert _primary_key(model.__table__) == expected_columns

    schema = PTG2V3SnapshotLayout.__table__.schema
    layout_target = f"{schema}.ptg2_v3_snapshot_layout.snapshot_key"
    block_target = f"{schema}.ptg2_v3_block.block_hash"
    scope_target = f"{schema}.ptg2_v3_snapshot_scope.snapshot_id"
    snapshot_target = f"{schema}.ptg2_snapshot.snapshot_id"
    trace_set_target = f"{schema}.ptg2_source_trace_set.source_trace_set_hash"
    witness_target = f"{schema}.ptg2_v3_source_audit_witness.snapshot_key"
    expected_foreign_keys_by_model = {
        PTG2V3LayoutFingerprint: {
            "ptg2_v3_layout_fingerprint_snapshot_key_fkey": (
                ("snapshot_key",),
                (layout_target,),
                "CASCADE",
            ),
        },
        PTG2V3SnapshotBinding: {
            "ptg2_v3_snapshot_binding_snapshot_id_fkey": (
                ("snapshot_id",),
                (snapshot_target,),
                "CASCADE",
            ),
            "ptg2_v3_snapshot_binding_snapshot_key_fkey": (
                ("snapshot_key",),
                (layout_target,),
                "RESTRICT",
            ),
        },
        PTG2V3SnapshotScope: {
            "ptg2_v3_snapshot_scope_snapshot_id_fkey": (
                ("snapshot_id",),
                (snapshot_target,),
                "CASCADE",
            ),
        },
        PTG2V3SnapshotSource: {
            "ptg2_v3_snapshot_source_snapshot_id_fkey": (
                ("snapshot_id",),
                (scope_target,),
                "CASCADE",
            ),
            "ptg2_v3_snapshot_source_trace_set_hash_fkey": (
                ("source_trace_set_hash",),
                (trace_set_target,),
                "RESTRICT",
            ),
        },
        PTG2V3SnapshotBlock: {
            "ptg2_v3_snapshot_block_snapshot_key_fkey": (
                ("snapshot_key",),
                (layout_target,),
                "CASCADE",
            ),
            "ptg2_v3_snapshot_block_block_hash_fkey": (
                ("block_hash",),
                (block_target,),
                None,
            ),
        },
        PTG2V3GCCandidate: {
            "ptg2_v3_gc_candidate_block_hash_fkey": (
                ("block_hash",),
                (block_target,),
                "CASCADE",
            ),
        },
        PTG2V3CandidateAuditAttestation: {
            "ptg2_v3_candidate_audit_attestation_snapshot_id_fkey": (
                ("snapshot_id",),
                (scope_target,),
                "CASCADE",
            ),
            "ptg2_v3_candidate_audit_attestation_snapshot_key_fkey": (
                ("snapshot_key",),
                (layout_target,),
                "RESTRICT",
            ),
        },
        PTG2WitnessPart: {
            "ptg2_v3_source_audit_witness_part_parent_fkey": (
                ("snapshot_key",),
                (witness_target,),
                "CASCADE",
            ),
        },
    }
    for model, expected_shapes in expected_foreign_keys_by_model.items():
        assert _foreign_key_shapes(model.__table__) == expected_shapes
    for dense_model in (
        PTG2V3GraphOwner,
        PTG2V3Code,
        PTG2V3ProviderGroup,
        PTG2V3ProviderSet,
        PTG2V3PriceAttr,
        PTG2V3NPIScope,
        PTG2V3AuditOccurrence,
        PTG2V3SourceAuditWitness,
    ):
        assert _foreign_key_shapes(dense_model.__table__) == {}

    assert not _constraints(PTG2V3SnapshotLayout.__table__, sa.UniqueConstraint)
    source_unique = _constraints(
        PTG2V3SnapshotSource.__table__, sa.UniqueConstraint
    )["ptg2_v3_snapshot_source_identity_key"]
    assert tuple(column.name for column in source_unique.columns) == (
        "snapshot_id",
        "source_type",
        "identity_kind",
        "identity_sha256",
    )

    code_uniques = _constraints(
        PTG2V3Code.__table__,
        sa.UniqueConstraint,
    )
    assert "ptg2_v3_code_identity_key" not in code_uniques
    assert tuple(
        column.name
        for column in code_uniques["ptg2_v3_code_global_id_key"].columns
    ) == ("snapshot_key", "code_global_id_128")

    price_unique = _constraints(
        PTG2V3PriceAttr.__table__,
        sa.UniqueConstraint,
    )["ptg2_v3_price_attr_value_key"]
    assert tuple(column.name for column in price_unique.columns) == (
        "snapshot_key",
        "attribute_kind",
        "value",
    )
    assert price_unique.dialect_options["postgresql"]["nulls_not_distinct"] is True


def test_v3_shared_models_define_checks_indexes_and_partition_intent():
    """Ensure shared models define required checks, indexes, and partitions."""

    block = PTG2V3Block.__table__
    npi_scope = PTG2V3NPIScope.__table__
    audit_occurrence = PTG2V3AuditOccurrence.__table__
    assert block.dialect_options["postgresql"]["partition_by"] == ("HASH (block_hash)")
    assert npi_scope.dialect_options["postgresql"]["partition_by"] == (
        "HASH (snapshot_key)"
    )
    assert audit_occurrence.dialect_options["postgresql"]["partition_by"] == (
        "HASH (snapshot_key)"
    )

    assert _index_shapes(PTG2V3SnapshotLayout.__table__) == {
        "ptg2_v3_snapshot_layout_state_idx": (
            ("state", "lease_until", "heartbeat_at"),
            (),
        ),
        "ptg2_v3_snapshot_layout_sealed_mapping_idx": (
            ("generation", "mapping_digest", "support_digest"),
            (),
        ),
    }
    sealed_mapping_index = next(
        index
        for index in PTG2V3SnapshotLayout.__table__.indexes
        if index.name == "ptg2_v3_snapshot_layout_sealed_mapping_idx"
    )
    assert sealed_mapping_index.unique is True
    assert (
        str(sealed_mapping_index.dialect_options["postgresql"]["where"])
        == "state = 'sealed' AND mapping_digest IS NOT NULL AND support_digest IS NOT NULL"
    )
    assert _index_shapes(PTG2V3LayoutFingerprint.__table__) == {
        "ptg2_v3_layout_fingerprint_snapshot_key_idx": (
            ("snapshot_key",),
            (),
        ),
    }
    assert _index_shapes(PTG2V3SnapshotBinding.__table__) == {
        "ptg2_v3_snapshot_binding_snapshot_key_idx": (
            ("snapshot_key",),
            (),
        ),
    }
    assert _index_shapes(PTG2V3SnapshotScope.__table__) == {
        "ptg2_v3_snapshot_scope_lookup_idx": (
            ("snapshot_id", "coverage_scope_id"),
            (),
        ),
    }
    assert _index_shapes(PTG2V3SnapshotSource.__table__) == {}
    assert _index_shapes(PTG2V3SnapshotBlock.__table__) == {
        "ptg2_v3_snapshot_block_block_hash_idx": (("block_hash",), ()),
        "ptg2_v3_snapshot_block_lookup_idx": (
            ("snapshot_key", "object_kind", "block_key"),
            (),
        ),
    }
    assert _index_shapes(PTG2V3GraphOwner.__table__) == {
        "ptg2_v3_graph_owner_lookup_idx": (
            ("snapshot_key", "direction", "owner_key"),
            ("first_chunk", "member_offset", "member_count"),
        ),
    }
    assert _index_shapes(PTG2V3Code.__table__) == {
        "ptg2_v3_code_lookup_idx": (
            (
                "snapshot_key",
                "coverage_scope_id",
                "reported_code_system",
                "reported_code",
            ),
            ("code_key", "negotiation_arrangement", "rate_count"),
        ),
    }
    # The primary key already supplies the exact (snapshot_key, npi) lookup
    # index; a second identical index would only add COPY and storage cost.
    assert _index_shapes(npi_scope) == {}
    assert _index_shapes(PTG2V3GCCandidate.__table__) == {
        "ptg2_v3_gc_candidate_eligible_at_idx": (("eligible_at",), ()),
    }
    assert _index_shapes(PTG2V3CandidateAuditAttestation.__table__) == {
        "ptg2_v3_candidate_audit_attestation_expiry_idx": (
            ("expires_at", "activated_at"),
            (),
        ),
        "ptg2_v3_candidate_audit_attestation_snapshot_key_idx": (
            ("snapshot_key",),
            (),
        ),
    }
    assert _index_shapes(PTG2V3SourceAuditWitness.__table__) == {}
    assert _index_shapes(PTG2WitnessPart.__table__) == {}

    expected_check_names_by_model = {
        PTG2V3SnapshotLayout: {
            "ptg2_v3_snapshot_layout_state_check",
            "ptg2_v3_snapshot_layout_mapping_digest_check",
            "ptg2_v3_snapshot_layout_support_digest_check",
            "ptg2_v3_snapshot_layout_logical_byte_count_check",
        },
        PTG2V3LayoutFingerprint: {
            "ptg2_v3_layout_fingerprint_digest_check",
        },
        PTG2V3SnapshotScope: {
            "ptg2_v3_snapshot_scope_coverage_scope_id_check",
        },
        PTG2V3SnapshotSource: {
            "ptg2_v3_snapshot_source_source_key_check",
            "ptg2_v3_snapshot_source_source_type_check",
            "ptg2_v3_snapshot_source_identity_kind_check",
            "ptg2_v3_snapshot_source_identity_sha256_check",
            "ptg2_v3_snapshot_source_raw_sha256_check",
            "ptg2_v3_snapshot_source_logical_sha256_check",
            "ptg2_v3_snapshot_source_identity_evidence_check",
            "ptg2_v3_snapshot_source_trace_set_hash_check",
        },
        PTG2V3Block: {
            "ptg2_v3_block_hash_check",
            "ptg2_v3_block_format_version_check",
            "ptg2_v3_block_codec_check",
            "ptg2_v3_block_entry_count_check",
            "ptg2_v3_block_raw_byte_count_check",
            "ptg2_v3_block_stored_byte_count_check",
            "ptg2_v3_block_payload_size_check",
        },
        PTG2V3SnapshotBlock: {
            "ptg2_v3_snapshot_block_fragment_no_check",
            "ptg2_v3_snapshot_block_block_key_check",
            "ptg2_v3_snapshot_block_entry_count_check",
        },
        PTG2V3GraphOwner: {
            "ptg2_v3_graph_owner_direction_check",
            "ptg2_v3_graph_owner_first_chunk_check",
            "ptg2_v3_graph_owner_member_offset_check",
            "ptg2_v3_graph_owner_member_count_check",
        },
        PTG2V3Code: {
            "ptg2_v3_code_global_id_check",
            "ptg2_v3_code_coverage_scope_id_check",
            "ptg2_v3_code_rate_count_check",
            "ptg2_v3_code_code_key_check",
        },
        PTG2V3ProviderSet: {
            "ptg2_v3_provider_set_global_id_check",
            "ptg2_v3_provider_set_provider_count_check",
            "ptg2_v3_provider_set_key_check",
        },
        PTG2V3ProviderGroup: {
            "ptg2_v3_provider_group_global_id_check",
            "ptg2_v3_provider_group_key_check",
        },
        PTG2V3NPIScope: {"ptg2_v3_npi_scope_npi_check"},
        PTG2V3AuditOccurrence: {
            "ptg2_v3_audit_occurrence_id_check",
            "ptg2_v3_audit_occurrence_code_key_check",
            "ptg2_v3_audit_occurrence_provider_set_key_check",
            "ptg2_v3_audit_occurrence_price_key_check",
            "ptg2_v3_audit_occurrence_source_key_check",
            "ptg2_v3_audit_occurrence_npi_check",
            "ptg2_v3_audit_occurrence_atom_ordinal_check",
            "ptg2_v3_audit_occurrence_atom_key_check",
        },
        PTG2V3SourceAuditWitness: {
            "ptg2_v3_source_audit_witness_source_set_digest_check",
            "ptg2_v3_source_audit_witness_sample_digest_check",
            "ptg2_v3_source_audit_witness_payload_sha256_check",
            "ptg2_v3_source_audit_witness_occurrence_population_check",
            "ptg2_v3_source_audit_witness_provider_population_check",
            "ptg2_v3_source_audit_witness_occurrence_count_check",
            "ptg2_v3_source_audit_witness_provider_count_check",
            "ptg2_v3_source_audit_witness_total_count_check",
            "ptg2_v3_source_audit_witness_payload_check",
        },
        PTG2WitnessPart: {
            "ptg2_v3_source_audit_witness_part_number_check",
            "ptg2_v3_source_audit_witness_part_sha256_check",
            "ptg2_v3_source_audit_witness_part_payload_check",
        },
        PTG2V3CandidateAuditAttestation: {
            "ptg2_v3_candidate_audit_attestation_scope_check",
            "ptg2_v3_candidate_audit_attestation_source_set_check",
            "ptg2_v3_candidate_audit_attestation_sample_check",
            "ptg2_v3_candidate_audit_attestation_witness_check",
            "ptg2_v3_candidate_audit_attestation_report_check",
            "ptg2_v3_candidate_audit_attestation_expiry_check",
        },
    }
    for model, expected_names in expected_check_names_by_model.items():
        assert set(_constraints(model.__table__, sa.CheckConstraint)) == expected_names


def test_v3_shared_migration_emits_tables_constraints_and_32_way_partitions(
    monkeypatch,
):
    """Ensure the migration emits every constraint and all 32 hash partitions."""

    migration, statements = _record_upgrade(monkeypatch, schema="ptg_shared")
    schema = '"ptg_shared"'

    assert migration.revision == "20260712120000_ptg2_v3_shared_schema"
    assert migration.down_revision == (
        "20260713237000_provider_directory_plan_scalars"
    )

    expected_parents = {
        "ptg2_v3_snapshot_layout",
        "ptg2_v3_layout_fingerprint",
        "ptg2_v3_snapshot_binding",
        "ptg2_v3_snapshot_scope",
        "ptg2_v3_snapshot_source",
        "ptg2_v3_block",
        "ptg2_v3_snapshot_block",
        "ptg2_v3_graph_owner",
        "ptg2_v3_code",
        "ptg2_v3_provider_group",
        "ptg2_v3_provider_set",
        "ptg2_v3_price_attr",
        "ptg2_v3_npi_scope",
        "ptg2_v3_audit_occurrence",
        "ptg2_v3_candidate_audit_attestation",
        "ptg2_v3_gc_candidate",
    }
    for table_name in expected_parents:
        prefix = f'CREATE TABLE {schema}."{table_name}" ('
        assert any(statement.startswith(prefix) for statement in statements)

    trace_set_statement_index = next(
        index
        for index, statement in enumerate(statements)
        if statement.startswith(
            f'CREATE TABLE IF NOT EXISTS {schema}."ptg2_source_trace_set" ('
        )
    )
    snapshot_source_statement_index = next(
        index
        for index, statement in enumerate(statements)
        if statement.startswith(
            f'CREATE TABLE {schema}."ptg2_v3_snapshot_source" ('
        )
    )
    assert trace_set_statement_index < snapshot_source_statement_index
    assert "source_trace_set_hash varchar(64) NOT NULL" in statements[
        trace_set_statement_index
    ]
    assert "source_trace_hashes varchar[]" in statements[trace_set_statement_index]

    layout_statement = next(
        statement
        for statement in statements
        if statement.startswith(f'CREATE TABLE {schema}."ptg2_v3_snapshot_layout" (')
    )
    assert "snapshot_id" not in layout_statement
    assert "semantic_fingerprint" not in layout_statement
    sealed_mapping_statement = next(
        statement
        for statement in statements
        if statement.startswith(
            'CREATE UNIQUE INDEX "ptg2_v3_snapshot_layout_sealed_mapping_idx"'
        )
    )
    assert "NULLS NOT DISTINCT" not in sealed_mapping_statement
    assert (
        "WHERE state = 'sealed' AND mapping_digest IS NOT NULL AND support_digest IS NOT NULL"
        in sealed_mapping_statement
    )
    audit_statement = next(
        statement
        for statement in statements
        if statement.startswith(
            f'CREATE TABLE {schema}."ptg2_v3_audit_occurrence" ('
        )
    )
    assert "PRIMARY KEY (snapshot_key, occurrence_id)" in audit_statement
    assert "FOREIGN KEY" not in audit_statement

    for parent in (
        "ptg2_v3_block",
        "ptg2_v3_npi_scope",
        "ptg2_v3_audit_occurrence",
    ):
        marker = f'PARTITION OF {schema}."{parent}"'
        partitions = [statement for statement in statements if marker in statement]
        assert len(partitions) == 32
        observed_partition_by_remainder = {}
        pattern = re.compile(
            rf'CREATE TABLE {schema}\."{parent}_p(\d{{2}})" '
            rf'PARTITION OF {schema}\."{parent}" FOR VALUES WITH '
            r"\( MODULUS 32, REMAINDER (\d+) \);"
        )
        for statement in partitions:
            match = pattern.fullmatch(statement)
            assert match is not None
            observed_partition_by_remainder[int(match.group(1))] = int(
                match.group(2)
            )
        assert observed_partition_by_remainder == {
            remainder: remainder
            for remainder in range(32)
        }

    joined = "\n".join(statements)
    required_fragments = (
        "GENERATED BY DEFAULT AS IDENTITY",
        "CHECK (state IN ('building', 'sealed'))",
        "CHECK (mapping_digest IS NULL OR octet_length(mapping_digest) = 32)",
        "CHECK (support_digest IS NULL OR octet_length(support_digest) = 32)",
        "CHECK (logical_byte_count >= 0)",
        "CHECK (octet_length(semantic_fingerprint) = 32)",
        "CHECK (octet_length(coverage_scope_id) = 32)",
            "CHECK (octet_length(block_hash) = 32)",
            "CHECK (format_version = 2)",
            "CHECK (codec IN ('none', 'zlib'))",
        "CHECK (entry_count >= 0)",
        "CHECK (raw_byte_count >= 0)",
        "CHECK (stored_byte_count >= 0)",
        "CHECK (octet_length(payload) = stored_byte_count)",
            "CHECK (fragment_no >= 0)",
            "CHECK (block_key >= 0)",
        "CHECK (direction BETWEEN 1 AND 4)",
        "CHECK (first_chunk >= 0)",
            "CHECK (member_offset >= 0 AND member_offset < 65536)",
        "CHECK (member_count >= 0)",
            "CHECK (rate_count >= 0)",
            "CHECK (code_key >= 0)",
        "CHECK (octet_length(provider_set_global_id_128) = 16)",
        "CHECK (octet_length(provider_group_global_id_128) = 16)",
            "CHECK (provider_group_key >= 0)",
            "CHECK (provider_count >= 0)",
            "CHECK (provider_set_key >= 0)",
            "CHECK (npi > 0)",
        "CHECK (octet_length(occurrence_id) = 32)",
        "CHECK (provider_set_key >= 0)",
        "CHECK (price_key >= 0)",
        "CHECK (source_key >= 0)",
        "CHECK (npi BETWEEN 1000000000 AND 9999999999)",
        "CHECK (atom_ordinal >= 0)",
        "CHECK (atom_key >= 0)",
        "CHECK (octet_length(audit_sample_digest) = 32)",
        "UNIQUE NULLS NOT DISTINCT",
        'CREATE UNIQUE INDEX "ptg2_v3_snapshot_layout_sealed_mapping_idx"',
        "(generation, mapping_digest, support_digest) WHERE state = 'sealed' AND mapping_digest IS NOT NULL AND support_digest IS NOT NULL",
        'REFERENCES "ptg_shared"."ptg2_v3_snapshot_layout" (snapshot_key) ON DELETE CASCADE',
        'REFERENCES "ptg_shared"."ptg2_v3_snapshot_layout" (snapshot_key) ON DELETE RESTRICT',
        'REFERENCES "ptg_shared"."ptg2_v3_block" (block_hash)',
        'REFERENCES "ptg_shared"."ptg2_v3_snapshot_scope" (snapshot_id) ON DELETE CASCADE',
        'REFERENCES "ptg_shared"."ptg2_source_trace_set" (source_trace_set_hash) ON DELETE RESTRICT',
        "PARTITION BY HASH (block_hash)",
        "PARTITION BY HASH (snapshot_key)",
        "INCLUDE (code_key, negotiation_arrangement, rate_count)",
        "INCLUDE (first_chunk, member_offset, member_count)",
        'CREATE INDEX "ptg2_v3_candidate_audit_attestation_snapshot_key_idx"',
    )
    for fragment in required_fragments:
        assert fragment in joined

    required_constraint_names = {
        "ptg2_v3_layout_fingerprint_snapshot_key_fkey",
        "ptg2_v3_snapshot_binding_snapshot_key_fkey",
        "ptg2_v3_snapshot_scope_coverage_scope_id_check",
        "ptg2_v3_snapshot_source_snapshot_id_fkey",
        "ptg2_v3_snapshot_source_trace_set_hash_fkey",
        "ptg2_v3_snapshot_source_source_key_check",
        "ptg2_v3_snapshot_source_identity_sha256_check",
        "ptg2_v3_snapshot_source_raw_sha256_check",
        "ptg2_v3_snapshot_source_logical_sha256_check",
        "ptg2_v3_snapshot_source_identity_evidence_check",
        "ptg2_v3_snapshot_block_snapshot_key_fkey",
        "ptg2_v3_snapshot_block_block_hash_fkey",
        "ptg2_v3_gc_candidate_block_hash_fkey",
        "ptg2_v3_snapshot_layout_state_check",
        "ptg2_v3_snapshot_layout_mapping_digest_check",
        "ptg2_v3_snapshot_layout_support_digest_check",
        "ptg2_v3_snapshot_layout_logical_byte_count_check",
        "ptg2_v3_layout_fingerprint_digest_check",
        "ptg2_v3_block_hash_check",
        "ptg2_v3_block_entry_count_check",
        "ptg2_v3_block_raw_byte_count_check",
        "ptg2_v3_block_stored_byte_count_check",
        "ptg2_v3_block_payload_size_check",
        "ptg2_v3_snapshot_block_fragment_no_check",
        "ptg2_v3_snapshot_block_entry_count_check",
        "ptg2_v3_graph_owner_direction_check",
        "ptg2_v3_graph_owner_first_chunk_check",
        "ptg2_v3_graph_owner_member_offset_check",
        "ptg2_v3_graph_owner_member_count_check",
        "ptg2_v3_code_rate_count_check",
        "ptg2_v3_code_coverage_scope_id_check",
        "ptg2_v3_provider_group_global_id_check",
        "ptg2_v3_provider_set_global_id_check",
        "ptg2_v3_provider_set_provider_count_check",
        "ptg2_v3_audit_occurrence_id_check",
        "ptg2_v3_audit_occurrence_code_key_check",
        "ptg2_v3_audit_occurrence_provider_set_key_check",
        "ptg2_v3_audit_occurrence_price_key_check",
        "ptg2_v3_audit_occurrence_source_key_check",
        "ptg2_v3_audit_occurrence_npi_check",
        "ptg2_v3_audit_occurrence_atom_ordinal_check",
        "ptg2_v3_audit_occurrence_atom_key_check",
    }
    for constraint_name in required_constraint_names:
        assert f'CONSTRAINT "{constraint_name}"' in joined


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
