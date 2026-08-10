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

from tests.ptg2_v3_shared_schema_models import (
    V3_DENSE_MODELS,
    V3_EXPECTED_COLUMNS_BY_TABLE,
    V3_EXPECTED_PRIMARY_KEYS_BY_MODEL,
    V3_FOREIGN_KEY_SHAPES_BY_MODEL,
    V3_INDEX_SHAPES_BY_MODEL,
    V3_SHARED_MODELS,
)
from tests.ptg2_v3_shared_schema_migrations import (
    FOLLOWUP_MIGRATION_PATH,
    HOLD_MIGRATION_PATH,
    MIGRATION_PATH,
    V3_CHECK_NAMES_BY_MODEL,
    V3_MIGRATION_PARENT_TABLES,
    V3_MIGRATION_REQUIRED_CONSTRAINT_NAMES,
    V3_MIGRATION_REQUIRED_FRAGMENTS,
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


def _assert_parent_columns():
    expected_schema = (
        os.getenv("HLTHPRT_DB_SCHEMA") or os.getenv("DB_SCHEMA") or "mrf"
    )
    assert {model.__tablename__ for model in V3_SHARED_MODELS} == set(
        V3_EXPECTED_COLUMNS_BY_TABLE
    )
    for model in V3_SHARED_MODELS:
        table = model.__table__
        assert table.schema == expected_schema
        assert tuple(table.columns.keys()) == V3_EXPECTED_COLUMNS_BY_TABLE[table.name]


def _assert_layout_types():
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


def _assert_block_provider_types():
    block = PTG2V3Block.__table__
    assert isinstance(block.c.block_hash.type, sa.LargeBinary)
    assert isinstance(block.c.payload.type, sa.LargeBinary)
    assert isinstance(block.c.entry_count.type, sa.BigInteger)
    assert isinstance(block.c.raw_byte_count.type, sa.BigInteger)
    assert isinstance(block.c.stored_byte_count.type, sa.BigInteger)
    provider_set = PTG2V3ProviderSet.__table__
    assert isinstance(provider_set.c.provider_set_global_id_128.type, sa.LargeBinary)
    assert isinstance(provider_set.c.network_names.type, sa.ARRAY)
    code = PTG2V3Code.__table__
    assert isinstance(code.c.code_global_id_128.type, sa.LargeBinary)
    assert isinstance(code.c.coverage_scope_id.type, sa.LargeBinary)
    snapshot_scope = PTG2V3SnapshotScope.__table__
    assert isinstance(snapshot_scope.c.coverage_scope_id.type, sa.LargeBinary)
    assert snapshot_scope.c.plan_market_type.server_default.arg.text == "''"
    snapshot_source = PTG2V3SnapshotSource.__table__
    assert isinstance(snapshot_source.c.source_key.type, sa.Integer)
    assert isinstance(snapshot_source.c.logical_hash_deferred.type, sa.Boolean)
    provider_group = PTG2V3ProviderGroup.__table__
    assert isinstance(
        provider_group.c.provider_group_global_id_128.type,
        sa.LargeBinary,
    )


def _assert_audit_attestation_types():
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


def _assert_v3_timezone_column_types():
    layout = PTG2V3SnapshotLayout.__table__
    fingerprint = PTG2V3LayoutFingerprint.__table__
    snapshot_scope = PTG2V3SnapshotScope.__table__
    block = PTG2V3Block.__table__
    attestation = PTG2V3CandidateAuditAttestation.__table__
    source_witness = PTG2V3SourceAuditWitness.__table__
    source_witness_part = PTG2WitnessPart.__table__
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


def _assert_v3_primary_keys():
    for model, expected_columns in V3_EXPECTED_PRIMARY_KEYS_BY_MODEL.items():
        assert _primary_key(model.__table__) == expected_columns


def _expected_v3_foreign_key_shapes(schema):
    return {
        model: {
            name: (
                local_columns,
                tuple(f"{schema}.{target}" for target in targets),
                ondelete,
            )
            for name, (local_columns, targets, ondelete) in shapes.items()
        }
        for model, shapes in V3_FOREIGN_KEY_SHAPES_BY_MODEL.items()
    }


def _assert_v3_foreign_keys():
    schema = PTG2V3SnapshotLayout.__table__.schema
    for model, expected_shapes in _expected_v3_foreign_key_shapes(schema).items():
        assert _foreign_key_shapes(model.__table__) == expected_shapes
    for model in V3_DENSE_MODELS:
        assert _foreign_key_shapes(model.__table__) == {}


def _assert_v3_unique_constraints():
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

    code_uniques = _constraints(PTG2V3Code.__table__, sa.UniqueConstraint)
    assert "ptg2_v3_code_identity_key" not in code_uniques
    assert tuple(
        column.name
        for column in code_uniques["ptg2_v3_code_global_id_key"].columns
    ) == ("snapshot_key", "code_global_id_128")

    price_unique = _constraints(
        PTG2V3PriceAttr.__table__, sa.UniqueConstraint
    )["ptg2_v3_price_attr_value_key"]
    assert tuple(column.name for column in price_unique.columns) == (
        "snapshot_key",
        "attribute_kind",
        "value",
    )
    assert price_unique.dialect_options["postgresql"]["nulls_not_distinct"] is True


def _assert_v3_partition_intent():
    expected_partition_by_model = {
        PTG2V3Block: "HASH (block_hash)",
        PTG2V3NPIScope: "HASH (snapshot_key)",
        PTG2V3AuditOccurrence: "HASH (snapshot_key)",
    }
    for model, expected_partition in expected_partition_by_model.items():
        assert model.__table__.dialect_options["postgresql"]["partition_by"] == (
            expected_partition
        )


def _assert_v3_index_shapes():
    for model, expected_shapes in V3_INDEX_SHAPES_BY_MODEL.items():
        assert _index_shapes(model.__table__) == expected_shapes


def _assert_v3_sealed_mapping_index():
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


def _assert_v3_check_constraint_names():
    for model, expected_names in V3_CHECK_NAMES_BY_MODEL.items():
        assert set(_constraints(model.__table__, sa.CheckConstraint)) == expected_names


def _assert_v3_migration_metadata(migration):
    assert migration.revision == "20260712120000_ptg2_v3_shared_schema"
    assert migration.down_revision == (
        "20260713237000_provider_directory_plan_scalars"
    )


def _assert_v3_migration_parent_tables(statements, schema):
    for table_name in V3_MIGRATION_PARENT_TABLES:
        prefix = f'CREATE TABLE {schema}."{table_name}" ('
        assert any(statement.startswith(prefix) for statement in statements)


def _assert_v3_migration_statement_order(statements, schema):
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
    trace_set_statement = statements[trace_set_statement_index]
    assert "source_trace_set_hash varchar(64) NOT NULL" in trace_set_statement
    assert "source_trace_hashes varchar[]" in trace_set_statement


def _assert_layout_audit_sql(statements, schema):
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


def _assert_v3_hash_partitions(statements, schema):
    for parent in (
        "ptg2_v3_block",
        "ptg2_v3_npi_scope",
        "ptg2_v3_audit_occurrence",
    ):
        marker = f'PARTITION OF {schema}."{parent}"'
        partitions = [statement for statement in statements if marker in statement]
        assert len(partitions) == 32
        partition_by_remainder = {}
        pattern = re.compile(
            rf'CREATE TABLE {schema}\."{parent}_p(\d{{2}})" '
            rf'PARTITION OF {schema}\."{parent}" FOR VALUES WITH '
            r"\( MODULUS 32, REMAINDER (\d+) \);"
        )
        for statement in partitions:
            match = pattern.fullmatch(statement)
            assert match is not None
            partition_by_remainder[int(match.group(1))] = int(match.group(2))
        assert partition_by_remainder == {
            remainder: remainder for remainder in range(32)
        }


def _assert_v3_required_migration_fragments(statements):
    joined = "\n".join(statements)
    for fragment in V3_MIGRATION_REQUIRED_FRAGMENTS:
        assert fragment in joined


def _assert_v3_required_constraint_names(statements):
    joined = "\n".join(statements)
    for constraint_name in V3_MIGRATION_REQUIRED_CONSTRAINT_NAMES:
        assert f'CONSTRAINT "{constraint_name}"' in joined
