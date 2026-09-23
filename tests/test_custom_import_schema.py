# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import importlib.util
from pathlib import Path

import sqlalchemy as sa

from db import maintenance
from db.models import (
    CustomImportCapture,
    CustomImportCaptureBundle,
    CustomImportCaptureParquetPart,
    CustomImportChildCollection,
    CustomImportDataset,
    CustomImportExecution,
    CustomImportField,
    CustomImportGeneration,
    CustomImportLease,
    CustomImportPublicationEvent,
    CustomImportRejection,
    CustomImportRootScalar,
    CustomImportSchemaRevision,
    CustomImportSourceStream,
    CustomImportWinner,
)

ROOT = Path(__file__).resolve().parents[1]
MIGRATION_PATH = ROOT / "alembic" / "versions" / "20260914120000_custom_import_v1_schema.py"
DURABLE_CAPTURE_MIGRATION_PATH = (
    ROOT / "alembic" / "versions" / "20260922000000_custom_import_durable_parquet_capture.py"
)
EXECUTION_REQUEST_IDENTITY_MIGRATION_PATH = (
    ROOT / "alembic" / "versions" / "20260922010000_custom_import_execution_request_identity.py"
)


def _migration():
    spec = importlib.util.spec_from_file_location("custom_import_v1_migration", MIGRATION_PATH)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _durable_capture_migration():
    spec = importlib.util.spec_from_file_location(
        "custom_import_durable_capture_migration",
        DURABLE_CAPTURE_MIGRATION_PATH,
    )
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _execution_request_identity_migration():
    spec = importlib.util.spec_from_file_location(
        "custom_import_execution_request_identity_migration",
        EXECUTION_REQUEST_IDENTITY_MIGRATION_PATH,
    )
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_schema_models_keep_identity_and_xml_record_path_rules_explicit():
    """Keep immutable schema identity and XML record-path rules explicit."""

    assert CustomImportDataset.__runtime_schema_sync__ is False
    assert isinstance(CustomImportSchemaRevision.__table__.c.canonical_schema.type, sa.Text)
    assert CustomImportSchemaRevision.__table__.c.schema_sha256.type.length == 32
    assert CustomImportField.__table__.c.projection_slot.nullable is False
    assert CustomImportSourceStream.__table__.c.record_path.nullable is True
    source_stream_checks_by_name = {
        constraint.name: str(constraint.sqltext)
        for constraint in CustomImportSourceStream.__table__.constraints
        if isinstance(constraint, sa.CheckConstraint)
    }
    assert (
        "decoder = 'xml' AND record_path IS NOT NULL"
        in source_stream_checks_by_name["custom_import_source_stream_record_path_check"]
    )
    assert (
        "decoder <> 'xml' AND record_path IS NULL"
        in source_stream_checks_by_name["custom_import_source_stream_record_path_check"]
    )


def test_schema_models_enforce_child_key_and_scalar_projection_identity():
    """Keep child collection keys and scalar slots tied to one schema revision."""

    child_collection_shape = next(
        constraint
        for constraint in CustomImportChildCollection.__table__.constraints
        if constraint.name == "custom_import_child_collection_shape_check"
    )
    assert "octet_length(key_shape_sha256) = 32" in str(child_collection_shape.sqltext)
    foreign_key_columns_by_name = {
        foreign_key.name: tuple(element.parent.name for element in foreign_key.elements)
        for foreign_key in CustomImportRootScalar.__table__.foreign_key_constraints
    }
    assert foreign_key_columns_by_name["custom_import_root_scalar_field_fkey"][-1] == "projection_slot"


def test_runtime_models_keep_generation_selection_and_lease_shapes_explicit():
    """Keep generations, winners, rejections, leases, and rollback references bounded."""

    rejection_shape = next(
        constraint
        for constraint in CustomImportRejection.__table__.constraints
        if constraint.name == "custom_import_rejection_shape_check"
    )
    assert "root_key_sha256 IS NULL OR octet_length(root_key_sha256) = 32" in str(rejection_shape.sqltext)
    lease_shape = next(
        constraint
        for constraint in CustomImportLease.__table__.constraints
        if constraint.name == "custom_import_lease_shape_check"
    )
    assert "fence > 0 AND token_sha256 IS NOT NULL" in str(lease_shape.sqltext)
    assert "state" not in CustomImportGeneration.__table__.c
    generation_constraint_names = {constraint.name for constraint in CustomImportGeneration.__table__.constraints}
    assert "custom_import_generation_execution_fence_key" in generation_constraint_names
    assert "custom_import_generation_content_key" not in generation_constraint_names
    assert "candidate_sha256" in CustomImportGeneration.__table__.c
    generation_shape = next(
        constraint
        for constraint in CustomImportGeneration.__table__.constraints
        if constraint.name == "custom_import_generation_shape_check"
    )
    assert "base_dataset_id IS NOT NULL" in str(generation_shape.sqltext)
    producing_authority_shape = next(
        constraint
        for constraint in CustomImportGeneration.__table__.constraints
        if constraint.name == "custom_import_generation_producing_authority_check"
    )
    assert "producing_fence IS NULL" in str(producing_authority_shape.sqltext)
    publication_event_shape = next(
        constraint
        for constraint in CustomImportPublicationEvent.__table__.constraints
        if constraint.name == "custom_import_publication_event_shape_check"
    )
    assert "from_generation_id IS NOT NULL" in str(publication_event_shape.sqltext)
    assert "event_kind = 'rolled_back' AND from_generation_id IS NOT NULL" in str(publication_event_shape.sqltext)
    assert tuple(CustomImportWinner.__table__.primary_key.columns.keys()) == (
        "generation_id",
        "profile_slot",
        "entity_binding_id",
        "context_key_sha256",
    )
    request_identity_shape = next(
        constraint
        for constraint in CustomImportExecution.__table__.constraints
        if constraint.name == "custom_import_execution_request_identity_shape_check"
    )
    assert "request_identity_sha256 IS NULL OR octet_length(request_identity_sha256) = 32" in str(
        request_identity_shape.sqltext
    )
    assert CustomImportExecution.__table__.c.request_identity_sha256.type.length == 32
    assert CustomImportExecution.__table__.c.request_identity_sha256.nullable is True


def test_durable_capture_models_bind_only_immutable_bounded_parquet_parts():
    """Keep retained payload metadata and physical part checks in the schema model."""

    payload_shape = next(
        constraint
        for constraint in CustomImportCapture.__table__.constraints
        if constraint.name == "custom_import_capture_payload_shape_check"
    )
    assert "payload_contract IS NULL AND payload_part_count IS NULL" in str(payload_shape.sqltext)
    assert "payload_set_sha256 IS NOT NULL" in str(payload_shape.sqltext)
    assert "custom-import/parquet-parts/v1" in str(payload_shape.sqltext)
    assert CustomImportCapture.__table__.c.payload_contract.type.length == 63
    assert CustomImportCapture.__table__.c.payload_set_sha256.type.length == 32
    snapshot_lookup_index = next(
        index
        for index in CustomImportCaptureBundle.__table__.indexes
        if index.name == "custom_import_capture_bundle_snapshot_digest_idx"
    )
    assert tuple(column.name for column in snapshot_lookup_index.columns) == (
        "dataset_id",
        "definition_revision_id",
        "schema_revision_id",
        "snapshot_token_sha256",
    )

    part_shape = next(
        constraint
        for constraint in CustomImportCaptureParquetPart.__table__.constraints
        if constraint.name == "custom_import_capture_parquet_part_shape_check"
    )
    assert "part_ordinal BETWEEN 1 AND 4096" in str(part_shape.sqltext)
    assert "octet_length(payload) = byte_count" in str(part_shape.sqltext)
    assert "pg_catalog.sha256(payload)" in str(part_shape.sqltext)
    capture_foreign_key = next(
        constraint
        for constraint in CustomImportCaptureParquetPart.__table__.foreign_key_constraints
        if constraint.name == "custom_import_capture_parquet_part_capture_fkey"
    )
    assert capture_foreign_key.ondelete == "RESTRICT"
    assert tuple(CustomImportCaptureParquetPart.__table__.primary_key.columns.keys()) == (
        "capture_bundle_id",
        "stream_slot",
        "part_ordinal",
    )


def test_durable_capture_migration_is_schema_only_and_downgrades_fail_closed(monkeypatch):
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "custom_import_test")
    monkeypatch.delenv("DB_SCHEMA", raising=False)
    migration = _durable_capture_migration()
    upgrade_statements: list[str] = []
    monkeypatch.setattr(migration.op, "execute", upgrade_statements.append)

    migration.upgrade()

    upgrade_sql = "\n".join(" ".join(statement.split()) for statement in upgrade_statements)
    assert migration.down_revision == "20260921000000_provider_quality_result_generation"
    assert "INSERT INTO" not in upgrade_sql
    assert 'UPDATE "custom_import_test"."custom_import_capture"' not in upgrade_sql
    assert "payload_contract VARCHAR(63)" in upgrade_sql
    assert "payload_set_sha256 IS NOT NULL" in upgrade_sql
    assert 'CREATE TABLE "custom_import_test"."custom_import_capture_parquet_part"' in upgrade_sql
    assert "ON DELETE RESTRICT" in upgrade_sql
    assert "pg_catalog.sha256(payload)" in upgrade_sql
    assert "custom_import_capture_parquet_part_parent_invalid" in upgrade_sql
    assert "decoder IS DISTINCT FROM 'parquet'" in upgrade_sql
    assert "compression IS DISTINCT FROM 'none'" in upgrade_sql
    assert "custom_import_capture_parquet_part_already_complete" in upgrade_sql
    assert upgrade_sql.count("CREATE CONSTRAINT TRIGGER") == 2
    assert upgrade_sql.count("DEFERRABLE INITIALLY DEFERRED") == 2
    assert "AFTER INSERT ON" in upgrade_sql
    assert "WHEN (NEW.payload_contract = 'custom-import/parquet-parts/v1')" in upgrade_sql
    assert "WHEN (NEW.part_ordinal = 1)" in upgrade_sql
    assert "AND capture.payload_contract = 'custom-import/parquet-parts/v1'" in upgrade_sql
    assert "pg_catalog.int4send(part.part_ordinal)" in upgrade_sql
    assert "pg_catalog.int8send(part.byte_count)" in upgrade_sql
    assert "custom_import_capture_parquet_payload_set_digest_mismatch" in upgrade_sql
    assert "custom_import_capture_bundle_snapshot_digest_idx" in upgrade_sql
    assert "custom_import_capture_parquet_part_immutable_row_guard" in upgrade_sql
    assert "custom_import_capture_parquet_bundle_limit_exceeded" in upgrade_sql

    downgrade_statements: list[str] = []
    monkeypatch.setattr(migration.op, "execute", downgrade_statements.append)
    migration.downgrade()

    downgrade_sql = "\n".join(" ".join(statement.split()) for statement in downgrade_statements)
    assert downgrade_statements[0].startswith("LOCK TABLE")
    assert "custom_import_capture_parquet_downgrade_blocked" in downgrade_sql
    assert (
        'DROP INDEX IF EXISTS "custom_import_test"."custom_import_capture_bundle_snapshot_digest_idx"' in downgrade_sql
    )
    assert 'DROP TABLE IF EXISTS "custom_import_test"."custom_import_capture_parquet_part"' in downgrade_sql
    assert "DROP COLUMN IF EXISTS payload_contract" in downgrade_sql


def test_execution_request_identity_migration_is_schema_only_and_downgrades_fail_closed(monkeypatch):
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "custom_import_test")
    monkeypatch.delenv("DB_SCHEMA", raising=False)
    migration = _execution_request_identity_migration()
    upgrade_statements: list[str] = []
    monkeypatch.setattr(migration.op, "execute", upgrade_statements.append)

    migration.upgrade()

    upgrade_sql = "\n".join(" ".join(statement.split()) for statement in upgrade_statements)
    assert migration.revision == "20260922010000_custom_import_execution_request_identity"
    assert migration.down_revision == "20260923021000_ms_drg_result_generation"
    assert 'ALTER TABLE "custom_import_test"."custom_import_execution"' in upgrade_sql
    assert 'ADD COLUMN "request_identity_sha256" BYTEA' in upgrade_sql
    assert 'request_identity_sha256" IS NULL OR octet_length("request_identity_sha256") = 32' in upgrade_sql
    assert "INSERT INTO" not in upgrade_sql
    assert "UPDATE " not in upgrade_sql
    assert "DEFAULT" not in upgrade_sql
    assert "INDEX" not in upgrade_sql

    downgrade_statements: list[str] = []
    monkeypatch.setattr(migration.op, "execute", downgrade_statements.append)
    migration.downgrade()

    downgrade_sql = "\n".join(" ".join(statement.split()) for statement in downgrade_statements)
    assert "LOCK TABLE" in downgrade_sql
    assert 'request_identity_sha256" IS NOT NULL' in downgrade_sql
    assert "custom_import_execution_request_identity_downgrade_blocked" in downgrade_sql
    assert 'DROP CONSTRAINT IF EXISTS "custom_import_execution_request_identity_shape_check"' in downgrade_sql
    assert 'DROP COLUMN IF EXISTS "request_identity_sha256"' in downgrade_sql


def test_migration_is_schema_only_and_installs_content_immutability(monkeypatch):
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "custom_import_test")
    monkeypatch.delenv("DB_SCHEMA", raising=False)
    migration = _migration()
    statements: list[str] = []
    monkeypatch.setattr(migration.op, "execute", statements.append)

    migration.upgrade()

    normalized = "\n".join(" ".join(statement.split()) for statement in statements)
    schema = migration._quote(migration._schema())
    assert migration.down_revision == "20260911100000_hospital_price_tall_notes"
    assert all(f"CREATE TABLE {schema}.{name}" in normalized for name in migration._TABLE_NAMES)
    assert "INSERT INTO" not in normalized
    assert "guard_custom_import_immutable_row" in normalized
    assert "base_generation_id IS NOT NULL AND base_dataset_id IS NOT NULL" in normalized
    assert "event_kind = 'no_change' AND from_generation_id IS NOT NULL" in normalized
    assert "event_kind = 'rolled_back' AND from_generation_id IS NOT NULL" in normalized
    assert normalized.count("BEFORE UPDATE OR DELETE") == len(migration._IMMUTABLE_TABLES)
    assert len(migration._TABLE_DDL) == len(migration._TABLE_NAMES)
    assert all("mrf." in statement for statement in migration._TABLE_DDL)
    index_ddl = "\n".join(migration._INDEX_DDL)
    assert "custom_import_pack_execution_stream_idx" not in index_ddl
    assert "custom_import_generation_family_lookup_idx" not in index_ddl
    assert "custom_import_entity_binding_lookup_idx" not in index_ddl
    assert "custom_import_winner_lookup_idx" in index_ddl
    source_stream_statement = next(
        statement for statement in migration._TABLE_DDL if "custom_import_source_stream" in statement
    )
    assert "record_path VARCHAR(63)" in source_stream_statement
    assert "decoder = 'xml' AND record_path IS NOT NULL" in source_stream_statement
    assert "decoder <> 'xml' AND record_path IS NULL" in source_stream_statement
    assert "octet_length(key_shape_sha256) = 32" in normalized
    assert "root_key_sha256 IS NULL OR octet_length(root_key_sha256) = 32" in normalized
    assert "fence > 0 AND token_sha256 IS NOT NULL" in normalized
    function_statement = next(statement for statement in statements if "CREATE FUNCTION" in statement)
    revoke_statement = next(statement for statement in statements if "REVOKE ALL ON FUNCTION" in statement)
    assert "REVOKE ALL ON FUNCTION" not in function_statement
    assert "CREATE FUNCTION" not in revoke_statement


def test_migration_downgrade_removes_only_v1_relations_in_reverse_dependency_order(
    monkeypatch,
):
    migration = _migration()
    assert not hasattr(migration, "models")
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "custom_import_test")
    monkeypatch.delenv("DB_SCHEMA", raising=False)
    statements: list[str] = []
    monkeypatch.setattr(migration.op, "execute", statements.append)

    migration.downgrade()

    schema = migration._quote(migration._schema())
    assert statements[0] == (f'DROP TABLE IF EXISTS {schema}."custom_import_winner"')
    assert statements[-2] == (f'DROP TABLE IF EXISTS {schema}."custom_import_dataset"')
    assert statements[-1] == (f"DROP FUNCTION IF EXISTS {schema}.guard_custom_import_immutable_row()")
    assert len(statements) == len(migration._TABLE_CREATION_ORDER) + 1


def test_runtime_sync_skips_migration_owned_table_before_inspection(monkeypatch):
    metadata = sa.MetaData()
    table = sa.Table("custom_import_dataset", metadata, sa.Column("dataset_id", sa.BigInteger))
    calls: list[str] = []

    class Inspector:
        def has_table(self, *_args, **_kwargs):
            calls.append("has_table")
            return False

    monkeypatch.setattr(maintenance.Base, "metadata", metadata)
    monkeypatch.setattr(maintenance, "inspect", lambda _connection: Inspector())
    monkeypatch.setattr(
        maintenance,
        "_model_by_table_fullname",
        lambda: {table.fullname: CustomImportDataset},
    )
    monkeypatch.setattr(maintenance, "_managed_schemas", lambda: ("public",))
    sync_results_by_kind = {
        kind: []
        for kind in (
            "tables",
            "columns",
            "indexes",
            "constraints",
            "skipped_columns",
            "retired",
        )
    }

    maintenance._sync_structure(None, sync_results_by_kind, add_columns=True, add_indexes=True)

    assert calls == []
    assert all(not result_values for result_values in sync_results_by_kind.values())
