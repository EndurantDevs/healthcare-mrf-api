# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Contracts for the bounded endpoint-dataset lifecycle guard."""

from __future__ import annotations

import importlib.util
from pathlib import Path

from db.models.system import ProviderDirectoryEndpointDataset


ROOT = Path(__file__).resolve().parents[1]
MIGRATION_PATH = (
    ROOT
    / "alembic"
    / "versions"
    / "20260807100000_provider_directory_endpoint_dataset_guard.py"
)


def _load_migration():
    module_spec = importlib.util.spec_from_file_location(
        "provider_directory_endpoint_dataset_guard_migration",
        MIGRATION_PATH,
    )
    assert module_spec is not None and module_spec.loader is not None
    migration = importlib.util.module_from_spec(module_spec)
    module_spec.loader.exec_module(migration)
    return migration


def _capture_migration(monkeypatch, action: str):
    migration = _load_migration()
    statements: list[str] = []
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "endpoint_guard_test")
    monkeypatch.delenv("DB_SCHEMA", raising=False)
    monkeypatch.setattr(migration.op, "execute", statements.append)
    getattr(migration, action)()
    return migration, statements, " ".join(
        " ".join(statement.split()) for statement in statements
    )


def test_upgrade_compares_only_declared_immutable_columns(monkeypatch):
    migration, statements, normalized_sql = _capture_migration(
        monkeypatch,
        "upgrade",
    )

    assert migration.revision == (
        "20260807100000_provider_directory_endpoint_dataset_guard"
    )
    assert migration.down_revision == "20260806110000_ptg_import_wave_contract"
    assert len(statements) == 3
    assert "CREATE OR REPLACE FUNCTION" in normalized_sql
    assert "SECURITY DEFINER SET search_path = pg_catalog" in normalized_sql
    assert "to_jsonb(NEW)" not in normalized_sql
    assert "to_jsonb(OLD)" not in normalized_sql
    assert "CREATE TRIGGER" not in normalized_sql
    assert "ENABLE ALWAYS TRIGGER" not in normalized_sql
    assert "ROW( NEW.dataset_id, NEW.endpoint_id" in normalized_sql
    assert "OLD.created_at, OLD.validated_at )" in normalized_sql
    assert "IS DISTINCT FROM ROW(" in normalized_sql
    assert "tin_npi_connector_endpoint_dataset_transition_invalid" in (
        normalized_sql
    )
    assert (
        'REVOKE ALL ON FUNCTION "endpoint_guard_test".'
        '"guard_tin_npi_connector_endpoint_dataset"() FROM PUBLIC'
    ) in normalized_sql


def test_guard_column_contract_matches_the_endpoint_dataset_model(monkeypatch):
    migration, _statements, normalized_sql = _capture_migration(
        monkeypatch,
        "upgrade",
    )
    model_columns = {
        column.name for column in ProviderDirectoryEndpointDataset.__table__.columns
    } - {
        "artifact_selection_receipt_json",
        "completion_proof_required_version",
        "completion_proof_json",
        "completion_proof_sha256",
    }

    assert model_columns == set(migration.ENDPOINT_DATASET_MUTABLE_COLUMNS) | set(
        migration.ENDPOINT_DATASET_IMMUTABLE_COLUMNS
    )
    assert set(migration.ENDPOINT_DATASET_MUTABLE_COLUMNS).isdisjoint(
        migration.ENDPOINT_DATASET_IMMUTABLE_COLUMNS
    )
    assert set(migration.ENDPOINT_DATASET_FORWARD_COMPATIBLE_COLUMNS) == {
        "completion_proof_required_version",
        "completion_proof_json",
        "completion_proof_sha256",
    }
    assert set(migration.ENDPOINT_DATASET_RECEIPT_COMPATIBLE_COLUMNS) == {
        *migration.ENDPOINT_DATASET_FORWARD_COMPATIBLE_COLUMNS,
        "artifact_selection_receipt_json",
    }
    assert normalized_sql.count("observed_columns IS DISTINCT FROM") == 3
    assert "provider_directory_endpoint_dataset_guard_schema_changed" in (
        normalized_sql
    )


def test_schema_fence_literal_quotes_valid_schema_identifiers():
    migration = _load_migration()

    schema_fence = migration._endpoint_dataset_schema_fence_sql("guard's")

    assert "'\"guard''s\".\"provider_directory_endpoint_dataset\"'" in (
        schema_fence
    )


def test_downgrade_restores_the_previous_comparison(monkeypatch):
    _migration, statements, normalized_sql = _capture_migration(
        monkeypatch,
        "downgrade",
    )

    assert len(statements) == 2
    assert "CREATE OR REPLACE FUNCTION" in normalized_sql
    assert "to_jsonb(NEW)" in normalized_sql
    assert "to_jsonb(OLD)" in normalized_sql
    assert "publication_metadata_json" in normalized_sql
    assert "REVOKE ALL ON FUNCTION" in normalized_sql
