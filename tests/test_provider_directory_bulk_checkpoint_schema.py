# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import importlib.util
from pathlib import Path

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from db.models import (
    ProviderDirectoryBulkAcquisitionCheckpoint,
    ProviderDirectoryBulkOutputCheckpoint,
    ProviderDirectoryEndpointDataset,
    ProviderDirectoryPaginationCheckpoint,
)


MIGRATION_PATH = (
    Path(__file__).resolve().parents[1]
    / "alembic"
    / "versions"
    / "20260710110000_provider_directory_bulk_checkpoints.py"
)
NEXT_POLL_MIGRATION_PATH = (
    Path(__file__).resolve().parents[1]
    / "alembic"
    / "versions"
    / "20260713200000_provider_directory_bulk_next_poll.py"
)
OUTPUT_RESUME_MIGRATION_PATH = (
    Path(__file__).resolve().parents[1]
    / "alembic"
    / "versions"
    / "20260720100000_provider_directory_bulk_output_resume.py"
)
OUTPUT_RESUME_COLUMNS = (
    "content_length_bytes",
    "etag_ciphertext",
    "etag_hash",
    "committed_bytes",
    "output_expires_at",
    "validator_checked_at",
    "last_error",
    "last_error_at",
)


def _load_migration():
    spec = importlib.util.spec_from_file_location(
        "provider_directory_bulk_checkpoint_migration",
        MIGRATION_PATH,
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _load_next_poll_migration():
    spec = importlib.util.spec_from_file_location(
        "provider_directory_bulk_next_poll_migration",
        NEXT_POLL_MIGRATION_PATH,
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _load_output_resume_migration():
    spec = importlib.util.spec_from_file_location(
        "provider_directory_bulk_output_resume_migration",
        OUTPUT_RESUME_MIGRATION_PATH,
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class _OpRecorder:
    def __init__(self):
        self.added_columns = []
        self.altered_columns = []
        self.executed_statements = []
        self.indexes = {}
        self.tables = {}

    def add_column(self, table_name, column, **kwargs):
        self.added_columns.append(
            {"table": table_name, "column": column, **kwargs}
        )

    def alter_column(self, table_name, column_name, **kwargs):
        self.altered_columns.append(
            {"table": table_name, "column": column_name, **kwargs}
        )

    def execute(self, statement):
        self.executed_statements.append(statement)

    def create_index(self, name, table_name, columns, **kwargs):
        self.indexes[name] = {
            "table": table_name,
            "columns": columns,
            **kwargs,
        }

    def create_table(self, name, *items, schema=None):
        self.tables[name] = {"items": items, "schema": schema}


def _recorded_columns(recorded_table):
    return {
        table_item.name: table_item
        for table_item in recorded_table["items"]
        if isinstance(table_item, sa.Column)
    }


def _record_bulk_migration(monkeypatch):
    migration = _load_migration()
    recorder = _OpRecorder()
    monkeypatch.delenv("HLTHPRT_DB_SCHEMA", raising=False)
    monkeypatch.setattr(migration, "op", recorder)
    migration.upgrade()
    return migration, recorder


def test_bulk_checkpoint_migration_matches_parent_and_root_columns(monkeypatch):
    migration, recorder = _record_bulk_migration(monkeypatch)

    assert migration.revision == "20260710110000_provider_directory_bulk_checkpoints"
    assert migration.down_revision == (
        "20260710010000_provider_directory_pagination_checkpoint"
    )
    assert [entry["table"] for entry in recorder.added_columns] == [
        "provider_directory_endpoint_dataset",
        "provider_directory_pagination_checkpoint",
    ]
    assert all(
        entry["column"].name == "acquisition_root_run_id"
        for entry in recorder.added_columns
    )
    assert recorder.altered_columns == [
        {
            "table": "provider_directory_pagination_checkpoint",
            "column": "acquisition_root_run_id",
            "existing_type": recorder.altered_columns[0]["existing_type"],
            "nullable": False,
            "schema": ProviderDirectoryPaginationCheckpoint.__table__.schema,
        }
    ]
    assert len(recorder.executed_statements) == 2
    pagination_backfill_sql = str(
        recorder.executed_statements[1].compile(
            dialect=postgresql.dialect(),
        )
    )
    assert "acquisition_root_run_id IS NULL" in pagination_backfill_sql


def test_bulk_checkpoint_migration_matches_models(monkeypatch):
    _migration, recorder = _record_bulk_migration(monkeypatch)
    models_by_table = {
        "provider_directory_bulk_acquisition_checkpoint": (
            ProviderDirectoryBulkAcquisitionCheckpoint
        ),
        "provider_directory_bulk_output_checkpoint": (
            ProviderDirectoryBulkOutputCheckpoint
        ),
    }
    assert set(recorder.tables) == set(models_by_table)
    for table_name, model_cls in models_by_table.items():
        recorded_columns = _recorded_columns(recorder.tables[table_name])
        model_columns = list(model_cls.__table__.columns.keys())
        if table_name == "provider_directory_bulk_acquisition_checkpoint":
            model_columns.remove("next_poll_at")
        elif table_name == "provider_directory_bulk_output_checkpoint":
            for extension_column in OUTPUT_RESUME_COLUMNS:
                model_columns.remove(extension_column)
        assert tuple(recorded_columns) == tuple(model_columns)
        assert recorder.tables[table_name]["schema"] == model_cls.__table__.schema

    acquisition_columns = _recorded_columns(
        recorder.tables["provider_directory_bulk_acquisition_checkpoint"]
    )
    output_columns = _recorded_columns(
        recorder.tables["provider_directory_bulk_output_checkpoint"]
    )
    assert isinstance(acquisition_columns["manifest_json"].type, postgresql.JSONB)
    assert "status_url" not in acquisition_columns
    assert "output_url" not in output_columns
    assert acquisition_columns["status_url_ciphertext"].nullable is True
    assert output_columns["output_url_ciphertext"].nullable is True

    root_index = recorder.indexes[
        "provider_directory_endpoint_dataset_acquisition_root_idx"
    ]
    assert root_index["table"] == ProviderDirectoryEndpointDataset.__tablename__
    assert root_index["columns"] == ["endpoint_id", "acquisition_root_run_id"]
    assert root_index["unique"] is True
    assert str(root_index["postgresql_where"]) == (
        "acquisition_root_run_id IS NOT NULL"
    )


def test_bulk_next_poll_migration_extends_acquisition_checkpoint(monkeypatch):
    migration = _load_next_poll_migration()
    recorder = _OpRecorder()
    monkeypatch.delenv("HLTHPRT_DB_SCHEMA", raising=False)
    monkeypatch.setattr(migration, "op", recorder)

    migration.upgrade()

    assert migration.revision == "20260713200000_provider_directory_bulk_next_poll"
    assert migration.down_revision == (
        "20260713193000_address_canonical_incomplete_line2_unit"
    )
    assert len(recorder.added_columns) == 1
    added_column = recorder.added_columns[0]
    assert added_column["table"] == (
        ProviderDirectoryBulkAcquisitionCheckpoint.__tablename__
    )
    assert added_column["column"].name == "next_poll_at"
    assert added_column["column"].nullable is True
    assert added_column["schema"] == (
        ProviderDirectoryBulkAcquisitionCheckpoint.__table__.schema
    )


def test_bulk_output_resume_migration_extends_output_checkpoint(monkeypatch):
    migration = _load_output_resume_migration()
    added_columns = []
    monkeypatch.delenv("HLTHPRT_DB_SCHEMA", raising=False)

    def record_column(_operations, table_name, column, *, schema):
        added_columns.append(
            {
                "table": table_name,
                "column": column,
                "schema": schema,
            }
        )

    monkeypatch.setattr(migration, "add_column_if_missing", record_column)

    migration.upgrade()

    assert migration.revision == (
        "20260720100000_provider_directory_bulk_output_resume"
    )
    assert migration.down_revision == (
        "20260717180000_ptg2_v4_witness_capacity"
    )
    assert [entry["column"].name for entry in added_columns] == list(
        OUTPUT_RESUME_COLUMNS
    )
    assert all(
        entry["table"] == ProviderDirectoryBulkOutputCheckpoint.__tablename__
        and entry["schema"]
        == ProviderDirectoryBulkOutputCheckpoint.__table__.schema
        for entry in added_columns
    )
    committed_bytes = next(
        entry["column"]
        for entry in added_columns
        if entry["column"].name == "committed_bytes"
    )
    assert committed_bytes.nullable is False
    assert str(committed_bytes.server_default.arg) == "0"
    model_default = (
        ProviderDirectoryBulkOutputCheckpoint.__table__
        .c.committed_bytes.server_default
    )
    assert model_default is not None
    assert str(model_default.arg) == "0"
