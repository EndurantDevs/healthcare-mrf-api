# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import importlib.util
from pathlib import Path

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql
from sqlalchemy.schema import CreateTable

from db.models import (
    ProviderDirectoryEndpointDataset,
    ProviderDirectoryPaginationCheckpoint,
)


MIGRATION_PATH = (
    Path(__file__).resolve().parents[1]
    / "alembic"
    / "versions"
    / "20260710010000_provider_directory_pagination_checkpoint.py"
)


def _primary_key_columns(model):
    return tuple(column.name for column in model.__table__.primary_key.columns)


def _load_migration():
    spec = importlib.util.spec_from_file_location(
        "provider_directory_pagination_checkpoint_migration",
        MIGRATION_PATH,
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class _OpRecorder:
    def __init__(self):
        self.tables = {}
        self.indexes = {}
        self.dropped_tables = []

    def create_table(self, name, *items, schema=None):
        self.tables[name] = {"items": items, "schema": schema}

    def create_index(self, name, table_name, columns, **kwargs):
        self.indexes[name] = {"table": table_name, "columns": columns, **kwargs}

    def drop_table(self, name, **kwargs):
        self.dropped_tables.append({"name": name, **kwargs})


def test_pagination_checkpoint_model_has_root_scoped_stream_identity():
    table = ProviderDirectoryPaginationCheckpoint.__table__

    assert table.name == "provider_directory_pagination_checkpoint"
    assert _primary_key_columns(ProviderDirectoryPaginationCheckpoint) == (
        "canonical_api_base",
        "resource_type",
        "source_scope_hash",
        "acquisition_root_run_id",
    )
    assert all(
        not table.c[name].nullable
        for name in _primary_key_columns(ProviderDirectoryPaginationCheckpoint)
    )
    assert table.c.source_ids.nullable is False
    dataset_foreign_keys = tuple(table.c.dataset_id.foreign_keys)
    assert len(dataset_foreign_keys) == 1
    assert dataset_foreign_keys[0].target_fullname == (
        f"{table.schema}.{ProviderDirectoryEndpointDataset.__tablename__}.dataset_id"
    )
    assert table.c.owner_run_id.nullable is False
    assert table.c.retry_of_run_id.nullable is True
    assert table.c.start_url_hash.nullable is False
    assert isinstance(table.c.next_url.type, sa.Text)
    assert table.c.next_url.nullable is True


def test_pagination_checkpoint_model_keeps_only_bounded_resume_metadata():
    table = ProviderDirectoryPaginationCheckpoint.__table__

    assert isinstance(table.c.source_ids.type, sa.JSON)
    assert isinstance(table.c.recent_cursor_hashes.type, sa.JSON)
    assert isinstance(table.c.pages_processed.type, sa.BigInteger)
    assert isinstance(table.c.rows_processed.type, sa.BigInteger)
    assert table.c.pages_processed.nullable is False
    assert table.c.rows_processed.nullable is False
    assert table.c.pages_processed.default.arg == 0
    assert table.c.rows_processed.default.arg == 0
    assert table.c.recent_cursor_hashes.default.is_callable
    assert table.c.recent_cursor_hashes.default.arg(None) == []
    assert tuple(table.c.keys())[-3:] == ("created_at", "updated_at", "completed_at")
    assert {
        "index_elements": ("owner_run_id",),
        "name": "provider_directory_pagination_checkpoint_owner_idx",
    } in ProviderDirectoryPaginationCheckpoint.__my_additional_indexes__
    assert {
        "index_elements": ("state", "updated_at"),
        "name": "provider_directory_pagination_checkpoint_state_updated_idx",
    } in ProviderDirectoryPaginationCheckpoint.__my_additional_indexes__
    assert {
        "index_elements": ("dataset_id",),
        "name": "provider_directory_pagination_checkpoint_dataset_idx",
    } in ProviderDirectoryPaginationCheckpoint.__my_additional_indexes__
    assert {
        "index_elements": ("acquisition_root_run_id", "updated_at"),
        "name": "provider_directory_pagination_checkpoint_root_updated_idx",
    } in ProviderDirectoryPaginationCheckpoint.__my_additional_indexes__


def test_pagination_checkpoint_model_compiles_for_postgresql_without_bounded_url_or_counters():
    ddl = str(
        CreateTable(ProviderDirectoryPaginationCheckpoint.__table__).compile(
            dialect=postgresql.dialect()
        )
    )

    assert "next_url TEXT" in ddl
    assert "pages_processed BIGINT NOT NULL" in ddl
    assert "rows_processed BIGINT NOT NULL" in ddl
    assert (
        "PRIMARY KEY (canonical_api_base, resource_type, source_scope_hash, "
        "acquisition_root_run_id)"
    ) in ddl


def test_pagination_checkpoint_migration_matches_model_and_parent(monkeypatch):
    migration = _load_migration()
    recorder = _OpRecorder()
    monkeypatch.delenv("HLTHPRT_DB_SCHEMA", raising=False)
    monkeypatch.setattr(migration, "op", recorder)

    migration.upgrade()

    assert migration.revision == "20260710010000_provider_directory_pagination_checkpoint"
    assert migration.down_revision == "20260710003000_provider_directory_endpoint_datasets"
    assert set(recorder.tables) == {"provider_directory_pagination_checkpoint"}
    recorded_table = recorder.tables["provider_directory_pagination_checkpoint"]
    assert recorded_table["schema"] == ProviderDirectoryPaginationCheckpoint.__table__.schema
    recorded_columns_by_name = {
        column_or_constraint.name: column_or_constraint
        for column_or_constraint in recorded_table["items"]
        if isinstance(column_or_constraint, sa.Column)
    }
    revision_model_columns = tuple(
        column_name
        for column_name in ProviderDirectoryPaginationCheckpoint.__table__.c.keys()
        if column_name != "acquisition_root_run_id"
    )
    assert tuple(recorded_columns_by_name) == revision_model_columns
    assert isinstance(recorded_columns_by_name["source_ids"].type, postgresql.JSONB)
    assert isinstance(recorded_columns_by_name["recent_cursor_hashes"].type, postgresql.JSONB)
    assert isinstance(recorded_columns_by_name["next_url"].type, sa.Text)
    assert str(recorded_columns_by_name["pages_processed"].server_default.arg) == "0"
    assert str(recorded_columns_by_name["rows_processed"].server_default.arg) == "0"
    assert (
        str(recorded_columns_by_name["recent_cursor_hashes"].server_default.arg)
        == "'[]'::jsonb"
    )
    assert recorder.indexes == {
        "provider_directory_pagination_checkpoint_owner_idx": {
            "table": "provider_directory_pagination_checkpoint",
            "columns": ["owner_run_id"],
            "schema": ProviderDirectoryPaginationCheckpoint.__table__.schema,
        },
        "provider_directory_pagination_checkpoint_state_updated_idx": {
            "table": "provider_directory_pagination_checkpoint",
            "columns": ["state", "updated_at"],
            "schema": ProviderDirectoryPaginationCheckpoint.__table__.schema,
        },
        "provider_directory_pagination_checkpoint_dataset_idx": {
            "table": "provider_directory_pagination_checkpoint",
            "columns": ["dataset_id"],
            "schema": ProviderDirectoryPaginationCheckpoint.__table__.schema,
        },
    }

    migration.downgrade()
    assert recorder.dropped_tables == [
        {
            "name": "provider_directory_pagination_checkpoint",
            "schema": ProviderDirectoryPaginationCheckpoint.__table__.schema,
        }
    ]
