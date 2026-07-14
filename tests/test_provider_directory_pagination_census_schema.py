# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import importlib.util
from pathlib import Path

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


MIGRATION_PATH = (
    Path(__file__).resolve().parents[1]
    / "alembic"
    / "versions"
    / "20260714150000_provider_directory_pagination_census.py"
)


def _load_migration():
    spec = importlib.util.spec_from_file_location(
        "provider_directory_pagination_census_migration",
        MIGRATION_PATH,
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class _OpRecorder:
    def __init__(self):
        self.columns = []
        self.dropped_columns = []

    def add_column(self, table_name, column, **kwargs):
        self.columns.append((table_name, column, kwargs))

    def drop_column(self, table_name, column_name, **kwargs):
        self.dropped_columns.append((table_name, column_name, kwargs))


def test_pagination_census_migration_adds_durable_jsonb_proof(monkeypatch):
    migration = _load_migration()
    recorder = _OpRecorder()
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "mrf_test")
    monkeypatch.setattr(migration, "op", recorder)

    migration.upgrade()

    assert migration.revision == "20260714150000_provider_directory_pagination_census"
    assert migration.down_revision == "20260714140000_mrf_file_source_cursor_index"
    assert len(recorder.columns) == 1
    table_name, column, options = recorder.columns[0]
    assert table_name == "provider_directory_pagination_checkpoint"
    assert column.name == "completeness_json"
    assert isinstance(column.type, postgresql.JSONB)
    assert column.nullable is False
    assert str(column.server_default.arg) == "'{}'::jsonb"
    assert options == {"schema": "mrf_test"}

    migration.downgrade()
    assert recorder.dropped_columns == [
        (
            "provider_directory_pagination_checkpoint",
            "completeness_json",
            {"schema": "mrf_test"},
        )
    ]
