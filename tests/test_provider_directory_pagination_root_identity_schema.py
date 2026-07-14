# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import importlib.util
from pathlib import Path

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


MIGRATION_PATH = (
    Path(__file__).resolve().parents[1]
    / "alembic"
    / "versions"
    / "20260710143000_provider_directory_pagination_root_identity.py"
)


def _load_migration():
    spec = importlib.util.spec_from_file_location(
        "provider_directory_pagination_root_identity_migration",
        MIGRATION_PATH,
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class _OpRecorder:
    def __init__(self):
        self.executed = []
        self.altered_columns = []
        self.dropped_constraints = []
        self.primary_keys = []
        self.created_indexes = []
        self.dropped_indexes = []

    def execute(self, statement):
        self.executed.append(statement)

    def alter_column(self, table_name, column_name, **kwargs):
        self.altered_columns.append((table_name, column_name, kwargs))

    def drop_constraint(self, name, table_name, **kwargs):
        self.dropped_constraints.append((name, table_name, kwargs))

    def create_primary_key(self, name, table_name, columns, **kwargs):
        self.primary_keys.append((name, table_name, columns, kwargs))

    def create_index(self, name, table_name, columns, **kwargs):
        self.created_indexes.append((name, table_name, columns, kwargs))

    def drop_index(self, name, **kwargs):
        self.dropped_indexes.append((name, kwargs))


def _assert_upgrade_backfill_sql(recorder):
    checkpoint_to_dataset_sql = str(
        recorder.executed[0].compile(dialect=postgresql.dialect())
    )
    legacy_dataset_sql = str(
        recorder.executed[1].compile(dialect=postgresql.dialect())
    )
    checkpoint_update_sql = str(
        recorder.executed[2].compile(dialect=postgresql.dialect())
    )
    root_assertion_sql = str(recorder.executed[3])
    assert "min(" in checkpoint_to_dataset_sql
    assert "provider_directory_pagination_checkpoint" in checkpoint_to_dataset_sql
    assert "provider_directory_endpoint_dataset.import_run_id" in legacy_dataset_sql
    assert "retry_of_run_id IS NOT NULL" in legacy_dataset_sql
    assert "provider_directory_pagination_checkpoint" in checkpoint_update_sql
    assert "provider_directory_endpoint_dataset.dataset_id" in checkpoint_update_sql
    assert "provider_directory_endpoint_dataset.acquisition_root_run_id" in (
        checkpoint_update_sql
    )
    assert "acquisition_root_run_id IS NULL" in checkpoint_update_sql
    assert "root_backfill_failed" in root_assertion_sql
    assert "IS DISTINCT FROM" in root_assertion_sql
    assert "checkpoint.dataset_id IS NOT NULL" in root_assertion_sql


def _assert_root_column_is_required(recorder):
    assert len(recorder.altered_columns) == 1
    altered_table, altered_column, altered_options = recorder.altered_columns[0]
    assert altered_table == "provider_directory_pagination_checkpoint"
    assert altered_column == "acquisition_root_run_id"
    assert isinstance(altered_options["existing_type"], sa.String)
    assert altered_options["existing_type"].length == 64
    assert altered_options["nullable"] is False
    assert altered_options["schema"] == "mrf"


def _assert_root_key_and_index(recorder):
    assert recorder.dropped_constraints == [
        (
            "provider_directory_pagination_checkpoint_pkey",
            "provider_directory_pagination_checkpoint",
            {"type_": "primary", "schema": "mrf"},
        )
    ]
    assert recorder.primary_keys == [
        (
            "provider_directory_pagination_checkpoint_pkey",
            "provider_directory_pagination_checkpoint",
            [
                "canonical_api_base",
                "resource_type",
                "source_scope_hash",
                "acquisition_root_run_id",
            ],
            {"schema": "mrf"},
        )
    ]
    assert recorder.created_indexes == [
        (
            "provider_directory_pagination_checkpoint_root_updated_idx",
            "provider_directory_pagination_checkpoint",
            ["acquisition_root_run_id", "updated_at"],
            {"schema": "mrf"},
        )
    ]


def test_pagination_root_identity_upgrade_backfills_and_rekeys(monkeypatch):
    """The forward migration preserves lineage and changes checkpoint identity."""
    migration = _load_migration()
    recorder = _OpRecorder()
    monkeypatch.delenv("HLTHPRT_DB_SCHEMA", raising=False)
    monkeypatch.setattr(migration, "op", recorder)

    migration.upgrade()

    assert migration.revision == (
        "20260710143000_provider_directory_pagination_root_identity"
    )
    assert migration.down_revision == (
        "20260710110000_provider_directory_bulk_checkpoints"
    )
    assert len(recorder.executed) == 4
    _assert_upgrade_backfill_sql(recorder)
    _assert_root_column_is_required(recorder)
    _assert_root_key_and_index(recorder)


def test_pagination_root_identity_adopts_current_key_without_rekey(monkeypatch):
    migration = _load_migration()
    recorder = _OpRecorder()
    monkeypatch.setattr(migration, "op", recorder)
    monkeypatch.setattr(migration, "column_is_nullable", lambda *_args, **_kwargs: False)
    monkeypatch.setattr(
        migration,
        "primary_key_columns",
        lambda *_args, **_kwargs: migration.ROOT_PRIMARY_KEY_COLUMNS,
    )
    monkeypatch.setattr(
        migration,
        "create_index_if_missing",
        lambda *_args, **_kwargs: None,
    )

    migration.upgrade()

    assert recorder.altered_columns == []
    assert recorder.dropped_constraints == []
    assert recorder.primary_keys == []


def test_pagination_root_identity_downgrade_restores_original_key(monkeypatch):
    migration = _load_migration()
    recorder = _OpRecorder()
    monkeypatch.setattr(migration, "op", recorder)

    migration.downgrade()

    assert len(recorder.executed) == 1
    assert "root_downgrade_requires_archive" in str(recorder.executed[0])
    assert recorder.dropped_indexes == [
        (
            "provider_directory_pagination_checkpoint_root_updated_idx",
            {
                "table_name": "provider_directory_pagination_checkpoint",
                "schema": "mrf",
            },
        )
    ]
    assert recorder.dropped_constraints[0][0] == (
        "provider_directory_pagination_checkpoint_pkey"
    )
    assert recorder.primary_keys == [
        (
            "provider_directory_pagination_checkpoint_pkey",
            "provider_directory_pagination_checkpoint",
            ["canonical_api_base", "resource_type", "source_scope_hash"],
            {"schema": "mrf"},
        )
    ]
