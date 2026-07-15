# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import importlib.util
from pathlib import Path

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql
from sqlalchemy.schema import CreateTable

from db.models import MRFDiscoveryBatch, MRFDiscoverySourceCheckpoint


MIGRATION_PATH = (
    Path(__file__).resolve().parents[1]
    / "alembic"
    / "versions"
    / "20260715160000_mrf_discovery_source_checkpoints.py"
)


def _load_migration():
    migration_spec = importlib.util.spec_from_file_location(
        "mrf_discovery_source_checkpoint_migration",
        MIGRATION_PATH,
    )
    assert migration_spec is not None and migration_spec.loader is not None
    migration = importlib.util.module_from_spec(migration_spec)
    migration_spec.loader.exec_module(migration)
    return migration


class _OpRecorder:
    def __init__(self) -> None:
        self.tables: dict[str, dict[str, object]] = {}
        self.indexes: dict[str, dict[str, object]] = {}
        self.executed_statements: list[object] = []
        self.dropped_indexes: list[str] = []
        self.dropped_tables: list[str] = []

    def create_table(self, name, *items, schema=None):
        self.tables[name] = {"items": items, "schema": schema}

    def create_index(self, name, table_name, columns, **kwargs):
        self.indexes[name] = {
            "table": table_name,
            "columns": columns,
            **kwargs,
        }

    def execute(self, statement):
        self.executed_statements.append(statement)

    def drop_index(self, name, **_kwargs):
        self.dropped_indexes.append(name)

    def drop_table(self, name, **_kwargs):
        self.dropped_tables.append(name)


def _record_migration(monkeypatch):
    migration = _load_migration()
    recorder = _OpRecorder()
    monkeypatch.delenv("DB_SCHEMA", raising=False)
    monkeypatch.delenv("HLTHPRT_DB_SCHEMA", raising=False)
    monkeypatch.setattr(migration, "op", recorder)
    migration.upgrade()
    return migration, recorder


def _recorded_columns(table_record: dict[str, object]) -> dict[str, sa.Column]:
    return {
        table_item.name: table_item
        for table_item in table_record["items"]
        if isinstance(table_item, sa.Column)
    }


def test_checkpoint_models_have_root_scoped_identity_and_owner_fences():
    batch_table = MRFDiscoveryBatch.__table__
    source_table = MRFDiscoverySourceCheckpoint.__table__

    assert tuple(batch_table.primary_key.columns.keys()) == ("root_run_id",)
    assert tuple(source_table.primary_key.columns.keys()) == (
        "root_run_id",
        "source_id",
    )
    assert source_table.c.owner_run_id.nullable is False
    assert source_table.c.source_payload.nullable is False
    assert source_table.c.source_payload_sha256.nullable is False
    assert source_table.c.lease_expires_at.nullable is True
    source_foreign_keys = tuple(source_table.c.root_run_id.foreign_keys)
    assert len(source_foreign_keys) == 1
    assert source_foreign_keys[0].target_fullname == (
        f"{batch_table.schema}.{batch_table.name}.root_run_id"
    )
    assert source_foreign_keys[0].ondelete == "CASCADE"


def test_checkpoint_models_compile_required_postgres_contract():
    batch_ddl = str(
        CreateTable(MRFDiscoveryBatch.__table__).compile(
            dialect=postgresql.dialect()
        )
    )
    source_ddl = str(
        CreateTable(MRFDiscoverySourceCheckpoint.__table__).compile(
            dialect=postgresql.dialect()
        )
    )

    assert "source_set_sha256 VARCHAR(64) NOT NULL" in batch_ddl
    assert "source_payload_set_sha256 VARCHAR(64) NOT NULL" in batch_ddl
    assert "lease_expires_at TIMESTAMP WITHOUT TIME ZONE" in batch_ddl
    assert "source_payload JSON NOT NULL" in source_ddl
    assert "PRIMARY KEY (root_run_id, source_id)" in source_ddl


def test_checkpoint_migration_matches_models_and_retry_index(monkeypatch):
    migration, recorder = _record_migration(monkeypatch)

    assert migration.revision == "20260715160000_mrf_discovery_source_checkpoints"
    assert migration.down_revision == "20260715120000_ptg2_v3_source_audit_witness"
    models_by_table = {
        MRFDiscoveryBatch.__tablename__: MRFDiscoveryBatch,
        MRFDiscoverySourceCheckpoint.__tablename__: MRFDiscoverySourceCheckpoint,
    }
    assert set(recorder.tables) == set(models_by_table)
    for table_name, model_class in models_by_table.items():
        recorded_columns = _recorded_columns(recorder.tables[table_name])
        assert tuple(recorded_columns) == tuple(model_class.__table__.columns.keys())
        assert recorder.tables[table_name]["schema"] == model_class.__table__.schema
    assert isinstance(
        _recorded_columns(
            recorder.tables[MRFDiscoverySourceCheckpoint.__tablename__]
        )["source_payload"].type,
        postgresql.JSONB,
    )
    retry_index = recorder.indexes[migration.RETRY_INDEX_NAME]
    assert retry_index["table"] == "import_run"
    assert retry_index["columns"] == ["retry_of_run_id"]
    assert retry_index["unique"] is True
    assert str(retry_index["postgresql_where"]) == (
        "importer = 'mrf-source-discovery' AND retry_of_run_id IS NOT NULL"
    )
    assert any(
        "mrf_discovery_duplicate_retry_children" in str(statement)
        for statement in recorder.executed_statements
    )

    migration.downgrade()
    assert recorder.dropped_indexes == [migration.RETRY_INDEX_NAME]
    assert recorder.dropped_tables == [
        "mrf_discovery_source_checkpoint",
        "mrf_discovery_batch",
    ]
