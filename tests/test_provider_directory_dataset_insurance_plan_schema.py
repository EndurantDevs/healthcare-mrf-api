# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import importlib.util
from pathlib import Path

import sqlalchemy as sa

from db.models import (
    ProviderDirectoryDatasetInsurancePlan,
    ProviderDirectoryEndpointDataset,
)


MIGRATION_PATH = (
    Path(__file__).resolve().parents[1]
    / "alembic"
    / "versions"
    / "20260713236000_provider_directory_dataset_insurance_plan.py"
)


def _load_migration():
    spec = importlib.util.spec_from_file_location(
        "provider_directory_dataset_insurance_plan_migration",
        MIGRATION_PATH,
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class _OpRecorder:
    def __init__(self):
        self.tables = {}
        self.executed = []

    def create_table(self, name, *items, schema=None):
        self.tables[name] = {"items": items, "schema": schema}

    def get_bind(self):
        return self

    def execute(self, statement):
        self.executed.append(statement)


def test_dataset_insurance_plan_model_is_immutable_and_dataset_scoped():
    table = ProviderDirectoryDatasetInsurancePlan.__table__
    foreign_key = next(iter(table.c.dataset_id.foreign_keys))

    assert tuple(column.name for column in table.primary_key.columns) == (
        "dataset_id",
        "resource_id",
    )
    assert foreign_key.target_fullname == (
        f"{ProviderDirectoryEndpointDataset.__table__.schema}."
        "provider_directory_endpoint_dataset.dataset_id"
    )
    assert foreign_key.ondelete == "CASCADE"
    assert table.c.payload_hash.nullable is False
    assert table.c.payload_json.nullable is False
    assert {
        "acquisition_root_run_id",
        "build_run_id",
        "built_at",
    }.isdisjoint(table.c.keys())


def test_dataset_insurance_plan_migration_creates_and_backfills_projection(
    monkeypatch,
):
    migration = _load_migration()
    operation_recorder = _OpRecorder()
    monkeypatch.delenv("HLTHPRT_DB_SCHEMA", raising=False)
    monkeypatch.setattr(migration, "op", operation_recorder)
    monkeypatch.setattr(migration, "_table_exists", lambda *_args: False)

    migration.upgrade()

    assert migration.revision == (
        "20260713236000_provider_directory_dataset_insurance_plan"
    )
    assert migration.down_revision == (
        "20260713235000_import_run_provider_directory_retry_child"
    )
    recorded_table = operation_recorder.tables[
        "provider_directory_dataset_insurance_plan"
    ]
    recorded_columns = [
        table_item.name
        for table_item in recorded_table["items"]
        if isinstance(table_item, sa.Column)
    ]
    assert recorded_columns == [
        "dataset_id",
        "resource_id",
        "payload_hash",
        "payload_json",
    ]
    foreign_key = next(
        table_item
        for table_item in recorded_table["items"]
        if isinstance(table_item, sa.ForeignKeyConstraint)
    )
    assert foreign_key.ondelete == "CASCADE"
    assert recorded_table["schema"] == "mrf"

    assert len(operation_recorder.executed) == 1
    backfill_sql = str(operation_recorder.executed[0])
    assert "INSERT INTO" in backfill_sql
    assert "provider_directory_dataset_resource" in backfill_sql
    assert "resource_type = 'InsurancePlan'" in backfill_sql
    assert "payload_hash" in backfill_sql
    assert "payload_json" in backfill_sql
    assert "ON CONFLICT (dataset_id, resource_id) DO UPDATE" in backfill_sql


def test_dataset_insurance_plan_migration_backfills_existing_model_table(monkeypatch):
    migration = _load_migration()
    operation_recorder = _OpRecorder()
    monkeypatch.setattr(migration, "op", operation_recorder)
    monkeypatch.setattr(migration, "_table_exists", lambda *_args: True)

    migration.upgrade()

    assert operation_recorder.tables == {}
    assert len(operation_recorder.executed) == 1
    assert "ON CONFLICT" in str(operation_recorder.executed[0])
