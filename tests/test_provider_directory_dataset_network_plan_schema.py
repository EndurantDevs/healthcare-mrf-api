# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import importlib.util
from pathlib import Path

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql
from sqlalchemy.schema import CreateTable

from db.models import (
    ProviderDirectoryDatasetAffiliationOrganization,
    ProviderDirectoryDatasetNetworkPlan,
    ProviderDirectoryEndpointDataset,
)


MIGRATION_PATH = (
    Path(__file__).resolve().parents[1]
    / "alembic"
    / "versions"
    / "20260713213000_provider_directory_dataset_serving_relations.py"
)


def _load_migration():
    spec = importlib.util.spec_from_file_location(
        "provider_directory_dataset_serving_relations_migration",
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

    def create_table(self, name, *items, schema=None):
        self.tables[name] = {"items": items, "schema": schema}

    def create_index(self, name, table_name, columns, **kwargs):
        self.indexes[name] = {
            "table_name": table_name,
            "columns": columns,
            **kwargs,
        }


def test_dataset_network_plan_model_is_logged_dataset_scoped_and_covering():
    table = ProviderDirectoryDatasetNetworkPlan.__table__
    primary_key_columns = tuple(
        column.name for column in table.primary_key.columns
    )
    dataset_foreign_key = next(iter(table.c.dataset_id.foreign_keys))
    lookup_index = ProviderDirectoryDatasetNetworkPlan.__my_additional_indexes__[0]
    ddl = str(CreateTable(table).compile(dialect=postgresql.dialect()))

    assert table.name == "provider_directory_dataset_network_plan"
    assert primary_key_columns == (
        "dataset_id",
        "network_resource_id",
        "insurance_plan_resource_id",
    )
    assert dataset_foreign_key.target_fullname == (
        f"{ProviderDirectoryEndpointDataset.__table__.schema}."
        "provider_directory_endpoint_dataset.dataset_id"
    )
    assert dataset_foreign_key.ondelete == "CASCADE"
    assert table.c.built_at.nullable is False
    assert lookup_index == {
        "index_elements": ("dataset_id", "network_resource_id"),
        "include": ("insurance_plan_resource_id",),
        "name": "provider_directory_dataset_network_plan_lookup_idx",
    }
    assert "CREATE TABLE" in ddl
    assert "UNLOGGED" not in ddl


def test_dataset_affiliation_organization_model_is_dataset_scoped_and_covering():
    table = ProviderDirectoryDatasetAffiliationOrganization.__table__
    primary_key_columns = tuple(
        column.name for column in table.primary_key.columns
    )
    dataset_foreign_key = next(iter(table.c.dataset_id.foreign_keys))
    lookup_index = (
        ProviderDirectoryDatasetAffiliationOrganization
        .__my_additional_indexes__[0]
    )
    ddl = str(CreateTable(table).compile(dialect=postgresql.dialect()))

    assert primary_key_columns == (
        "dataset_id",
        "participating_organization_resource_id",
        "affiliation_resource_id",
    )
    assert dataset_foreign_key.ondelete == "CASCADE"
    assert table.c.built_at.nullable is False
    assert lookup_index == {
        "index_elements": (
            "dataset_id",
            "participating_organization_resource_id",
        ),
        "include": ("affiliation_resource_id",),
        "name": (
            "provider_directory_dataset_affiliation_organization_lookup_idx"
        ),
    }
    assert "CREATE TABLE" in ddl
    assert "UNLOGGED" not in ddl


def test_dataset_serving_relations_migration_matches_models(monkeypatch):
    migration = _load_migration()
    recorder = _OpRecorder()
    monkeypatch.delenv("HLTHPRT_DB_SCHEMA", raising=False)
    monkeypatch.setattr(migration, "op", recorder)

    migration.upgrade()

    assert migration.revision == (
        "20260713213000_provider_directory_dataset_serving_relations"
    )
    assert migration.down_revision == (
        "20260713210000_provider_directory_profile_fields"
    )
    model_by_table_name = {
        "provider_directory_dataset_network_plan": (
            ProviderDirectoryDatasetNetworkPlan
        ),
        "provider_directory_dataset_affiliation_organization": (
            ProviderDirectoryDatasetAffiliationOrganization
        ),
    }
    assert set(recorder.tables) == set(model_by_table_name)
    for table_name, relation_model in model_by_table_name.items():
        recorded_table = recorder.tables[table_name]
        recorded_columns = [
            schema_item.name for schema_item in recorded_table["items"]
            if isinstance(schema_item, sa.Column)
        ]
        assert recorded_columns == list(relation_model.__table__.columns.keys())
        assert recorded_table["schema"] == "mrf"
        foreign_key = next(
            schema_item for schema_item in recorded_table["items"]
            if isinstance(schema_item, sa.ForeignKeyConstraint)
        )
        assert foreign_key.ondelete == "CASCADE"
    lookup_index = recorder.indexes[
        "provider_directory_dataset_network_plan_lookup_idx"
    ]
    assert lookup_index["table_name"] == (
        "provider_directory_dataset_network_plan"
    )
    assert lookup_index["columns"] == ["dataset_id", "network_resource_id"]
    assert lookup_index["postgresql_include"] == [
        "insurance_plan_resource_id"
    ]
    affiliation_lookup_index = recorder.indexes[
        "provider_directory_dataset_affiliation_organization_lookup_idx"
    ]
    assert affiliation_lookup_index["columns"] == [
        "dataset_id",
        "participating_organization_resource_id",
    ]
    assert affiliation_lookup_index["postgresql_include"] == [
        "affiliation_resource_id"
    ]
