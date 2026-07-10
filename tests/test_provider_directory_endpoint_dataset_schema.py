# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import importlib.util
from pathlib import Path

import sqlalchemy as sa

from db.models import (
    ProviderDirectoryAPIEndpoint,
    ProviderDirectoryDatasetResource,
    ProviderDirectoryEndpoint,
    ProviderDirectoryEndpointDataset,
    ProviderDirectorySource,
)


MIGRATION_PATH = (
    Path(__file__).resolve().parents[1]
    / "alembic"
    / "versions"
    / "20260710003000_provider_directory_endpoint_datasets.py"
)


def _primary_key_columns(model):
    return tuple(column.name for column in model.__table__.primary_key.columns)


def _foreign_key(column):
    assert len(column.foreign_keys) == 1
    return next(iter(column.foreign_keys))


def _load_migration():
    spec = importlib.util.spec_from_file_location("provider_directory_endpoint_datasets_migration", MIGRATION_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class _OpRecorder:
    def __init__(self):
        self.tables = {}
        self.indexes = {}
        self.added_columns = []
        self.foreign_keys = []

    def create_table(self, name, *items, schema=None):
        self.tables[name] = {"items": items, "schema": schema}

    def create_index(self, name, table_name, columns, **kwargs):
        self.indexes[name] = {"table": table_name, "columns": columns, **kwargs}

    def add_column(self, table_name, column, **kwargs):
        self.added_columns.append({"table": table_name, "column": column, **kwargs})

    def create_foreign_key(self, name, source, referent, local_columns, remote_columns, **kwargs):
        self.foreign_keys.append(
            {
                "name": name,
                "source": source,
                "referent": referent,
                "local_columns": local_columns,
                "remote_columns": remote_columns,
                **kwargs,
            }
        )


def test_api_endpoint_model_keeps_transport_identity_distinct_from_fhir_endpoint():
    table = ProviderDirectoryAPIEndpoint.__table__

    assert table.name == "provider_directory_api_endpoint"
    assert table.name != ProviderDirectoryEndpoint.__table__.name
    assert _primary_key_columns(ProviderDirectoryAPIEndpoint) == ("endpoint_id",)
    assert all(
        not table.c[name].nullable
        for name in (
            "endpoint_id",
            "canonical_api_base",
            "credential_descriptor_hash",
            "endpoint_signature_hash",
        )
    )

    identity = next(
        constraint
        for constraint in table.constraints
        if isinstance(constraint, sa.UniqueConstraint)
        and constraint.name == "provider_directory_api_endpoint_identity_key"
    )
    assert tuple(column.name for column in identity.columns) == (
        "canonical_api_base",
        "credential_descriptor_hash",
        "endpoint_signature_hash",
    )


def test_dataset_models_preserve_publication_and_resource_identity_contracts():
    endpoint_schema = ProviderDirectoryAPIEndpoint.__table__.schema
    dataset_endpoint_fk = _foreign_key(ProviderDirectoryEndpointDataset.__table__.c.endpoint_id)
    resource_dataset_fk = _foreign_key(ProviderDirectoryDatasetResource.__table__.c.dataset_id)

    assert _primary_key_columns(ProviderDirectoryEndpointDataset) == ("dataset_id",)
    assert dataset_endpoint_fk.target_fullname == (
        f"{endpoint_schema}.provider_directory_api_endpoint.endpoint_id"
    )
    assert _primary_key_columns(ProviderDirectoryDatasetResource) == (
        "dataset_id",
        "resource_type",
        "resource_id",
    )
    assert resource_dataset_fk.target_fullname == (
        f"{endpoint_schema}.provider_directory_endpoint_dataset.dataset_id"
    )
    assert ProviderDirectoryDatasetResource.__table__.c.payload_hash.nullable is False
    assert ProviderDirectoryDatasetResource.__table__.c.payload_json.nullable is False

    current_index = next(
        index
        for index in ProviderDirectoryEndpointDataset.__my_additional_indexes__
        if index["name"] == "provider_directory_endpoint_dataset_current_idx"
    )
    assert current_index == {
        "index_elements": ("endpoint_id",),
        "name": "provider_directory_endpoint_dataset_current_idx",
        "unique": True,
        "where": "is_current = true",
    }


def test_source_endpoint_link_is_nullable_indexed_and_set_null_on_delete():
    endpoint_column = ProviderDirectorySource.__table__.c.endpoint_id
    endpoint_fk = _foreign_key(endpoint_column)

    assert endpoint_column.nullable is True
    assert endpoint_fk.target_fullname == (
        f"{ProviderDirectorySource.__table__.schema}.provider_directory_api_endpoint.endpoint_id"
    )
    assert endpoint_fk.ondelete == "SET NULL"
    assert {
        "index_elements": ("endpoint_id",),
        "name": "provider_directory_source_endpoint_id_idx",
    } in ProviderDirectorySource.__my_additional_indexes__


def test_endpoint_dataset_migration_mirrors_models(monkeypatch):
    migration = _load_migration()
    recorder = _OpRecorder()
    monkeypatch.delenv("HLTHPRT_DB_SCHEMA", raising=False)
    monkeypatch.setattr(migration, "op", recorder)

    migration.upgrade()

    assert migration.revision == "20260710003000_provider_directory_endpoint_datasets"
    assert migration.down_revision == "20260709110000_provider_directory_overlay_coordinates"
    models_by_table = {
        "provider_directory_api_endpoint": ProviderDirectoryAPIEndpoint,
        "provider_directory_endpoint_dataset": ProviderDirectoryEndpointDataset,
        "provider_directory_dataset_resource": ProviderDirectoryDatasetResource,
    }
    assert set(recorder.tables) == set(models_by_table)
    for table_name, model_cls in models_by_table.items():
        columns = [
            table_item.name
            for table_item in recorder.tables[table_name]["items"]
            if isinstance(table_item, sa.Column)
        ]
        revision_model_columns = [
            column_name
            for column_name in model_cls.__table__.columns.keys()
            if not (
                table_name == "provider_directory_endpoint_dataset"
                and column_name == "acquisition_root_run_id"
            )
        ]
        assert columns == revision_model_columns
        assert recorder.tables[table_name]["schema"] == model_cls.__table__.schema

    current_index = recorder.indexes["provider_directory_endpoint_dataset_current_idx"]
    assert current_index["table"] == "provider_directory_endpoint_dataset"
    assert current_index["columns"] == ["endpoint_id"]
    assert current_index["unique"] is True
    assert str(current_index["postgresql_where"]) == "is_current = true"
    assert recorder.added_columns == [
        {
            "table": "provider_directory_source",
            "column": recorder.added_columns[0]["column"],
            "schema": ProviderDirectorySource.__table__.schema,
        }
    ]
    assert recorder.added_columns[0]["column"].name == "endpoint_id"
    assert recorder.added_columns[0]["column"].nullable is True
    assert recorder.foreign_keys == [
        {
            "name": "provider_directory_source_endpoint_id_fkey",
            "source": "provider_directory_source",
            "referent": "provider_directory_api_endpoint",
            "local_columns": ["endpoint_id"],
            "remote_columns": ["endpoint_id"],
            "source_schema": ProviderDirectorySource.__table__.schema,
            "referent_schema": ProviderDirectoryAPIEndpoint.__table__.schema,
            "ondelete": "SET NULL",
        }
    ]
