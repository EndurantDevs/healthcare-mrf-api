# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import importlib
import importlib.util
from pathlib import Path
from types import SimpleNamespace

import pytest
from sqlalchemy.dialects import postgresql
from sqlalchemy.schema import CreateIndex

from db.maintenance import _iter_index_specs
from db.models import ProviderDirectoryDatasetResource

importer = importlib.import_module("process.provider_directory_fhir")


def test_content_proof_cursor_uses_the_canonical_composite_range():
    query = importer._endpoint_dataset_hash_page_sql(
        True,
        include_payload_json=True,
    )

    assert '(dataset_id, resource_type COLLATE "C", resource_id COLLATE "C") >' in query
    assert ":dataset_id, :after_resource_type, :after_resource_id" in query
    assert "resource_type > :after_resource_type" not in query
    assert "OR (" not in query


def test_first_content_proof_page_has_no_cursor_predicate():
    query = importer._endpoint_dataset_hash_page_sql(False)

    assert ":after_resource_type" not in query
    assert ":after_resource_id" not in query
    assert 'ORDER BY resource_type COLLATE "C", resource_id COLLATE "C"' in query


def test_acquired_pages_use_the_same_canonical_cursor():
    query = importer._subset_acquired_page_sql(True)
    assert '(dataset_id, resource_type COLLATE "C", resource_id COLLATE "C") >' in query
    assert 'ORDER BY resource_type COLLATE "C", resource_id COLLATE "C"' in query
    assert ":dataset_id, :after_resource_type, :after_resource_id" in query
    assert "OR (" not in query


def _cursor_migration():
    path = Path(__file__).parents[1] / "alembic/versions/20261007150000_provider_directory_content_cursor_index.py"
    specification = importlib.util.spec_from_file_location("content_cursor_migration", path)
    migration = importlib.util.module_from_spec(specification)
    specification.loader.exec_module(migration)
    return migration


def test_model_and_online_migration_declare_the_same_index():
    migration = _cursor_migration()
    declared = next(
        index for index in ProviderDirectoryDatasetResource.__table__.indexes if index.name == migration.INDEX_NAME
    )
    ddl = str(CreateIndex(declared).compile(dialect=postgresql.dialect()))
    assert '(dataset_id, resource_type COLLATE "C", resource_id COLLATE "C")' in ddl
    assert "CREATE INDEX CONCURRENTLY IF NOT EXISTS" in migration._create_index_sql("fixture")
    assert migration.INDEX_KEYS in migration._create_index_sql("fixture")
    assert migration.INDEX_NAME not in {
        specification["name"] for specification in _iter_index_specs(ProviderDirectoryDatasetResource)
    }
    assert migration.INDEX_NAME not in {
        index["name"] for index in ProviderDirectoryDatasetResource.__my_additional_indexes__
    }


@pytest.mark.parametrize(
    "valid,ready,live,outcome",
    [
        (True, True, True, "adopt"),
        (False, False, True, "retry"),
        (False, True, True, "retry"),
        (True, False, True, "reject"),
        (False, False, False, "reject"),
        (True, True, False, "reject"),
    ],
)
def test_only_live_invalid_residue_is_retried(monkeypatch, valid, ready, live, outcome):
    migration = _cursor_migration()
    flags_by_name = {"indisvalid": valid, "indisready": ready, "indislive": live}
    bind = SimpleNamespace(execute=lambda *args: SimpleNamespace(first=lambda: ("fixture", migration.TABLE_NAME)))
    monkeypatch.setattr(migration, "op", SimpleNamespace(get_bind=lambda: bind))
    monkeypatch.setattr(migration, "_index_catalog_record", lambda *args: flags_by_name)
    monkeypatch.setattr(migration, "_shape_from_catalog", lambda record: "exact-shape")
    if outcome == "reject":
        with pytest.raises(RuntimeError, match="^provider_directory_content_cursor_index_mismatch$"):
            migration._matching_index_record("fixture", "exact-shape")
    else:
        assert migration._matching_index_record("fixture", "exact-shape") is (
            flags_by_name if outcome == "adopt" else False
        )


def test_partition_parent_ddl_is_metadata_only_and_leaf_names_are_bounded():
    migration = _cursor_migration()
    parent = migration._create_index_sql("fixture", metadata_only=True)
    assert "ON ONLY" in parent
    assert "CONCURRENTLY" not in parent
    name = migration._leaf_index_name("fixture", "partition" * 20)
    assert len(name.encode()) < 63
    assert name == migration._leaf_index_name("fixture", "partition" * 20)
    assert name != migration._leaf_index_name("other", "partition" * 20)
    assert "CONCURRENTLY" in migration._create_index_sql("fixture", "leaf", name)


@pytest.mark.parametrize("action", ["upgrade", "downgrade"])
def test_offline_cursor_migration_requires_native_partition_discovery(monkeypatch, action):
    migration = _cursor_migration()
    monkeypatch.setattr(migration, "op", SimpleNamespace(get_context=lambda: SimpleNamespace(as_sql=True)))
    with pytest.raises(RuntimeError, match="^provider_directory_content_cursor_requires_online_catalog$"):
        getattr(migration, action)()
