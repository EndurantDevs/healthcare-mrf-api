# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest


MIGRATION_PATH = (
    Path(__file__).resolve().parents[1]
    / "alembic"
    / "versions"
    / "20260717100000_nearby_geo_count_covering_index.py"
)


def _load_migration():
    module_spec = importlib.util.spec_from_file_location(
        "nearby_geo_count_covering_index_migration",
        MIGRATION_PATH,
    )
    assert module_spec is not None and module_spec.loader is not None
    migration_module = importlib.util.module_from_spec(module_spec)
    module_spec.loader.exec_module(migration_module)
    return migration_module


def test_live_index_without_include_is_a_known_upgrade_shape(monkeypatch):
    migration = _load_migration()
    calls = []

    def is_index_shape_matching(*_args, **options):
        include_columns = tuple(options.get("postgresql_include", ()))
        calls.append(include_columns)
        if include_columns == migration.INCLUDE_COLUMNS:
            raise RuntimeError(
                "existing_schema_index_mismatch:fixture.entity_address_unified_idx_geo_bbox"
            )
        return True

    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "fixture")
    monkeypatch.setattr(migration, "has_matching_index", is_index_shape_matching)

    assert (
        migration._index_matches(migration.INDEX_NAME, migration.INCLUDE_COLUMNS)
        is False
    )
    assert calls == [migration.INCLUDE_COLUMNS, ()]


def test_unknown_live_index_shape_is_rejected(monkeypatch):
    migration = _load_migration()

    def is_index_shape_matching(*_args, **_options):
        raise RuntimeError(
            "existing_schema_index_mismatch:fixture.entity_address_unified_idx_geo_bbox"
        )

    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "fixture")
    monkeypatch.setattr(migration, "has_matching_index", is_index_shape_matching)

    with pytest.raises(
        RuntimeError,
        match="existing_schema_index_mismatch:fixture.entity_address_unified_idx_geo_bbox",
    ):
        migration._index_matches(migration.INDEX_NAME, migration.INCLUDE_COLUMNS)


def test_interrupted_private_replacement_is_rebuilt(monkeypatch):
    migration = _load_migration()

    def is_index_shape_matching(*_args, **_options):
        raise RuntimeError(
            "existing_schema_index_invalid:fixture."
            "entity_address_unified_idx_geo_bbox_replacement"
        )

    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "fixture")
    monkeypatch.setattr(
        migration,
        "has_matching_index",
        is_index_shape_matching,
    )

    assert (
        migration._index_matches(
            migration.REPLACEMENT_INDEX_NAME,
            migration.INCLUDE_COLUMNS,
        )
        is False
    )
