from __future__ import annotations

import importlib.util
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
MIGRATION_PATH = (
    REPO_ROOT
    / "alembic"
    / "versions"
    / "20260720130000_uhc_retained_artifact_admission.py"
)


def _load_migration():
    spec = importlib.util.spec_from_file_location("uhc_retained_migration", MIGRATION_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_admission_schema_normalizes_raw_layout_ranges_bindings_and_references(
    monkeypatch,
):
    migration = _load_migration()
    created_tables = []
    created_indexes = []
    monkeypatch.setattr(
        migration.op,
        "create_table",
        lambda name, *items, **kwargs: created_tables.append(
            (name, items, kwargs)
        ),
    )
    monkeypatch.setattr(
        migration,
        "create_index_if_missing",
        lambda *args, **kwargs: created_indexes.append((args, kwargs)),
    )

    migration.upgrade()

    assert [table[0] for table in created_tables] == [
        "provider_directory_uhc_raw_artifact",
        "provider_directory_uhc_raw_layout",
        "provider_directory_uhc_source_binding",
        "provider_directory_uhc_raw_range",
        "provider_directory_uhc_artifact_reference",
    ]
    item_by_table = {name: items for name, items, _kwargs in created_tables}
    raw_columns = {item.name for item in item_by_table["provider_directory_uhc_raw_artifact"]}
    assert "contract_version" not in raw_columns
    assert "range_set_sha256" not in raw_columns
    layout_columns = {
        item.name for item in item_by_table["provider_directory_uhc_raw_layout"]
    }
    assert {
        "contract_version",
        "range_count",
        "range_set_sha256",
        "manifest_sha256",
        "producer_build_id",
    }.issubset(layout_columns)
    range_columns = {
        item.name for item in item_by_table["provider_directory_uhc_raw_range"]
    }
    assert {
        "raw_byte_start",
        "raw_byte_end",
        "raw_sha256",
        "canonical_sha256",
    }.issubset(range_columns)
    assert "storage_uri" not in range_columns
    binding_columns = {
        item.name
        for item in item_by_table["provider_directory_uhc_source_binding"]
    }
    assert "contract_version" not in binding_columns
    assert "range_count" not in binding_columns
    assert len(created_indexes) == 1


def test_schema_primary_keys_allow_shared_raw_and_multiple_layouts():
    source = MIGRATION_PATH.read_text(encoding="utf-8")

    assert '"provider_directory_uhc_raw_artifact_pkey"' in source
    assert (
        '"artifact_sha256",\n            "contract_version",\n'
        '            "range_count",\n'
        '            name="provider_directory_uhc_raw_layout_pkey"'
    ) in source
    assert (
        '"artifact_sha256",\n            "contract_version",\n'
        '            "range_count",\n            "range_ordinal",'
    ) in source
    assert "provider_directory_uhc_raw_range_layout_fkey" in source
    assert "provider_directory_uhc_artifact_reference_layout_fkey" in source
    assert "provider_directory_uhc_raw_layout_manifest_key" in source
    assert "artifact_kind = 'raw' AND layout_artifact_sha256 IS NULL" in source
    assert "AND contract_version = 0" in source
    assert "artifact_kind = 'manifest'" in source
    assert "AND layout_artifact_sha256 IS NOT NULL" in source
    assert "artifact_kind = 'shard'" not in source


def test_downgrade_drops_dependents_before_raw_tables(monkeypatch):
    migration = _load_migration()
    dropped = []
    monkeypatch.setattr(
        migration.op,
        "drop_table",
        lambda table_name, **_kwargs: dropped.append(table_name),
    )

    migration.downgrade()

    assert dropped == [
        "provider_directory_uhc_artifact_reference",
        "provider_directory_uhc_raw_range",
        "provider_directory_uhc_source_binding",
        "provider_directory_uhc_raw_layout",
        "provider_directory_uhc_raw_artifact",
    ]


def test_production_surface_has_no_python_physical_shard_fallback():
    process_root = REPO_ROOT / "process"

    assert not (process_root / "uhc_retained_artifacts.py").exists()
    assert not (process_root / "uhc_retained_framing.py").exists()
    assert not (process_root / "uhc_retained_manifest.py").exists()
    registry_source = (process_root / "uhc_retained_source_registry.py").read_text(
        encoding="utf-8"
    )
    native_source = (process_root / "uhc_retained_native.py").read_text(
        encoding="utf-8"
    )
    assert "prepare_raw_shards" not in registry_source
    assert "verify_raw_canonical_derivation" not in registry_source
    assert "retain_source_native" in registry_source
    assert "Python fallback" not in native_source
