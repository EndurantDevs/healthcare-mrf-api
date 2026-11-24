# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Companion migrations preserve the compiled shared catalog shape and lineage."""

import importlib.util
from pathlib import Path
from unittest.mock import Mock

import pytest
from alembic.script import ScriptDirectory
from sqlalchemy import MetaData, Table
from sqlalchemy.dialects import postgresql
from sqlalchemy.schema import CreateTable

from db.models import CodeRelationship, CodeSynonym

ROOT = Path(__file__).parents[1]
REVISION = "20261115010000_ms_drg_companion_tables"


def migration(revision=REVISION):
    spec = importlib.util.spec_from_file_location(revision, ROOT / "alembic" / "versions" / f"{revision}.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_companion_migration_matches_models_without_runtime_imports(monkeypatch):
    module = migration()
    created_tables = []

    def capture(_operations, name, *elements, schema):
        created_tables.append(Table(name, MetaData(), *elements, schema=schema))

    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "synthetic_catalog")
    monkeypatch.delenv("DB_SCHEMA", raising=False)
    monkeypatch.setattr(module, "create_table_or_validate", capture)
    module.upgrade()
    assert len(created_tables) == 2
    for table, model in zip(created_tables, (CodeSynonym, CodeRelationship)):
        expected = model.__table__.to_metadata(MetaData(), schema="synthetic_catalog")
        assert str(CreateTable(table).compile(dialect=postgresql.dialect())) == str(
            CreateTable(expected).compile(dialect=postgresql.dialect())
        )
        assert not table.foreign_keys
    assert "from db.models" not in Path(module.__file__).read_text()


def test_companion_migration_keeps_prior_head_and_refuses_destructive_downgrade():
    scripts = ScriptDirectory(str(ROOT / "alembic"))
    assert scripts.get_heads() == ["20261104020000_custom_import_family_blocks"]
    assert (
        scripts.get_revision("20261123182000_custom_import_root_boolean_index").down_revision
        == "20261121050000_custom_import_rooted_text"
    )
    assert (
        scripts.get_revision("20261121050000_custom_import_rooted_text").down_revision
        == "20261119230000_clinical_code_catalog_page_index"
    )
    assert (
        scripts.get_revision("20261119230000_clinical_code_catalog_page_index").down_revision
        == "20261119090000_cms_npd_typed_release"
    )
    assert (
        scripts.get_revision("20261119090000_cms_npd_typed_release").down_revision
        == "20261116090000_profile_capacity_logical_names"
    )
    assert (
        scripts.get_revision("20261116090000_profile_capacity_logical_names").down_revision
        == "20261115120000_named_output_recovery_document"
    )
    assert (
        scripts.get_revision("20261115120000_named_output_recovery_document").down_revision
        == "20261115110000_network_retained_read_grant_ledger"
    )
    assert scripts.get_revision("20261114130000_named_preparation_catalog").down_revision == (
        "20261114020000_custom_import_snapshot_metadata"
    )
    assert scripts.get_revision("20261114020000_custom_import_snapshot_metadata").down_revision == (
        "20261114010000_custom_import_snapshot_names"
    )
    assert scripts.get_revision("20261114010000_custom_import_snapshot_names").down_revision == REVISION
    assert scripts.get_revision(REVISION).down_revision == "20261113010000_address_canonical_space_normalization"
    with pytest.raises(RuntimeError, match="retained-data migration"):
        migration().downgrade()


def test_companion_migration_refuses_conflicting_schema_settings_before_sql(monkeypatch):
    module = migration()
    create = Mock()
    monkeypatch.setattr(module, "create_table_or_validate", create)
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "synthetic_catalog")
    monkeypatch.setenv("DB_SCHEMA", "other_catalog")
    with pytest.raises(RuntimeError, match="same schema"):
        module.upgrade()
    create.assert_not_called()
