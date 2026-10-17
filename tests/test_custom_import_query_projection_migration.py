# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Projection metadata is additive and preserves immutable historical seals."""

from pathlib import Path

from alembic.config import Config
from alembic.script import ScriptDirectory
from sqlalchemy.dialects import postgresql
from sqlalchemy.schema import CreateTable

from db.models.custom_import import CustomImportGenerationSeal
from db.models.custom_import_query_projection import CustomImportQueryProjection
from tests.custom_import_postgres_support import _migration


def migration():
    return _migration(
        Path(__file__).parents[1] / "alembic" / "versions" / "20261015020000_custom_import_query_projections.py",
        "query_projection_migration_test",
    )


def test_projection_migration_extends_the_single_installed_head():
    """Keep projection metadata in the sole chain beneath registry admission."""
    root = Path(__file__).parents[1]
    config = Config(str(root / "alembic.ini"))
    config.set_main_option("script_location", str(root / "alembic"))
    script = ScriptDirectory.from_config(config)
    module = migration()
    assert module.down_revision == "20261013010000_registry_custom_history_events"
    head = "20261016070000_registry_allowed_plan_scope"
    assert script.get_heads() == [head]
    assert script.get_revision(head).down_revision == module.revision
    assert [step.revision.revision for step in script._upgrade_revs("head", module.down_revision)] == [
        module.revision,
        head,
    ]


def test_migration_creates_only_additive_native_metadata_in_the_selected_schema():
    module = migration()
    table = module._manifest_table("synthetic_control")
    sql = str(CreateTable(table).compile(dialect=postgresql.dialect()))
    assert "CREATE TABLE synthetic_control.custom_import_query_projection" in sql
    assert "REFERENCES synthetic_control.custom_import_generation" in sql
    assert "REFERENCES synthetic_control.custom_import_snapshot_family" in sql
    assert "mrf." not in sql
    source = Path(module.__file__).read_text()
    assert all(term not in source for term in ("CREATE FUNCTION", "CREATE TRIGGER", "UPDATE ", "SET ROLE"))
    assert not CustomImportQueryProjection.__runtime_schema_sync__


def test_historical_nullable_seal_fields_require_complete_projection_evidence():
    module = migration()
    seal = CustomImportGenerationSeal.__table__
    assert all(seal.c[name].nullable and seal.c[name].server_default is None for name in module._SEAL_COLUMNS)
    check = next(constraint for constraint in seal.constraints if constraint.name == module._CHECK)
    sql = str(check.sqltext)
    assert all(f"{name} IS NULL" in sql for name in module._SEAL_COLUMNS)
    assert all(f"{name} IS NOT NULL" in sql for name in module._SEAL_COLUMNS[1:])
    assert "query_projection_root_count >= 0" in sql and "query_projection_child_count >= 0" in sql
