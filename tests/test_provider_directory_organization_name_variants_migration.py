# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""DDL contract for lossless Organization primary-name variants."""

from __future__ import annotations

import importlib.util
from pathlib import Path

import sqlalchemy as sa

from db.models import ProviderDirectoryOrganization


MIGRATION_PATH = (
    Path(__file__).resolve().parents[1]
    / "alembic/versions"
    / "20260810030000_provider_directory_organization_name_variants.py"
)


def _load_migration():
    module_spec = importlib.util.spec_from_file_location(
        "provider_directory_organization_name_variants_migration",
        MIGRATION_PATH,
    )
    assert module_spec is not None and module_spec.loader is not None
    migration = importlib.util.module_from_spec(module_spec)
    module_spec.loader.exec_module(migration)
    return migration


class _OperationRecorder:
    def __init__(self) -> None:
        self.added_columns: list[tuple[str, sa.Column, dict[str, object]]] = []
        self.dropped_columns: list[tuple[str, str, dict[str, object]]] = []

    def add_column(self, table_name, column, **options) -> None:
        self.added_columns.append((table_name, column, options))

    def drop_column(self, table_name, column_name, **options) -> None:
        self.dropped_columns.append((table_name, column_name, options))


def test_name_variants_migration_is_additive_and_reversible(monkeypatch) -> None:
    """Add one nullable JSON column after the exact prior repository head."""

    migration = _load_migration()
    recorder = _OperationRecorder()
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "organization_union_test")
    monkeypatch.setattr(migration, "op", recorder)

    migration.upgrade()
    migration.downgrade()

    assert migration.down_revision == (
        "20260810020000_provider_directory_terminal_scope_binding"
    )
    assert len(recorder.added_columns) == 1
    table_name, column, options = recorder.added_columns[0]
    assert table_name == "provider_directory_organization"
    assert column.name == "name_variants"
    assert isinstance(column.type, sa.JSON)
    assert column.nullable is True
    assert options == {"schema": "organization_union_test"}
    assert recorder.dropped_columns == [
        (
            "provider_directory_organization",
            "name_variants",
            {"schema": "organization_union_test"},
        )
    ]


def test_organization_model_exposes_nullable_json_name_variants() -> None:
    """Keep the runtime model aligned with the additive migration."""

    column = ProviderDirectoryOrganization.__table__.c.name_variants
    assert isinstance(column.type, sa.JSON)
    assert column.nullable is True
