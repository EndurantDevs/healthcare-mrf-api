# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Capacity-preflight receipt migration head and closed-schema tests."""

from __future__ import annotations

import importlib.util
from pathlib import Path

import sqlalchemy as sa
from alembic.config import Config
from alembic.script import ScriptDirectory

from db.models.system import ProviderDirectoryProfileCapacityPreflightReceipt
from tests.provider_directory_profile_capacity_v2_migration_support import (
    load_capacity_v2_migration,
)
from tests.test_provider_directory_profile_capacity_attestation_schema import (
    _OperationsRecorder,
)


PREFLIGHT_MIGRATION_PATH = (
    Path(__file__).resolve().parents[1]
    / "alembic/versions/20260811010000_provider_directory_profile_capacity_preflight_receipt.py"
)


def _load_preflight_migration():
    module_spec = importlib.util.spec_from_file_location(
        "provider_directory_profile_capacity_preflight_schema_test",
        PREFLIGHT_MIGRATION_PATH,
    )
    assert module_spec is not None and module_spec.loader is not None
    migration = importlib.util.module_from_spec(module_spec)
    module_spec.loader.exec_module(migration)
    return migration


def _assert_receipt_columns_match_model(elements) -> None:
    """Require ORM column storage semantics to match the migration exactly."""
    migration_columns_by_name = {
        element.name: element for element in elements if isinstance(element, sa.Column)
    }
    model_columns_by_name = {
        column.name: column
        for column in ProviderDirectoryProfileCapacityPreflightReceipt.__table__.columns
    }
    assert set(migration_columns_by_name) == set(model_columns_by_name)
    for column_name, migration_column in migration_columns_by_name.items():
        model_column = model_columns_by_name[column_name]
        assert type(migration_column.type) is type(model_column.type)
        assert migration_column.nullable is model_column.nullable
        assert getattr(migration_column.type, "length", None) == getattr(
            model_column.type, "length", None
        )
        assert getattr(migration_column.type, "timezone", None) == getattr(
            model_column.type, "timezone", None
        )
        migration_default = migration_column.server_default
        model_default = model_column.server_default
        assert (migration_default is None) is (model_default is None)
        if migration_default is not None:
            assert str(migration_default.arg) == str(model_default.arg)


def test_capacity_v2_migration_precedes_the_unique_repository_head():
    script = ScriptDirectory.from_config(Config("alembic.ini"))
    assert script.get_heads() == [
        "20260812020000_provider_directory_endpoint_dataset_admission_seal"
    ]
    migration = load_capacity_v2_migration()
    assert migration.down_revision == "20260801010000_uhc_semantic_layout_identity"


def test_capacity_preflight_migration_guardedly_admits_only_lease_v3(monkeypatch):
    migration = _load_preflight_migration()
    recorder = _OperationsRecorder()
    monkeypatch.setattr(migration, "op", recorder)
    migration.upgrade()
    statements = "\n".join(recorder.statements)
    assert migration.down_revision == (
        "20260810130000_provider_directory_reviewed_subset_terminal_window"
    )
    for contract_id in (
        "provider-directory-database-capacity-lease-v1",
        "provider-directory-database-capacity-lease-v2",
        "provider-directory-database-capacity-lease-v3",
    ):
        assert contract_id in statements
    assert statements.count("ADD CONSTRAINT") == 3
    assert statements.count("NOT VALID") == 3
    assert statements.count("VALIDATE CONSTRAINT") == 1
    assert "provider_directory_capacity_lease_constraint_drift" in statements
    assert "IN ACCESS EXCLUSIVE MODE NOWAIT" in statements
    assert statements.index("LOCK TABLE") < statements.index("ADD CONSTRAINT")
    assert recorder.created_table[0] == (
        "provider_directory_profile_capacity_preflight_receipt"
    )
    _assert_receipt_columns_match_model(recorder.created_table[1])
