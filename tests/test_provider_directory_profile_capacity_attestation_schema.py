# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Migration/model contract for one-time database-capacity leases."""

from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest
import sqlalchemy as sa
from alembic.config import Config
from alembic.script import ScriptDirectory

from db.models.system import (
    ProviderDirectoryProfileBuildCheckpoint,
    ProviderDirectoryProfileCapacityLeaseConsumption,
    ProviderDirectoryProfileDeltaReceipt,
    ProviderDirectoryProfileServingGeneration,
)
from tests.provider_directory_profile_capacity_v2_migration_support import (
    load_capacity_v2_migration as _load_capacity_v2_migration,
)


MIGRATION_PATH = (
    Path(__file__).resolve().parents[1]
    / "alembic/versions/20260730110000_provider_directory_profile_delta.py"
)

class _OperationsRecorder:
    def __init__(self):
        self.created_table = None
        self.created_indexes = []
        self.statements = []
        self.added_columns = []
        self.created_checks = []
        self.dropped_constraints = []

    def create_table(self, table_name, *elements, **options):
        self.created_table = (table_name, elements, options)

    def create_index(self, index_name, table_name, columns, **options):
        self.created_indexes.append(
            (index_name, table_name, tuple(columns), options)
        )

    def execute(self, statement):
        self.statements.append(str(statement))

    def add_column(self, table_name, column, **options):
        self.added_columns.append((table_name, column, options))

    def create_check_constraint(
        self,
        constraint_name,
        table_name,
        condition,
        **options,
    ):
        self.created_checks.append(
            (constraint_name, table_name, str(condition), options)
        )

    def drop_constraint(
        self,
        constraint_name,
        table_name,
        **options,
    ):
        self.dropped_constraints.append(
            (constraint_name, table_name, options)
        )


def _load_migration():
    module_spec = importlib.util.spec_from_file_location(
        "provider_directory_profile_delta_capacity_test",
        MIGRATION_PATH,
    )
    assert module_spec is not None and module_spec.loader is not None
    migration = importlib.util.module_from_spec(module_spec)
    module_spec.loader.exec_module(migration)
    return migration


def test_capacity_v2_migration_precedes_the_unique_repository_head():
    script = ScriptDirectory.from_config(Config("alembic.ini"))
    assert script.get_heads() == [
        "20260801140000_ptg2_legacy_v3_metadata_reconcile"
    ]
    migration = _load_capacity_v2_migration()
    assert migration.down_revision == (
        "20260801010000_uhc_semantic_layout_identity"
    )


def test_capacity_v2_migration_replaces_only_the_guarded_constraint(
    monkeypatch,
):
    migration = _load_capacity_v2_migration()
    recorder = _OperationsRecorder()
    monkeypatch.setattr(migration, "op", recorder)

    migration.upgrade()

    statements = "\n".join(recorder.statements)
    assert "provider-directory-database-capacity-lease-v1" in statements
    assert "provider-directory-database-capacity-lease-v2" in statements
    assert statements.count("ADD CONSTRAINT") == 3
    assert statements.count("NOT VALID") == 3
    assert statements.count("VALIDATE CONSTRAINT") == 1
    assert statements.count("DROP CONSTRAINT") == 3
    assert statements.count("RENAME CONSTRAINT") == 1
    assert "provider_directory_capacity_lease_constraint_drift" in statements
    assert "IN ACCESS EXCLUSIVE MODE NOWAIT" in statements
    assert statements.index("LOCK TABLE") < statements.index(
        "ADD CONSTRAINT"
    )
    assert "UPDATE " not in statements
    assert "DELETE " not in statements
    assert "TRUNCATE " not in statements


def test_capacity_v2_downgrade_refuses_consumed_v2_history(monkeypatch):
    migration = _load_capacity_v2_migration()
    recorder = _OperationsRecorder()
    monkeypatch.setattr(migration, "op", recorder)

    migration.downgrade()

    statements = "\n".join(recorder.statements)
    assert "provider_directory_capacity_lease_v2_history_exists" in statements
    assert "WHERE contract_id =" in statements
    assert "LOCK TABLE" in statements
    assert "IN ACCESS EXCLUSIVE MODE NOWAIT" in statements
    assert statements.index("LOCK TABLE") < statements.index(
        "v2_history_exists"
    )
    assert statements.index("v2_history_exists") < statements.index(
        "ADD CONSTRAINT"
    )


def test_delta_migration_supports_legacy_schema_name(monkeypatch):
    migration = _load_migration()
    monkeypatch.setenv("DB_SCHEMA", "legacy_schema")
    monkeypatch.delenv("HLTHPRT_DB_SCHEMA", raising=False)

    assert migration._schema() == "legacy_schema"


def test_delta_migration_rejects_conflicting_database_schemas(monkeypatch):
    migration = _load_migration()
    monkeypatch.setenv("DB_SCHEMA", "legacy_schema")
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "runtime_schema")

    with pytest.raises(RuntimeError, match="must identify the same schema"):
        migration._schema()


def _assert_consumption_columns_match_model(elements) -> None:
    """Compare migration column nullability and types with the ORM table."""
    migration_columns_by_name = {
        element.name: element
        for element in elements
        if isinstance(element, sa.Column)
    }
    model_columns_by_name = {
        column.name: column
        for column in (
            ProviderDirectoryProfileCapacityLeaseConsumption.__table__.columns
        )
    }
    assert set(migration_columns_by_name) == set(model_columns_by_name)
    assert all(
        column.nullable is False
        for column in migration_columns_by_name.values()
    )
    for column_name, migration_column in migration_columns_by_name.items():
        model_column = model_columns_by_name[column_name]
        assert type(migration_column.type) is type(model_column.type)
        assert getattr(migration_column.type, "length", None) == getattr(
            model_column.type, "length", None
        )
        assert getattr(migration_column.type, "timezone", None) == getattr(
            model_column.type, "timezone", None
        )


def _assert_consumption_unique_keys(elements) -> None:
    """Require immutable attestation, reservation, and run identities."""
    assert any(
        isinstance(element, sa.PrimaryKeyConstraint)
        and tuple(element._pending_colargs) == ("attestation_id",)
        for element in elements
    )
    for column_name in ("reservation_id", "run_id"):
        assert any(
            isinstance(element, sa.UniqueConstraint)
            and tuple(element._pending_colargs) == (column_name,)
            for element in elements
        )


def test_capacity_consumption_migration_matches_model_and_unique_keys(
    monkeypatch,
):
    """Keep the migration ledger contract aligned with its ORM model."""
    migration = _load_migration()
    recorder = _OperationsRecorder()
    monkeypatch.setattr(migration, "op", recorder)

    migration._create_capacity_lease_consumption_table("profile_test")

    table_name, elements, options = recorder.created_table
    assert table_name == (
        "provider_directory_profile_capacity_lease_consumption"
    )
    assert options == {"schema": "profile_test"}
    _assert_consumption_columns_match_model(elements)
    _assert_consumption_unique_keys(elements)
    assert recorder.created_indexes == [
        (
            "pd_profile_capacity_consumption_build_idx",
            table_name,
            ("build_id",),
            {"schema": "profile_test"},
        )
    ]


@pytest.mark.parametrize(
    ("builder_name", "model"),
    (
        (
            "_create_serving_generation_table",
            ProviderDirectoryProfileServingGeneration,
        ),
        (
            "_create_delta_receipt_table",
            ProviderDirectoryProfileDeltaReceipt,
        ),
    ),
)
def test_delta_migration_columns_match_models(builder_name, model):
    migration = _load_migration()
    recorder = _OperationsRecorder()
    migration.op = recorder

    getattr(migration, builder_name)("profile_test")

    table_name, elements, options = recorder.created_table
    migration_columns = {
        element.name
        for element in elements
        if isinstance(element, sa.Column)
    }
    assert table_name == model.__tablename__
    assert options == {"schema": "profile_test"}
    assert migration_columns == {
        column.name for column in model.__table__.columns
    }


def test_checkpoint_upgrade_adds_forecast_contract_without_preexisting_drop():
    migration = _load_migration()
    recorder = _OperationsRecorder()
    migration.op = recorder

    migration._add_checkpoint_columns("profile_test")

    added_columns = {
        column.name for _table_name, column, _options in recorder.added_columns
    }
    assert {
        "cutover_forecast_status",
        "cutover_forecast_hash",
        "cutover_forecast_json",
    } <= added_columns
    assert recorder.dropped_constraints == []
    assert {
        constraint_name
        for (
            constraint_name,
            _table_name,
            _condition,
            _options,
        ) in recorder.created_checks
    } >= {
        "pd_profile_build_checkpoint_forecast_check",
        "pd_profile_build_checkpoint_delta_identity_check",
    }


def test_capacity_consumption_is_always_immutable_and_private():
    migration = _load_migration()
    recorder = _OperationsRecorder()
    migration.op = recorder

    migration._create_capacity_lease_consumption_table("private_profile")

    statements = "\n".join(recorder.statements)
    assert (
        "provider_directory_profile_capacity_consumption_immutable"
        in statements
    )
    assert "BEFORE UPDATE OR DELETE" in statements
    assert "BEFORE TRUNCATE" in statements
    assert statements.count("ENABLE ALWAYS TRIGGER") == 2
    assert "CREATE FUNCTION" in statements
    assert "CREATE PUBLIC" not in statements
    assert "GRANT" not in statements


def test_capacity_consumption_constraints_bind_full_build_and_lease_identity():
    table = ProviderDirectoryProfileCapacityLeaseConsumption.__table__
    constraints = "\n".join(
        str(constraint.sqltext)
        for constraint in table.constraints
        if isinstance(constraint, sa.CheckConstraint)
    )

    for field_name in (
        "attestation_id",
        "reservation_id",
        "lease_digest",
        "capacity_geometry_hash",
        "executable_plan_hash",
        "selection_proof_id",
        "source_vector_hash",
        "source_context_vector_hash",
        "run_id",
        "build_id",
        "profile_as_of",
        "database_system_identifier",
        "database_oid",
        "tablespace_identity_hash",
        "volume_identity_hash",
        "observed_at",
        "issued_at",
        "accepted_at",
        "expires_at",
        "max_build_deadline",
    ):
        assert field_name in constraints
    assert "provider-directory-database-capacity-lease-v1" in constraints
    assert "provider-directory-database-capacity-lease-v2" in constraints
    assert "interval '300 seconds'" in constraints
    assert "interval '305 seconds'" in constraints
    assert "interval '5 seconds'" in constraints
    assert "interval '86400 seconds'" in constraints
    assert "recorded_at = accepted_at" in constraints
    assert "recorded_at < expires_at" in constraints
    assert "recorded_at < max_build_deadline" in constraints


def _profile_geometry_column_sets_by_table() -> dict[str, set[str]]:
    """Return checkpoint, serving, and receipt columns keyed by table role."""
    return {
        "checkpoint": {
        column.name
        for column in ProviderDirectoryProfileBuildCheckpoint.__table__.columns
        },
        "serving": {
            column.name
            for column in (
                ProviderDirectoryProfileServingGeneration.__table__.columns
            )
        },
        "receipt": {
            column.name
            for column in ProviderDirectoryProfileDeltaReceipt.__table__.columns
        },
    }


def _assert_profile_geometry_constraints() -> None:
    """Require the same verified-or-legacy geometry shape on every table."""
    for model in (
        ProviderDirectoryProfileBuildCheckpoint,
        ProviderDirectoryProfileServingGeneration,
        ProviderDirectoryProfileDeltaReceipt,
    ):
        constraints = "\n".join(
            str(constraint.sqltext)
            for constraint in model.__table__.constraints
            if isinstance(constraint, sa.CheckConstraint)
        )
        for contract_fragment in (
            "capacity_geometry_status",
            "capacity_geometry_hash",
            "capacity_geometry_json",
            "legacy_unavailable",
            "verified",
            "jsonb_typeof",
        ):
            assert contract_fragment in constraints


def _cutover_columns_by_table() -> dict[str, set[str]]:
    """Return forecast and observation columns keyed by table role."""
    return {
        "checkpoint": {
            "cutover_forecast_status",
            "cutover_forecast_hash",
            "cutover_forecast_json",
        },
        "receipt": {
            "cutover_forecast_hash",
            "cutover_forecast_json",
            "cutover_actual_hash",
            "cutover_actual_json",
            "cutover_wal_start_lsn",
            "cutover_wal_observed_lsn",
            "cutover_wal_bytes",
            "evidence_target_bytes_before",
            "evidence_target_bytes_after",
            "evidence_target_growth_bytes",
            "profile_target_bytes_before",
            "profile_target_bytes_after",
            "profile_target_growth_bytes",
        },
    }


def _assert_geometry_columns_in_migration(
    geometry_columns: set[str],
    cutover_columns_by_table: dict[str, set[str]],
) -> None:
    """Require every ORM geometry and cutover column in migration source."""
    migration_source = MIGRATION_PATH.read_text(encoding="utf-8")
    expected_columns = (
        *sorted(geometry_columns),
        "from_capacity_geometry_status",
        "from_capacity_geometry_hash",
        "from_capacity_geometry_json",
        *sorted(cutover_columns_by_table["checkpoint"]),
        *sorted(cutover_columns_by_table["receipt"]),
    )
    for column_name in expected_columns:
        assert column_name in migration_source


def test_capacity_geometry_chain_is_schema_bound_end_to_end():
    """Persist verified geometry through checkpoint, receipt, and serving."""
    columns_by_table = _profile_geometry_column_sets_by_table()
    geometry_columns = {
        "capacity_geometry_status",
        "capacity_geometry_hash",
        "capacity_geometry_json",
    }
    assert geometry_columns <= columns_by_table["checkpoint"]
    assert geometry_columns <= columns_by_table["serving"]
    assert geometry_columns <= columns_by_table["receipt"]
    assert {
        "from_capacity_geometry_status",
        "from_capacity_geometry_hash",
        "from_capacity_geometry_json",
    } <= columns_by_table["receipt"]
    cutover_columns = _cutover_columns_by_table()
    assert cutover_columns["checkpoint"] <= columns_by_table["checkpoint"]
    assert {"cutover_forecast_hash"} <= columns_by_table["serving"]
    assert cutover_columns["receipt"] <= columns_by_table["receipt"]
    _assert_profile_geometry_constraints()

    checkpoint_constraints = "\n".join(
        str(constraint.sqltext)
        for constraint in (
            ProviderDirectoryProfileBuildCheckpoint.__table__.constraints
        )
        if isinstance(constraint, sa.CheckConstraint)
    )
    receipt_constraints = "\n".join(
        str(constraint.sqltext)
        for constraint in (
            ProviderDirectoryProfileDeltaReceipt.__table__.constraints
        )
        if isinstance(constraint, sa.CheckConstraint)
    )
    assert "cutover_forecast_status" in checkpoint_constraints
    for column_name in cutover_columns["receipt"]:
        assert column_name in receipt_constraints
    _assert_geometry_columns_in_migration(geometry_columns, cutover_columns)


def test_capacity_consumption_model_retains_only_closed_redacted_evidence():
    column_names = {
        column.name
        for column in (
            ProviderDirectoryProfileCapacityLeaseConsumption.__table__.columns
        )
    }

    assert "canonical_lease_json" in column_names
    assert "signature" in column_names
    assert "hostname" not in column_names
    assert "host_path" not in column_names
    assert "mount_path" not in column_names
    assert "private_key" not in column_names
