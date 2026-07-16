# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import importlib.util
from pathlib import Path

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql
from sqlalchemy.schema import CreateTable

from db.models import (
    PTG2AllowedAmountItem,
    PTG2AllowedAmountPayment,
    PTG2AllowedAmountPlan,
    PTG2AllowedAmountProviderPayment,
)


MIGRATION_PATH = (
    Path(__file__).resolve().parents[1]
    / "alembic"
    / "versions"
    / "20260716120000_ptg2_allowed_amounts_v3.py"
)


def _load_migration():
    migration_spec = importlib.util.spec_from_file_location(
        "ptg2_allowed_amounts_v3_migration",
        MIGRATION_PATH,
    )
    assert migration_spec is not None and migration_spec.loader is not None
    migration = importlib.util.module_from_spec(migration_spec)
    migration_spec.loader.exec_module(migration)
    return migration


class _OpRecorder:
    def __init__(self) -> None:
        self.tables: dict[str, dict[str, object]] = {}
        self.indexes: dict[str, dict[str, object]] = {}
        self.dropped_tables: list[str] = []

    def create_table(self, name, *items, schema=None):
        self.tables[name] = {"items": items, "schema": schema}

    def create_index(self, name, table_name, columns, **kwargs):
        self.indexes[name] = {
            "table": table_name,
            "columns": columns,
            **kwargs,
        }

    def drop_table(self, name, **_kwargs):
        self.dropped_tables.append(name)


def _recorded_columns(table_record: dict[str, object]) -> dict[str, sa.Column]:
    return {
        item.name: item
        for item in table_record["items"]
        if isinstance(item, sa.Column)
    }


def _model_indexes(model) -> dict[str, tuple[str, ...]]:
    return {
        index["name"]: tuple(index["index_elements"])
        for index in model.__my_additional_indexes__
    }


def _assert_allowed_model_primary_keys(
    plan_table,
    item_table,
    payment_table,
    provider_table,
) -> None:
    assert tuple(plan_table.primary_key.columns.keys()) == (
        "snapshot_id",
        "plan_hash",
    )
    assert tuple(item_table.primary_key.columns.keys()) == (
        "snapshot_id",
        "allowed_item_hash",
    )
    assert item_table.c.file_id.nullable is False
    assert tuple(payment_table.primary_key.columns.keys()) == (
        "snapshot_id",
        "payment_hash",
    )
    assert tuple(provider_table.primary_key.columns.keys()) == (
        "snapshot_id",
        "provider_payment_hash",
    )


def _assert_allowed_model_cascade_ownership(
    plan_table,
    payment_table,
    provider_table,
) -> None:
    plan_fk = next(iter(plan_table.foreign_key_constraints))
    payment_fk = next(iter(payment_table.foreign_key_constraints))
    provider_fk = next(iter(provider_table.foreign_key_constraints))
    assert tuple(plan_fk.column_keys) == ("snapshot_id",)
    assert plan_fk.ondelete == "CASCADE"
    assert tuple(payment_fk.column_keys) == (
        "snapshot_id",
        "allowed_item_hash",
    )
    assert payment_fk.ondelete == "CASCADE"
    assert tuple(provider_fk.column_keys) == (
        "snapshot_id",
        "payment_hash",
    )
    assert provider_fk.ondelete == "CASCADE"


def _assert_allowed_model_indexes() -> None:
    assert _model_indexes(PTG2AllowedAmountPlan)[
        "ptg2_allowed_plan_snapshot_lookup_idx"
    ] == (
        "snapshot_id",
        "plan_id",
        "plan_market_type",
        "file_id",
    )
    assert _model_indexes(PTG2AllowedAmountItem)[
        "ptg2_allowed_item_snapshot_code_plan_idx"
    ] == (
        "snapshot_id",
        "billing_code",
        "plan_id",
        "file_id",
        "billing_code_type",
    )
    assert _model_indexes(PTG2AllowedAmountProviderPayment)[
        "ptg2_allowed_provider_npi_gin_idx"
    ] == ("npi",)


def test_allowed_amount_models_are_snapshot_scoped_with_cascade_ownership():
    """Keep allowed-amount model identity and ownership snapshot-scoped."""
    plan_table = PTG2AllowedAmountPlan.__table__
    item_table = PTG2AllowedAmountItem.__table__
    payment_table = PTG2AllowedAmountPayment.__table__
    provider_table = PTG2AllowedAmountProviderPayment.__table__

    _assert_allowed_model_primary_keys(
        plan_table,
        item_table,
        payment_table,
        provider_table,
    )
    _assert_allowed_model_cascade_ownership(
        plan_table,
        payment_table,
        provider_table,
    )
    provider_ddl = str(
        CreateTable(provider_table).compile(dialect=postgresql.dialect())
    )
    assert "npi BIGINT[] NOT NULL" in provider_ddl
    _assert_allowed_model_indexes()


def _assert_migration_tables_match_models(recorder, models) -> None:
    assert set(recorder.tables) == {
        model.__tablename__ for model in models
    }
    for model in models:
        table_record = recorder.tables[model.__tablename__]
        assert tuple(_recorded_columns(table_record)) == tuple(
            model.__table__.columns.keys()
        )
        assert table_record["schema"] == "mrf"


def _assert_allowed_migration_indexes(recorder) -> None:
    assert set(recorder.indexes) == {
        "ptg2_allowed_plan_snapshot_lookup_idx",
        "ptg2_allowed_plan_file_idx",
        "ptg2_allowed_item_snapshot_code_plan_idx",
        "ptg2_allowed_payment_item_idx",
        "ptg2_allowed_payment_tin_idx",
        "ptg2_allowed_provider_payment_idx",
        "ptg2_allowed_provider_npi_gin_idx",
    }
    assert recorder.indexes[
        "ptg2_allowed_plan_snapshot_lookup_idx"
    ]["columns"] == [
        "snapshot_id",
        "plan_id",
        "plan_market_type",
        "file_id",
    ]
    assert recorder.indexes[
        "ptg2_allowed_item_snapshot_code_plan_idx"
    ]["columns"] == [
        "snapshot_id",
        "billing_code",
        "plan_id",
        "file_id",
        "billing_code_type",
    ]
    assert recorder.indexes["ptg2_allowed_provider_npi_gin_idx"][
        "postgresql_using"
    ] == "gin"


def test_allowed_amount_migration_matches_fixed_v3_models(monkeypatch):
    """Keep the fixed V3 migration aligned with model columns and indexes."""
    migration = _load_migration()
    recorder = _OpRecorder()
    monkeypatch.delenv("DB_SCHEMA", raising=False)
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "mrf")
    monkeypatch.setattr(migration, "op", recorder)

    migration.upgrade()

    assert migration.revision == "20260716120000_ptg2_allowed_amounts_v3"
    assert migration.down_revision == (
        "20260715160000_mrf_discovery_source_checkpoints"
    )
    models = (
        PTG2AllowedAmountPlan,
        PTG2AllowedAmountItem,
        PTG2AllowedAmountPayment,
        PTG2AllowedAmountProviderPayment,
    )
    _assert_migration_tables_match_models(recorder, models)
    _assert_allowed_migration_indexes(recorder)

    migration.downgrade()
    assert recorder.dropped_tables == [
        "ptg2_allowed_amount_provider_payment",
        "ptg2_allowed_amount_payment",
        "ptg2_allowed_amount_item",
        "ptg2_allowed_amount_plan",
    ]
