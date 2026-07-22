# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import importlib.util
from pathlib import Path


MIGRATION_PATH = (
    Path(__file__).resolve().parents[1]
    / "alembic"
    / "versions"
    / "20260722160000_plan_release_serving_projection.py"
)


def _load_migration():
    module_spec = importlib.util.spec_from_file_location(
        "plan_release_serving_projection_migration",
        MIGRATION_PATH,
    )
    assert module_spec is not None and module_spec.loader is not None
    migration = importlib.util.module_from_spec(module_spec)
    module_spec.loader.exec_module(migration)
    return migration


def test_plan_release_projection_migration_has_exact_binding_and_pin_contract(
    monkeypatch,
):
    migration = _load_migration()
    statements: list[str] = []
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "release_projection")
    monkeypatch.delenv("DB_SCHEMA", raising=False)
    monkeypatch.setattr(migration.op, "execute", statements.append)

    migration.upgrade()

    assert (
        migration.down_revision
        == "20260722130000_provider_directory_projection_child_read_lease"
    )
    assert len(statements) == 8
    assert all(";" not in statement for statement in statements)
    statement = " ".join(" ".join(statements).split())
    assert (
        'CREATE TABLE "release_projection"."plan_release_serving_revision"'
        in statement
    )
    assert (
        'CREATE TABLE "release_projection"."plan_release_snapshot_binding"'
        in statement
    )
    assert "PRIMARY KEY (serving_revision_id, role, binding_ordinal)" in statement
    assert "role IN ('in_network', 'allowed_amounts')" in statement
    assert (
        'CREATE TABLE "release_projection"."ptg2_snapshot_pin"'
        in statement
    )
    assert "PRIMARY KEY (owner_type, owner_id, snapshot_id)" in statement
    assert "ON DELETE RESTRICT" in statement
    assert (
        'CREATE UNIQUE INDEX "plan_release_serving_current_release_uidx" '
        'ON "release_projection"."plan_release_serving_revision" '
        "(plan_release_id) WHERE is_current"
    ) in statement
    assert (
        'CREATE UNIQUE INDEX "plan_release_serving_current_plan_uidx" '
        'ON "release_projection"."plan_release_serving_revision" '
        "(healthporta_plan_id) WHERE is_current"
    ) in statement
    assert "plan_market_type varchar(64)" in statement
    assert (
        "NOT is_current OR ( serving_status = 'published' "
        "AND release_status = 'published' )"
    ) in statement


def test_plan_release_projection_orm_matches_partial_indexes_and_market_width():
    from db.models._legacy import (
        PlanReleaseServingRevision,
        PlanReleaseSnapshotBinding,
    )

    indexes_by_name = {
        index.name: index
        for index in PlanReleaseServingRevision.__table__.indexes
    }
    for index_name, column_name in (
        (
            "plan_release_serving_current_release_uidx",
            "plan_release_id",
        ),
        (
            "plan_release_serving_current_plan_uidx",
            "healthporta_plan_id",
        ),
    ):
        index = indexes_by_name[index_name]
        assert index.unique is True
        assert [column.name for column in index.columns] == [column_name]
        assert str(index.dialect_options["postgresql"]["where"]) == "is_current"
    assert (
        PlanReleaseSnapshotBinding.__table__.c.plan_market_type.type.length
        == 64
    )
    constraints_by_name = {
        constraint.name: constraint
        for constraint in PlanReleaseServingRevision.__table__.constraints
        if constraint.name
    }
    assert "release_status = 'published'" in str(
        constraints_by_name[
            "plan_release_serving_current_state_check"
        ].sqltext
    )


def test_plan_release_projection_downgrade_drops_dependents_first(monkeypatch):
    migration = _load_migration()
    statements: list[str] = []
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "release_projection")
    monkeypatch.delenv("DB_SCHEMA", raising=False)
    monkeypatch.setattr(migration.op, "execute", statements.append)

    migration.downgrade()

    assert statements == [
        'DROP TABLE IF EXISTS "release_projection"."ptg2_snapshot_pin";',
        'DROP TABLE IF EXISTS "release_projection".'
        '"plan_release_snapshot_binding";',
        'DROP TABLE IF EXISTS "release_projection".'
        '"plan_release_serving_revision";',
    ]
