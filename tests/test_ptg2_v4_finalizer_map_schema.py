# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest
import sqlalchemy as sa

from db.models import (
    PTG2V4FinalizerMapPack,
    PTG2V4FinalizerMapRoot,
    PTG2V4FinalizerMapTarget,
)
from process.ptg_parts.ptg2_v4_finalizer_maps import (
    PTG2_V4_FINALIZER_MAP_CONTRACT,
    PTG2_V4_FINALIZER_PACKED_OBJECT_KINDS,
)
from process.ptg_parts.ptg2_v4_snapshot_maps import PTG2_V4_MAP_FORMAT


ROOT = Path(__file__).resolve().parents[1]
MIGRATION_PATH = (
    ROOT
    / "alembic"
    / "versions"
    / "20260825120000_ptg_v4_finalizer_map_pack.py"
)


def _load_migration():
    spec = importlib.util.spec_from_file_location(
        "ptg2_v4_finalizer_map_pack_migration",
        MIGRATION_PATH,
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class _OpRecorder:
    def __init__(self):
        self.executed: list[str] = []

    def execute(self, statement):
        self.executed.append(str(statement))


def _sql(recorder: _OpRecorder) -> str:
    return " ".join(" ".join(statement.split()) for statement in recorder.executed)


def _constraint(table, name, constraint_type):
    return next(
        constraint
        for constraint in table.constraints
        if isinstance(constraint, constraint_type) and constraint.name == name
    )


def _assert_safe_downgrade(migration, recorder: _OpRecorder) -> None:
    recorder.executed.clear()
    migration.downgrade()
    sql = _sql(recorder)
    target_drop = sql.index(
        'DROP TABLE IF EXISTS "ptg_finalizer_test".'
        '"ptg2_v4_finalizer_map_target"'
    )
    pack_drop = sql.index(
        'DROP TABLE IF EXISTS "ptg_finalizer_test".'
        '"ptg2_v4_finalizer_map_pack"'
    )
    root_drop = sql.index(
        'DROP TABLE IF EXISTS "ptg_finalizer_test".'
        '"ptg2_v4_finalizer_map_root"'
    )
    assert target_drop < pack_drop < root_drop
    assert "ptg2_v4_finalizer_map_downgrade_requires_empty_root" in sql


def test_finalizer_map_migration_is_additive_explicit_and_guarded(monkeypatch):
    """Prove the additive schema has explicit immutable lifecycle guards."""

    migration = _load_migration()
    recorder = _OpRecorder()
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "ptg_finalizer_test")
    monkeypatch.delenv("DB_SCHEMA", raising=False)
    monkeypatch.setattr(migration, "op", recorder)

    migration.upgrade()
    sql = _sql(recorder)

    assert (
        migration.down_revision
        == "20260825090000_geo_assurance_projection"
    )
    for table_name in (
        "ptg2_v4_finalizer_map_root",
        "ptg2_v4_finalizer_map_pack",
        "ptg2_v4_finalizer_map_target",
    ):
        assert f'CREATE TABLE "ptg_finalizer_test"."{table_name}"' in sql
    assert f"contract = '{PTG2_V4_FINALIZER_MAP_CONTRACT}'" in sql
    assert f"map_format = '{PTG2_V4_MAP_FORMAT}'" in sql
    assert tuple(migration._FINALIZER_KINDS) == PTG2_V4_FINALIZER_PACKED_OBJECT_KINDS
    assert tuple(sorted(PTG2_V4_FINALIZER_PACKED_OBJECT_KINDS)) == (
        PTG2_V4_FINALIZER_PACKED_OBJECT_KINDS
    )
    for object_kind in PTG2_V4_FINALIZER_PACKED_OBJECT_KINDS:
        assert f"'{object_kind}'" in sql
    assert "price_set_atom_memberships_v3" not in sql
    assert "price_atoms_v3" not in sql
    assert "ptg2_v4_finalizer_map_root_summary_mismatch" in sql
    assert "resolved_target_block_count" in sql
    assert "ptg2_v4_finalizer_map_mixed_storage" in sql
    assert "ROW_NUMBER() OVER" in sql
    assert "LAG(mapping.last_block_key) OVER" in sql
    assert "ptg2_v4_finalizer_map_pack_sequence_invalid" in sql
    assert "ptg2_v4_finalizer_map_target_immutable" in sql
    assert "REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT" in sql
    assert "ptg2_v4_finalizer_map_truncate_forbidden" in sql
    assert sql.count("BEFORE TRUNCATE ON") == 3
    assert (
        'BEFORE UPDATE OR DELETE ON "ptg_finalizer_test".'
        '"ptg2_v4_finalizer_map_target" FOR EACH STATEMENT'
    ) in sql

    _assert_safe_downgrade(migration, recorder)


def test_finalizer_map_migration_rejects_conflicting_schema_aliases(monkeypatch):
    migration = _load_migration()
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "runtime")
    monkeypatch.setenv("DB_SCHEMA", "legacy")

    with pytest.raises(RuntimeError, match="must identify the same schema"):
        migration._schema()


def test_finalizer_map_models_match_primary_keys():
    """Keep roots, packs, and target anchors snapshot-local."""

    root_table = PTG2V4FinalizerMapRoot.__table__
    pack_table = PTG2V4FinalizerMapPack.__table__
    target_table = PTG2V4FinalizerMapTarget.__table__

    assert tuple(column.name for column in root_table.primary_key.columns) == (
        "snapshot_key",
    )
    assert tuple(column.name for column in pack_table.primary_key.columns) == (
        "snapshot_key",
        "object_kind",
        "pack_no",
    )
    assert tuple(column.name for column in target_table.primary_key.columns) == (
        "snapshot_key",
        "block_hash",
    )


def test_finalizer_map_models_match_cascade_and_cas_contracts():
    """Cascade layout removal while retaining CAS delete protection."""

    root_table = PTG2V4FinalizerMapRoot.__table__
    pack_table = PTG2V4FinalizerMapPack.__table__
    target_table = PTG2V4FinalizerMapTarget.__table__

    assert (
        _constraint(
            root_table,
            "ptg2_v4_finalizer_map_root_layout_fkey",
            sa.ForeignKeyConstraint,
        ).ondelete
        == "CASCADE"
    )
    for child_table, root_fkey, block_fkey in (
        (
            pack_table,
            "ptg2_v4_finalizer_map_pack_root_fkey",
            "ptg2_v4_finalizer_map_pack_block_fkey",
        ),
        (
            target_table,
            "ptg2_v4_finalizer_map_target_root_fkey",
            "ptg2_v4_finalizer_map_target_block_fkey",
        ),
    ):
        assert (
            _constraint(child_table, root_fkey, sa.ForeignKeyConstraint).ondelete
            == "CASCADE"
        )
        assert (
            _constraint(child_table, block_fkey, sa.ForeignKeyConstraint).ondelete
            == "RESTRICT"
        )
    assert "ptg2_v4_finalizer_map_pack_block_hash_idx" in {
        index.name for index in pack_table.indexes
    }
    assert "ptg2_v4_finalizer_map_target_block_hash_idx" in {
        index.name for index in target_table.indexes
    }


def test_finalizer_map_models_allow_only_six_finalizer_kinds():
    """Exclude the independent relational price-map kinds."""

    pack_table = PTG2V4FinalizerMapPack.__table__
    kind_check = _constraint(
        pack_table,
        "ptg2_v4_finalizer_map_pack_kind_check",
        sa.CheckConstraint,
    )
    kind_sql = str(kind_check.sqltext)
    for object_kind in PTG2_V4_FINALIZER_PACKED_OBJECT_KINDS:
        assert object_kind in kind_sql
    assert "price_set_atom_memberships_v3" not in kind_sql
    assert "price_atoms_v3" not in kind_sql
