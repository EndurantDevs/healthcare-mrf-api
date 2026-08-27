# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Focused migration and metadata contract for packed hospital-price blocks."""

from __future__ import annotations

import importlib.util
from pathlib import Path

from sqlalchemy import CheckConstraint

from db.models.hospital_price_facts import (
    HospitalPriceDataBlock,
    HospitalPricePackedRoot,
)


MIGRATION_PATH = (
    Path(__file__).resolve().parents[1]
    / "alembic"
    / "versions"
    / "20260826090000_hospital_price_packed_blocks.py"
)
SELECTOR_PACKING_MIGRATION_PATH = (
    Path(__file__).resolve().parents[1]
    / "alembic"
    / "versions"
    / "20260827160000_hospital_price_selector_page_packing.py"
)
SCHEMA = "hospital_price_packed_test"

ROOT_COLUMNS = [
    "version_id",
    "format_version",
    "service_count",
    "charge_count",
    "fact_count",
    "code_selector_key_count",
    "payer_plan_selector_key_count",
    "code_selector_ref_count",
    "payer_plan_selector_ref_count",
    "service_block_count",
    "fact_block_count",
    "code_selector_page_count",
    "payer_plan_selector_page_count",
    "code_selector_block_count",
    "payer_plan_selector_block_count",
    "created_at",
]
BLOCK_COLUMNS = [
    "version_id",
    "block_kind",
    "block_ordinal",
    "logical_first",
    "logical_count",
    "secondary_first",
    "secondary_count",
    "page_index",
    "page_count",
    "key_sha256",
    "parent_sha256",
    "payload_sha256",
    "payload",
]


class _Recorder:
    def __init__(self) -> None:
        self.statements: list[str] = []

    def execute(self, statement: str) -> None:
        self.statements.append(str(statement))


def _normalized(value: str) -> str:
    return " ".join(value.split())


def _load_migration(name: str, path: Path = MIGRATION_PATH):
    module_spec = importlib.util.spec_from_file_location(name, path)
    assert module_spec is not None and module_spec.loader is not None
    migration = importlib.util.module_from_spec(module_spec)
    module_spec.loader.exec_module(migration)
    return migration


def _capture(monkeypatch, operation: str, path: Path = MIGRATION_PATH):
    migration = _load_migration(f"hospital_price_packed_{operation}", path)
    recorder = _Recorder()
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", SCHEMA)
    monkeypatch.delenv("DB_SCHEMA", raising=False)
    migration.op = recorder
    getattr(migration, operation)()
    statements = [_normalized(statement) for statement in recorder.statements]
    return migration, statements, " ".join(statements)


def test_selector_packing_migration_backfills_and_guards_rollback(monkeypatch) -> None:
    migration, statements, upgrade_sql = _capture(
        monkeypatch, "upgrade", SELECTOR_PACKING_MIGRATION_PATH
    )
    assert migration.revision == "20260827160000_hospital_price_selector_page_packing"
    assert migration.down_revision == "20260827120000_hospital_price_source_format"
    assert len(statements) == 6
    assert "ADD COLUMN code_selector_block_count bigint" in upgrade_sql
    assert "DISABLE TRIGGER hospital_price_packed_root_reject_update" in upgrade_sql
    assert "WHERE child.version_id=root.version_id AND child.block_kind=3" in upgrade_sql
    assert "ENABLE TRIGGER hospital_price_packed_root_reject_update" in upgrade_sql
    assert "CHECK (format_version IN (1, 2))" in upgrade_sql
    assert "logical_count BETWEEN 1 AND 256" in upgrade_sql

    _migration, statements, downgrade_sql = _capture(
        monkeypatch, "downgrade", SELECTOR_PACKING_MIGRATION_PATH
    )
    assert len(statements) == 4
    assert "IF EXISTS (SELECT 1" in downgrade_sql
    assert "WHERE format_version = 2" in downgrade_sql
    assert "cannot downgrade while hospital selector v2 roots exist" in downgrade_sql
    assert "DROP COLUMN code_selector_block_count" in downgrade_sql


def test_upgrade_sql_matches_root_copy_and_lookup_contract(monkeypatch) -> None:
    migration, statements, sql = _capture(monkeypatch, "upgrade")

    assert migration.revision == "20260826090000_hospital_price_packed_blocks"
    assert migration.down_revision == "20260825120000_ptg_v4_finalizer_map_pack"
    assert len(statements) == 11
    assert (
        f'CREATE FUNCTION "{SCHEMA}"."hospital_price_reject_packed_update"()'
        in statements[0]
    )
    assert f'CREATE TABLE "{SCHEMA}"."hospital_price_packed_root"' in statements[1]
    assert f'CREATE TABLE "{SCHEMA}"."hospital_price_data_block"' in statements[2]
    assert "hospital_id" not in sql
    assert "storage_layout" not in sql
    assert "format_version smallint NOT NULL DEFAULT 1" in sql
    assert "charge_count >= service_count" in sql
    assert "service_block_count BETWEEN 1 AND charge_count" in sql
    assert "code_selector_key_count <= code_selector_page_count" in sql
    assert "code_selector_page_count <= code_selector_ref_count" in sql
    assert "payer_plan_selector_ref_count = fact_count" in sql

    assert "PRIMARY KEY (version_id, block_kind, block_ordinal)" in sql
    assert "block_kind BETWEEN 1 AND 4" in sql
    assert "logical_count BETWEEN 1 AND 512" in sql
    assert "secondary_count BETWEEN 1 AND 524288" in sql
    assert "logical_first < 1000000" in sql
    assert "page_count > 0 AND page_index < page_count" in sql
    assert "payload_sha256 = pg_catalog.sha256(payload)" in sql
    assert "octet_length(payload) BETWEEN 1 AND 4259912" in sql
    assert (
        'CREATE UNIQUE INDEX "hospital_price_data_block_selector_ordinal_key" '
        f'ON "{SCHEMA}"."hospital_price_data_block" '
        "(version_id, logical_first, page_index) "
        "WHERE block_kind IN (3, 4)"
    ) in sql
    assert (
        "( version_id, block_kind, key_sha256, logical_first, page_index ) "
        "WHERE block_kind IN (3, 4)"
    ) in sql
    assert (
        "( version_id, parent_sha256, logical_first, page_index ) "
        "WHERE block_kind = 4"
    ) in sql
    assert "(version_id, secondary_first DESC) WHERE block_kind = 1" in sql
    assert "(version_id, logical_first DESC) WHERE block_kind = 2" in sql
    assert (
        f'ALTER TABLE "{SCHEMA}"."hospital_price_data_block" '
        "ALTER COLUMN payload SET STORAGE EXTERNAL"
    ) in sql
    assert "hospital_price_packed_root_reject_update" in sql
    assert "hospital_price_data_block_reject_update" in sql


def test_models_have_exact_root_and_binary_copy_columns() -> None:
    root = HospitalPricePackedRoot.__table__
    block = HospitalPriceDataBlock.__table__

    assert list(root.columns.keys()) == ROOT_COLUMNS
    assert list(block.columns.keys()) == BLOCK_COLUMNS
    assert "hospital_id" not in root.c and "hospital_id" not in block.c
    assert "storage_layout" not in root.c and "storage_layout" not in block.c
    assert [column.name for column in root.primary_key.columns] == ["version_id"]
    assert [column.name for column in block.primary_key.columns] == [
        "version_id",
        "block_kind",
        "block_ordinal",
    ]

    root_fkey = next(iter(root.foreign_key_constraints))
    block_fkey = next(iter(block.foreign_key_constraints))
    assert root_fkey.name == "hospital_price_packed_root_version_fkey"
    assert root_fkey.ondelete == "CASCADE"
    assert block_fkey.name == "hospital_price_data_block_root_fkey"
    assert block_fkey.ondelete == "CASCADE"
    assert next(iter(block_fkey.elements)).target_fullname.endswith(
        ".hospital_price_packed_root.version_id"
    )


def test_models_have_exact_packed_check_constraints() -> None:
    root = HospitalPricePackedRoot.__table__
    block = HospitalPriceDataBlock.__table__
    root_checks_by_name = {
        constraint.name: _normalized(str(constraint.sqltext))
        for constraint in root.constraints
        if isinstance(constraint, CheckConstraint)
    }
    block_checks_by_name = {
        constraint.name: _normalized(str(constraint.sqltext))
        for constraint in block.constraints
        if isinstance(constraint, CheckConstraint)
    }
    assert set(root_checks_by_name) == {
        "hospital_price_packed_root_format_check",
        "hospital_price_packed_root_counts_check",
    }
    assert (
        "payer_plan_selector_ref_count = fact_count"
        in root_checks_by_name["hospital_price_packed_root_counts_check"]
    )
    assert "format_version IN (1, 2)" in root_checks_by_name[
        "hospital_price_packed_root_format_check"
    ]
    assert (
        "code_selector_block_count BETWEEN 1 AND code_selector_page_count"
        in root_checks_by_name["hospital_price_packed_root_counts_check"]
    )
    assert (
        "payer_plan_selector_block_count BETWEEN 1 AND payer_plan_selector_page_count"
        in root_checks_by_name["hospital_price_packed_root_counts_check"]
    )
    assert set(block_checks_by_name) == {
        "hospital_price_data_block_common_check",
        "hospital_price_data_block_payload_check",
        "hospital_price_data_block_kind_shape_check",
    }
    assert (
        "payload_sha256 = pg_catalog.sha256(payload)"
        in block_checks_by_name["hospital_price_data_block_payload_check"]
    )
    assert (
        "secondary_count BETWEEN 1 AND 524288"
        in block_checks_by_name["hospital_price_data_block_kind_shape_check"]
    )
    assert (
        "logical_count BETWEEN 1 AND 256"
        in block_checks_by_name["hospital_price_data_block_kind_shape_check"]
    )
    assert block.c.key_sha256.type.length == 32
    assert block.c.parent_sha256.type.length == 32
    assert block.c.payload_sha256.type.length == 32


def test_model_has_exact_packed_indexes() -> None:
    block = HospitalPriceDataBlock.__table__
    indexes_by_name = {index.name: index for index in block.indexes}
    assert set(indexes_by_name) == {
        "hospital_price_data_block_selector_ordinal_key",
        "hospital_price_data_block_selector_lookup_idx",
        "hospital_price_data_block_selector_secondary_lookup_idx",
        "hospital_price_data_block_parent_lookup_idx",
        "hospital_price_data_block_charge_range_idx",
        "hospital_price_data_block_fact_range_idx",
    }
    selector_key = indexes_by_name["hospital_price_data_block_selector_ordinal_key"]
    assert selector_key.unique is True
    assert [str(expression) for expression in selector_key.expressions] == [
        "hospital_price_data_block.version_id",
        "hospital_price_data_block.logical_first",
        "hospital_price_data_block.page_index",
    ]
    assert str(selector_key.dialect_options["postgresql"]["where"]) == (
        "block_kind IN (3, 4)"
    )
    selector_secondary = indexes_by_name[
        "hospital_price_data_block_selector_secondary_lookup_idx"
    ]
    assert [str(expression) for expression in selector_secondary.expressions] == [
        "hospital_price_data_block.version_id",
        "hospital_price_data_block.block_kind",
        "hospital_price_data_block.key_sha256",
        "hospital_price_data_block.secondary_first",
    ]
    assert str(selector_secondary.dialect_options["postgresql"]["where"]) == (
        "block_kind IN (3, 4)"
    )
    assert [
        str(expression)
        for expression in indexes_by_name[
            "hospital_price_data_block_charge_range_idx"
        ].expressions
    ][-1] == "secondary_first DESC"
    assert [
        str(expression)
        for expression in indexes_by_name[
            "hospital_price_data_block_fact_range_idx"
        ].expressions
    ][-1] == "logical_first DESC"


def test_downgrade_drops_block_before_root(monkeypatch) -> None:
    _migration, statements, _sql = _capture(monkeypatch, "downgrade")

    assert statements == [
        f'LOCK TABLE "{SCHEMA}"."hospital_price_packed_root" '
        "IN ACCESS EXCLUSIVE MODE;",
        (
            "DO $hospital_price_packed_downgrade$ BEGIN IF EXISTS (SELECT 1 FROM "
            f'"{SCHEMA}"."hospital_price_packed_root" LIMIT 1) THEN RAISE '
            "EXCEPTION 'HOSPITAL_PRICE_PACKED_DOWNGRADE_BLOCKED: packed versions "
            "exist' USING ERRCODE = '55000'; END IF; END "
            "$hospital_price_packed_downgrade$;"
        ),
        f'DROP TABLE IF EXISTS "{SCHEMA}"."hospital_price_data_block";',
        f'DROP TABLE IF EXISTS "{SCHEMA}"."hospital_price_packed_root";',
        f'DROP FUNCTION IF EXISTS "{SCHEMA}"."hospital_price_reject_packed_update"();',
    ]
