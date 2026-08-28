# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Modifier-only paired-null payer identity schema and migration contract."""

from __future__ import annotations

import importlib.util
from contextlib import contextmanager
from pathlib import Path

from db.models.hospital_price_facts import (
    HospitalPriceModifierPayer,
    HospitalPricePayerCharge,
)


MIGRATION_PATH = (
    Path(__file__).resolve().parents[1]
    / "alembic/versions/20260828120000_hospital_price_modifier_payer_identity.py"
)


class _Recorder:
    def __init__(self) -> None:
        self.statements: list[str] = []
        self.events: list[tuple[str, str]] = []

    def execute(self, statement: str) -> None:
        normalized = " ".join(str(statement).split())
        self.statements.append(normalized)
        self.events.append(("execute", normalized))

    def get_context(self):
        return self

    @contextmanager
    def autocommit_block(self):
        self.events.append(("autocommit", "enter"))
        try:
            yield
        finally:
            self.events.append(("autocommit", "exit"))


def _migration():
    spec = importlib.util.spec_from_file_location(
        "hospital_price_modifier_payer_identity_migration", MIGRATION_PATH
    )
    assert spec is not None and spec.loader is not None
    migration = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(migration)
    return migration


def test_only_modifier_payer_identity_is_paired_nullable() -> None:
    modifier = HospitalPriceModifierPayer.__table__
    ordinary = HospitalPricePayerCharge.__table__
    constraint = next(
        item
        for item in modifier.constraints
        if item.name == "hospital_price_modifier_payer_shape_check"
    )
    sql = " ".join(str(constraint.sqltext).split())

    assert modifier.c.payer_name.nullable and modifier.c.plan_name.nullable
    assert not ordinary.c.payer_name.nullable and not ordinary.c.plan_name.nullable
    assert "payer_name IS NULL AND plan_name IS NULL" in sql
    assert "btrim(payer_name) <> '' AND btrim(plan_name) <> ''" in sql


def test_migration_replaces_only_modifier_identity_guard(monkeypatch) -> None:
    migration = _migration()
    recorder = _Recorder()
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "fixture")
    monkeypatch.delenv("DB_SCHEMA", raising=False)
    migration.op = recorder

    migration.upgrade()

    assert migration.revision == "20260828120000_hospital_price_modifier_payer_identity"
    assert migration.down_revision == "20260827210000_entity_address_geo_taxonomy"
    assert len(recorder.statements) == 5
    assert recorder.statements[0] == "SET lock_timeout = '5s'"
    sql = recorder.statements[1]
    assert 'ALTER TABLE "fixture"."hospital_price_modifier_payer"' in sql
    assert "IF EXISTS" not in sql
    assert "ALTER COLUMN payer_name DROP NOT NULL" in sql
    assert "ALTER COLUMN plan_name DROP NOT NULL" in sql
    assert "payer_name IS NULL AND plan_name IS NULL" in sql
    assert sql.endswith("NOT VALID")
    assert "hospital_price_payer_charge" not in sql
    assert recorder.statements[2] == "RESET lock_timeout"
    assert recorder.statements[3] == "SET LOCAL lock_timeout = '5s'"
    assert recorder.statements[4].endswith(
        'VALIDATE CONSTRAINT "hospital_price_modifier_payer_shape_check"'
    )
    assert recorder.events.index(("autocommit", "exit")) < recorder.events.index(
        ("execute", recorder.statements[4])
    )

    recorder.statements.clear()
    migration.downgrade()
    assert recorder.statements == []
