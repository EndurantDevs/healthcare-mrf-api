# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import importlib.util
from pathlib import Path

import sqlalchemy as sa

from db.models._legacy import PTG2V3SourceAuditWitness


ROOT = Path(__file__).resolve().parents[1]
MIGRATION_PATH = (
    ROOT
    / "alembic"
    / "versions"
    / "20260717180000_ptg2_v4_witness_capacity.py"
)


def _load_migration():
    module_spec = importlib.util.spec_from_file_location(
        "ptg2_v4_witness_capacity_migration",
        MIGRATION_PATH,
    )
    assert module_spec is not None and module_spec.loader is not None
    migration = importlib.util.module_from_spec(module_spec)
    module_spec.loader.exec_module(migration)
    return migration


def _constraint_sql() -> dict[str, str]:
    return {
        constraint.name: str(constraint.sqltext)
        for constraint in PTG2V3SourceAuditWitness.__table__.constraints
        if isinstance(constraint, sa.CheckConstraint)
    }


def test_v4_witness_model_accepts_the_full_persisted_sample():
    constraints = _constraint_sql()

    assert constraints[
        "ptg2_v3_source_audit_witness_occurrence_count_check"
    ] == "occurrence_witness_count BETWEEN 1 AND 10000"
    assert constraints[
        "ptg2_v3_source_audit_witness_provider_count_check"
    ] == "provider_witness_count BETWEEN 0 AND 1000"
    assert constraints[
        "ptg2_v3_source_audit_witness_total_count_check"
    ] == "occurrence_witness_count + provider_witness_count <= 11000"


def test_v4_witness_capacity_migration_replaces_all_count_constraints(monkeypatch):
    migration = _load_migration()
    statements: list[str] = []
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "ptg_shared")
    monkeypatch.setattr(migration.op, "execute", statements.append)

    migration.upgrade()

    assert migration.down_revision == "20260716150000_npi_provider_sex_lookup_index"
    assert len(statements) == 1
    statement = " ".join(statements[0].split())
    assert 'ALTER TABLE "ptg_shared"."ptg2_v3_source_audit_witness"' in statement
    assert "occurrence_witness_count BETWEEN 1 AND 10000" in statement
    assert "provider_witness_count BETWEEN 0 AND 1000" in statement
    assert "occurrence_witness_count + provider_witness_count <= 11000" in statement


def test_v4_witness_capacity_downgrade_restores_previous_limits(monkeypatch):
    migration = _load_migration()
    statements: list[str] = []
    monkeypatch.setattr(migration.op, "execute", statements.append)

    migration.downgrade()

    statement = " ".join(statements[0].split())
    assert "occurrence_witness_count BETWEEN 1 AND 2048" in statement
    assert "provider_witness_count BETWEEN 0 AND 2048" in statement
    assert "occurrence_witness_count + provider_witness_count <= 2048" in statement
