# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Schema contract for the exact release-bound v4 state scan."""

from __future__ import annotations

import importlib.util
from pathlib import Path


MIGRATION_PATH = (
    Path(__file__).resolve().parents[1]
    / "alembic/versions/20260903160000_plan_pricing_state_scan.py"
)


class _Recorder:
    def __init__(self) -> None:
        self.statements: list[str] = []

    def execute(self, statement: str) -> None:
        self.statements.append(" ".join(str(statement).split()))


def _migration(name: str):
    module_spec = importlib.util.spec_from_file_location(name, MIGRATION_PATH)
    assert module_spec is not None and module_spec.loader is not None
    migration = importlib.util.module_from_spec(module_spec)
    module_spec.loader.exec_module(migration)
    return migration


def test_state_scan_migration_adds_both_exact_v4_children(monkeypatch):
    migration = _migration("plan_pricing_state_scan_upgrade")
    recorder = _Recorder()
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "state_scan_test")
    monkeypatch.delenv("DB_SCHEMA", raising=False)
    monkeypatch.setattr(migration, "op", recorder)

    migration.upgrade()

    sql = " ".join(recorder.statements)
    assert migration.down_revision == "20260903130000_hospital_price_csv_v1_labels"
    assert 'CREATE TABLE "state_scan_test"."plan_pricing_provider_state"' in sql
    assert 'CREATE TABLE "state_scan_test"."plan_pricing_rate_occurrence"' in sql
    assert "PRIMARY KEY (projection_id, state, npi)" in sql
    assert "plan_pricing_factorized_v4" in sql
    assert "provider_state_count" in sql
    assert "provider_fragment bytea NOT NULL" in sql
    assert "octet_length(provider_fragment) BETWEEN 2" in sql
    assert "SUM(octet_length(provider_fragment))" in sql
    assert "rate_occurrence_count" in sql
    assert "provider_set_ref" in sql
    assert "price_set_ref" in sql
    assert "rate_pack_ref" in sql
    assert "source_artifact_key" in sql
    assert "group_fragment jsonb" in sql
    assert "occurrence_multiplicity" in sql
    assert "SELECT DISTINCT" in sql
    assert "convert_from(fragment, 'UTF8')::jsonb ->> 'state'" in sql
    assert sql.count("EXCEPT") >= 2
    assert "provider-state index is incomplete" in sql
    assert "rate-occurrence index is incomplete" in sql
    assert "BEFORE INSERT OR UPDATE OR DELETE" in sql
    assert "BEFORE TRUNCATE" in sql


def test_state_scan_downgrade_refuses_v4_then_restores_v3(monkeypatch):
    migration = _migration("plan_pricing_state_scan_downgrade")
    recorder = _Recorder()
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "state_scan_test")
    monkeypatch.delenv("DB_SCHEMA", raising=False)
    monkeypatch.setattr(migration, "op", recorder)

    migration.downgrade()

    sql = " ".join(recorder.statements)
    assert "DROP TABLE" in sql
    assert "plan_pricing_provider_state" in sql
    assert "plan_pricing_rate_occurrence" in sql
    assert 'DROP TABLE "state_scan_test"."plan_pricing_provider_cell"' not in sql
    assert 'DROP TABLE "state_scan_test"."plan_pricing_provider_membership"' not in sql
    assert "cannot downgrade while v4 pricing projections exist" in sql
    assert "plan_pricing_factorized_v3" in sql
