# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Schema contract for bounded factorized plan-pricing projections."""

from __future__ import annotations

import importlib.util
from pathlib import Path

from alembic.config import Config
from alembic.script import ScriptDirectory
import pytest

from tests.provider_directory_profile_capacity_v2_migration_support import (
    load_capacity_v2_migration,
)


MIGRATION_PATH = (
    Path(__file__).resolve().parents[1]
    / "alembic/versions/20260828120000_plan_pricing_factorized_projection.py"
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


def test_factorized_migration_is_additive_bounded_and_authenticated(
    monkeypatch,
) -> None:
    migration = _migration("factorized_projection_upgrade")
    recorder = _Recorder()
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "factorized_test")
    monkeypatch.delenv("DB_SCHEMA", raising=False)
    monkeypatch.setattr(migration, "op", recorder)

    migration.upgrade()

    sql = " ".join(recorder.statements)
    assert migration.revision == (
        "20260828120000_plan_pricing_factorized_projection"
    )
    assert migration.down_revision == (
        "20260829100000_activate_import_run_idempotency_scope"
    )
    assert "plan_pricing_card_v2" in sql
    assert "plan_pricing_factorized_v3" in sql
    assert (
        'CREATE TABLE "factorized_test".'
        '"plan_pricing_provider_membership"' in sql
    )
    assert 'CREATE TABLE "factorized_test"."plan_pricing_provider_cell"' in sql
    assert 'CREATE TABLE "factorized_test"."plan_pricing_rate_profile"' in sql
    assert 'CREATE TABLE "factorized_test"."plan_pricing_aggregate_pack"' in sql
    assert 'CREATE TABLE "factorized_test"."plan_pricing_prewarm_shape"' in sql
    assert "shape_rank BETWEEN 1 AND 768" in sql
    assert "payload_sha256 = pg_catalog.sha256(payload)" in sql
    assert "stored_byte_count BETWEEN 45 AND 558124" in sql
    assert "raw_byte_count = ( pg_catalog.get_byte(payload, 8)" in sql
    assert "content_digest IS NOT NULL" in sql
    assert "build_seconds IS NOT NULL" in sql
    assert "rate_profile_count IS NOT NULL" in sql
    assert "plan_pricing_rate_profile_cost_idx" in sql
    assert "plan_pricing_rates_strictly_increasing" in sql
    assert "rates[position] >= rates[position + 1]" in sql
    assert "array_ndims(negotiated_rates) = 1" in sql
    assert "array_lower(rate_multiplicities, 1) = 1" in sql
    assert "BEFORE INSERT OR UPDATE OR DELETE" in sql
    assert sql.count("BEFORE TRUNCATE ON") == 5
    assert sql.count('"plan_pricing_projection_truncate_guard"()') == 5
    assert "factorized plan-pricing projection receipt counts" in sql


def test_factorized_migration_rejects_conflicting_schema_environment(
    monkeypatch,
) -> None:
    migration = _migration("factorized_projection_schema_conflict")
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "factorized_test")
    monkeypatch.setenv("DB_SCHEMA", "different_schema")

    with pytest.raises(RuntimeError, match="must identify the same schema"):
        migration.upgrade()


def test_factorized_downgrade_refuses_immutable_v3_candidates(monkeypatch) -> None:
    migration = _migration("factorized_projection_downgrade")
    recorder = _Recorder()
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "factorized_test")
    monkeypatch.delenv("DB_SCHEMA", raising=False)
    monkeypatch.setattr(migration, "op", recorder)

    migration.downgrade()

    sql = " ".join(recorder.statements)
    assert "cannot downgrade while factorized pricing projections exist" in sql
    assert "DROP TABLE \"factorized_test\".\"plan_pricing_prewarm_shape\"" in sql
    assert "DROP COLUMN provider_cell_count" in sql
    assert "DROP COLUMN provider_membership_count" in sql
    assert 'DROP TABLE "factorized_test"."plan_pricing_rate_profile"' in sql
    assert '"plan_pricing_rates_strictly_increasing"(numeric[])' in sql
    assert "CHECK (contract_version = 'plan_pricing_card_v2')" in sql


def test_factorized_projection_precedes_the_unique_repository_head() -> None:
    script = ScriptDirectory.from_config(Config("alembic.ini"))
    assert script.get_heads() == [
        "20260904223000_provider_directory_michigan_generation_retirement"
    ]
    factorized = script.get_revision(
        "20260828120000_plan_pricing_factorized_projection"
    )
    assert factorized.down_revision == (
        "20260829100000_activate_import_run_idempotency_scope"
    )
    assert script.get_revision(
        "20260825150000_plan_pricing_card_projection"
    ).down_revision == "20260826090000_hospital_price_packed_blocks"
    assert load_capacity_v2_migration().down_revision == (
        "20260801010000_uhc_semantic_layout_identity"
    )
