# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Standalone inverse authority keeps native ledger history and existing key custody."""

from types import SimpleNamespace

import pytest

from tests.test_provider_directory_profile_initial_migration import _migration as historical_migration


def load_migration():
    return historical_migration("20261031020000_serving_inverse_capacity.py")


def _recorded_migration(*, history=False, exact=True):
    migration = load_migration()
    previous, ordinary, initial = migration._previous()
    statements = []

    def execute(statement, _params=None):
        sql = str(statement)
        statements.append(sql)
        observed = exact if "FROM pg_constraint live" in sql else history
        return SimpleNamespace(scalar=lambda: observed)

    recorder = SimpleNamespace(execute=execute, get_bind=lambda: SimpleNamespace(execute=execute))
    migration.op = ordinary.op = recorder
    migration._previous = lambda: (previous, ordinary, initial)
    return migration, statements


def test_inverse_upgrade_preserves_predecessor_and_all_keys_guards_rows(monkeypatch):
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "inverse_capacity_fixture")
    monkeypatch.setenv("DB_SCHEMA", "inverse_capacity_fixture")
    migration, statements = _recorded_migration()
    migration.upgrade()
    previous, ordinary, initial = migration._previous()
    assert migration.down_revision == previous.revision
    assert previous._recovered_preflight_check(ordinary, initial) in migration._values_check(
        previous, ordinary, initial
    )
    assert "lock_timeout" in statements[0] and "pg_advisory_xact_lock" in statements[1]
    assert "ACCESS EXCLUSIVE" in statements[2]
    assert statements[2].count('"inverse_capacity_fixture".') == 2
    assert sum("VALIDATE CONSTRAINT" in sql for sql in statements) == 2
    assert sum("FROM pg_constraint live" in sql for sql in statements) == 2
    assert all(
        word not in " ".join(statements)
        for word in ("UNIQUE", "TRIGGER", "FUNCTION", "GRANT", "CREATE TABLE", "DELETE FROM", "DO $$")
    )


def test_inverse_predicate_binds_real_provenance_and_keeps_native_use_checks():
    migration = load_migration()
    _previous, _ordinary, initial = migration._previous()
    condition = migration._inverse_values_check(initial)
    original = initial._ORIGINAL._values_check()
    assert "receipt_sha256 ~ " + original.split("AND receipt_sha256 ~ ", 1)[1] in condition
    assert "materialization_mode='serving_inverse'" in condition
    assert "profile_strategy_version = 'source-fact" not in condition
    assert "materialization_mode = 'source_delta'" not in condition
    for name in (
        "control_generation",
        "profile_schema_version",
        "profile_strategy_version",
        "selection_proof_id",
        "profile_input_digest",
    ):
        assert f"->'profile_provenance'->'{name}'=to_jsonb({name})" in condition
    assert "->'scope'->>'input_sha256'=serving_preflight_sha256" in condition
    assert "->>'operation'='recover'" in condition
    assert "->>'admission_purpose'='serving_inverse'" in condition


@pytest.mark.parametrize("history", [False, True, None])
def test_inverse_downgrade_refuses_all_issued_or_consumed_history(history):
    migration, statements = _recorded_migration(history=history)
    if history is False:
        migration.downgrade()
        assert sum("VALIDATE CONSTRAINT" in sql for sql in statements) == 2
    else:
        with pytest.raises(RuntimeError, match="history_requires_retention"):
            migration.downgrade()
        assert all("DROP CONSTRAINT" not in sql for sql in statements)
    assert "WHERE admission_purpose=:purpose" in statements[3]
    assert "contract_id=:receipt OR request_contract_id=:request" in statements[3]
    assert "materialization_mode=:purpose" in statements[3]
    assert "consumed_at IS NULL" not in statements[3]


def test_changed_predecessor_fails_before_original_check_removal():
    migration, statements = _recorded_migration(exact=False)
    with pytest.raises(RuntimeError, match="constraint_drift"):
        migration.upgrade()
    assert all("DROP CONSTRAINT" not in sql for sql in statements)
