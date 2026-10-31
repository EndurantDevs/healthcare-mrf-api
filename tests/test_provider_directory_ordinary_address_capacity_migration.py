# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Exact existing CHECK extensions and retained-history downgrade refusal."""

import importlib.util
from pathlib import Path
from types import SimpleNamespace

import pytest

from tests.test_provider_directory_profile_initial_migration import migration as previous


def load_migration():
    """Load the candidate against the unchanged historical predicate."""
    path = Path(__file__).resolve().parents[1] / "alembic/versions/20261030010000_ordinary_address_capacity.py"
    spec = importlib.util.spec_from_file_location("ordinary_address_capacity_migration", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    module._previous_migration = lambda: previous
    return module


def _migration(*, has_history=False, has_exact_check=True):
    module = load_migration()
    statements = []

    def execute(statement, _params=None):
        sql = str(statement)
        statements.append(sql)
        observed = has_exact_check if "FROM pg_constraint live" in sql else has_history
        return SimpleNamespace(scalar=lambda: observed)

    module.op = SimpleNamespace(execute=execute, get_bind=lambda: SimpleNamespace(execute=execute))
    return module, statements


def test_upgrade_keeps_all_old_predicates_and_only_extends_two_checks(monkeypatch):
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "ordinary_capacity_fixture")
    monkeypatch.setenv("DB_SCHEMA", "ordinary_capacity_fixture")
    migration, statements = _migration()
    migration.upgrade()
    assert "ACCESS EXCLUSIVE" in statements[1]
    assert '"ordinary_capacity_fixture".' in statements[1]
    assert all(
        word not in " ".join(statements) for word in ("UNIQUE", "TRIGGER", "FUNCTION", "GRANT", "CREATE TABLE", "DO $$")
    )
    assert sum("VALIDATE CONSTRAINT" in sql for sql in statements) == 2
    assert sum("FROM pg_constraint live" in sql for sql in statements) == 2
    assert sum("relation.relkind='r' AND relation.relpersistence='p'" in sql for sql in statements) == 2
    assert previous._initial_preflight_check() in migration._ordinary_values_check(previous)
    assert "('profile','cms_nonprofile','serving_nonprofile')" in " ".join(statements)


def test_preflight_extension_binds_exact_purpose_versions_and_both_pair_modes():
    migration, _statements = _migration()
    condition = migration._ordinary_values_check(previous)
    added = condition[len(previous._initial_preflight_check()) + 2 :]
    for exact_value in (
        migration._RECEIPT,
        migration._REQUEST,
        "provider-directory-serving-nonprofile-capacity.v1",
        "serving_nonprofile",
        "provider-directory-ordinary-serving-scope.v1",
        "'source_delta'",
        "'delta_preimage'",
        "'full_swap'",
        "'snapshot'",
        previous._TARGET,
    ):
        assert exact_value in added
    assert "COALESCE" in added and added.endswith(",false)")
    original_common = (
        "limits_contract_id = " + previous._ORIGINAL._values_check().split("AND limits_contract_id = ", 1)[1]
    )
    assert original_common.replace("AND materialization_mode = 'source_delta' ", "") in added


@pytest.mark.parametrize("has_history", [False, True, None])
def test_downgrade_keeps_issued_and_consumed_ordinary_history(has_history):
    migration, statements = _migration(has_history=has_history)
    if has_history is not False:
        with pytest.raises(RuntimeError, match="history_requires_retention"):
            migration.downgrade()
        assert len(statements) == 3 and all("DROP CONSTRAINT" not in sql for sql in statements)
    else:
        migration.downgrade()
        assert sum("VALIDATE CONSTRAINT" in sql for sql in statements) == 2
    assert "ACCESS EXCLUSIVE" in statements[1]
    assert "WHERE admission_purpose='serving_nonprofile'" in statements[2]
    assert "WHERE contract_id=:receipt OR request_contract_id=:request" in statements[2]


def test_changed_existing_check_refuses_before_original_constraint_is_removed():
    migration, statements = _migration(has_exact_check=False)
    with pytest.raises(RuntimeError, match="constraint_drift"):
        migration.upgrade()
    assert all("DROP CONSTRAINT" not in sql for sql in statements)
