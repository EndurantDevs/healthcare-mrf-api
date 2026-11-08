# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""The forward archive repair preserves its historical callable contracts."""

import importlib.util
import re
from pathlib import Path
from types import SimpleNamespace

import pytest

from process.provider_directory_cms_callable_custody import _body_digest

_VERSIONS = Path(__file__).parents[1] / "alembic/versions"


def _load(prefix):
    (path,) = _VERSIONS.glob(prefix + "*.py")
    spec = importlib.util.spec_from_file_location("archive_migration_" + prefix, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _historical_bodies():
    statements = []
    receipt = _load("20260930100000")
    receipt.op = SimpleNamespace(execute=statements.append)
    receipt._create_archive_check('"snapshot_schema"')
    counter = _load("20260930120000")
    counter.op = SimpleNamespace(execute=statements.append)
    counter._schema = lambda: '"snapshot_schema"'
    counter.upgrade()
    return {
        re.search(r'CREATE FUNCTION "snapshot_schema"\.(\w+)', statement)[1]: re.search(
            r"AS (\$\w*\$)(.*)\1", statement, re.S
        )[2]
        for statement in statements
        if "CREATE FUNCTION" in statement
    }


@pytest.mark.parametrize(
    "name", ["cms_native_input_advance", "cms_native_input_revision_guard", "cms_serving_archive_result_matches"]
)
@pytest.mark.parametrize("schema", ["snapshot_schema", "another_schema"])
def test_forward_repair_matches_authentic_historical_body_and_exact_new_contract(name, schema):
    migration = _load("20261019130000")
    body = (
        _historical_bodies()[name]
        .replace('"snapshot_schema"', f'"{schema}"')
        .replace("'snapshot_schema'", f"'{schema}'")
    )
    contract = next(item for item in migration._CONTRACTS if item[0] == name)
    assert _body_digest(body, schema) == contract[1]
    for previous, current in migration._replacements(schema)[name]:
        assert body.count(previous) == 1
        body = body.replace(previous, current)
    assert _body_digest(body, schema) == contract[2]
    if name == "cms_serving_archive_result_matches":
        assert "xmin" not in body and "pg_locks" not in body
        assert "mutation_xid=pg_current_xact_id()" in body
    elif name == "cms_native_input_advance":
        assert body.count("pg_current_xact_id()") == 1
        assert "SET revision=revision+1,mutation_xid=" in body
    else:
        assert "pg_trigger_depth()=2" in body
        assert "AND NEW.mutation_xid=pg_current_xact_id() THEN" in body


def test_forward_upgrade_uses_one_existing_counter_column_and_three_existing_functions(monkeypatch):
    migration = _load("20261019130000")
    statements = []
    migration.op = SimpleNamespace(execute=statements.append)
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "snapshot_schema")
    migration.upgrade()
    assert migration.down_revision == "20261019030000_provider_directory_role_schema"
    assert len(statements) == 6
    assert "ACCESS EXCLUSIVE MODE NOWAIT" in statements[2]
    assert "SHARE ROW EXCLUSIVE MODE NOWAIT" in statements[3]
    assert "ADD COLUMN mutation_xid pg_catalog.xid8" in statements[4]
    assert (
        "SET DEFAULT" not in statements[4]
        and 'UPDATE "snapshot_schema"' not in statements[4].split("DO $migration$")[0]
    )
    for statement in statements[4:]:
        assert "to_jsonb(updated)-'prosrc'" in statement
        assert statement.count("cms_archive_guard_function_set_changed") == 2
        assert "pg_get_functiondef(original.oid)" in statement
        assert "CREATE FUNCTION" not in statement and "CREATE TRIGGER" not in statement
    with pytest.raises(RuntimeError, match="cms_archive_transaction_guard_downgrade_unsupported"):
        migration.downgrade()
