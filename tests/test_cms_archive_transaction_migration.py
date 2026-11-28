# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""The forward archive repair preserves its historical callable contracts."""

import importlib.util
import re
from hashlib import sha256
from pathlib import Path
from types import SimpleNamespace

import pytest

_VERSIONS = Path(__file__).parents[1] / "alembic/versions"


def _load(prefix):
    (path,) = _VERSIONS.glob(prefix + "*.py")
    spec = importlib.util.spec_from_file_location("archive_migration_" + prefix, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def body_digest(body, schema):
    """Digest one function body with its schema name masked, as the revisions pin it."""
    canonical = body.replace('"' + schema + '"', '"__schema__"').replace("'" + schema + "'", "'__schema__'")
    return sha256(canonical.encode("utf-8")).hexdigest()


def _migration_functions(*, named=True):
    path = Path(__file__).parents[1] / "alembic/versions/20260930100000_cms_npd_serving_receipt.py"
    spec = importlib.util.spec_from_file_location("serving_receipt_migration", path)
    migration = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(migration)
    statements = []
    migration.op = SimpleNamespace(execute=statements.append)
    for name in (
        "_create_native_readers",
        "_create_table",
        "_create_archive_check",
        "_create_result_check",
        "_create_current_check",
        "_create_snapshot_check",
        "_create_insert_guard",
        "_create_fresh_guard",
        "_create_native_transition_guards",
        "_create_source_transition_guards",
        "_create_truncate_guards",
    ):
        getattr(migration, name)('"snapshot_schema"')
    function_by_name = {}
    for statement in statements:
        match = re.search(
            r'CREATE FUNCTION "snapshot_schema"\.(cms_serving_\w+)\((.*?)\) RETURNS (\w+)\s+'
            r"LANGUAGE (\w+)(.*?)AS \$\$(.*)\$\$",
            statement,
            re.S,
        )
        if match:
            name, arguments, returns, language, options, body = match.groups()
            args = tuple(tuple(arg.strip().split()) for arg in arguments.split(",")) if arguments else ()
            function_by_name[name] = (args, returns, language, "s" if "STABLE" in options else "v", body)
    repair_path = path.with_name("20261016030000_cms_source_guard_argument_types.py")
    repair_spec = importlib.util.spec_from_file_location("source_guard_argument_types", repair_path)
    repair = importlib.util.module_from_spec(repair_spec)
    repair_spec.loader.exec_module(repair)
    old_call, new_call = repair._calls("snapshot_schema")
    for name, previous_hash, next_hash in repair._CONTRACTS:
        *attributes, body = function_by_name[name]
        assert body_digest(body, "snapshot_schema") == previous_hash
        assert body.count(old_call) == 1
        replacement = body.replace(old_call, new_call)
        assert body_digest(replacement, "snapshot_schema") == next_hash
        function_by_name[name] = (*attributes, replacement)
    _repair_archive_body(path, function_by_name)
    if named:
        _apply_named_function_bodies(function_by_name)
    return function_by_name


def _apply_named_function_bodies(function_by_name):
    named = _load("20261114190000")
    for name, pairs in named._replacements("snapshot_schema").items():
        if name not in function_by_name:
            continue
        *attributes, body = function_by_name[name]
        for pair in pairs:
            previous, current = pair[:2]
            assert body.count(previous) == (pair[2] if len(pair) == 3 else 1)
            body = body.replace(previous, current)
        function_by_name[name] = (*attributes, body)


def _repair_archive_body(path, function_by_name):
    archive_path = path.with_name("20261019130000_cms_archive_transaction_guard.py")
    archive_spec = importlib.util.spec_from_file_location("archive_transaction_guard", archive_path)
    archive_guard = importlib.util.module_from_spec(archive_spec)
    archive_spec.loader.exec_module(archive_guard)
    name = "cms_serving_archive_result_matches"
    *attributes, body = function_by_name[name]
    contract = next(item for item in archive_guard._CONTRACTS if item[0] == name)
    assert body_digest(body, "snapshot_schema") == contract[1]
    for previous, current in archive_guard._replacements("snapshot_schema")[name]:
        assert body.count(previous) == 1
        body = body.replace(previous, current)
    assert body_digest(body, "snapshot_schema") == contract[2]
    function_by_name[name] = (*attributes, body)


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
    assert body_digest(body, schema) == contract[1]
    for previous, current in migration._replacements(schema)[name]:
        assert body.count(previous) == 1
        body = body.replace(previous, current)
    assert body_digest(body, schema) == contract[2]
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
