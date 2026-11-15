# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Only existing lookup keys change; counter and immutable-row semantics survive."""

import importlib.util
from pathlib import Path
from types import SimpleNamespace

import pytest

from process import provider_directory_cms_native_inputs as inputs
from tests.test_cms_archive_transaction_migration import _historical_bodies, _load


def _migration():
    path = Path(__file__).parents[1] / "alembic/versions/20261114190000_cms_native_input_named_revision.py"
    spec = importlib.util.spec_from_file_location("named_native_revision", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _current_body(name, schema):
    body = (
        _historical_bodies()[name]
        .replace('"snapshot_schema"', f'"{schema}"')
        .replace("'snapshot_schema'", f"'{schema}'")
    )
    for old, new in _load("20261019130000")._replacements(schema)[name]:
        assert body.count(old) == 1
        body = body.replace(old, new)
    return body


@pytest.mark.parametrize("schema", ["snapshot_schema", "another_schema"])
@pytest.mark.parametrize(
    "name", ["cms_native_input_advance", "cms_native_input_revision_guard", "cms_serving_archive_result_matches"]
)
def test_named_revision_replaces_only_existing_predicates_and_archive_location(name, schema):
    migration = _migration()
    previous = _current_body(name, schema)
    updated = previous
    for old, new in migration._replacements(schema)[name]:
        assert updated.count(old) == 1, (name, old)
        updated = updated.replace(old, new)
    if name == "cms_native_input_advance":
        assert updated == inputs._advance_body(schema)
        assert updated == previous.replace(
            "WHERE relation_oid=TG_RELID::bigint;", "WHERE schema_name=TG_TABLE_SCHEMA AND table_name=TG_TABLE_NAME;"
        )
    elif name == "cms_native_input_revision_guard":
        assert updated == previous.replace("AND NEW.relation_oid=OLD.relation_oid ", "")
        assert "pg_trigger_depth()=2" in updated and "NEW.mutation_xid=pg_current_xact_id()" in updated
    else:
        assert "target_oid" not in updated and "relation_oid" not in updated
        assert "mutation_xid=pg_current_xact_id()" in updated
        assert "current_revision=(a->>'to_revision')::bigint" in updated
        assert updated.index("IF prior->'archive'=a") < updated.index("jsonb_typeof(a)")
        assert "WHERE schema_name=$1 AND table_name=''address_archive_v2''" in updated


def test_named_revision_preserves_rows_and_refuses_ambiguous_saved_locations(monkeypatch):
    migration = _migration()
    statements = []
    monkeypatch.setattr(migration, "op", SimpleNamespace(execute=statements.append))
    monkeypatch.setattr(migration, "plan_release_schema", lambda: "snapshot_schema")
    migration.upgrade()
    assert migration.down_revision == "20261114180000_reference_family_named_generation"
    assert "ACCESS EXCLUSIVE MODE NOWAIT" in statements[1]
    assert "GROUP BY schema_name,table_name HAVING count(*)>1" in statements[2]
    assert "pg_get_functiondef" in statements[3] and "EXECUTE definition" in statements[3]
    assert "ADD PRIMARY KEY(schema_name,table_name)" in statements[4]
    assert "DROP COLUMN relation_oid" in statements[4]
    assert all(
        "CREATE TRIGGER" not in str(sql) and not str(sql).startswith("UPDATE ") and "DELETE FROM" not in str(sql)
        for sql in statements
    )
    with pytest.raises(RuntimeError):
        migration.downgrade()


def test_native_fence_requires_the_actual_named_npi_statement_body():
    import re

    migration = _load("20261114160000")
    statements = []
    migration.op = SimpleNamespace(execute=statements.append)
    migration._replace_revision_body("snapshot_schema")
    body = re.search(r"AS \$function\$(.*?)\$function\$", statements[0], re.S)[1]
    assert " ".join(body.split()) == " ".join(inputs._npi_body("snapshot_schema").split())


@pytest.mark.parametrize("name", tuple(_migration()._FUNCTION_BODY_DIGESTS))
def test_existing_serving_and_counter_body_contracts_are_exact_and_replayable(name):
    from process.provider_directory_cms_callable_custody import _body_digest
    from tests.test_provider_directory_cms_callable_custody import _migration_functions

    migration = _migration()
    previous = (
        _current_body(name, "snapshot_schema")
        if name.startswith("cms_native_input_")
        else _migration_functions(named=False)[name][-1]
    )
    updated = previous
    for pair in migration._replacements("snapshot_schema")[name]:
        old, new = pair[:2]
        assert updated.count(old) == (pair[2] if len(pair) == 3 else 1)
        updated = updated.replace(old, new)
    assert (
        tuple(_body_digest(body, "snapshot_schema") for body in (previous, updated))
        == migration._FUNCTION_BODY_DIGESTS[name]
    )
    statement = migration._function_sql("snapshot_schema", names=(name,))
    assert statement.count("EXECUTE definition") == 1 and "ELSIF" in statement
    assert "proowner" not in statement and "CREATE TRIGGER" not in statement


def test_prepared_named_ledger_requires_semantic_shape_without_constraint_name():
    check, key = _migration()._key_sql("snapshot_schema")
    assert "GROUP BY schema_name,table_name HAVING count(*)>1" in check
    assert "ARRAY['schema_name','table_name','revision','mutation_xid']" in check
    assert "ARRAY['schema_name','table_name']" in check and "(revision >= 0)" in check
    assert "pg_get_expr(definition.adbin,definition.adrelid)" in check
    assert "named_pkey" not in check and "IF EXISTS" in key
