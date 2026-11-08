# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""The forward repair changes only two source-guard dispatch expressions."""

import importlib.util
import io
from pathlib import Path

import pytest
from alembic.migration import MigrationContext
from alembic.operations import Operations

from process import provider_directory_cms_callable_custody as custody
from tests.test_provider_directory_cms_callable_custody import _migration_functions


def _migration():
    path = Path(__file__).parents[1] / "alembic/versions/20261016030000_cms_source_guard_argument_types.py"
    spec = importlib.util.spec_from_file_location("source_guard_argument_types", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_forward_revision_targets_only_the_two_existing_source_functions():
    migration = _migration()
    assert migration.revision == "20261016030000_cms_source_guard_argument_types"
    assert migration.down_revision == "20261016010000_cms_registry_source_pair_slot"
    assert [entry[0] for entry in migration._CONTRACTS] == [
        "cms_serving_dataset_transition",
        "cms_serving_source_transition",
    ]


@pytest.mark.parametrize("name", ["cms_serving_dataset_transition", "cms_serving_source_transition"])
def test_authentic_successor_body_requires_text_and_jsonb_without_an_untyped_fallback(name):
    migration = _migration()
    previous_hash, next_hash = next(entry[1:] for entry in migration._CONTRACTS if entry[0] == name)
    body = _migration_functions()[name][-1]
    old_call, new_call = migration._calls("snapshot_schema")
    assert old_call not in body and body.count(new_call) == 1
    assert custody._body_digest(body, "snapshot_schema") == next_hash
    assert custody._body_digest(body.replace(new_call, old_call), "snapshot_schema") == previous_hash
    assert custody._CONTRACTS[name][-1] == next_hash


def test_offline_repair_keeps_native_identity_acl_and_all_nonbody_attributes(monkeypatch):
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "snapshot_schema")
    monkeypatch.setenv("DB_SCHEMA", "snapshot_schema")
    migration = _migration()
    output = io.StringIO()
    context = MigrationContext.configure(dialect_name="postgresql", opts={"as_sql": True, "output_buffer": output})
    with Operations.context(context):
        migration.upgrade()
    sql = output.getvalue()
    assert sql.index("SET LOCAL search_path=pg_catalog") < sql.index("LOCK TABLE") < sql.index("DO $migration$")
    assert '"snapshot_schema"."provider_directory_endpoint_dataset"' in sql
    assert '"snapshot_schema"."provider_directory_source"' in sql
    assert "IN SHARE ROW EXCLUSIVE MODE NOWAIT" in sql
    assert "pg_get_functiondef(original.oid)" in sql
    assert "EXECUTE replace(definition," in sql
    assert "WHERE oid=original.oid" in sql
    assert "(to_jsonb(updated)-'prosrc') IS DISTINCT FROM (to_jsonb(original)-'prosrc')" in sql
    assert "updated.prosrc IS DISTINCT FROM replacement" in sql
    assert not any(word in sql.upper() for word in ("GRANT ", "REVOKE ", "OWNER TO", "CREATE TRIGGER", "DROP "))


def test_repair_refuses_native_drift_and_accepts_exact_replay():
    sql = _migration()._repair_sql("snapshot_schema")
    for error in (
        "cms_source_guard_function_set_changed",
        "cms_source_guard_definition_changed",
        "cms_source_guard_dispatch_changed",
        "cms_source_guard_body_changed",
        "cms_source_guard_replacement_changed",
    ):
        assert "RAISE EXCEPTION '" + error + "'" in sql
    assert "original.proconfig IS DISTINCT FROM ARRAY['search_path=pg_catalog']" in sql
    assert "original.prosecdef OR original.proleakproof" in sql
    assert "original.pronargs<>0 OR original.pronargdefaults<>0 OR original.provariadic<>0" in sql
    assert "ELSIF body_hash=expected.next_hash THEN\n                replacement:=original.prosrc" in sql


def test_schema_mismatch_and_unsafe_identifier_refuse_before_body_repair(monkeypatch):
    migration = _migration()
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "snapshot_schema")
    monkeypatch.setenv("DB_SCHEMA", "other_schema")
    with pytest.raises(RuntimeError, match="same schema"):
        migration.upgrade()
    with pytest.raises(ValueError, match="identifier_invalid"):
        migration._repair_sql("invalid;schema")


def test_downgrade_does_not_restore_unknown_argument_dispatch():
    with pytest.raises(RuntimeError, match="downgrade_unsupported"):
        _migration().downgrade()
