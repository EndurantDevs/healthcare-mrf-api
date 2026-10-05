# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Closed statement guards preserve model keys and historical pin contracts."""

import hashlib
import importlib.util
from io import StringIO
from pathlib import Path

import pytest
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy import PrimaryKeyConstraint, UniqueConstraint

from process import source_profile_result_archive as archive
from process import source_profile_result_pins as pins


def test_historical_pin_statements_keep_their_original_digest():
    assert hashlib.sha256("\n".join(pins.pin_guard_statements("fixture")).encode()).hexdigest() == (
        "d8c97782387576c62ac6e45a391a7c1d057f2bb8592531171ba8ec9ee0f389f4"
    )


def test_attached_collision_keys_cover_each_installed_model_unique_constraint():
    expected_keys_by_table = {
        model.__tablename__: sorted(
            tuple(constraint.columns.keys())
            for constraint in model.__table__.constraints
            if isinstance(constraint, (PrimaryKeyConstraint, UniqueConstraint))
        )
        for model in archive.MODELS
    }
    assert {name: sorted(keys) for name, keys in pins._UNIQUE_KEYS_BY_TABLE.items()} == expected_keys_by_table
    statements = "\n".join(pins.statement_pin_guard_statements("fixture"))
    assert "FOR EACH ROW" not in statements
    assert statements.count("FOR EACH STATEMENT") == 19
    assert statements.count("stored.tableoid<>TG_RELID") == sum(map(len, expected_keys_by_table.values()))
    assert "FROM profile_guard_new incoming" in statements
    assert "EXISTS(SELECT 1 FROM pg_catalog.pg_inherits WHERE inhparent=TG_RELID)" in statements
    assert "source profile attached key conflicts" in statements
    assert "hashtext('profile-run-seal:'" in statements
    assert "md5(" not in statements and "to_jsonb(" not in statements


def test_attachment_seal_guard_uses_old_metadata_and_exact_child_oids():
    statements = tuple(pins.statement_pin_guard_statements("fixture"))
    function = next(
        statement for statement in statements if 'FUNCTION "fixture".provider_profile_attached_pin_guard' in statement
    )
    assert "SET search_path = pg_catalog, pg_temp" in function
    assert "SECURITY DEFINER" not in function and " STABLE" not in function
    assert "FROM profile_pin_old" in function
    assert "authority_json::jsonb->'created_here'='true'::jsonb" in function
    assert "source-profile-attachment.v2" in function
    assert "inherited.inhrelid::text=child.value->>2" in function
    assert function.index("require read committed") < function.index("pg_catalog.pg_inherits")
    assert any(
        "BEFORE TRUNCATE" in statement and '"fixture".provider_profile_source_pin' in statement
        for statement in statements
    )
    assert any("OLD TABLE AS profile_pin_old NEW TABLE AS profile_pin_new" in statement for statement in statements)


@pytest.mark.parametrize("action", ("upgrade", "downgrade"))
def test_statement_migration_compiles_complete_offline_checks(monkeypatch, action):
    path = Path(__file__).resolve().parents[1] / "alembic/versions/20261005030000_source_profile_statement_pins.py"
    spec = importlib.util.spec_from_file_location("statement_pin_migration", path)
    migration = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(migration)
    output = StringIO()
    context = MigrationContext.configure(url="postgresql://", opts={"as_sql": True, "output_buffer": output})
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "fixture")
    monkeypatch.delenv("DB_SCHEMA", raising=False)
    with Operations.context(context):
        getattr(migration, action)()
    sql = output.getvalue()
    assert '"fixture".provider_profile_source_pin IN ACCESS EXCLUSIVE MODE' in sql
    if action == "upgrade":
        assert sql.index("DO $preflight$") < sql.index('FUNCTION "fixture".provider_profile_pinned_run_guard')
        assert "source profile historical pin guard missing' USING ERRCODE='55000'" in sql
        assert "INTO STRICT guard_owner" in sql
        assert "namespace.nspname='fixture'" in sql
        assert "'fixture', guard_owner" in sql
        assert sql.index('FUNCTION "fixture".provider_profile_attached_pin_guard') < sql.index("DO $owner$")
    else:
        assert "pg_catalog.pg_inherits" in sql
        assert "namespace.nspname='fixture'" in sql
        guard = "RAISE EXCEPTION 'source profile attached tables remain' USING ERRCODE='55000'"
        assert sql.index(guard) < sql.index("DROP TRIGGER provider_profile_attached_pin_guard_")
        assert 'DROP FUNCTION "fixture".provider_profile_attached_pin_guard() RESTRICT' in sql
        assert sql.count("DROP TRIGGER provider_profile_attached_pin_guard_") == 3
        assert "FOR EACH ROW" in sql
