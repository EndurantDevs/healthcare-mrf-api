# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import importlib.util
from pathlib import Path
from unittest.mock import Mock


MIGRATION_PATH = (
    Path(__file__).resolve().parents[1]
    / "alembic/versions/20260904163000_provider_directory_exact_guard_scope.py"
)
SCHEMA = "provider_directory_exact_guard_scope_test"


def _migration():
    spec = importlib.util.spec_from_file_location(
        "provider_directory_exact_guard_scope_migration",
        MIGRATION_PATH,
    )
    assert spec is not None and spec.loader is not None
    migration = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(migration)
    return migration


def _normalized(value: str) -> str:
    return " ".join(value.split())


def test_guard_skips_only_unrelated_parent_rows() -> None:
    migration = _migration()
    rooted = migration._rooted()
    scoped = _normalized(
        migration._logical_current_guard_sql(SCHEMA, scoped=True)
    )
    predecessor = _normalized(
        migration._logical_current_guard_sql(SCHEMA, scoped=False)
    )

    assert migration.revision == (
        "20260904163000_provider_directory_exact_guard_scope"
    )
    assert migration.down_revision == "20260903160000_plan_pricing_state_scan"
    assert f"TG_TABLE_NAME = '{rooted._DATASET}'" in scoped
    assert "TG_OP = 'INSERT' AND NEW.endpoint_id NOT IN" in scoped
    assert "TG_OP = 'UPDATE' AND OLD.endpoint_id NOT IN" in scoped
    assert "TG_OP = 'DELETE' AND OLD.endpoint_id NOT IN" in scoped
    assert rooted._LEGACY_ENDPOINT_ID in scoped
    assert rooted._ROOTED_ENDPOINT_ID in scoped
    assert scoped.index("TG_TABLE_NAME") < scoped.index("pg_advisory_xact_lock")
    assert "TG_TABLE_NAME" not in predecessor
    assert scoped.count("RETURN NULL") == predecessor.count("RETURN NULL") + 3
    assert migration._function_body_md5(scoped) != migration._function_body_md5(
        predecessor
    )


def test_upgrade_and_downgrade_replace_the_guard_under_table_locks(
    monkeypatch,
) -> None:
    migration = _migration()
    operation = Mock()
    monkeypatch.setattr(migration, "op", operation)
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", SCHEMA)
    monkeypatch.delenv("DB_SCHEMA", raising=False)

    migration.upgrade()
    upgrade_statements = [
        call.args[0] for call in operation.execute.call_args_list
    ]
    assert upgrade_statements[0] == "SET LOCAL lock_timeout = '5s';"
    assert upgrade_statements[1].startswith("LOCK TABLE ")
    assert upgrade_statements[1].endswith(" IN ACCESS EXCLUSIVE MODE;")
    assert "TG_TABLE_NAME" in " ".join(upgrade_statements)
    assert "expected.trigger_name::name" in upgrade_statements[2]
    assert "trigger_row.tgqual IS NULL" in upgrade_statements[2]
    assert "function_language.lanname = 'plpgsql'" in upgrade_statements[2]
    assert upgrade_statements[-2].startswith("REVOKE ALL ON FUNCTION ")
    assert "expected.trigger_name::name" in upgrade_statements[-1]

    operation.reset_mock()
    migration.downgrade()
    downgrade_statements = [
        call.args[0] for call in operation.execute.call_args_list
    ]
    assert "TG_TABLE_NAME" not in downgrade_statements[3]
    assert "trigger_row.tgoldtable IS NULL" in downgrade_statements[2]
    assert downgrade_statements[-2].startswith("REVOKE ALL ON FUNCTION ")
    assert "expected.trigger_name::name" in downgrade_statements[-1]
