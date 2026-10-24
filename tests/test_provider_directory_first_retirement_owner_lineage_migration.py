# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Extend only authenticated first-acquisition lineage controls."""

from pathlib import Path
from types import SimpleNamespace

import pytest
from alembic.config import Config
from alembic.script import ScriptDirectory

from db import migration_provider_directory_first_retirement_owner_lineage as owner_lineage

REVISION = "20261024050000_provider_directory_first_retirement_owner_lineage"
PREDECESSOR = "20261023220000_merge_directory_pricing_controls"


def _migration():
    root = Path(__file__).resolve().parents[1]
    config = Config(str(root / "alembic.ini"))
    config.set_main_option("script_location", str(root / "alembic"))
    return ScriptDirectory.from_config(config).get_revision(REVISION).module


def test_first_owner_lineage_preserves_all_checks_except_first_candidate_leaf_position():
    for old, new in owner_lineage.replacements("owner_lineage_test"):
        assert new.count("pg_catalog.bool_and(parent.previous_dataset_id IS NULL) OR") == 2
        assert "WHERE (lineage_walk.import_row).run_id = parent.import_run_id\n            ) = 1" in new
        assert "lineage_edges.child_count > 1" in new
        assert "lineage_edges.invalid_edge" in new
        assert "lineage_edges.run_id = parent.import_run_id" in new
        assert "pg_catalog.max(lineage_walk.depth)" in new
        assert "'provider-directory-fhir'" in new
        assert new.count("CREATE FUNCTION") == old.count("CREATE FUNCTION") == 1


@pytest.mark.parametrize("action", ["upgrade", "downgrade"])
def test_first_owner_migration_preserves_bodies_identity_and_bounded_waits(monkeypatch, action):
    migration = _migration()
    statements = []
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "owner_lineage_test")
    monkeypatch.delenv("DB_SCHEMA", raising=False)
    monkeypatch.setattr(migration, "op", SimpleNamespace(execute=statements.append))
    getattr(migration, action)()
    sql = "\n".join(statements)
    assert migration.down_revision == PREDECESSOR
    assert migration.branch_labels is None and migration.depends_on is None
    assert statements[:2] == ["SET LOCAL lock_timeout='1s'", "SET LOCAL statement_timeout='10s'"]
    assert sql.count("CREATE OR REPLACE FUNCTION") == 2
    assert "provider_directory_first_retirement_function_changed" in sql
    assert "provider_directory_retirement_owner_lineage_control_changed" in sql
    assert "p.proargdefaults IS NULL" in sql
    assert "other.oid<>p.oid" in sql
    assert "search_path=pg_catalog" in sql and "TimeZone=UTC" in sql
    for forbidden in ("DROP FUNCTION", "CREATE TRIGGER", "ALTER TABLE", "UPDATE ", "GRANT ", "REVOKE "):
        assert forbidden not in sql
    if action == "downgrade":
        assert sql.index("LOCK TABLE") < sql.index("IF EXISTS") < sql.index("CREATE OR REPLACE FUNCTION")
        assert (
            'LOCK TABLE "owner_lineage_test"."provider_directory_endpoint_dataset" IN SHARE ROW EXCLUSIVE MODE' in sql
        )
        assert "child.retry_of_run_id=parent.import_run_id" in sql
        assert "provider_directory_retirement_owner_lineage_downgrade_blocked" in sql
    else:
        assert "LOCK TABLE" not in sql


@pytest.mark.parametrize(
    "runtime,legacy,expected", [(None, None, "mrf"), (None, "legacy", "legacy"), ("runtime", None, "runtime")]
)
def test_first_owner_migration_uses_existing_schema_contract(monkeypatch, runtime, legacy, expected):
    for name, value in (("HLTHPRT_DB_SCHEMA", runtime), ("DB_SCHEMA", legacy)):
        if value is None:
            monkeypatch.delenv(name, raising=False)
        else:
            monkeypatch.setenv(name, value)
    assert _migration()._schema() == expected


def test_first_owner_migration_refuses_conflicting_schema(monkeypatch):
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "runtime")
    monkeypatch.setenv("DB_SCHEMA", "different")
    with pytest.raises(RuntimeError, match="must match"):
        _migration().upgrade()
