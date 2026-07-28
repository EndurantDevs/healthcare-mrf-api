# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Schema contract tests for immutable frozen source-file bindings."""

from __future__ import annotations

import importlib.util
from pathlib import Path

from db.migration_ptg2_frozen_source_file_binding import (
    install_frozen_source_file_binding,
    uninstall_frozen_source_file_binding,
)


class _Operations:
    def __init__(self) -> None:
        self.statements: list[str] = []

    def execute(self, statement: str) -> None:
        self.statements.append(statement)


def test_binding_schema_is_standalone_exact_and_immutable():
    operations = _Operations()

    install_frozen_source_file_binding(operations, "mrf")

    sql = "\n".join(operations.statements)
    assert 'CREATE TABLE "mrf"."ptg2_frozen_source_file_binding"' in sql
    assert "source_file_import_id varchar(64) PRIMARY KEY" in sql
    assert "internal_run_id = 'ptg2:' || source_file_import_id" in sql
    assert "ptg_frozen_source_file_binding_v1" in sql
    assert "ptg_frozen_rate_file_set_v1" in sql
    assert "frozen_rate_file_count BETWEEN 2 AND 128" in sql
    assert "binding_payload->'plan_ids' = plan_ids" in sql
    assert "BEFORE UPDATE OR DELETE" in sql
    assert "BEFORE TRUNCATE" in sql
    assert sql.count("ENABLE ALWAYS TRIGGER") == 2
    assert "REFERENCES" not in sql


def test_binding_downgrade_refuses_to_erase_evidence():
    operations = _Operations()

    uninstall_frozen_source_file_binding(operations, "mrf")

    sql = "\n".join(operations.statements)
    assert "LOCK TABLE" in sql
    assert "PTG2_FROZEN_SOURCE_FILE_BINDING_DOWNGRADE_REFUSED" in sql
    assert sql.index("DOWNGRADE_REFUSED") < sql.index("DROP TABLE")


def test_binding_migration_uses_reserved_main_chain_revision():
    migration_path = (
        Path(__file__).parents[1]
        / "alembic"
        / "versions"
        / "20260727140000_ptg2_frozen_rate_file_binding.py"
    )
    spec = importlib.util.spec_from_file_location(
        "ptg_frozen_rate_file_binding_migration",
        migration_path,
    )
    assert spec is not None and spec.loader is not None
    migration = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(migration)

    assert migration.revision == "20260727140000_ptg2_frozen_rate_file_binding"
    assert (
        migration.down_revision
        == "20260727130000_ptg2_predecessor_retirement_audit"
    )
