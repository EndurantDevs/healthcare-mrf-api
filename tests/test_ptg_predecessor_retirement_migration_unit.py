# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import importlib.util
from pathlib import Path

from db import migration_ptg2_predecessor_retirement_audit as migration


MIGRATION_PATH = (
    Path(__file__).resolve().parents[1]
    / "alembic"
    / "versions"
    / "20260727130000_ptg2_predecessor_retirement_audit.py"
)


class _Operations:
    def __init__(self):
        self.statements: list[str] = []

    def execute(self, statement: str) -> None:
        self.statements.append(statement)


def _load_migration_wrapper():
    module_spec = importlib.util.spec_from_file_location(
        "ptg2_predecessor_retirement_audit_migration",
        MIGRATION_PATH,
    )
    assert module_spec is not None and module_spec.loader is not None
    wrapper = importlib.util.module_from_spec(module_spec)
    module_spec.loader.exec_module(wrapper)
    return wrapper


def test_migration_wrapper_uses_current_head_and_shared_schema(monkeypatch):
    wrapper = _load_migration_wrapper()
    calls: list[tuple[str, object, str]] = []
    operations = object()
    monkeypatch.setattr(wrapper, "op", operations)
    monkeypatch.setattr(
        wrapper,
        "resolve_ptg2_schema",
        lambda: "tenant_shared",
    )
    monkeypatch.setattr(
        wrapper,
        "install_predecessor_retirement_audit",
        lambda received_operations, schema: calls.append(
            ("upgrade", received_operations, schema)
        ),
    )
    monkeypatch.setattr(
        wrapper,
        "uninstall_predecessor_retirement_audit",
        lambda received_operations, schema: calls.append(
            ("downgrade", received_operations, schema)
        ),
    )

    wrapper.upgrade()
    wrapper.downgrade()

    assert wrapper.revision == (
        "20260727130000_ptg2_predecessor_retirement_audit"
    )
    assert wrapper.down_revision == (
        "20260727120000_provider_profile_facts"
    )
    assert calls == [
        ("upgrade", operations, "tenant_shared"),
        ("downgrade", operations, "tenant_shared"),
    ]


def test_audit_install_is_standalone_strict_and_immutable():
    operations = _Operations()

    migration.install_predecessor_retirement_audit(
        operations,
        'tenant"x',
    )

    ddl = "\n".join(operations.statements)
    assert (
        'CREATE TABLE "tenant""x"."ptg2_predecessor_retirement_audit"' in ddl
    )
    assert "REFERENCES" not in ddl
    assert "cleared_source_pointer_count = 1" in ddl
    assert "cleared_plan_pointer_count > 0" in ddl
    assert "rollback_pin_mode = 'owned'" in ddl
    assert "deleted_rollback_pin_count = 1" in ddl
    assert "rollback_pin_mode = 'absent'" in ddl
    assert "rollback_owner_id IS NULL" in ddl
    assert "deleted_rollback_pin_count = 0" in ddl
    assert "BEFORE UPDATE OR DELETE" in ddl
    assert "BEFORE TRUNCATE" in ddl
    assert ddl.count("ENABLE ALWAYS TRIGGER") == 2
    assert "PTG2_PREDECESSOR_RETIREMENT_AUDIT_IMMUTABLE" in ddl


def test_audit_uninstall_refuses_evidence_then_drops_guard_and_table():
    operations = _Operations()

    migration.uninstall_predecessor_retirement_audit(
        operations,
        "tenant",
    )

    ddl = "\n".join(operations.statements)
    assert "LOCK TABLE" in operations.statements[0]
    assert "PTG2_PREDECESSOR_RETIREMENT_AUDIT_DOWNGRADE_REFUSED" in ddl
    assert "DROP TRIGGER" in ddl
    assert "DROP FUNCTION" in ddl
    assert operations.statements[-1].startswith("DROP TABLE")
