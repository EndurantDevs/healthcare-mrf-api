# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Exact native authority overlays retain provenance and close stale epochs."""

import importlib.util
from pathlib import Path
from types import SimpleNamespace

import pytest


def _migration():
    path = Path(__file__).resolve().parents[1] / "alembic/versions/20261019010000_custom_import_build_continuation.py"
    spec = importlib.util.spec_from_file_location("continuation_migration_test", path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_exact_existing_function_inventory_has_no_new_native_entry_points():
    migration = _migration()
    definitions = list(migration._control_definitions("synthetic_control"))
    assert [name for name, *_ in definitions] == [
        "lock_custom_import_build",
        "lock_custom_import_snapshot_attempt",
        "begin_custom_import_build",
        "custom_import_materialization_budget",
        "lock_custom_import_materialization_set",
        "check_custom_import_materialization_expected",
        "check_custom_import_generation_materialization_authority",
        "guard_custom_import_generation_insert",
        "guard_custom_import_no_change_seal_insert",
    ]
    for _name, _arguments, old, new in definitions:
        assert old != new
        assert "__CONTROL__" not in new and "__SCHEMA__" not in new
        assert "DISABLE TRIGGER" not in new and "CREATE FUNCTION" not in new
    begin = next(new for name, _args, _old, new in definitions if name == "begin_custom_import_build")
    assert "IF l.continuation_id IS NOT NULL OR e.state" in begin


def test_native_epoch_matches_current_lease_exact_retained_scope_and_finite_budget():
    migration = _migration()
    authority = migration._authority()
    for clause in (
        "continuation.continuation_id=l.continuation_id",
        "current_setting('custom_import.continuation_id',true)=l.continuation_id::text",
        "current_setting('custom_import.continuation_nonce_sha256',true)=encode(l.token_sha256,'hex')",
        "continuation.nonce_sha256=l.token_sha256",
        "l.continuation_started_at IS NOT NULL",
        "l.continuation_started_at<continuation.launch_expires_at",
        "isfinite(l.expires_at)",
        "l.expires_at>clock_timestamp()",
        "e.state='running'",
        "b.source_frozen_at IS NOT NULL AND b.plan_complete_at IS NOT NULL",
        "continuation.producer_token_sha256",
        "continuation.original_deadline_at",
        "continuation.base_generation_id",
        "continuation.base_pointer_version",
        "snapshot.producing_token_sha256",
        "snapshot.generation_id IS NULL OR snapshot.generation_id=b.generation_id",
    ):
        assert clause in authority
    assert "continuation.launch_expires_at>clock_timestamp()" not in authority
    assert "decode(current_setting" not in authority
    body = migration._build_body(migration._previous("20261002010000_custom_import_bounded_build"))
    assert "IF NOT EXISTS (" in body
    assert "processing_deadline:=b.build_deadline_at" in body
    assert "processing_deadline:=l.expires_at" in body
    assert "timeout_ms > b.statement_timeout_ms" in body
    assert "least(l.expires_at,processing_deadline)" in body
    assert "l.continuation_id IS NULL AND l.token_sha256 IS DISTINCT FROM b.producing_token_sha256" in body


def test_materialization_preserves_expected_producer_and_never_grants_authority_from_budget():
    definition_by_name = {
        name: new for name, _args, _old, new in _migration()._control_definitions("synthetic_control")
    }
    budget = definition_by_name["custom_import_materialization_budget"]
    assert "This read only sets a statement budget" in budget
    assert "FOR UPDATE" not in budget and "lock_custom_import_build" not in budget
    assert "ELSE '-infinity'::timestamptz END" in budget
    writer = definition_by_name["lock_custom_import_materialization_set"]
    assert "a.token_sha256 IS DISTINCT FROM p_expected_token" in writer
    assert "p_expected_deadline>l.expires_at" in writer
    assert "ORDER BY e.execution_id FOR UPDATE OF e" in writer
    assert "ORDER BY l.execution_id FOR UPDATE OF l" in writer
    assert "OR EXISTS(SELECT 1 FROM" in writer
    assert "custom_import_generation_seal" in writer and "custom_import_no_change_seal" in writer


@pytest.mark.parametrize("name", _migration()._LEAF_NAMES)
def test_leaf_final_check_reuses_existing_gate_without_changing_data_or_validation(name):
    migration = _migration()
    old = migration._leaf_statement(name, "synthetic_control", corrected=False)
    new = migration._leaf_statement(name, "synthetic_control", corrected=True)
    start = old.index("    SELECT e.state,l.fence,l.token_sha256,l.expires_at INTO producer")
    end = old.index(" END IF;", start) + len(" END IF;")
    assert new == old[:start] + '    PERFORM "synthetic_control".lock_custom_import_build(b.build_id);' + old[end:]
    assert "producer_token IS DISTINCT FROM b.producing_token_sha256" not in new
    assert "least(producer.expires_at,b.build_deadline_at)" not in new


def test_refresh_preserves_all_catalog_metadata_and_existing_execute_grants(monkeypatch):
    migration = _migration()
    metadata = dict(oid=101, proowner=10, proacl=["owner=X/owner"], proconfig=["search_path=pg_catalog"])
    before = SimpleNamespace(
        prosrc="old body", metadata=metadata, definition="CREATE OR REPLACE FUNCTION x() AS $$old body$$"
    )
    after = SimpleNamespace(prosrc="new body", metadata=dict(metadata))
    states = iter((before, after))
    calls, writes = [], []
    bind = SimpleNamespace(
        execute=lambda sql, params: calls.append((str(sql), params)) or SimpleNamespace(first=lambda: next(states))
    )
    monkeypatch.setattr(migration.op, "get_bind", lambda: bind)
    monkeypatch.setattr(migration.op, "execute", lambda value: writes.append(str(value)))
    migration._refresh("synthetic_identity", "old body", "new body")
    assert writes == ["CREATE OR REPLACE FUNCTION x() AS $$new body$$"]
    assert calls[0][1]["preserve_execute_grants"] is True
    for clause in (
        "p.prosecdef",
        "p.proowner=owner_table.relowner",
        "NOT p.proisstrict",
        "p.procost=100",
        "acl.grantee=0",
    ):
        assert clause in calls[0][0]


@pytest.mark.parametrize("before", [None, SimpleNamespace(prosrc="unknown", metadata={})])
def test_unknown_function_body_or_identity_fails_closed(monkeypatch, before):
    migration = _migration()
    writes = []
    bind = SimpleNamespace(execute=lambda *_: SimpleNamespace(first=lambda: before))
    monkeypatch.setattr(migration.op, "get_bind", lambda: bind)
    monkeypatch.setattr(migration.op, "execute", writes.append)
    with pytest.raises(RuntimeError, match="function_identity_mismatch"):
        migration._refresh("synthetic_identity", "old", "new")
    assert writes == []


def test_schema_keeps_history_without_record_hooks():
    migration = _migration()
    schema = (migration._RESOURCES / "schema.sql").read_text()
    assert "CREATE TRIGGER" not in schema and "CREATE FUNCTION" not in schema
    assert "custom_import_build_continuation" in schema
    for name in ("custom_import_root_revision", "custom_import_child_revision", "custom_import_family_revision"):
        assert name not in schema
    assert "REFERENCES __CONTROL__.custom_import_build_attempt(build_id) ON DELETE RESTRICT" in schema
    assert "REFERENCES __CONTROL__.custom_import_snapshot_family(family_id) ON DELETE RESTRICT" in schema
    assert "isfinite(expires_at)" in migration._LEASE_CHECK
    with pytest.raises(RuntimeError, match="requires_forward_migration"):
        migration.downgrade()
