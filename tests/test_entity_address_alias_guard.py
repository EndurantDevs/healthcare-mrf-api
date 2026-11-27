# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Authority and ordinary-writer contracts for the protected alias generation guard."""

import hashlib
import re
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from process import address_numeric_grid_alias_store as alias_store
from process import entity_address_alias_guard as guard
from tests.test_address_numeric_grid_alias import _load_migration


def _immutable_body():
    return re.search(
        r"addr_alias_immutable_v1\(\).*?AS \$\$(.*?)\$\$;", _load_migration()._alias_schema_sql("example"), re.S
    ).group(1)


def _session(rows=()):
    result = Mock()
    result.mappings.return_value.all.return_value = list(rows)
    return SimpleNamespace(execute=AsyncMock(return_value=result), scalar=AsyncMock(return_value=False))


def test_migration_preserves_immutable_semantics_and_installs_trigger_only_code():
    assert hashlib.sha256(_immutable_body().encode()).hexdigest() == guard.IMMUTABLE_SOURCE_SHA256
    statements = guard.generation_guard_statements("example")
    assert sum("SECURITY DEFINER SET search_path=pg_catalog" in statement for statement in statements) == 3
    assert sum("ENABLE ALWAYS TRIGGER" in statement for statement in statements) == 4
    assert sum("REVOKE ALL ON FUNCTION" in statement for statement in statements) == 4
    assert not any(" OWNER TO " in statement or "GRANT " in statement for statement in statements)
    for body in guard.generation_function_bodies("example").values():
        assert 'UPDATE "example"."address_alias_state_v1"' in body
        assert "pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtext(" in body
        assert "IF NOT FOUND" in body


def test_guard_sql_rejects_untrusted_namespace_input():
    with pytest.raises(guard.alias.EntityAddressSnapshotAliasError):
        guard.generation_guard_statements("not-a-schema")


async def test_ordinary_alias_state_read_needs_no_update_privilege():
    session = _session()
    session.execute.return_value.first.return_value = SimpleNamespace(
        schema_version=2, active_ruleset_version=1, generation=7
    )
    assert await alias_store._alias_state(session, schema="example") == (2, 1, 7)
    assert "FOR UPDATE" not in str(session.execute.await_args.args[0])


def _trigger_rows():
    body_by_function = {**guard.generation_function_bodies("example"), "addr_alias_immutable_v1": _immutable_body()}
    return [
        {
            "oid": index + 10,
            "function_oid": index + 20,
            "proname": name,
            "tgname": shape[0],
            "tgtype": shape[1],
            "tgoldtable": shape[2],
            "tgnewtable": shape[3],
            "prosecdef": shape[4],
            "prosrc": body_by_function[name],
            "tgenabled": "A",
            "valid": True,
            "relname": "address_alias_v1",
        }
        for index, (name, shape) in enumerate(guard.GUARDS.items())
    ]


async def test_exact_trigger_catalog_is_accepted_and_binds_function_identity():
    rows = _trigger_rows()
    assert await guard._trigger_catalog(_session(rows), "example", [1, 2]) == rows


@pytest.mark.parametrize(
    "field,value",
    [
        ("valid", False),
        ("tgenabled", "O"),
        ("tgtype", 4),
        ("tgnewtable", "other_rows"),
        ("prosecdef", True),
        ("prosrc", "BEGIN RETURN NEW; END;"),
    ],
)
async def test_changed_guard_contract_is_refused(field, value):
    rows = _trigger_rows()
    rows[0][field] = value
    with pytest.raises(guard.alias.EntityAddressSnapshotAliasError, match="trigger authority differs"):
        await guard._trigger_catalog(_session(rows), "example", [1, 2])


def _relation_rows():
    return [
        {
            "oid": index + 1,
            "relname": name,
            "relkind": "r",
            "relpersistence": "p",
            "relrowsecurity": False,
            "relforcerowsecurity": False,
            "relispartition": False,
            "unsafe": False,
        }
        for index, name in enumerate((guard.alias._STATE_TABLE, guard.alias._ALIAS_TABLE))
    ]


@pytest.mark.parametrize("changed", ["rules", "row_security"])
async def test_ordinary_destructive_authority_is_rejected_before_reading_aliases(changed):
    rows = _relation_rows()
    rows[0]["unsafe" if changed == "rules" else "relrowsecurity"] = True
    with pytest.raises(guard.alias.EntityAddressSnapshotAliasError, match="relation authority is unavailable"):
        await guard._relation_catalog(_session(rows), "example")


async def test_relation_catalog_reads_no_owner_file_or_role_membership(monkeypatch):
    session = _session(_relation_rows())
    monkeypatch.setattr(guard.alias, "_require_relation_shape", AsyncMock())
    assert await guard._relation_catalog(session, "example") == _relation_rows()
    sql = str(session.execute.await_args.args[0])
    for removed in ("relowner", "nspowner", "filenode", "pg_has_role", "pg_roles"):
        assert removed not in sql


async def test_source_defined_state_expressions_are_accepted():
    session = _session()
    session.execute.side_effect = [
        Mock(all=Mock(return_value=guard.STATE_CHECK_CONTRACT)),
        Mock(all=Mock(return_value=guard.STATE_DEFAULT_CONTRACT)),
    ]
    await guard._require_state_expressions(session, 1)
    session.scalar.assert_awaited_once()


@pytest.mark.parametrize("changed", ["extra_check", "check_validation", "default", "missing_default"])
async def test_state_expression_contract_is_closed_even_for_builtin_expressions(changed):
    checks = list(guard.STATE_CHECK_CONTRACT)
    defaults = list(guard.STATE_DEFAULT_CONTRACT)
    if changed == "extra_check":
        checks.append(("benign_extra", "CHECK ((generation >= -1))", True, False, False))
    elif changed == "check_validation":
        checks[0] = (*checks[0][:2], False, False, False)
    elif changed == "default":
        defaults[3] = ("generation", "abs(0)")
    else:
        defaults.pop()
    session = _session()
    session.execute.side_effect = [Mock(all=Mock(return_value=checks)), Mock(all=Mock(return_value=defaults))]
    with pytest.raises(guard.alias.EntityAddressSnapshotAliasError, match="state expressions are unsupported"):
        await guard._require_state_expressions(session, 1)
    session.scalar.assert_not_awaited()


@pytest.mark.parametrize("is_guarded", [False, True])
async def test_capture_selects_guarded_read_lock_or_legacy_share(monkeypatch, is_guarded):
    session = _session()
    session.scalar.return_value = is_guarded
    require_guard = AsyncMock()
    monkeypatch.setattr(guard, "require_entity_address_alias_guard", require_guard)
    await guard.lock_entity_address_alias_capture(session, schema="example")
    statements = [str(call.args[0]) for call in session.execute.await_args_list]
    assert "pg_advisory_xact_lock" in statements[0]
    assert statements[1].endswith(" IN ACCESS SHARE MODE")
    if is_guarded:
        assert len(statements) == 2
        require_guard.assert_awaited_once_with(session, schema="example")
    else:
        assert statements[3].endswith(" IN SHARE MODE")
        require_guard.assert_not_awaited()
    statement, parameters = session.scalar.await_args.args
    assert "prosecdef" in str(statement) and "pg_roles" not in str(statement)
    assert parameters == {"function": '"example".addr_alias_generation_after_insert_v1()'}


async def test_full_capture_refuses_changed_guard_before_reading_state(monkeypatch):
    session = _session()
    session.in_transaction = lambda: True
    session.scalar.return_value = True
    require_guard = AsyncMock(side_effect=guard.alias.EntityAddressSnapshotAliasError("guard changed"))
    read_state = AsyncMock()
    monkeypatch.setattr(guard, "require_entity_address_alias_guard", require_guard)
    monkeypatch.setattr(guard.alias, "_alias_state", read_state)
    with pytest.raises(guard.alias.EntityAddressSnapshotAliasError, match="guard changed"):
        await guard.alias.capture_entity_address_alias_semantic_receipt(session, schema_name="example")
    require_guard.assert_awaited_once_with(session, schema="example")
    read_state.assert_not_awaited()


def _catalog_result(rows):
    result = Mock()
    result.all.return_value = rows
    result.mappings.return_value.all.return_value = rows
    return result


async def test_guard_checks_shape_state_and_bodies_without_privilege_audit(monkeypatch):
    session = _session()
    session.execute.side_effect = [
        _catalog_result(_relation_rows()),
        _catalog_result(guard.STATE_CHECK_CONTRACT),
        _catalog_result(guard.STATE_DEFAULT_CONTRACT),
        _catalog_result(_trigger_rows()),
    ]
    relation_shape = AsyncMock()
    monkeypatch.setattr(guard.alias, "_require_relation_shape", relation_shape)
    assert await guard.require_entity_address_alias_guard(session, schema="example") is None
    assert [call.kwargs["table_name"] for call in relation_shape.await_args_list] == [
        guard.alias._STATE_TABLE,
        guard.alias._ALIAS_TABLE,
    ]
    # Only the state-expression probe remains; no ordinary-role privilege query runs.
    assert session.scalar.await_count == 1
    trigger_sql = str(session.execute.await_args_list[-1].args[0])
    assert "proowner" not in trigger_sql and "aclexplode" not in trigger_sql


async def test_provisional_capture_cannot_skip_relation_shape(monkeypatch):
    session = _session()
    session.in_transaction = lambda: True
    shape = AsyncMock(side_effect=guard.alias.EntityAddressSnapshotAliasError("shape differs"))
    attestation = AsyncMock()
    monkeypatch.setattr(guard.alias, "_require_relation_shape", shape)
    monkeypatch.setattr(guard, "lock_entity_address_alias_capture", attestation)
    with pytest.raises(guard.alias.EntityAddressSnapshotAliasError, match="shape differs"):
        await guard.alias._capture_alias_semantic_receipt(session, schema_name="example", provisional=True)
    attestation.assert_not_awaited()
    assert str(session.execute.await_args_list[1].args[0]).endswith("IN ACCESS SHARE MODE")
