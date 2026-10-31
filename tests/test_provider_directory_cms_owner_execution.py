# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Refuse unbound SQL changes before mutation and preserve caller-owned transactions."""

import asyncio
from contextlib import asynccontextmanager
from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import provider_directory_cms_owner_execution as execution
from tests.test_provider_directory_cms_owner_permissions import _plan
from tests.test_provider_directory_cms_owner_permissions import owner_case as owner_case
from tests.test_provider_directory_cms_owner_postimage import structure_case as structure_case


def test_native_owner_substitution_merges_duplicate_edges_without_losing_readers():
    edges = [[1, 1, "SELECT", False], [2, 1, "SELECT", False], [3, 1, "SELECT", False]]
    assert execution._owner_edges(edges, 1, 2) == {(2, 2, "SELECT", False), (3, 2, "SELECT", False)}
    assert execution._owner_edges([[4, *edge] for edge in edges], 1, 2) == {
        (4, 2, 2, "SELECT", False),
        (4, 3, 2, "SELECT", False),
    }


def test_surgical_delta_keeps_independent_reader_grantors():
    before_edges = [[3, 9, "SELECT", False], [1, 2, "INSERT", False]]
    desired_edges = [[3, 9, "SELECT", False], [1, 2, "SELECT", False]]
    assert execution._acl_statements(
        before_edges, desired_edges, owner=2, resource='TABLE "catalog"."root"', roles={1: "writer"}
    ) == [
        'REVOKE INSERT ON TABLE "catalog"."root" FROM "writer"',
        'GRANT SELECT ON TABLE "catalog"."root" TO "writer"',
    ]


@pytest.mark.parametrize("grantor,grantable", [(9, False), (2, True)])
@pytest.mark.parametrize("direction", ["grant", "revoke"])
def test_unowned_or_grantable_changes_refuse(grantor, grantable, direction):
    edges = [[1, grantor, "SELECT", grantable]]
    before, after = ([], edges) if direction == "grant" else (edges, [])
    with pytest.raises(ValueError, match="grantor_transition_unsupported"):
        execution._acl_statements(before, after, owner=2, resource='TABLE "catalog"."root"', roles={1: "writer"})


def test_column_rights_and_public_function_execution_have_distinct_native_sql():
    assert execution._acl_statements(
        [],
        [[1, 3, 2, "UPDATE", False]],
        owner=2,
        resource='TABLE "catalog"."root"',
        roles={3: "writer"},
        columns={1: "select"},
    ) == ['GRANT UPDATE("select") ON TABLE "catalog"."root" TO "writer"']
    assert execution._acl_statements(
        [[0, 2, "EXECUTE", False]], [], owner=2, resource='FUNCTION "catalog"."helper"()', roles={}
    ) == ['REVOKE EXECUTE ON FUNCTION "catalog"."helper"() FROM PUBLIC']


def test_complete_table_plan_uses_real_acl_renderer(owner_case):
    case = owner_case
    plan = _plan(case)
    roles_by_oid = {int(role["oid"]): role["rolname"] for role in case.inventory["roles"]["roles"]}
    statements = execution._table_statements(case.inventory, plan, roles_by_oid)
    assert statements and any(statement.startswith("ALTER TABLE ONLY ") for statement in statements)
    assert any("UPDATE(" in statement for statement in statements)
    assert all(statement.startswith(("ALTER TABLE ONLY ", "GRANT ", "REVOKE ")) for statement in statements)


@pytest.mark.asyncio
async def test_complete_routine_plan_uses_real_acl_renderer(owner_case):
    case = owner_case
    plan = _plan(case)
    roles_by_oid = {int(role["oid"]): role["rolname"] for role in case.inventory["roles"]["roles"]}
    rows = [
        dict(oid=row["oid"], nspname=row["nspname"], proname=row["proname"], arguments="")
        for row in plan["routines"]
        if row["nspname"] != "pg_catalog"
    ]
    connection = SimpleNamespace(fetch=AsyncMock(return_value=rows), execute=AsyncMock())
    statements = await execution._routine_statements(connection, case.inventory, plan, roles_by_oid)
    assert statements and any(statement.startswith("ALTER FUNCTION ") for statement in statements)
    assert all(statement.startswith(("ALTER FUNCTION ", "GRANT ", "REVOKE ")) for statement in statements)
    connection.execute.assert_not_awaited()


@pytest.mark.parametrize("fault", ["missing", "duplicate", "substitution"])
@pytest.mark.asyncio
async def test_routine_identity_failures_are_read_only(owner_case, fault):
    plan = _plan(owner_case)
    selected_routines = [row for row in plan["routines"] if row["nspname"] != "pg_catalog"]
    rows = [
        dict(oid=row["oid"], nspname=row["nspname"], proname=row["proname"], arguments="") for row in selected_routines
    ]
    rows.sort(key=lambda row: row["oid"])
    if fault == "missing":
        rows.pop()
    elif fault == "duplicate":
        rows.append(rows[-1])
    else:
        rows[0]["proname"] = "substitute"
    connection = SimpleNamespace(fetch=AsyncMock(return_value=rows), execute=AsyncMock())
    with pytest.raises(ValueError, match="routine_identity_changed"):
        await execution._routine_statements(connection, owner_case.inventory, plan, {})
    connection.execute.assert_not_awaited()


@pytest.mark.parametrize("positions", [[1], [2, 1], [1, 1], [1, 2, 3]])
@pytest.mark.asyncio
async def test_acl_expansion_requires_exact_ordinals(positions):
    connection = SimpleNamespace(
        fetch=AsyncMock(return_value=[dict(position=value, edges="[]") for value in positions])
    )
    with pytest.raises(ValueError, match="acl_expansion_changed"):
        await execution._expanded_acls(connection, [{"raw_acl": None}, {"raw_acl": []}])


@pytest.mark.parametrize("section", ["canonical", "private", "routines", "roles"])
@pytest.mark.asyncio
async def test_changed_protected_preimage_refuses_before_transport(owner_case, section):
    case = owner_case
    damaged = deepcopy(case.inventory)
    damaged[section] = {} if isinstance(damaged[section], list) else []
    connection = SimpleNamespace(execute=AsyncMock(), fetch=AsyncMock(), fetchrow=AsyncMock())
    with pytest.raises((ValueError, TypeError), match="inventory|changed"):
        await execution.apply_cms_common_owner_permissions(
            connection,
            inventory=damaged,
            storage_binding=case.storage,
            policy=case.policy,
            cms_profile=case.profile,
            source_document=case.source,
            deadline=asyncio.get_running_loop().time() + 2,
        )
    connection.execute.assert_not_awaited()
    connection.fetch.assert_not_awaited()
    connection.fetchrow.assert_not_awaited()


def test_complete_raw_family_preserves_unplanned_descendants(structure_case):
    case = structure_case
    before, after = deepcopy(execution._relations(case.before)), deepcopy(execution._relations(case.after))
    unrelated = deepcopy(before[0])
    unrelated.update(oid=999999, relname="unplanned_child")
    before.append(unrelated)
    after.append(deepcopy(unrelated))
    assert execution._require_family(before, after, case.before, case.after, case.plan) is None
    after[-1]["native"]["triggers"].append({"changed": True})
    with pytest.raises(ValueError, match="unplanned_descendant_changed"):
        execution._require_family(before, after, case.before, case.after, case.plan)


@pytest.mark.parametrize("field", ["constraints", "rules", "dependencies", "triggers"])
def test_raw_parent_fields_omitted_by_fixed_projection_stay_exact(structure_case, field):
    case = structure_case
    before, after = deepcopy(execution._relations(case.before)), deepcopy(execution._relations(case.after))
    after[0]["native"][field].append({"unexpected": "change"})
    with pytest.raises(ValueError, match="family_structure_changed"):
        execution._require_family(before, after, case.before, case.after, case.plan)


@pytest.mark.parametrize("fault", [None, "database", "ddl", "execute"])
@pytest.mark.asyncio
async def test_scope_recheck_and_failures_never_commit_or_reopen(monkeypatch, owner_case, fault):
    case = owner_case
    recheck = AsyncMock()

    @asynccontextmanager
    async def custody_scope(*arguments, **keywords):
        yield recheck

    monkeypatch.setattr(execution.reverse, "cms_reverse_hook_custody", custody_scope)
    postimage_by_field = {"verified": "data"}
    failure = RuntimeError("original operation failure")
    operator = AsyncMock(return_value=postimage_by_field, side_effect=failure if fault == "execute" else None)
    monkeypatch.setattr(execution, "_execute", operator)
    connection = SimpleNamespace(
        fetchrow=AsyncMock(
            return_value={"oid": 999 if fault == "database" else case.storage.database_oid, "datdba": 10}
        ),
        fetchval=AsyncMock(return_value=fault != "ddl"),
        execute=AsyncMock(),
        commit=AsyncMock(),
        transaction=AsyncMock(),
    )
    arguments_by_field = dict(
        inventory=case.inventory,
        storage_binding=case.storage,
        policy=case.policy,
        cms_profile=case.profile,
        source_document=case.source,
        deadline=asyncio.get_running_loop().time() + 2,
    )
    if fault:
        with pytest.raises((RuntimeError, ValueError)) as caught:
            await execution.apply_cms_common_owner_permissions(connection, **arguments_by_field)
        if fault == "execute":
            assert caught.value is failure
        else:
            operator.assert_not_awaited()
        recheck.assert_not_awaited()
    else:
        assert (
            await execution.apply_cms_common_owner_permissions(connection, **arguments_by_field) is postimage_by_field
        )
        recheck.assert_awaited_once()
    connection.commit.assert_not_awaited()
    connection.transaction.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("outer_seconds", [0.05, 15])
async def test_admin_clock_caps_first_exclusive_lock_and_caller_commit(
    monkeypatch, owner_case, structure_case, outer_seconds
):
    from process import provider_directory_cms_owner_role_rights as role_rights

    case = owner_case
    inventory, plan = structure_case.before, structure_case.plan
    monkeypatch.setattr(execution.custody, "_lock_allocation_inventory", AsyncMock())
    monkeypatch.setattr(execution.profiles, "_collect_profile", AsyncMock(return_value=inventory))
    monkeypatch.setattr(execution, "_family", AsyncMock(return_value=[]))
    monkeypatch.setattr(role_rights, "expected_cms_owner_roles", AsyncMock(return_value={}))
    monkeypatch.setattr(execution, "_expanded_acls", AsyncMock(return_value=[]))
    monkeypatch.setattr(execution, "_table_statements", lambda *arguments: [])
    monkeypatch.setattr(execution, "_routine_statements", AsyncMock(return_value=[]))
    loop = asyncio.get_running_loop()
    original_deadline = loop.time() + outer_seconds
    with pytest.raises(TimeoutError):
        async with asyncio.timeout_at(original_deadline) as commit_clock:

            async def acquire(query):
                assert query.endswith("IN ACCESS EXCLUSIVE MODE NOWAIT")
                assert loop.time() < commit_clock.when() <= min(original_deadline, loop.time() + 3)

            connection = SimpleNamespace(execute=AsyncMock(side_effect=acquire))
            await execution._mutate_owner(
                connection,
                inventory,
                case.storage,
                case.policy,
                case.profile,
                plan,
                min(original_deadline, loop.time() + 3),
                commit_clock=commit_clock,
            )
            connection.execute.assert_awaited_once()
            # Model a stalled caller COMMIT after the helper has already returned.
            await asyncio.Future()
    assert commit_clock.expired()
