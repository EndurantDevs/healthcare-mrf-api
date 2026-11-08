# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Original publisher authority and failure-safe control adoption without PostgreSQL."""

from contextlib import asynccontextmanager
from copy import deepcopy
from functools import partial
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import nppes_public_evidence_control_custody as custody
from process.nppes_public_evidence_writer_contract import NppesPublicEvidenceWriterError
from process.provider_directory_generic_source_policy import GenericSourcePolicy


async def _capture_control_functions(state, policy, names, connection, schema, *, archive_guard_present):
    assert connection.is_in_transaction() and schema == policy.schema
    assert archive_guard_present is (state.guard is not None)
    mutators = tuple(
        deepcopy(record)
        for record in state.functions.values()
        if record["name"] in {custody.callable_contract.ARCHIVE, custody.callable_contract.CHAIN}
    )
    readonly_entries = tuple(
        oid
        for oid, record in state.functions.items()
        if record["name"] in names and record["name"] not in {item["name"] for item in mutators}
    )
    return readonly_entries, mutators


async def _fetch_control_rows(state, policy, catalog_rows, query, *args):
    if query == custody.CATALOG_SQL:
        return catalog_rows
    if query == custody._ROLE_CLOSURE:
        return state.bypass_roles
    if query == custody._CALLABLE_CLOSURE:
        return state.bypass_functions
    if "p.oid=ANY($2::oid[]) AND p.proname=ANY($3::text[])" in query:
        assert args[0] == policy.schema
        return [
            deepcopy(record)
            for oid, record in state.functions.items()
            if oid in args[1] and record["name"] in args[2] and oid not in state.hidden_append_oids
        ]
    if "aclexplode" in query and "p.oid=$1" in query:
        return deepcopy(state.function_grants[args[0]])
    return []


async def _fetch_control_identity(state, query, *args):
    return deepcopy(state.guard if "FROM pg_trigger" in query else state.actor)


async def _fetch_control_value(state, query, *args):
    if "pg_get_userbyid" in query:
        return "namespace_owner"
    if "to_regclass($1)::oid" in query:
        return 100
    if "pg_trigger" in query:
        return state.guard is not None
    if "WHERE NOT EXISTS" in query:
        return state.unsealed
    return False


async def _execute_control_query(state, policy, query, *args):
    state.sql.append(query)
    if query.startswith("DROP TRIGGER"):
        state.guard = None
    for record in state.functions.values():
        if query == f'ALTER FUNCTION {record["signature"]} OWNER TO "{policy.owner_role}"':
            previous = record["owner"]
            record.update(owner=500, owner_role=policy.owner_role, owner_protected=True)
            for grant in state.function_grants[record["oid"]]:
                if grant["grantee"] == previous:
                    grant.update(grantee=500, rolname=policy.owner_role)
        if query.startswith(f"REVOKE EXECUTE ON FUNCTION {record['signature']} FROM "):
            grants = state.function_grants[record["oid"]]
            grants[:] = [
                grant
                for grant in grants
                if query
                != f"REVOKE EXECUTE ON FUNCTION {record['signature']} FROM "
                + ("PUBLIC" if grant["grantee"] == 0 else f'"{grant["rolname"]}"')
                + " CASCADE"
            ]
            record["execute_closed"] = not any(grant["grantee"] != record["owner"] for grant in grants)
    return "OK"


@asynccontextmanager
async def _control_savepoint(state):
    original = deepcopy((state.guard, state.functions, state.function_grants))
    state.savepoints.append("begin")
    try:
        yield
    except BaseException:
        state.guard, state.functions, state.function_grants = original
        state.savepoints.append("rollback")
        raise
    else:
        state.savepoints.append("release")


def _new_control_harness_state(policy):
    state = SimpleNamespace(
        actor={
            "session": "source_publisher",
            "current": "source_owner",
            "native_admin": False,
            "administrator": False,
            "backend": 1234,
            "xid": "5678",
        },
        guard=None,
        bypass_roles=[],
        bypass_functions=[],
        unsealed=False,
        sql=[],
        active=True,
        functions={},
        function_grants={},
        hidden_append_oids=set(),
        savepoints=[],
    )
    catalog_rows = [
        dict(
            relname=name,
            oid=index + 100,
            relowner=500,
            nspname=policy.schema,
            relkind="r",
            relpersistence="p",
            parents=[],
            relispartition=False,
            relrowsecurity=False,
            relforcerowsecurity=False,
            direct_acl_closed=True,
            readers_closed=True,
        )
        for index, name in enumerate(custody.CONTROL_TABLES)
    ]
    names = custody.callable_contract.expected_nppes_callable_bodies(policy.schema)
    for oid, name in enumerate(sorted(names), 800):
        state.functions[oid] = dict(
            oid=oid,
            name=name,
            owner=500,
            owner_role=policy.owner_role,
            owner_protected=True,
            execute_closed=True,
            signature=f'"{policy.schema}"."{name}"()',
            acl=(),
        )
        state.function_grants[oid] = []

    return state, catalog_rows, names


@pytest.fixture
def harness(monkeypatch):
    policy = GenericSourcePolicy(
        "source_schema", "source_owner", "source_publisher", ("source_reader",), ("source_reader",)
    )
    state, catalog_rows, names = _new_control_harness_state(policy)

    capture_exact = partial(_capture_control_functions, state, policy, names)

    fetch = partial(_fetch_control_rows, state, policy, catalog_rows)

    fetchrow = partial(_fetch_control_identity, state)

    fetchval = partial(_fetch_control_value, state)

    execute = partial(_execute_control_query, state, policy)

    transaction = partial(_control_savepoint, state)

    connection = SimpleNamespace(
        fetch=AsyncMock(side_effect=fetch),
        fetchrow=AsyncMock(side_effect=fetchrow),
        fetchval=AsyncMock(side_effect=fetchval),
        execute=AsyncMock(side_effect=execute),
        is_in_transaction=lambda: state.active,
        transaction=transaction,
    )
    publisher = SimpleNamespace(connection=connection, policy=policy)
    monkeypatch.setattr(custody.source_policy, "bound_publisher", lambda: publisher)
    capability = AsyncMock(return_value=(700, 701))
    monkeypatch.setattr(custody.source_policy, "source_callable_capability", capability)
    monkeypatch.setattr(custody, "storage_roles", AsyncMock(return_value=(500, [501])))
    exact_capture = AsyncMock(side_effect=capture_exact)
    monkeypatch.setattr(custody.callable_contract, "capture_nppes_callable_contract", exact_capture)
    from process import nppes_public_evidence_catalog as catalog
    from process import provider_directory_source_callable_custody as source_callables

    legacy = AsyncMock()
    monkeypatch.setattr(catalog, "assert_nppes_admission_catalog", legacy)
    capture = AsyncMock(return_value=(700, 701))
    monkeypatch.setattr(source_callables, "attest_source_callable_capability", capture)
    return SimpleNamespace(
        policy=policy,
        state=state,
        rows=catalog_rows,
        connection=connection,
        publisher=publisher,
        capability=capability,
        capture=capture,
        legacy=legacy,
        exact_capture=exact_capture,
        expected_names=set(names),
    )


@pytest.mark.asyncio
async def test_original_publisher_accepts_only_closed_control_authority(harness):
    await custody.assert_nppes_control_custody(harness.connection, "source_schema", harness.policy)
    harness.capability.assert_awaited_once_with(harness.connection, harness.policy)
    call = next(call for call in harness.connection.fetch.await_args_list if call.args[0] == custody._CALLABLE_CLOSURE)
    expected_readonly_entries = {
        oid
        for oid, record in harness.state.functions.items()
        if record["name"] not in {custody.callable_contract.ARCHIVE, custody.callable_contract.CHAIN}
    }
    assert call.args[-1] == sorted({700, 701} | expected_readonly_entries)
    assert harness.state.sql == []


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["connection", "policy", "actor", "transaction"])
async def test_impostor_actor_or_physical_transaction_fails_before_capability(harness, change):
    if change == "connection":
        harness.publisher.connection = object()
    elif change == "policy":
        harness.publisher.policy = GenericSourcePolicy(
            "source_schema", "source_owner", "source_publisher", ("source_reader",)
        )
    elif change == "actor":
        harness.state.actor = {"session": "ordinary", "current": "source_owner"}
    else:
        harness.state.active = False
    with pytest.raises(NppesPublicEvidenceWriterError):
        await custody.assert_nppes_control_custody(harness.connection, "source_schema", harness.policy)
    harness.capability.assert_not_awaited()
    assert harness.state.sql == []


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["direct_acl_closed", "readers_closed", "owner", "named_role", "callable"])
async def test_native_table_column_role_and_callable_bypasses_are_rejected(harness, change):
    if change in {"direct_acl_closed", "readers_closed"}:
        harness.rows[0][change] = False
    elif change == "owner":
        harness.rows[0]["relowner"] = 600
    elif change == "named_role":
        harness.state.bypass_roles = [{"rolname": "unapproved_role"}]
    else:
        harness.state.bypass_functions = [{"oid": 900}]
    with pytest.raises(NppesPublicEvidenceWriterError):
        await custody.assert_nppes_control_custody(harness.connection, "source_schema", harness.policy)
    assert harness.state.sql == []


def test_role_queries_cover_inherited_usage_and_set_paths():
    assert "'SET'" in custody._ROLE_CLOSURE and "'USAGE'" in custody._ROLE_CLOSURE
    assert "has_function_privilege" in custody._CALLABLE_CLOSURE


def _adoption(harness):
    harness.state.actor = {
        "session": "independent_admin",
        "current": "independent_admin",
        "native_admin": True,
        "administrator": True,
        "backend": 1234,
        "xid": "5678",
    }
    harness.state.guard = dict(proname=custody.ARCHIVE_VALIDATOR, tgenabled="A", tgdeferrable=True, tginitdeferred=True)
    for oid, record in harness.state.functions.items():
        if record["name"] != custody.callable_contract.IMMUTABLE:
            record.update(owner=600, owner_role="legacy_owner", owner_protected=False, execute_closed=False)
            harness.state.function_grants[oid] = [
                dict(grantee=0, rolname=None),
                dict(grantee=600, rolname="legacy_owner"),
            ]
    return dict(migration_role="migration", namespace_owner_role="namespace_owner")


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["actor", "capability", "guard", "legacy_catalog", "unsealed"])
async def test_failed_adoption_preconditions_never_retire_the_guard(harness, change):
    arguments = _adoption(harness)
    if change == "actor":
        harness.state.actor["current"] = "source_owner"
    elif change == "capability":
        harness.capture.side_effect = RuntimeError("unaccepted callable")
    elif change == "guard":
        harness.state.guard["proname"] = "unexpected_guard"
    elif change == "legacy_catalog":
        harness.legacy.side_effect = RuntimeError("catalog drift")
    else:
        harness.state.unsealed = True
    with pytest.raises((NppesPublicEvidenceWriterError, RuntimeError, ValueError)):
        await custody.adopt_nppes_control_custody(harness.connection, "source_schema", harness.policy, **arguments)
    assert harness.state.guard is not None
    assert not any(sql.startswith("DROP TRIGGER") for sql in harness.state.sql)


@pytest.mark.asyncio
async def test_completed_adoption_is_idempotent_and_does_not_commit_the_callers_transaction(harness):
    arguments = _adoption(harness)
    await custody.adopt_nppes_control_custody(harness.connection, "source_schema", harness.policy, **arguments)
    assert harness.state.guard is None
    assert sum(sql.startswith("DROP TRIGGER") for sql in harness.state.sql) == 1
    harness.state.sql.clear()
    await custody.adopt_nppes_control_custody(harness.connection, "source_schema", harness.policy, **arguments)
    assert len(harness.state.sql) == 1 and harness.state.sql[0].startswith("LOCK TABLE")
    assert harness.state.active


@pytest.mark.asyncio
async def test_rejected_final_closure_restores_guard_inside_the_callers_transaction(harness):
    arguments = _adoption(harness)
    harness.state.bypass_functions = [{"oid": 900}]
    with pytest.raises(NppesPublicEvidenceWriterError):
        await custody.adopt_nppes_control_custody(harness.connection, "source_schema", harness.policy, **arguments)
    assert harness.state.guard is not None
    assert harness.state.active


@pytest.mark.asyncio
async def test_adoption_transfers_exact_eight_verified_functions_and_closes_execute(harness):
    arguments = _adoption(harness)
    await custody.adopt_nppes_control_custody(harness.connection, "source_schema", harness.policy, **arguments)
    expected_entries = {
        record["signature"]
        for record in harness.state.functions.values()
        if record["name"] != custody.callable_contract.IMMUTABLE
    }
    transfers = [sql for sql in harness.state.sql if sql.startswith("ALTER FUNCTION")]
    assert len(transfers) == len(expected_entries) == 8
    assert set(transfers) == {f'ALTER FUNCTION {signature} OWNER TO "source_owner"' for signature in expected_entries}
    assert all(
        record["owner"] == 500 and record["owner_protected"] and record["execute_closed"]
        for record in harness.state.functions.values()
    )
    for signature in expected_entries:
        assert f"REVOKE EXECUTE ON FUNCTION {signature} FROM PUBLIC CASCADE" in harness.state.sql
    assert [call.kwargs["archive_guard_present"] for call in harness.exact_capture.await_args_list] == [True, False]
    assert all("starts_with" not in call.args[0] for call in harness.connection.fetch.await_args_list)
    assert not any(sql.startswith(("COMMIT", "ROLLBACK")) for sql in harness.state.sql)
    assert harness.state.savepoints == ["begin", "release"]
    harness.capture.assert_awaited_once_with(
        harness.connection, harness.policy, migration_role="migration", namespace_owner_role="namespace_owner"
    )
    actor_calls = [call for call in harness.connection.fetchrow.await_args_list if "AS native_admin" in call.args[0]]
    assert len(actor_calls) == 2


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ("nonsuper", "set_role", "owner", "publisher", "runtime"))
async def test_adoption_requires_an_independent_original_native_administrator(harness, change):
    arguments = _adoption(harness)
    if change == "nonsuper":
        harness.state.actor["native_admin"] = False
    elif change == "set_role":
        harness.state.actor["current"] = "another_admin"
    else:
        role = {
            "owner": harness.policy.owner_role,
            "publisher": harness.policy.publisher_role,
            "runtime": harness.policy.runtime_roles[0],
        }[change]
        harness.state.actor.update(session=role, current=role)
    with pytest.raises(ValueError, match="independent_admin_required"):
        await custody.adopt_nppes_control_custody(harness.connection, "source_schema", harness.policy, **arguments)
    harness.capture.assert_not_awaited()
    assert harness.state.sql == [] and harness.state.guard is not None


@pytest.mark.asyncio
@pytest.mark.parametrize("already_adopted", (False, True))
@pytest.mark.parametrize("change", ("administrator", "backend", "xid", "task"))
async def test_final_administrator_or_task_drift_rolls_back_even_on_idempotent_adoption(
    harness, monkeypatch, already_adopted, change
):
    arguments = _adoption(harness)
    if already_adopted:
        await custody.adopt_nppes_control_custody(harness.connection, "source_schema", harness.policy, **arguments)
        harness.state.sql.clear()
        harness.state.savepoints.clear()
    original = deepcopy((harness.state.guard, harness.state.functions, harness.state.function_grants))
    capture = harness.exact_capture.side_effect
    initial_task, changed_task = object(), object()
    current_task_entries = [initial_task]
    monkeypatch.setattr(custody, "asyncio", SimpleNamespace(current_task=lambda: current_task_entries[0]))

    async def drift(connection, schema, *, archive_guard_present):
        result = await capture(connection, schema, archive_guard_present=archive_guard_present)
        if not archive_guard_present:
            if change == "administrator":
                harness.state.actor.update(session="another_admin", current="another_admin")
            elif change == "backend":
                harness.state.actor["backend"] += 1
            elif change == "xid":
                harness.state.actor["xid"] = "changed_xid"
            else:
                current_task_entries[0] = changed_task
        return result

    harness.exact_capture.side_effect = drift
    with pytest.raises(NppesPublicEvidenceWriterError):
        await custody.adopt_nppes_control_custody(harness.connection, "source_schema", harness.policy, **arguments)
    assert (harness.state.guard, harness.state.functions, harness.state.function_grants) == original
    assert harness.state.savepoints == ["begin", "rollback"] and harness.state.active
    assert any(sql.startswith("DROP TRIGGER") for sql in harness.state.sql) is not already_adopted


@pytest.mark.asyncio
@pytest.mark.parametrize("phase", ("before", "after"))
async def test_exact_callable_rejection_rolls_back_adoption_even_when_caught(harness, phase):
    arguments = _adoption(harness)
    original = deepcopy((harness.state.guard, harness.state.functions, harness.state.function_grants))
    capture = harness.exact_capture.side_effect

    async def reject(connection, schema, *, archive_guard_present):
        if archive_guard_present is (phase == "before"):
            raise ValueError("nppes_callable_contract_changed")
        return await capture(connection, schema, archive_guard_present=archive_guard_present)

    harness.exact_capture.side_effect = reject
    with pytest.raises(ValueError, match="nppes_callable_contract_changed"):
        await custody.adopt_nppes_control_custody(harness.connection, "source_schema", harness.policy, **arguments)
    assert (harness.state.guard, harness.state.functions, harness.state.function_grants) == original
    assert any(sql.startswith("DROP TRIGGER") for sql in harness.state.sql) is (phase == "after")
    assert harness.state.savepoints == ["begin", "rollback"] and harness.state.active


@pytest.mark.asyncio
@pytest.mark.parametrize("field,value", (("owner", 600), ("owner_protected", False), ("execute_closed", False)))
async def test_runtime_refuses_wrong_validator_owner_protection_or_execute(harness, field, value):
    record = next(
        record for record in harness.state.functions.values() if record["name"] == custody.callable_contract.ARCHIVE
    )
    record[field] = value
    with pytest.raises(NppesPublicEvidenceWriterError):
        await custody.assert_nppes_control_custody(harness.connection, "source_schema", harness.policy)
    assert harness.state.sql == []


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ("missing", "wrong_owner"))
async def test_runtime_requires_all_six_append_definers_under_control_owner(harness, change):
    record = next(
        record
        for record in harness.state.functions.values()
        if record["name"].startswith("guard_public_evidence_source_record_")
    )
    if change == "missing":
        harness.state.hidden_append_oids.add(record["oid"])
    else:
        record.update(owner=600, owner_role="legacy_owner")
    with pytest.raises(NppesPublicEvidenceWriterError):
        await custody.assert_nppes_control_custody(harness.connection, "source_schema", harness.policy)
    assert harness.state.sql == []


@pytest.mark.asyncio
async def test_same_prefix_unrelated_function_is_never_adopted_or_whitelisted(harness):
    rogue_by_key = dict(
        oid=9999,
        name="validate_public_evidence_nppes_unrelated",
        owner=600,
        owner_role="legacy_owner",
        owner_protected=False,
        execute_closed=False,
        signature='"source_schema"."validate_public_evidence_nppes_unrelated"()',
        acl=(),
    )
    harness.state.functions[rogue_by_key["oid"]] = rogue_by_key
    harness.state.function_grants[rogue_by_key["oid"]] = []
    arguments = _adoption(harness)
    original = deepcopy(harness.state.functions[rogue_by_key["oid"]])
    await custody.adopt_nppes_control_custody(harness.connection, "source_schema", harness.policy, **arguments)
    assert harness.state.functions[rogue_by_key["oid"]] == original
    assert all(rogue_by_key["name"] not in sql for sql in harness.state.sql)
    closure = next(
        call for call in harness.connection.fetch.await_args_list if call.args[0] == custody._CALLABLE_CLOSURE
    )
    assert rogue_by_key["oid"] not in closure.args[-1]


@pytest.mark.asyncio
@pytest.mark.parametrize("administrator", (False, True))
async def test_readonly_attestation_accepts_admin_or_reader_without_publisher_authority(
    harness, monkeypatch, administrator
):
    role = "independent_admin" if administrator else harness.policy.runtime_roles[0]
    harness.state.actor.update(session=role, current=role, administrator=administrator, native_admin=administrator)
    monkeypatch.setattr(custody.source_policy, "bound_publisher", lambda: pytest.fail("attestation claimed authority"))
    await custody.attest_nppes_control_custody(
        harness.connection,
        "source_schema",
        harness.policy,
        migration_role="migration",
        namespace_owner_role="namespace_owner",
    )
    harness.capture.assert_awaited_once_with(
        harness.connection, harness.policy, migration_role="migration", namespace_owner_role="namespace_owner"
    )
    harness.capability.assert_not_awaited()
    harness.connection.execute.assert_not_awaited()
    assert harness.state.sql == [] and harness.state.savepoints == []
    identities = [call for call in harness.connection.fetchrow.await_args_list if "AS administrator" in call.args[0]]
    assert len(identities) == 2


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ("schema", "transaction"))
async def test_readonly_attestation_rejects_wrong_schema_or_inactive_transaction_before_capture(harness, change):
    if change == "transaction":
        harness.state.active = False
    with pytest.raises(NppesPublicEvidenceWriterError):
        await custody.attest_nppes_control_custody(
            harness.connection,
            "wrong_schema" if change == "schema" else "source_schema",
            harness.policy,
            migration_role="migration",
            namespace_owner_role="namespace_owner",
        )
    harness.capture.assert_not_awaited()
    harness.connection.fetchrow.assert_not_awaited()
    harness.connection.execute.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ("session", "current", "backend", "xid", "administrator", "task"))
async def test_readonly_attestation_rejects_native_identity_or_task_drift_without_mutations(
    harness, monkeypatch, change
):
    capture = harness.exact_capture.side_effect
    original_task, changed_task = object(), object()
    task_entries = [original_task]
    monkeypatch.setattr(custody, "asyncio", SimpleNamespace(current_task=lambda: task_entries[0]))

    async def drift(connection, schema, *, archive_guard_present):
        result = await capture(connection, schema, archive_guard_present=archive_guard_present)
        if change == "task":
            task_entries[0] = changed_task
        else:
            harness.state.actor[change] = {
                "session": "another_session",
                "current": "another_role",
                "backend": 9876,
                "xid": "changed_xid",
                "administrator": True,
            }[change]
        return result

    harness.exact_capture.side_effect = drift
    with pytest.raises(NppesPublicEvidenceWriterError):
        await custody.attest_nppes_control_custody(
            harness.connection,
            "source_schema",
            harness.policy,
            migration_role="migration",
            namespace_owner_role="namespace_owner",
        )
    harness.connection.execute.assert_not_awaited()
    assert harness.state.sql == [] and harness.state.savepoints == []


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ("source_contract", "control_closure"))
async def test_readonly_attestation_propagates_rejected_contract_without_mutations(harness, change):
    if change == "source_contract":
        harness.capture.side_effect = RuntimeError("source contract changed")
    else:
        harness.state.bypass_functions = [{"oid": 9999}]
    with pytest.raises((NppesPublicEvidenceWriterError, RuntimeError)):
        await custody.attest_nppes_control_custody(
            harness.connection,
            "source_schema",
            harness.policy,
            migration_role="migration",
            namespace_owner_role="namespace_owner",
        )
    harness.connection.execute.assert_not_awaited()
    assert harness.state.sql == [] and harness.state.savepoints == []


def test_enrolled_migrator_is_not_a_runtime_source_callable_actor(harness):
    from process.provider_directory_source_callable_custody import _require_actor

    with pytest.raises(ValueError, match="actor_changed"):
        _require_actor(
            dict(administrator=False, session="migration", current="migration"), harness.policy, legacy=False
        )
