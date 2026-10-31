# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Unit catalog transport and original-task binding; native authority is verified separately."""

import asyncio
import re
from copy import deepcopy
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from db.migration_generic_source_candidates import _canonical_copy_guard_bodies, _cms_canonical_copy_guard_body
from db.migration_provider_directory_dataset_candidates import candidate_functions
from db.plan_release_storage import storage_relation
from process import provider_directory_generic_source_provision as provision
from process import provider_directory_generic_source_release_checks as checks
from process.provider_directory_generic_candidate import GenericCandidateError
from process.provider_directory_generic_source_policy import GenericSourcePolicy
from tests.test_provider_directory_source_callable_custody import _entry
from tests.test_source_validator_argument_types_migration import _load


async def release_harness(monkeypatch):
    """Double external custody qualification, retaining the real release checker and context guards."""
    policy = GenericSourcePolicy("source_schema", "source_owner", "publisher", ("builder", "reader"), ("builder",))
    role_graph_by_field = dict(roles=[dict(rolname="builder", oid=103)], memberships=[])
    identity_by_field = dict(session="administrator", current="administrator", administrator=True, backend=17, xid="19")
    connection = SimpleNamespace(
        is_in_transaction=lambda: True,
        fetchrow=AsyncMock(side_effect=lambda *_: dict(identity_by_field)),
        fetchval=AsyncMock(side_effect=lambda *_: deepcopy(role_graph_by_field)),
        fetch=AsyncMock(),
        execute=AsyncMock(),
    )
    receipt_by_field = dict(
        role_graph=deepcopy(role_graph_by_field),
        roles=dict(builder=103, namespace_owner=104, migration=105),
        schema_oid=201,
        functions={},
    )
    capture = AsyncMock(return_value=receipt_by_field)
    monkeypatch.setattr(checks, "capture_source_callable_custody", capture)
    verifier = await checks.source_release_checks(
        connection, policy, migration_role="migration", namespace_owner_role="namespace_owner"
    )
    capture.assert_awaited_once_with(
        connection, policy, migration_role="migration", namespace_owner_role="namespace_owner", legacy=True
    )
    return SimpleNamespace(
        policy=policy,
        graph=role_graph_by_field,
        identity=identity_by_field,
        connection=connection,
        receipt=receipt_by_field,
        verifier=verifier,
    )


def legacy_function_rows(monkeypatch):
    """Use genuine migration bodies with synthetic pg_proc/ACL transport rows."""
    statements = [candidate_functions("source_schema")]
    migration = _load("20260930010000_provider_directory_resource_identity.py")
    monkeypatch.setattr(migration, "_schema", lambda: '"source_schema"')
    monkeypatch.setattr(migration, "op", SimpleNamespace(execute=statements.append))
    migration.upgrade()
    pattern = re.compile(
        r'CREATE FUNCTION "source_schema"\."?(\w+)"?\((.*?)\)\s+RETURNS\s+(.*?)\s+LANGUAGE\s+(.*?)AS \$(\w*)\$(.*?)\$\5\$',
        re.S,
    )
    bodies_by_signature = {
        name + "(" + ",".join(arg.strip().split(None, 1)[1] for arg in args.split(",") if arg) + ")": body
        for name, args, _returns, _options, _delimiter, body in pattern.findall("\n".join(statements))
    }
    assert bodies_by_signature.keys() == checks._LEGACY_FUNCTIONS.keys()
    return [
        legacy_catalog_entry(index, signature, spec, bodies_by_signature[signature])
        for index, (signature, spec) in enumerate(checks._LEGACY_FUNCTIONS.items(), 501)
    ]


def legacy_catalog_entry(index, signature, spec, body):
    oid_by_type = {"trigger": "2279", "void": "2278", "text[]": "1009", "text": "25", "jsonb": "3802"}
    args, returns, _outputs, language, volatility, strict, parallel, security, _digest = spec
    definition = _entry("guard_provider_directory_terminal_root_retirement_child()")["definition"]
    definition.update(
        oid=str(index),
        proname=signature.split("(")[0],
        proowner="105",
        pronamespace="201",
        prorettype=oid_by_type[returns],
        pronargs=len(args),
        proargtypes=[oid_by_type[kind] for _, kind in args],
        proargnames=[name for name, _ in args] or None,
        prosrc=body,
        provolatile=volatility,
        proisstrict=strict,
        proparallel=parallel,
        prosecdef=security,
    )
    acl_rows = [[105, 105, "EXECUTE", False]]
    if signature == "guard_provider_directory_resource_identity()" or "_rooted_dataset_candidate(" in signature:
        acl_rows.append([0, 105, "EXECUTE", False])
    if "_practitioner_dataset_candidate(" in signature:
        acl_rows.append([103, 105, "EXECUTE", False])
    return dict(
        signature=storage_relation("source_schema", signature.split("(")[0]) + "(" + signature.split("(")[1],
        definition=definition,
        language=language,
        acl=acl_rows,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", [None, "missing", "owner", "body", "acl", "writer role", "duplicate", "keys"])
async def test_candidate_callable_contract_and_exact_writer_acl(monkeypatch, damage):
    harness = await release_harness(monkeypatch)
    function_rows = legacy_function_rows(monkeypatch)
    plan_by_field = dict(writers=dict(practitioner=["builder"], rooted=["PUBLIC"]))
    if damage == "missing":
        function_rows.pop()
    if damage == "owner":
        function_rows[0]["definition"]["proowner"] = "999"
    if damage == "body":
        function_rows[0]["definition"]["prosrc"] += " RETURN NULL;"
    if damage == "acl":
        function_rows[0]["acl"].append([999, 105, "EXECUTE", False])
    if damage == "writer role":
        plan_by_field["writers"]["rooted"] = ["reader"]
    if damage == "duplicate":
        plan_by_field["writers"]["practitioner"] = ["builder", "builder"]
    if damage == "keys":
        plan_by_field["writers"]["other"] = []
    harness.connection.fetch.return_value = function_rows
    if damage is None:
        actual = await harness.verifier._candidate_functions(plan_by_field)
        assert len(actual) == 17 and actual.keys() == {int(row["definition"]["oid"]) for row in function_rows}
    else:
        with pytest.raises(ValueError if damage == "body" else GenericCandidateError):
            await harness.verifier._candidate_functions(plan_by_field)
    harness.connection.execute.assert_not_awaited()


def allocator_guard_rows(monkeypatch, *, guard_version="v0", additional_execute=True):
    """Retain real source bodies while modeling the exact protected allocator postimage."""
    rows = legacy_function_rows(monkeypatch)
    by_name = {row["definition"]["proname"]: row for row in rows}
    allocator = by_name["prepare_pd_generic_dataset_storage"]
    allocator["definition"]["proowner"] = "104"
    allocator["acl"] = [[104, 104, "EXECUTE", False]]
    guard = by_name["guard_pd_dataset_candidate_parent"]
    if guard_version == "v1":
        guard["definition"]["prosrc"] = _canonical_copy_guard_bodies("source_schema", "hp_snapshot_retention")[1]
    elif guard_version == "v2":
        guard["definition"]["prosrc"] = _cms_canonical_copy_guard_body("source_schema", "hp_snapshot_retention")
    if additional_execute:
        guard["acl"].append([104, 105, "EXECUTE", False])
    return rows, allocator, guard


@pytest.mark.asyncio
@pytest.mark.parametrize("guard_version", ["v0", "v1", "v2"])
@pytest.mark.parametrize("additional_execute", [False, True])
async def test_allocator_guard_allows_only_exact_optional_postimage(monkeypatch, guard_version, additional_execute):
    harness = await release_harness(monkeypatch)
    rows, _, _ = allocator_guard_rows(monkeypatch, guard_version=guard_version, additional_execute=additional_execute)
    harness.connection.fetch.return_value = list(reversed(rows))
    before = deepcopy(rows)

    actual = await harness.verifier._candidate_functions(
        dict(writers=dict(practitioner=["builder"], rooted=["PUBLIC"]))
    )

    assert actual == {int(row["definition"]["oid"]): row["definition"] for row in rows}
    assert rows == before
    harness.connection.fetch.assert_awaited_once()
    harness.connection.execute.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "damage",
    ["allocator_owner", "allocator_body", "allocator_acl", "grantee", "grantor", "grantable", "privilege", "deny"],
)
async def test_allocator_guard_refuses_unapproved_authority(monkeypatch, damage):
    harness = await release_harness(monkeypatch)
    rows, allocator, guard = allocator_guard_rows(monkeypatch, guard_version="v1")
    if damage == "allocator_owner":
        allocator["definition"]["proowner"] = "105"
        allocator["acl"] = [[105, 105, "EXECUTE", False]]
    elif damage == "allocator_body":
        allocator["definition"]["prosrc"] += " RETURN NULL;"
    elif damage == "allocator_acl":
        allocator["acl"].append([999, 104, "EXECUTE", False])
    elif damage == "deny":
        deny = next(row for row in rows if row["definition"]["proname"] == "deny_pd_dataset_candidate_write")
        deny["acl"].append([104, 105, "EXECUTE", False])
    else:
        position, changed = {
            "grantee": (0, 103),
            "grantor": (1, 104),
            "grantable": (3, True),
            "privilege": (2, "SELECT"),
        }[damage]
        guard["acl"][-1][position] = changed
    harness.connection.fetch.return_value = rows
    with pytest.raises(ValueError if damage == "allocator_body" else GenericCandidateError):
        await harness.verifier._candidate_functions(dict(writers=dict(practitioner=["builder"], rooted=["PUBLIC"])))
    harness.connection.execute.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("guard_version", ["v1", "v2"])
@pytest.mark.parametrize("damage", ["changed", "preparation_schema"])
async def test_allocator_guard_rejects_unapproved_current_body(monkeypatch, guard_version, damage):
    harness = await release_harness(monkeypatch)
    rows, _, guard = allocator_guard_rows(monkeypatch, guard_version=guard_version)
    if damage == "changed":
        guard["definition"]["prosrc"] += " RETURN NULL;"
    else:
        guard["definition"]["prosrc"] = guard["definition"]["prosrc"].replace(
            "hp_snapshot_retention", "other_retention"
        )
    harness.connection.fetch.return_value = rows
    with pytest.raises(ValueError, match="definition_changed"):
        await harness.verifier._candidate_functions(dict(writers=dict(practitioner=["builder"], rooted=["PUBLIC"])))


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["connection", "policy", "task", "identity", "graph"])
async def test_verifier_remains_bound_to_original_context(monkeypatch, damage):
    harness = await release_harness(monkeypatch)
    connection, policy = harness.connection, harness.policy
    if damage == "connection":
        connection = SimpleNamespace()
    if damage == "policy":
        policy = replace(policy)
    if damage == "identity":
        harness.identity["xid"] = "20"
    if damage == "graph":
        harness.graph["memberships"].append(dict(roleid=999, member=103))
    operation = harness.verifier._context(connection, policy)
    with pytest.raises(GenericCandidateError, match="original_transaction_changed"):
        await (asyncio.create_task(operation) if damage == "task" else operation)


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["transaction", "administrator", "set role", "runtime actor", "control roles"])
async def test_factory_refuses_nonindependent_context_before_custody(monkeypatch, damage):
    policy = GenericSourcePolicy("source_schema", "source_owner", "publisher", ("builder", "reader"), ("builder",))
    identity_by_field = dict(session="administrator", current="administrator", administrator=True)
    if damage == "administrator":
        identity_by_field["administrator"] = False
    if damage == "set role":
        identity_by_field["current"] = "other_admin"
    if damage == "runtime actor":
        identity_by_field.update(session="builder", current="builder")
    if damage == "control roles":
        policy = replace(policy, control_runtime_roles=())
    connection = SimpleNamespace(
        is_in_transaction=lambda: damage != "transaction", fetchrow=AsyncMock(return_value=identity_by_field)
    )
    custody = AsyncMock(side_effect=AssertionError("unexpected custody qualification"))
    monkeypatch.setattr(checks, "capture_source_callable_custody", custody)
    with pytest.raises(GenericCandidateError):
        await checks.source_release_checks(
            connection, policy, migration_role="migration", namespace_owner_role="namespace"
        )
    custody.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "damage", [None, "missing proof", "schema", "owner", "acl", "reader", "oid", "write", "sequence"]
)
async def test_writer_closure_checks_exact_family_and_transitive_reach(monkeypatch, damage):
    harness = await release_harness(monkeypatch)
    relation_by_field = dict(
        oid=301,
        root=checks.PAYLOAD_ROOTS[0],
        nspname=harness.policy.schema,
        relname="retained_payload",
        source_root=checks.PAYLOAD_ROOTS[0],
        relowner=101,
        direct_acl_closed=True,
        readers_closed=True,
    )
    harness.verifier.family = None if damage == "missing proof" else {301: deepcopy(relation_by_field)}
    if damage in {"schema", "owner", "acl", "reader", "oid"}:
        field, changed_value = {
            "schema": ("nspname", "foreign_schema"),
            "owner": ("relowner", 999),
            "acl": ("direct_acl_closed", False),
            "reader": ("readers_closed", False),
            "oid": ("oid", 999),
        }[damage]
        relation_by_field[field] = changed_value
    harness.connection.fetch.return_value = [relation_by_field]

    async def fetchval(statement, *args):
        if statement == checks._GRAPH:
            return deepcopy(harness.graph)
        assert args == ([relation_by_field["oid"]], list(harness.policy.runtime_roles))
        return damage == ("write" if statement == checks._WRITE_REACH else "sequence")

    harness.connection.fetchval.side_effect = fetchval
    roles = AsyncMock(return_value=(101, [102]))
    control = AsyncMock()
    monkeypatch.setattr(checks, "storage_roles", roles)
    monkeypatch.setattr(provision, "_verify_control_closure", control)
    if damage is None:
        await harness.verifier.verify_writer_closure(harness.connection, harness.policy)
        roles.assert_awaited_once_with(harness.connection, harness.policy.schema, **harness.policy.role_arguments())
        control.assert_awaited_once_with(harness.connection, harness.policy)
        selected = harness.connection.fetch.await_args.args[1]
        assert storage_relation(harness.policy.schema, "retained_payload") in selected
        assert harness.connection.fetchrow.await_count == 3
    else:
        with pytest.raises(GenericCandidateError):
            await harness.verifier.verify_writer_closure(harness.connection, harness.policy)
    harness.connection.execute.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [None, "family", "epoch", "context drift"])
async def test_epoch_closure_preserves_order_and_final_context(monkeypatch, failure):
    from process import provider_directory_source_cms_epoch_control as epochs

    harness = await release_harness(monkeypatch)
    calls = []

    async def verify_family(connection, policy):
        assert connection is harness.connection and policy is harness.policy
        calls.append("family")
        if failure == "family":
            raise RuntimeError("family unavailable")

    async def verify_epoch(connection, policy):
        assert connection is harness.connection and policy is harness.policy
        calls.append("epoch")
        if failure == "epoch":
            raise RuntimeError("epoch unavailable")
        if failure == "context drift":
            harness.identity["backend"] += 1

    monkeypatch.setattr(provision, "verify_generic_source_family", verify_family)
    monkeypatch.setattr(epochs, "capture_source_cms_epoch_controls", verify_epoch)
    if failure is None:
        await harness.verifier.verify_epoch_writer_closure(harness.connection, harness.policy)
        assert harness.connection.fetchrow.await_count == 3
    else:
        with pytest.raises(GenericCandidateError if failure == "context drift" else RuntimeError):
            await harness.verifier.verify_epoch_writer_closure(harness.connection, harness.policy)
    assert calls == (["family"] if failure == "family" else ["family", "epoch"])
    harness.connection.execute.assert_not_awaited()
