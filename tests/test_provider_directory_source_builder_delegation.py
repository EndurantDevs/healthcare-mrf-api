# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Existing group grants remain bounded across inherited and SET role paths."""

import asyncio
from copy import deepcopy
from dataclasses import FrozenInstanceError
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import provider_directory_source_builder_delegation as delegation

OWNER, PUBLISHER, BUILDER, NAMESPACE, MIGRATOR, SNAPSHOT, READER, SNAPSHOT_OWNER = range(17001, 17009)


def _role(oid, **changes):
    """Represent native role flags without ambient elevated authority."""
    return (
        dict(
            oid=oid,
            rolname="principal_" + str(oid),
            rolcanlogin=False,
            **{field: False for field in delegation._ELEVATED},
        )
        | changes
    )


def _membership(member, target, *, inherit=True, can_set=True, admin=False):
    """Represent the independent native membership options."""
    return dict(
        member=member,
        roleid=target,
        grantor=10,
        inherit_option=inherit,
        set_option=can_set,
        admin_option=admin,
    )


def _receipt():
    """Use separate protected owners and an ordinary Builder group delegate."""
    role_ids = range(OWNER, SNAPSHOT_OWNER + 1)
    return dict(
        roles=dict(owner=OWNER, publisher=PUBLISHER, builder=BUILDER, namespace_owner=NAMESPACE, migration=MIGRATOR),
        role_graph=dict(
            roles=[_role(oid, rolcanlogin=oid in {PUBLISHER, BUILDER, SNAPSHOT, READER}) for oid in role_ids],
            memberships=[_membership(SNAPSHOT, BUILDER), _membership(SNAPSHOT, SNAPSHOT_OWNER)],
        ),
        functions={
            "existing_batch()": dict(
                definition=dict(proowner=OWNER, proname="existing_batch"),
                acl=[[OWNER, OWNER, "EXECUTE", False], [BUILDER, OWNER, "EXECUTE", False]],
                access=[
                    [oid, False, False, False, False, oid in {OWNER, PUBLISHER, BUILDER, MIGRATOR, SNAPSHOT}]
                    for oid in role_ids
                ],
            )
        },
    )


def _access(receipt, *, writers=(BUILDER, SNAPSHOT), tables=("SELECT", "MAINTAIN"), columns=("SELECT", "UPDATE")):
    """Supply complete native table/column observations for one two-column control."""
    return [
        dict(
            role=role["oid"],
            superuser=role["rolsuper"],
            builtin=role["oid"] < 16384 and not role["rolcanlogin"],
            owner_reach=role["oid"] in {OWNER, PUBLISHER, MIGRATOR},
            builder_reach=role["oid"] in {BUILDER, SNAPSHOT},
            table=list(tables if role["oid"] in writers else ("SELECT",)),
            columns=[[1, list(columns if role["oid"] in writers else ("SELECT",))], [2, ["SELECT"]]],
        )
        for role in receipt["role_graph"]["roles"]
    ]


def _require_access(receipt, access, *, locked=(), legacy=False):
    """Use the same immutable proof consumed by both custody guards."""
    proof = delegation.builder_delegation(receipt, locked_relations=frozenset(locked))
    delegation.require_delegated_access(
        access,
        proof,
        owner=OWNER,
        builder=BUILDER,
        relation_oid=901,
        legacy_builder=legacy,
    )
    return proof


@pytest.mark.parametrize("inherit,can_set", [(True, True), (True, False), (False, True)])
def test_native_builder_group_options_preserve_existing_bounded_authority(inherit, can_set):
    receipt = _receipt()
    receipt["role_graph"]["memberships"][0] = _membership(SNAPSHOT, BUILDER, inherit=inherit, can_set=can_set)
    if not inherit:
        receipt["functions"]["existing_batch()"]["access"][SNAPSHOT - OWNER][-1] = False
    access = _access(receipt, writers=(BUILDER, SNAPSHOT) if inherit else (BUILDER,))
    original = deepcopy((receipt, access))
    proof = _require_access(receipt, access)
    assert SNAPSHOT in proof.delegates
    assert (receipt, access) == original
    with pytest.raises(FrozenInstanceError):
        proof.builder = READER


def test_set_to_an_intermediate_role_with_inherited_builder_rights_is_included():
    receipt = _receipt()
    bridge = 17009
    receipt["role_graph"]["roles"].append(_role(bridge))
    receipt["role_graph"]["memberships"] = [
        _membership(SNAPSHOT, bridge, inherit=False),
        _membership(bridge, BUILDER, can_set=False),
    ]
    function = receipt["functions"]["existing_batch()"]
    function["access"][SNAPSHOT - OWNER][-1] = False
    function["access"].append([bridge, False, False, False, False, True])
    access = _access(receipt, writers=(BUILDER, bridge))
    next(row for row in access if row["role"] == SNAPSHOT)["builder_reach"] = False
    next(row for row in access if row["role"] == bridge)["builder_reach"] = True
    assert SNAPSHOT in _require_access(receipt, access).delegates


@pytest.mark.parametrize("flag", delegation._ELEVATED[1:])
def test_elevated_delegated_actor_is_refused(flag):
    receipt = _receipt()
    next(role for role in receipt["role_graph"]["roles"] if role["oid"] == SNAPSHOT)[flag] = True
    with pytest.raises(ValueError, match="elevated_authority"):
        delegation.builder_delegation(receipt)


def test_nologin_set_reachable_bypass_role_cannot_hide_behind_safe_login():
    receipt = _receipt()
    receipt["role_graph"]["memberships"].append(_membership(SNAPSHOT, SNAPSHOT_OWNER, inherit=False))
    receipt["role_graph"]["memberships"].pop(1)
    next(role for role in receipt["role_graph"]["roles"] if role["oid"] == SNAPSHOT_OWNER)["rolbypassrls"] = True
    with pytest.raises(ValueError, match="elevated_authority"):
        delegation.builder_delegation(receipt)


@pytest.mark.parametrize("target", [OWNER, PUBLISHER, NAMESPACE])
def test_inherited_or_set_protected_owner_authority_is_refused(target):
    receipt = _receipt()
    receipt["role_graph"]["memberships"].append(_membership(SNAPSHOT, target, inherit=False))
    with pytest.raises(ValueError, match="owner_authority"):
        delegation.builder_delegation(receipt)


def test_membership_admin_option_cannot_expand_the_admitted_role_closure():
    receipt = _receipt()
    receipt["role_graph"]["memberships"][0]["admin_option"] = True
    with pytest.raises(ValueError, match="membership_admin_authority"):
        delegation.builder_delegation(receipt)


@pytest.mark.parametrize("fault", ["table", "column", "unrelated", "membership", "missing_role"])
def test_no_extra_effective_table_column_or_unrelated_writer_authority(fault):
    receipt = _receipt()
    access = _access(receipt)
    snapshot = next(row for row in access if row["role"] == SNAPSHOT)
    if fault == "table":
        snapshot["table"].append("TRIGGER")
    elif fault == "column":
        snapshot["columns"][1][1].append("UPDATE")
    elif fault == "unrelated":
        next(row for row in access if row["role"] == READER)["table"].append("DELETE")
    elif fault == "membership":
        snapshot["builder_reach"] = False
    else:
        access.pop()
    with pytest.raises(ValueError, match="writer_authority|unrelated_writer|native_membership|access_coverage"):
        _require_access(receipt, access)


def test_private_callable_execute_cannot_escape_builder_delegation():
    receipt = _receipt()
    receipt["functions"]["existing_batch()"]["access"][READER - OWNER][-1] = True
    with pytest.raises(ValueError, match="callable_authority"):
        delegation.builder_delegation(receipt)


def test_existing_namespace_owned_pure_callable_keeps_its_native_owner():
    from process.provider_directory_source_callable_custody import _require_acl

    receipt = _receipt()
    function = receipt["functions"]["existing_batch()"]
    function["definition"] = dict(proowner=NAMESPACE, proname="public_evidence_npi_valid")
    function["acl"] = [
        [NAMESPACE, NAMESPACE, "EXECUTE", False],
        [BUILDER, NAMESPACE, "EXECUTE", False],
        [OWNER, NAMESPACE, "EXECUTE", False],
    ]
    function["access"][NAMESPACE - OWNER][-1] = True
    _require_acl(function, receipt["roles"], legacy=False)
    delegation.builder_delegation(receipt)
    function["access"][READER - OWNER][-1] = True
    with pytest.raises(ValueError, match="callable_authority"):
        delegation.builder_delegation(receipt)


def test_namespace_ownership_does_not_authorize_other_private_callables():
    receipt = _receipt()
    receipt["functions"]["existing_batch()"]["access"][NAMESPACE - OWNER][-1] = True
    with pytest.raises(ValueError, match="callable_authority"):
        delegation.builder_delegation(receipt)


def test_public_callable_execute_preserves_the_exact_existing_public_contract():
    receipt = _receipt()
    function = receipt["functions"]["existing_batch()"]
    function["acl"].append([0, OWNER, "EXECUTE", False])
    function["access"][READER - OWNER][-1] = True
    _require_access(receipt, _access(receipt))


@pytest.mark.parametrize("change", ["graph", "policy", "callable"])
def test_immutable_proof_cannot_authorize_changed_native_receipt(change):
    receipt = _receipt()
    proof = delegation.builder_delegation(receipt)
    assert delegation.receipt_delegation(receipt, proof) is proof
    if change == "graph":
        receipt["role_graph"]["memberships"][0]["set_option"] = False
    elif change == "policy":
        receipt["roles"]["publisher"] = READER
    else:
        receipt["functions"]["existing_batch()"]["access"][READER - OWNER][-1] = True
    with pytest.raises(ValueError, match="receipt_changed|callable_authority"):
        delegation.receipt_delegation(receipt, proof)


def test_legacy_inherited_owner_observation_needs_the_actual_locked_relation():
    receipt = _receipt()
    access = _access(receipt)
    for locked in ((), (902,), (901,)):
        with pytest.raises(ValueError, match="legacy_transition_required"):
            _require_access(receipt, access, locked=locked, legacy=True)


def test_builtin_writer_refuses_untrusted_set_path():
    receipt = _receipt()
    builtin = 6181
    receipt["role_graph"]["roles"].append(_role(builtin))
    receipt["functions"]["existing_batch()"]["access"].append([builtin, False, False, False, False, False])
    access = _access(receipt, writers=(BUILDER, SNAPSHOT, builtin))
    _require_access(receipt, access)
    receipt["role_graph"]["memberships"].append(_membership(SNAPSHOT, builtin, inherit=False))
    with pytest.raises(ValueError, match="unrelated_writer"):
        _require_access(receipt, access)


def _control_catalog(columns, rights, *, legacy=False):
    """Build complete ACL/effective evidence for the unchanged static writer contract."""
    from process.provider_directory_cms_control_custody import _RIGHTS

    table_rights, by_right = rights
    native_by_field = dict(oid=901, relowner=str(BUILDER if legacy else OWNER))
    attributes = [dict(attrelid=901, attname=name, attnum=number) for number, name in enumerate(columns, 1)]
    builder_columns = [
        [
            number,
            sorted(
                (table_rights & {"SELECT", "INSERT", "UPDATE", "REFERENCES"})
                | {right for right, selected in by_right.items() if name in selected}
            ),
        ]
        for number, name in enumerate(columns, 1)
    ]
    receipt = _receipt()
    access = _access(receipt)
    for access_entry in access:
        access_entry["columns"] = (
            deepcopy(builder_columns)
            if access_entry["role"] in {BUILDER, SNAPSHOT}
            else [[number, ["SELECT"]] for number in range(1, len(columns) + 1)]
        )
        if access_entry["role"] in {BUILDER, SNAPSHOT}:
            access_entry["table"] = sorted(table_rights)
    catalog_by_field = dict(
        classes=[native_by_field],
        attributes=attributes,
        access=access,
        table_acl=[[OWNER, OWNER, right, False] for right in _RIGHTS]
        + [[BUILDER, OWNER, right, False] for right in table_rights],
        column_acl=[
            [number, BUILDER, OWNER, right, False]
            for number, name in enumerate(columns, 1)
            for right, selected in by_right.items()
            if name in selected
        ],
    )
    return receipt, catalog_by_field, native_by_field


def _acquisition_contracts():
    """Cover read-only, low-volume, bulk-parent and loading-child grant categories."""
    from process.provider_directory_source_acquisition_custody import ACQUISITION_ROOTS, HISTORY_ROOTS, METADATA_ROOTS

    return [(root, root in HISTORY_ROOTS, False) for root in (*ACQUISITION_ROOTS, *HISTORY_ROOTS, *METADATA_ROOTS)] + [
        (ACQUISITION_ROOTS[3], True, True),
    ]


@pytest.mark.parametrize("root,history,loading", _acquisition_contracts())
def test_acquisition_guard_preserves_each_exact_existing_builder_grant_category(root, history, loading):
    from process import provider_directory_source_acquisition_custody as custody

    canonical = root.removesuffix("_legacy") if history and not loading else root
    columns = (
        list(custody._models()[canonical].__table__.columns.keys()) if canonical in custody._models() else ["singleton"]
    )
    receipt, catalog, native = _control_catalog(
        columns,
        custody._writer_rights(canonical, columns, history=history, loading=loading),
    )
    custody._require_relation_access(
        catalog,
        native,
        receipt["roles"],
        legacy=False,
        root=canonical,
        history=history,
        loading=loading,
        delegation=delegation.builder_delegation(receipt),
    )
    if loading:
        snapshot = next(access_entry for access_entry in catalog["access"] if access_entry["role"] == SNAPSHOT)
        snapshot["table"].append("SELECT")
        for _, rights in snapshot["columns"]:
            rights.append("SELECT")
        custody._require_relation_access(
            catalog,
            native,
            receipt["roles"],
            legacy=False,
            root=canonical,
            history=history,
            loading=True,
            delegation=delegation.builder_delegation(receipt),
        )
        snapshot["columns"][0][1].append("UPDATE")
        with pytest.raises(ValueError, match="writer_authority"):
            custody._require_relation_access(
                catalog,
                native,
                receipt["roles"],
                legacy=False,
                root=canonical,
                history=True,
                loading=True,
                delegation=delegation.builder_delegation(receipt),
            )


@pytest.mark.parametrize("position", range(10))
def test_cms_guard_preserves_every_exact_existing_builder_control_grant(position):
    from process import provider_directory_cms_control_custody as custody

    root = custody.CONTROL_ROOTS[position]
    columns = (
        list(custody._models()[root].__table__.columns.keys())
        if root in custody._models()
        else [name for name, _, _ in custody._EXTRA_COLUMNS[root]]
    )
    receipt, catalog, native = _control_catalog(columns, custody._writer_rights(root, columns))
    native["relname"] = root
    proof = delegation.builder_delegation(receipt)
    custody._require_relation_acl(root, catalog, native, OWNER, BUILDER, legacy=False, delegation=proof)
    catalog["column_acl"].append([1, READER, OWNER, "UPDATE", False])
    with pytest.raises(ValueError, match="relation_acl_changed"):
        custody._require_relation_acl(root, catalog, native, OWNER, BUILDER, legacy=False, delegation=proof)


@pytest.mark.asyncio
async def test_legacy_observation_uses_actual_locks_and_rechecks_original_transaction(monkeypatch):
    from process import provider_directory_cms_control_custody as custody

    actor_by_field = dict(session="administrator", current="administrator", backend=1, xid="123", native_admin=True)
    connection = SimpleNamespace(
        is_in_transaction=lambda: True,
        fetch=AsyncMock(return_value=[dict(oid=901)]),
        fetchrow=AsyncMock(return_value=actor_by_field),
    )
    policy = SimpleNamespace(owner_role="owner", publisher_role="publisher", runtime_roles=("builder", "reader"))
    monkeypatch.setattr(custody, "_admin_identity", AsyncMock(return_value=actor_by_field))
    proof = await custody._control_delegation(connection, policy, _receipt(), True)
    assert proof.locked_relations == frozenset({901})
    query = connection.fetch.await_args.args[0]
    assert "pid=pg_backend_pid()" in query and "mode='AccessExclusiveLock' AND granted" in query
    assert "database=(SELECT oid FROM pg_database WHERE datname=current_database())" in query
    await custody._require_legacy_observation(connection, policy, (actor_by_field, asyncio.current_task()), proof)
    connection.fetch.return_value = []
    with pytest.raises(ValueError, match="legacy_locks_changed"):
        await custody._require_legacy_observation(connection, policy, (actor_by_field, asyncio.current_task()), proof)
    connection.fetch.return_value = [dict(oid=901)]
    custody._admin_identity.return_value = dict(actor_by_field, backend=2)
    with pytest.raises(ValueError, match="admin_transaction_changed"):
        await custody._require_legacy_observation(connection, policy, (actor_by_field, asyncio.current_task()), proof)


@pytest.mark.asyncio
async def test_legacy_control_capture_refuses_nonadmin_before_other_io(monkeypatch):
    from process import provider_directory_cms_control_custody as custody
    from process import provider_directory_generic_source_provision as provision
    from process.provider_directory_generic_source_policy import GenericSourcePolicy

    connection = SimpleNamespace(is_in_transaction=lambda: True, fetch=AsyncMock(), fetchval=AsyncMock())
    monkeypatch.setattr(custody, "_admin_identity", AsyncMock(side_effect=ValueError("independent_admin_required")))
    monkeypatch.setattr(provision, "_verify_control_closure", AsyncMock())
    policy = GenericSourcePolicy("sample", "owner", "publisher", ("builder", "reader"), ("builder",))
    with pytest.raises(ValueError, match="independent_admin_required"):
        await custody.capture_cms_control_custody(
            connection,
            policy,
            migration_role="migrator",
            namespace_owner_role="namespace",
            legacy=True,
        )
    provision._verify_control_closure.assert_not_awaited()
    connection.fetch.assert_not_awaited()
    connection.fetchval.assert_not_awaited()


def test_unbound_legacy_acquisition_guard_keeps_original_inherited_writer_refusal():
    from process import provider_directory_source_acquisition_custody as custody

    with pytest.raises(ValueError, match="inherited_writer_authority"):
        custody._require_effective_access(
            _access(_receipt()), ["one", "two"], _receipt()["roles"], BUILDER, (set(), {})
        )
