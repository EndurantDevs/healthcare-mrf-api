# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Fixed epoch controls reject foreign custody and unreviewed adoption deltas."""

import json
from contextlib import asynccontextmanager
from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from process import provider_directory_source_cms_epoch_control as control
from process.provider_directory_generic_candidate import GenericCandidateError
from process.provider_directory_generic_source_policy import GenericSourcePolicy


def _relation(root, oid):
    return dict(
        oid=oid,
        nspname="source_schema",
        relname=root,
        relowner=17,
        relkind="r",
        relpersistence="p",
        relispartition=False,
        parents=[],
        relrowsecurity=False,
        relforcerowsecurity=False,
        native_checks=True,
        direct_acl_closed=True,
        readers_closed=True,
        columns=[list(column) for column in control._LAYOUTS[root]],
        constraints=[dict(type="c", definition=sql, valid=valid) for sql, valid in control._CHECKS[root].items()]
        + [dict(type=kind, definition=kind, valid=True) for kind in ("p", "f")],
        indexes=[
            dict(
                oid=oid + 2,
                primary=True,
                unique=True,
                valid=True,
                method="btree",
                predicate=None,
                expression=None,
                keys=list(control._KEYS[root]),
            )
        ],
    )


@pytest.fixture
def catalog(monkeypatch):
    from process import network_fhir_source_custody as custody

    policy = GenericSourcePolicy(
        "source_schema", "source_owner", "source_publisher", ("source_reader", "legacy_builder"), ("legacy_builder",)
    )
    rows = [_relation(root, oid) for root, oid in zip(control.EPOCH_CONTROL_ROOTS, (42, 43), strict=True)]
    guards_by_oid = {
        row["oid"]: [
            dict(tgname=name, function_owner="source_owner", proowner=17, provolatile="v")
            for name in custody._GUARDS[row["relname"]]
        ]
        for row in rows
    }
    acl_by_scope = dict(
        table=[[17, 17, right, False] for right in control._OWNER_RIGHTS]
        + [[reader, 17, "SELECT", False] for reader in (18, 19)],
        columns=[],
        index_owners=[[44, 17]],
    )
    driver = SimpleNamespace(
        fetch=AsyncMock(side_effect=lambda sql, *args: rows if sql == control.CATALOG_SQL else guards_by_oid[args[0]]),
        fetchval=AsyncMock(side_effect=lambda sql, *args: deepcopy(acl_by_scope) if sql == control._ACL_SQL else True),
    )
    guard_check = Mock()
    monkeypatch.setattr(custody, "_require_guard", guard_check)
    monkeypatch.setattr(control, "storage_roles", AsyncMock(return_value=(17, [18, 19])))
    return SimpleNamespace(policy=policy, rows=rows, guards=guards_by_oid, driver=driver, guard_check=guard_check)


@pytest.mark.asyncio
async def test_exact_two_native_controls_keep_guards_and_distinct_fk_actions(catalog):
    captured = await control.capture_source_cms_epoch_controls(catalog.driver, catalog.policy)
    assert set(captured) == set(control.EPOCH_CONTROL_ROOTS)
    assert catalog.guard_check.call_count == 5
    native_calls = [call for call in catalog.driver.fetchval.await_args_list if call.args[0] != control._ACL_SQL]
    assert [call.args[-1] for call in native_calls] == ["a", "r"]
    receipts = control.cms_epoch_control_receipts(captured)
    assert all(len(receipt[key]) == 64 for receipt in receipts.values() for key in ("catalog", "guards", "acl"))


@pytest.mark.asyncio
async def test_raw_driver_and_session_json_codecs_produce_the_same_receipt(catalog):
    decoded = await control.capture_source_cms_epoch_controls(catalog.driver, catalog.policy)
    for relation in catalog.rows:
        for field in ("columns", "constraints", "indexes"):
            relation[field] = json.dumps(relation[field])
    encoded = await control.capture_source_cms_epoch_controls(catalog.driver, catalog.policy)
    assert control.cms_epoch_control_receipts(encoded) == control.cms_epoch_control_receipts(decoded)


@pytest.mark.asyncio
@pytest.mark.parametrize("mutation", ["owner", "column_dml", "inherited_dml", "rls", "layout", "fk", "guard_owner"])
async def test_untrusted_custody_cannot_become_an_input_receipt(catalog, mutation):
    row = catalog.rows[0]
    match mutation:
        case "owner":
            row["relowner"] = 999
        case "column_dml":
            row["direct_acl_closed"] = False
        case "inherited_dml":
            row["readers_closed"] = False
        case "rls":
            row["relrowsecurity"] = True
        case "layout":
            row["columns"][0][1] = "text"
        case "fk":
            catalog.driver.fetchval.side_effect = lambda *_: False
        case _:
            catalog.guards[row["oid"]][0]["function_owner"] = "legacy_builder"
    with pytest.raises(GenericCandidateError, match="cms_epoch_control_.*changed"):
        await control.capture_source_cms_epoch_controls(catalog.driver, catalog.policy)


@pytest.mark.asyncio
@pytest.mark.parametrize("mutation", [None, "body", "oid", "column", "acl", "index_owner"])
async def test_adoption_allows_only_fixed_owner_and_acl_changes(catalog, mutation):
    after = await control.capture_source_cms_epoch_controls(catalog.driver, catalog.policy)
    before = deepcopy(after)
    for entry in before.values():
        entry["relation"].update(relowner=19, direct_acl_closed=False, readers_closed=False)
        for guard in entry["guards"]:
            guard.update(proowner=19, function_owner="legacy_builder")
        entry["acl"]["index_owners"] = [[44, 19]]
    entry = next(iter(after.values()))
    match mutation:
        case "body":
            entry["guards"][0]["body"] = "changed"
        case "oid":
            entry["relation"]["oid"] = 999
        case "column":
            entry["relation"]["columns"][0][1] = "text"
        case "acl":
            entry["acl"]["columns"] = [[1, 18, 17, "INSERT", False]]
        case "index_owner":
            entry["acl"]["index_owners"] = [[44, 19]]
    if mutation is None:
        control.require_cms_epoch_adoption_delta(before, after, 17, [18, 19], "source_owner")
    else:
        with pytest.raises(GenericCandidateError, match="cms_epoch_adoption_.*changed"):
            control.require_cms_epoch_adoption_delta(before, after, 17, [18, 19], "source_owner")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "session,current,native_admin",
    [("publisher", "publisher", False), ("admin", "source_owner", True), ("source_owner", "source_owner", True)],
)
async def test_no_ordinary_or_substituted_admin_reaches_the_transition(catalog, session, current, native_admin):
    catalog.driver.is_in_transaction = lambda: True
    catalog.driver.fetchrow = AsyncMock(return_value=dict(session=session, current=current, native_admin=native_admin))
    verify = AsyncMock()
    with pytest.raises(GenericCandidateError, match="independent_admin_required"):
        await control.provision_source_cms_epoch_controls(
            catalog.driver, catalog.policy, verify_role_profile=verify, verify_writer_closure=verify
        )
    verify.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [RuntimeError("late_failure"), __import__("asyncio").CancelledError()])
async def test_late_failure_rolls_back_the_whole_adoption(catalog, monkeypatch, failure):
    events = []

    @asynccontextmanager
    async def savepoint():
        try:
            yield
        except BaseException:
            events.append("rollback")
            raise

    captured = await control.capture_source_cms_epoch_controls(catalog.driver, catalog.policy)
    monkeypatch.setattr(control, "capture_source_cms_epoch_controls", AsyncMock(return_value=captured))
    monkeypatch.setattr(control, "require_cms_epoch_adoption_delta", Mock())
    monkeypatch.setattr(control, "_close_relation_grants", AsyncMock())
    catalog.driver.is_in_transaction = lambda: True
    catalog.driver.transaction = savepoint
    catalog.driver.execute = AsyncMock()
    catalog.driver.fetch = AsyncMock(return_value=[])
    catalog.driver.fetchrow = AsyncMock(return_value=dict(session="admin", current="admin", native_admin=True))
    verify = AsyncMock(side_effect=[None, failure])
    with pytest.raises(type(failure)):
        await control.provision_source_cms_epoch_controls(
            catalog.driver, catalog.policy, verify_role_profile=verify, verify_writer_closure=AsyncMock()
        )
    assert events == ["rollback"]
    statements = [call.args[0] for call in catalog.driver.execute.await_args_list]
    assert len([sql for sql in statements if sql.startswith("ALTER FUNCTION")]) == 5
    assert not any("CREATE" in sql or "DROP" in sql for sql in statements)
