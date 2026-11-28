# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Fixed epoch controls are captured only with their reviewed layout, guards and grants."""

import json
from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from process import provider_directory_source_cms_epoch_control as control
from process.provider_directory_generic_candidate import GenericCandidateError
from process.provider_directory_generic_source_policy import GenericSourcePolicy

_OWNER_RIGHTS = ("INSERT", "SELECT", "UPDATE", "DELETE", "TRUNCATE", "REFERENCES", "TRIGGER", "MAINTAIN")


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
        table=[[17, 17, right, False] for right in _OWNER_RIGHTS]
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


@pytest.mark.asyncio
async def test_raw_driver_and_session_json_codecs_produce_the_same_capture(catalog):
    decoded = await control.capture_source_cms_epoch_controls(catalog.driver, catalog.policy)
    for relation in catalog.rows:
        for field in ("columns", "constraints", "indexes"):
            relation[field] = json.dumps(relation[field])
    encoded = await control.capture_source_cms_epoch_controls(catalog.driver, catalog.policy)
    assert encoded == decoded


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
