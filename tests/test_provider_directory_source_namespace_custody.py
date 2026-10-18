# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""An operator may add one schema CREATE without widening other native rights."""

import json
from contextlib import asynccontextmanager
from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import provider_directory_source_namespace_custody as custody
from process.provider_directory_generic_source_policy import GenericSourcePolicy


def _native_roles():
    role_names = (
        (10, "native_admin"),
        (16, "namespace_owner"),
        (17, "source_owner"),
        (18, "source_publisher"),
        (19, "legacy_builder"),
        (20, "source_reader"),
        (21, "native_migrator"),
    )
    role_rows = [
        dict(
            oid=str(oid),
            rolname=name,
            rolcanlogin=oid not in {16, 17},
            rolsuper=oid == 10,
            rolcreatedb=False,
            rolcreaterole=False,
            rolreplication=False,
            rolbypassrls=False,
        )
        for oid, name in role_names
    ]
    return role_names, role_rows


def _native_namespace_catalog(policy):
    """Model native role/ACL observations independently of the grant driver."""
    role_names, role_rows = _native_roles()
    acl_items = [
        dict(value="namespace_owner=UC/namespace_owner", grantee=16, grantor=16),
        dict(value="source_owner=U/namespace_owner", grantee=17, grantor=16),
        dict(value="legacy_builder=UC/namespace_owner", grantee=19, grantor=16),
    ]
    catalog_by_field = dict(
        namespace=dict(
            oid="42", nspname=policy.schema, nspowner="16", nspacl=[acl_item["value"] for acl_item in acl_items]
        ),
        acl=[
            [16, 16, "CREATE", False],
            [16, 16, "USAGE", False],
            [17, 16, "USAGE", False],
            [19, 16, "CREATE", False],
            [19, 16, "USAGE", False],
        ],
        items=acl_items,
        create_item="source_owner=UC/namespace_owner",
        role_graph=dict(roles=role_rows, memberships=[]),
        access=[
            dict(
                oid=oid,
                superuser=oid == 10,
                usage=True,
                create=oid in {10, 16, 18, 19, 21},
                owner_usage=oid in {10, 17, 18, 21},
                owner_set=oid in {10, 17, 18, 21},
                namespace_member=oid in {10, 16, 21},
                namespace_set=oid in {10, 16, 21},
                database_create=oid == 10,
                ordinary=oid in {18, 19, 20},
            )
            for oid, _ in role_names
        ],
    )
    return catalog_by_field


@pytest.fixture
def catalog(monkeypatch):
    policy = GenericSourcePolicy(
        "snapshot_schema", "source_owner", "source_publisher", ("legacy_builder", "source_reader"), ("legacy_builder",)
    )
    catalog_by_field = _native_namespace_catalog(policy)
    events = []

    @asynccontextmanager
    async def transaction():
        original = deepcopy(catalog_by_field)
        try:
            yield
        except BaseException:
            catalog_by_field.clear()
            catalog_by_field.update(original)
            events.append("rollback")
            raise

    async def execute(sql):
        assert sql == 'GRANT CREATE ON SCHEMA "snapshot_schema" TO "source_owner"'
        after = custody._expected_namespace(dict(owner_oid=17, namespace_owner_oid=16, **catalog_by_field))
        catalog_by_field.clear()
        catalog_by_field.update(
            {key: value for key, value in after.items() if key not in {"owner_oid", "namespace_owner_oid"}}
        )

    driver = SimpleNamespace(
        is_in_transaction=lambda: True,
        transaction=transaction,
        execute=AsyncMock(side_effect=execute),
        fetchval=AsyncMock(side_effect=lambda *_: deepcopy(catalog_by_field)),
        fetchrow=AsyncMock(
            return_value=dict(session="native_admin", current="native_admin", backend=7, xid="9", native_admin=True)
        ),
    )
    monkeypatch.setattr(custody, "storage_roles", AsyncMock(return_value=(17, [19, 20])))
    return SimpleNamespace(policy=policy, driver=driver, catalog_by_field=catalog_by_field, events=events)


async def _capture(catalog):
    return await custody.capture_source_namespace_custody(
        catalog.driver, catalog.policy, namespace_owner_role="namespace_owner"
    )


async def _provision(catalog, verify=None):
    return await custody.provision_source_namespace_custody(
        catalog.driver,
        catalog.policy,
        namespace_owner_role="namespace_owner",
        verify_role_profile=verify or AsyncMock(),
    )


@pytest.mark.asyncio
async def test_exact_one_grant_preserves_order_ordinary_create_and_database_rights(catalog):
    before = await _capture(catalog)
    verify = AsyncMock()
    after = await _provision(catalog, verify)
    custody.require_source_namespace_adoption_delta(before, after)
    catalog.driver.execute.assert_awaited_once()
    assert verify.await_count == 2
    assert before["namespace"]["nspacl"][0] == after["namespace"]["nspacl"][0]
    assert before["namespace"]["nspacl"][2] == after["namespace"]["nspacl"][2]
    changed_create_oids = [
        current["oid"]
        for previous, current in zip(before["access"], after["access"], strict=True)
        if current["create"] and not previous["create"]
    ]
    assert changed_create_oids == [17]
    catalog.driver.execute.reset_mock()
    assert await _provision(catalog) == after
    catalog.driver.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_raw_native_json_capture_is_supported(catalog):
    expected = await _capture(catalog)
    catalog.driver.fetchval.side_effect = lambda *_: json.dumps(catalog.catalog_by_field)
    assert await _capture(catalog) == expected


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mutation", ["foreign_owner", "owner_login", "grantor", "grant_option", "ordinary_usage", "namespace_reach"]
)
async def test_untrusted_preexisting_shape_refuses_before_grant(catalog, mutation):
    match mutation:
        case "foreign_owner":
            catalog.catalog_by_field["namespace"]["nspowner"] = "19"
        case "owner_login":
            catalog.catalog_by_field["role_graph"]["roles"][1]["rolcanlogin"] = True
        case "grantor":
            catalog.catalog_by_field["acl"][2][1] = 19
        case "grant_option":
            catalog.catalog_by_field["acl"][2][3] = True
        case "ordinary_usage":
            catalog.catalog_by_field["access"][5]["owner_usage"] = True
        case _:
            catalog.catalog_by_field["access"][2]["namespace_member"] = True
    with pytest.raises(ValueError, match="source_namespace_"):
        await _provision(catalog)
    catalog.driver.execute.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mutation", ["extra_acl", "owner", "namespace_oid", "role_graph", "usage", "database", "grantor"]
)
async def test_full_native_delta_refuses_unrelated_postcut_changes(catalog, mutation):
    before = await _capture(catalog)
    after = await _provision(catalog)
    match mutation:
        case "extra_acl":
            after["acl"].append([20, 16, "CREATE", False])
        case "owner":
            after["namespace"]["nspowner"] = "17"
        case "namespace_oid":
            after["namespace"]["oid"] = "43"
        case "role_graph":
            after["role_graph"]["memberships"] = [{"roleid": "17", "member": "20"}]
        case "usage":
            after["access"][5]["usage"] = False
        case "database":
            after["access"][2]["database_create"] = True
        case _:
            after["acl"][0][1] = 10
    with pytest.raises(ValueError, match="adoption_changed"):
        custody.require_source_namespace_adoption_delta(before, after)


@pytest.mark.asyncio
async def test_late_failure_rolls_back_native_grant_and_preserves_original_transaction(catalog):
    before = await _capture(catalog)
    calls = []

    async def verify(*_args, **_kwargs):
        calls.append(None)
        if len(calls) == 2:
            raise RuntimeError("postprofile_refused")

    with pytest.raises(RuntimeError, match="postprofile_refused"):
        await _provision(catalog, verify)
    assert await _capture(catalog) == before and catalog.events == ["rollback"]


@pytest.mark.asyncio
async def test_native_transaction_substitution_is_refused(catalog):
    original = catalog.driver.fetchrow.return_value
    catalog.driver.fetchrow.side_effect = [original, {**original, "xid": "10"}]
    before = await _capture(catalog)
    with pytest.raises(ValueError, match="admin_transaction_changed"):
        await _provision(catalog)
    assert await _capture(catalog) == before


@pytest.mark.asyncio
@pytest.mark.parametrize("refusal", ["no_transaction", "ordinary_actor", "ambient_owner", "profile"])
async def test_unbound_or_ordinary_authority_never_reaches_the_grant(catalog, refusal):
    verify = AsyncMock()
    match refusal:
        case "no_transaction":
            catalog.driver.is_in_transaction = lambda: False
        case "ordinary_actor":
            catalog.driver.fetchrow.return_value["native_admin"] = False
        case "ambient_owner":
            catalog.driver.fetchrow.return_value.update(session="source_owner", current="source_owner")
        case _:
            verify.side_effect = RuntimeError("full_profile_refused")
    with pytest.raises((ValueError, RuntimeError)):
        await _provision(catalog, verify)
    catalog.driver.execute.assert_not_awaited()
