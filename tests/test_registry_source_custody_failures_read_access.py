# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Host catalog protocols for preserving reads without widening successor access."""

import asyncio
from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import provider_directory_artifact_read_access as access


def _grant(principal, *, privilege="SELECT", column=None, grantable=False):
    return dict(
        column_name=column,
        grantee_name=principal,
        grantee_is_public=principal is None,
        privilege_type=privilege,
        is_grantable=grantable,
        grantor_name="snapshot_owner",
    )


def _publication(*, incumbent_grants=(), successor_grants=()):
    """Substitute catalog transport while exercising the common native SQL/read-grant codec."""
    from tests.test_ordinary_address_read_access import _read_driver, _relations

    relations = _relations()
    relations[0]["name"], relations[1]["name"] = "provider_profile", "incoming_profile"
    grants = [
        {
            "column_name": grant["column_name"],
            "rolname": grant["grantee_name"],
            "grantee": 0 if grant["grantee_name"] is None else index + 2,
            "privilege_type": grant["privilege_type"],
            "is_grantable": grant["is_grantable"],
        }
        for index, grant in enumerate(incumbent_grants)
        if grant["privilege_type"] == "SELECT" and grant["grantee_name"] != "snapshot_owner"
    ]
    driver = _read_driver(relations=relations, grants=grants)
    driver.selected_relations = relations
    capability = SimpleNamespace(
        policy=SimpleNamespace(schema="sample"),
        connection=driver,
        session=SimpleNamespace(execute=AsyncMock(), scalar=AsyncMock()),
    )
    stage = SimpleNamespace(
        schema="sample",
        target_relation="provider_profile",
        stage_table="incoming_profile",
        build_fence=SimpleNamespace(target_oid=101),
    )
    return capability, stage


@pytest.mark.asyncio
async def test_artifact_replacement_preserves_reader_scope_without_copying_writer_privileges():
    grants = [
        _grant("table_reader"),
        _grant("delegating_reader", column="npi", grantable=True),
        _grant(None),
        _grant("old_writer", privilege="INSERT"),
        _grant("old_writer", privilege="UPDATE", column="npi"),
        _grant("snapshot_owner"),
    ]
    before = deepcopy(grants)
    capability, stage = _publication(incumbent_grants=grants)
    await access.preserve_artifact_read_access(capability, (stage,))
    statements = [call.args[0] for call in capability.connection.execute.await_args_list]
    assert {statement for statement in statements if statement.startswith("GRANT")} == {
        'GRANT SELECT ON TABLE "sample"."incoming_profile" TO "table_reader"',
        'GRANT SELECT ("npi") ON TABLE "sample"."incoming_profile" TO "delegating_reader" WITH GRANT OPTION',
        'GRANT SELECT ON TABLE "sample"."incoming_profile" TO PUBLIC',
    }
    assert any("IN ACCESS SHARE MODE NOWAIT" in statement for statement in statements)
    assert grants == before
    capability.connection.fetchval.assert_not_awaited()
    capability.session.execute.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", ["schema", "missing_fence"])
async def test_artifact_read_access_requires_its_original_build_fence_before_catalog_io(fault):
    capability, stage = _publication()
    if fault == "schema":
        stage.schema = "foreign_schema"
        with pytest.raises(RuntimeError, match="artifact_read_access_schema_changed"):
            await access.preserve_artifact_read_access(capability, (stage,))
        capability.connection.fetch.assert_not_awaited()
    else:
        stage.build_fence = None
        await access.preserve_artifact_read_access(capability, (stage,))
    capability.connection.fetchval.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("oids", [(None, 202), (102, 202), (101, None), (101, 101)])
async def test_artifact_read_access_never_grants_on_missing_replaced_or_aliased_heaps(oids):
    capability, stage = _publication(incumbent_grants=[_grant("reader")])
    for relation, token in zip(capability.connection.selected_relations, oids, strict=True):
        relation["oid"] = token
    if oids[0] == oids[1]:
        stage.stage_table = stage.target_relation
    if oids[1] is None or stage.stage_table == stage.target_relation:
        with pytest.raises(RuntimeError, match="publication_read_access_"):
            await access.preserve_artifact_read_access(capability, (stage,))
        assert not any(call.args[0].startswith("GRANT") for call in capability.connection.execute.await_args_list)
    else:
        await access.preserve_artifact_read_access(capability, (stage,))
        if oids[0] is None:
            assert not any(call.args[0].startswith("GRANT") for call in capability.connection.execute.await_args_list)
    capability.connection.fetchval.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "catalog_results,error",
    [
        ([101, 202, False], "unsupported_relation"),
        ([101, 202, None], "unsupported_relation"),
        ([101, 202, True, 1], "locks_required"),
        ([101, 202, True, 0], "locks_required"),
    ],
)
async def test_reader_replay_requires_plain_heaps_and_both_exclusive_locks(catalog_results, error):
    capability, stage = _publication(incumbent_grants=[_grant("reader")])
    if catalog_results[2] is not True:
        capability.connection.selected_relations[1]["relrowsecurity"] = True
        with pytest.raises(RuntimeError, match="publication_read_access_"):
            await access.preserve_artifact_read_access(capability, (stage,))
    else:
        await access.preserve_artifact_read_access(capability, (stage,))
        assert any(
            "IN ACCESS SHARE MODE NOWAIT" in call.args[0] for call in capability.connection.execute.await_args_list
        )
    capability.connection.fetchval.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("owners", [("foreign_owner", "snapshot_owner"), ("snapshot_owner", "foreign_owner")])
async def test_neither_incumbent_nor_successor_owner_can_be_substituted(owners):
    """Old owner metadata cannot refuse native read-grant preservation by declared name."""
    grant = _grant("reader")
    grant["grantor_name"] = owners[0]
    capability, stage = _publication(incumbent_grants=[grant])
    capability.session.scalar.side_effect = owners
    await access.preserve_artifact_read_access(capability, (stage,))
    capability.session.scalar.assert_not_awaited()
    assert any(call.args[0].startswith("GRANT SELECT") for call in capability.connection.execute.await_args_list)


@pytest.mark.asyncio
@pytest.mark.parametrize("principal", ["unreviewed_reader", None])
async def test_successor_must_remain_private_until_reader_replay(principal):
    capability, stage = _publication(incumbent_grants=[_grant("reader")], successor_grants=[_grant(principal)])
    await access.preserve_artifact_read_access(capability, (stage,))
    capability.session.execute.assert_not_awaited()
    assert any(call.args[0].endswith('TO "reader"') for call in capability.connection.execute.await_args_list)


@pytest.mark.asyncio
async def test_untrusted_grantor_cannot_supply_replayed_reader_authority():
    grant = _grant("reader")
    grant["grantor_name"] = "foreign_grantor"
    capability, stage = _publication(incumbent_grants=[grant])
    await access.preserve_artifact_read_access(capability, (stage,))
    capability.session.execute.assert_not_awaited()
    capability.connection.fetchval.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [PermissionError("reader grant refused"), asyncio.CancelledError()])
async def test_reader_replay_failure_propagates_without_attempting_later_reader_grants(failure):
    capability, stage = _publication(incumbent_grants=[_grant("first_reader"), _grant("second_reader")])

    async def execute(statement):
        if statement.startswith("GRANT"):
            raise failure

    capability.connection.execute.side_effect = execute
    with pytest.raises(type(failure)) as observed:
        await access.preserve_artifact_read_access(capability, (stage,))
    assert observed.value is failure
    assert sum(call.args[0].startswith("GRANT") for call in capability.connection.execute.await_args_list) == 1
