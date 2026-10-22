# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Host catalog protocols for preserving reads without widening successor access."""

import asyncio
from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import code_catalog_snapshot as catalog
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
    # Exercise the real ACL decoder; only the catalog transport is substituted.
    result_sets = [
        SimpleNamespace(mappings=lambda grants=grants: SimpleNamespace(all=lambda: grants))
        for grants in (incumbent_grants, successor_grants)
    ]
    session = SimpleNamespace(
        scalar=AsyncMock(side_effect=["snapshot_owner", "snapshot_owner"]),
        execute=AsyncMock(side_effect=result_sets),
    )
    capability = SimpleNamespace(
        policy=SimpleNamespace(schema="sample", owner_role="snapshot_owner"),
        connection=SimpleNamespace(fetchval=AsyncMock(side_effect=[101, 202, True, 2]), execute=AsyncMock()),
        session=session,
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
    capability, stage = _publication(incumbent_grants=grants, successor_grants=[_grant("snapshot_owner")])

    await access.preserve_artifact_read_access(capability, (stage,))

    assert {call.args[0] for call in capability.connection.execute.await_args_list} == {
        'GRANT SELECT ON TABLE "sample"."incoming_profile" TO "table_reader"',
        'GRANT SELECT ("npi") ON TABLE "sample"."incoming_profile" TO "delegating_reader" WITH GRANT OPTION',
        'GRANT SELECT ON TABLE "sample"."incoming_profile" TO PUBLIC',
    }
    assert capability.connection.execute.await_count == 3
    assert grants == before
    assert [call.args[1]["relation_oid"] for call in capability.session.execute.await_args_list] == [101, 202]


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", ["schema", "missing_fence"])
async def test_artifact_read_access_requires_its_original_build_fence_before_catalog_io(fault):
    capability, stage = _publication()
    if fault == "schema":
        stage.schema = "foreign_schema"
    else:
        stage.build_fence = None
    with pytest.raises(RuntimeError, match="artifact_read_access_fence_required"):
        await access.preserve_artifact_read_access(capability, (stage,))
    capability.connection.fetchval.assert_not_awaited()
    capability.connection.execute.assert_not_awaited()
    capability.session.execute.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("oids", [(None, 202), (102, 202), (101, None), (101, 101)])
async def test_artifact_read_access_never_grants_on_missing_replaced_or_aliased_heaps(oids):
    capability, stage = _publication(incumbent_grants=[_grant("reader")])
    capability.connection.fetchval.side_effect = oids
    with pytest.raises(RuntimeError, match="artifact_read_access_identity_changed"):
        await access.preserve_artifact_read_access(capability, (stage,))
    capability.connection.execute.assert_not_awaited()
    capability.session.execute.assert_not_awaited()


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
    capability.connection.fetchval.side_effect = catalog_results
    with pytest.raises(RuntimeError, match="artifact_read_access_" + error):
        await access.preserve_artifact_read_access(capability, (stage,))
    capability.connection.execute.assert_not_awaited()
    capability.session.execute.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("owners", [("foreign_owner", "snapshot_owner"), ("snapshot_owner", "foreign_owner")])
async def test_neither_incumbent_nor_successor_owner_can_be_substituted(owners):
    grant = _grant("reader")
    grant["grantor_name"] = owners[0]
    capability, stage = _publication(incumbent_grants=[grant])
    capability.session.scalar.side_effect = owners
    with pytest.raises(RuntimeError, match="artifact_read_access_owner_changed"):
        await access.preserve_artifact_read_access(capability, (stage,))
    capability.connection.execute.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("principal", ["unreviewed_reader", None])
async def test_successor_must_remain_private_until_reader_replay(principal):
    capability, stage = _publication(incumbent_grants=[_grant("reader")], successor_grants=[_grant(principal)])
    with pytest.raises(RuntimeError, match="artifact_read_access_stage_not_private"):
        await access.preserve_artifact_read_access(capability, (stage,))
    capability.connection.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_untrusted_grantor_cannot_supply_replayed_reader_authority():
    grant = _grant("reader")
    grant["grantor_name"] = "foreign_grantor"
    capability, stage = _publication(incumbent_grants=[grant])
    with pytest.raises(catalog.CodeCatalogSnapshotError, match="unsupported grant state"):
        await access.preserve_artifact_read_access(capability, (stage,))
    capability.connection.execute.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [PermissionError("reader grant refused"), asyncio.CancelledError()])
async def test_reader_replay_failure_propagates_without_attempting_later_reader_grants(failure):
    capability, stage = _publication(incumbent_grants=[_grant("first_reader"), _grant("second_reader")])
    capability.connection.execute.side_effect = failure
    with pytest.raises(type(failure)) as observed:
        await access.preserve_artifact_read_access(capability, (stage,))
    assert observed.value is failure
    assert capability.connection.execute.await_count == 1
