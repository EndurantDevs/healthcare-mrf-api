# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Frozen reference verification needs SELECT; native DDL retains exclusive swap locks."""

from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import reference_family_archive as archive
from tests.test_reference_family_archive import _incumbent, _ownership


@asynccontextmanager
async def unbounded_fixture(_session):
    yield


@pytest.mark.asyncio
@pytest.mark.parametrize("read_only", [False, True])
@pytest.mark.parametrize("wait", [False, True])
async def test_activation_lock_mode_preserves_mutable_and_frozen_contracts(monkeypatch, read_only, wait):
    from process import entity_address_cutover_contract as cutover

    lock, drain = AsyncMock(), AsyncMock()
    owner = _ownership()
    monkeypatch.setattr(archive, "_bounded_capture", unbounded_fixture)
    monkeypatch.setattr(archive, "_lock_family", lock)
    monkeypatch.setattr(cutover, "lock_live_serving_relations", drain)
    verify = AsyncMock(return_value=owner)
    incumbent = AsyncMock()
    monkeypatch.setattr(archive, "verify_reference_family_stage_ownership", verify)
    monkeypatch.setattr(archive, "_verify_incumbent", incumbent)
    session = SimpleNamespace(execute=AsyncMock())
    expected = _incumbent()
    assert (
        await archive._lock_and_verify_activation(
            session,
            archive.reference_family_spec("places-zcta"),
            owner,
            expected,
            wait_for_readers=wait,
            stage_read_only=read_only,
        )
        is owner
    )
    mode = "ACCESS SHARE" if read_only else "ACCESS EXCLUSIVE"
    assert lock.await_args_list[0].args == (
        session,
        owner.schema_name,
        archive.reference_family_spec("places-zcta").archive_names,
        mode,
    )
    assert drain.await_count == (wait and not read_only)
    assert lock.await_count == (1 if wait and not read_only else 2)
    assert all(call.args[-1] == mode and call.kwargs == {"nowait": True} for call in lock.await_args_list)
    verify.assert_awaited_once_with(session, owner)
    incumbent.assert_awaited_once_with(session, expected)


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [None, RuntimeError("DDL failed")])
async def test_frozen_ddl_wait_restores_only_successful_transaction_setting(monkeypatch, failure):
    read = AsyncMock(return_value="7s")
    write = AsyncMock()
    monkeypatch.setattr(archive, "_timeout_value", read)
    monkeypatch.setattr(archive, "_set_local_timeout", write)
    session = object()

    async def publish():
        async with archive._frozen_activation_lock_wait(session, True):
            assert write.await_count == 1
            if failure:
                raise failure

    if failure:
        with pytest.raises(RuntimeError, match="DDL failed") as caught:
            await publish()
        assert caught.value is failure and write.await_count == 1
    else:
        await publish()
        assert [call.args for call in write.await_args_list] == [
            (session, "lock_timeout", archive._LOCK_TIMEOUT),
            (session, "lock_timeout", "7s"),
        ]
    read.assert_awaited_once_with(session, "lock_timeout")


@pytest.mark.asyncio
async def test_mutable_activation_does_not_change_ddl_wait_policy(monkeypatch):
    read, write = AsyncMock(), AsyncMock()
    monkeypatch.setattr(archive, "_timeout_value", read)
    monkeypatch.setattr(archive, "_set_local_timeout", write)
    async with archive._frozen_activation_lock_wait(object(), False):
        read.assert_not_awaited()
    read.assert_not_awaited()
    write.assert_not_awaited()
