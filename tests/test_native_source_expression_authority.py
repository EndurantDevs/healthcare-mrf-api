# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Explicit native custody checks both admitted writer principals."""

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import mrf_address_publication as publication


@pytest.mark.asyncio
async def test_default_expression_authority_still_uses_only_current_user():
    session = SimpleNamespace(scalar=AsyncMock(return_value=False))
    await publication._require_native_expression_owners(session, 42)
    statement, parameters = session.scalar.await_args.args
    assert "pg_has_role(current_user" in str(statement)
    assert parameters == {"oid": 42}
    session.scalar.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize("unsafe_at", [None, 0, 1])
async def test_explicit_policy_checks_both_owner_and_publisher_authority(unsafe_at):
    answers = [False, False]
    if unsafe_at is not None:
        answers[unsafe_at] = True
    session = SimpleNamespace(scalar=AsyncMock(side_effect=answers))
    roles = ("snapshot_owner", "snapshot_publisher")
    if unsafe_at is None:
        await publication._require_native_expression_owners(session, 42, executable_roles=roles)
    else:
        with pytest.raises(RuntimeError, match="executable ownership is unsupported"):
            await publication._require_native_expression_owners(session, 42, executable_roles=roles)
    calls = session.scalar.await_args_list
    assert [call.args[1]["execution_role"] for call in calls] == list(roles[: len(calls)])
    assert all("current_user" not in str(call.args[0]) for call in calls)
    assert len(calls) == (2 if unsafe_at is None else unsafe_at + 1)


@pytest.mark.asyncio
@pytest.mark.parametrize("roles", [(), ("owner",), ("owner", "owner"), ["owner", "publisher"], ("owner", "bad-role")])
async def test_malformed_explicit_subject_cannot_reach_native_catalog(roles):
    session = SimpleNamespace(scalar=AsyncMock())
    with pytest.raises((RuntimeError, ValueError)):
        await publication._require_native_expression_owners(session, 42, executable_roles=roles)
    session.scalar.assert_not_awaited()
