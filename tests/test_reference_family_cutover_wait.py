# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""A frozen reference-family cutover shares one short wait across its live-table locks."""

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import reference_family_archive as archive


@pytest.mark.asyncio
async def test_frozen_cutover_spends_one_shared_budget_across_live_tables():
    session = SimpleNamespace(execute=AsyncMock(), scalar=AsyncMock(return_value="2s"))
    assert archive.cutover_wait_deadline() is None
    await archive.bound_cutover_wait(session, None)
    session.execute.assert_not_awaited()
    async with archive._frozen_activation_lock_wait(session, True):
        deadline = archive.cutover_wait_deadline()
        assert deadline is not None and deadline - asyncio.get_running_loop().time() <= 0.1
        session.execute.reset_mock()
        await archive.bound_cutover_wait(session, deadline)
        await archive.bound_cutover_wait(session, deadline - 1)
    assert archive.cutover_wait_deadline() is None
    timeouts = [call.args[1]["value"] for call in session.execute.await_args_list[:2]]
    assert timeouts[0].endswith("ms") and 1 <= int(timeouts[0][:-2]) <= 100
    assert timeouts[1] == "1ms"
