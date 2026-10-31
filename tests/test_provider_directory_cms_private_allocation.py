# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Private allocation data checks; native workspace behavior is tested separately."""

from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import provider_directory_cms_prepared_allocation as allocation
from process import provider_directory_cms_prepared_candidate as candidate

pytestmark = pytest.mark.asyncio


def _workspace():
    return SimpleNamespace(
        binding={"indexed": True, "tables": {"resource": {"oid": 101}}},
        connection=SimpleNamespace(
            is_in_transaction=lambda: True,
            is_closed=lambda: False,
            fetchval=AsyncMock(return_value="100"),
            execute=AsyncMock(),
        ),
    )


async def test_private_descriptor_is_detached_and_does_not_write(monkeypatch):
    workspace, handoff = _workspace(), object()
    inputs = AsyncMock(return_value=("frozen", {"dataset_id": "dataset"}))
    monkeypatch.setattr(candidate, "_inputs", inputs)
    expected = deepcopy(workspace.binding)
    result = await allocation.retain_prepared_cms_workspace(workspace, handoff)
    assert result == dict(contract=allocation.PRIVATE_ALLOCATION_CONTRACT, workspace=expected, transaction="100")
    assert inputs.await_count == 2
    assert all(call.args == (workspace, handoff) for call in inputs.await_args_list)
    result["workspace"]["tables"].clear()
    assert workspace.binding == expected
    workspace.connection.execute.assert_not_awaited()


async def test_missing_transaction_refuses_before_input_reads(monkeypatch):
    workspace = _workspace()
    workspace.connection.is_in_transaction = lambda: False
    inputs = AsyncMock()
    monkeypatch.setattr(candidate, "_inputs", inputs)
    with pytest.raises(ValueError, match="transaction_required"):
        await allocation.retain_prepared_cms_workspace(workspace, None)
    inputs.assert_not_awaited()
    workspace.connection.fetchval.assert_not_awaited()


@pytest.mark.parametrize("change", ["binding", "connection", "transaction", "header"])
async def test_changed_inputs_or_transaction_refuse_without_writes(monkeypatch, change):
    workspace = _workspace()
    connection = workspace.connection
    inputs = AsyncMock(return_value=("frozen", {"dataset_id": "dataset"}))

    async def current_transaction(_sql):
        if connection.fetchval.await_count == 1:
            if change == "binding":
                workspace.binding["tables"]["resource"]["oid"] = 102
            elif change == "connection":
                workspace.connection = SimpleNamespace()
            elif change == "header":
                inputs.return_value = ("frozen", {"dataset_id": "changed"})
            return "100"
        return "101" if change == "transaction" else "100"

    connection.fetchval.side_effect = current_transaction
    monkeypatch.setattr(candidate, "_inputs", inputs)
    with pytest.raises(ValueError, match="workspace_changed|transaction_changed|frozen_header_changed"):
        await allocation.retain_prepared_cms_workspace(workspace, None)
    connection.execute.assert_not_awaited()
