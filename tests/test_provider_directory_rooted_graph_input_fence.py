# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import sys
import types
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock

import pytest

from process import provider_directory_rooted_graph_current_preflight as preflight
from process.provider_directory_rooted_graph_acquisition import (
    ProviderDirectoryRootedGraphAcquisitionError,
)
from process.provider_directory_rooted_graph_acquisition_runtime import (
    revalidate_provider_directory_rooted_graph_inputs,
)
from tests.provider_directory_rooted_graph_acquisition_test_support import identity
from tests.provider_directory_rooted_graph_runtime_test_support import configure_rooted_generation


class _InputFenceDatabase:
    def __init__(self) -> None:
        self.events: list[object] = []
        self.lock_acquired = True

    @asynccontextmanager
    async def transaction(self):
        self.events.append("transaction")
        yield

    async def status(self, statement):
        assert statement == "SET LOCAL lock_timeout='1500ms'"

    async def scalar(self, _query, **parameters):
        self.events.append(("advisory", parameters["identity"]))
        return self.lock_acquired

    async def first(self, _query, **parameters):
        self.events.append(("acquisition", parameters["acquisition_id"]))
        return None


def _configure_current_fence(monkeypatch, *, matches: bool, split_brain: bool = False):
    """Bind the validated current-root proof that the locked fence must replay."""
    module = types.ModuleType("process.provider_directory_dataset_scoped_publication")
    module.EXACT_DATASET_PUBLICATION_LOCK_IDENTITY = "synthetic-current-lock"
    current = object()
    proof = object()
    module.exact_uhc_dataset_pair = object
    module.lock_exact_current_dataset = AsyncMock(side_effect=AssertionError("unfenced current selector"))

    async def require_current_root_fence(database, validated, *, lock_rows):
        assert validated is proof and lock_rows is False and ("advisory", "synthetic-current-lock") in database.events
        if split_brain:
            raise RuntimeError("synthetic split brain")
        return current

    module.exact_current_matches_root = lambda selected, root: selected is current and root == identity() and matches
    configure_rooted_generation(monkeypatch)
    monkeypatch.setattr(preflight, "prepare_current_root", AsyncMock(return_value=proof))
    monkeypatch.setattr(preflight, "require_current_root_fence", require_current_root_fence)
    monkeypatch.setitem(sys.modules, module.__name__, module)


@pytest.mark.asyncio
@pytest.mark.parametrize("split_brain", [False, True])
async def test_default_input_fence_rejects_unmatched_or_split_current(
    monkeypatch,
    split_brain: bool,
) -> None:
    _configure_current_fence(monkeypatch, matches=False, split_brain=split_brain)

    with pytest.raises(ProviderDirectoryRootedGraphAcquisitionError) as error_info:
        await revalidate_provider_directory_rooted_graph_inputs(
            identity(),
            database=_InputFenceDatabase(),
        )

    assert error_info.value.code == "input_drift"


@pytest.mark.asyncio
async def test_default_input_fence_reads_acquisition_under_the_current_root_fence(
    monkeypatch,
) -> None:
    _configure_current_fence(monkeypatch, matches=True)
    database = _InputFenceDatabase()

    selected = await revalidate_provider_directory_rooted_graph_inputs(
        identity(),
        database=database,
    )

    assert selected.is_identity_match(identity())
    assert database.events == [
        "transaction",
        ("advisory", "synthetic-current-lock"),
        ("acquisition", identity().acquisition_id),
    ]
