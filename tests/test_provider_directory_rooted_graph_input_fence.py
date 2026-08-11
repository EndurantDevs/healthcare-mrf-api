# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

from contextlib import asynccontextmanager
import sys
import types

import pytest

from process.provider_directory_rooted_graph_acquisition import (
    ProviderDirectoryRootedGraphAcquisitionError,
)
from process.provider_directory_rooted_graph_acquisition_runtime import (
    revalidate_provider_directory_rooted_graph_inputs,
)
from tests.provider_directory_rooted_graph_acquisition_test_support import identity


class _InputFenceDatabase:
    def __init__(self) -> None:
        self.events: list[object] = []

    @asynccontextmanager
    async def transaction(self):
        self.events.append("transaction")
        yield

    async def scalar(self, _query, **parameters):
        self.events.append(("advisory", parameters["identity"]))

    async def first(self, _query, **parameters):
        self.events.append(("acquisition", parameters["acquisition_id"]))
        return None


def _publication_fence_module(*, matches: bool, split_brain: bool = False):
    module = types.ModuleType("process.provider_directory_dataset_scoped_publication")
    module.EXACT_DATASET_PUBLICATION_LOCK_IDENTITY = "synthetic-current-lock"
    pair = object()
    current = object()
    module.exact_uhc_dataset_pair = lambda: pair

    async def lock_exact_current_dataset(database, *, pair):
        del database
        if split_brain:
            raise RuntimeError("synthetic split brain")
        return current

    def exact_current_matches_root(selected, root):
        return selected is current and root == identity() and matches

    module.lock_exact_current_dataset = lock_exact_current_dataset
    module.exact_current_matches_root = exact_current_matches_root
    return module


@pytest.mark.asyncio
@pytest.mark.parametrize("split_brain", [False, True])
async def test_default_input_fence_rejects_unmatched_or_split_current(
    monkeypatch,
    split_brain: bool,
) -> None:
    monkeypatch.setitem(
        sys.modules,
        "process.provider_directory_dataset_scoped_publication",
        _publication_fence_module(matches=False, split_brain=split_brain),
    )

    with pytest.raises(ProviderDirectoryRootedGraphAcquisitionError) as error_info:
        await revalidate_provider_directory_rooted_graph_inputs(
            identity(),
            database=_InputFenceDatabase(),
        )

    assert error_info.value.code == "input_drift"


@pytest.mark.asyncio
async def test_default_input_fence_uses_replaceable_exact_current_selector(
    monkeypatch,
) -> None:
    monkeypatch.setitem(
        sys.modules,
        "process.provider_directory_dataset_scoped_publication",
        _publication_fence_module(matches=True),
    )
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
