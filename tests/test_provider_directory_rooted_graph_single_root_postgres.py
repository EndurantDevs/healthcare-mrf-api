# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Disposable-PostgreSQL proof for reviewed rooted-graph authority."""

from __future__ import annotations

from contextlib import asynccontextmanager

import pytest

from process.provider_directory_rooted_graph_single_root_contract import derive_single_root_identity
from process.provider_directory_rooted_graph_twin_store import (
    ProviderDirectoryRootedGraphTwinError,
    admit_rooted_graph_single_root,
)
from tests.provider_directory_rooted_graph_rotation_pg_support import publish_legacy_root
from tests.test_provider_directory_rooted_graph_acquisition_postgres import _complete_success


@asynccontextmanager
async def _single_root_scope(monkeypatch):
    from tests.test_provider_directory_rooted_graph_publication_postgres import _lifecycle_scope

    async with _lifecycle_scope(monkeypatch) as context:
        yield context.database


async def _prove_authority(database) -> None:
    current = await publish_legacy_root(database)
    operation_key = "7" * 64
    identity = derive_single_root_identity(current, operation_key=operation_key)
    sealed = await _complete_success(database, identity.candidate)
    admission = await admit_rooted_graph_single_root(
        identity.candidate.acquisition_id,
        acquisition_operation_key=operation_key,
        database=database,
    )
    replay = await admit_rooted_graph_single_root(
        identity.candidate.acquisition_id,
        acquisition_operation_key=operation_key,
        database=database,
    )
    assert admission == replay
    assert admission.rooted_graph_sha256 == sealed.rooted_graph_sha256
    rotated = await publish_legacy_root(database, "2" * 64)
    assert rotated.dataset_id != current.dataset_id
    with pytest.raises(ProviderDirectoryRootedGraphTwinError) as stale:
        await admit_rooted_graph_single_root(
            identity.candidate.acquisition_id,
            acquisition_operation_key=operation_key,
            database=database,
        )
    assert stale.value.code == "stale"


@pytest.mark.asyncio
async def test_reviewed_single_root_binds_the_current_dataset(monkeypatch) -> None:
    """Admit and replay one current root, then reject parent rotation."""

    async with _single_root_scope(monkeypatch) as database:
        await _prove_authority(database)
