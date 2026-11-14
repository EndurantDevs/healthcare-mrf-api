# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Drift boundaries for exact Provider Directory current datasets."""

from dataclasses import replace
from unittest.mock import AsyncMock, Mock

import pytest

from process import provider_directory_dataset_scoped_publication as publication
from process.provider_directory_dataset_scoped_publication import (
    ProviderDirectoryDatasetScopedPublicationError,
    exact_uhc_dataset_pair,
    lock_exact_current_dataset,
    supersede_exact_current_dataset,
)
from tests.test_provider_directory_dataset_scoped_publication_contract import (
    _header_for,
    _legacy_current,
    _LegacyCurrentDatabase,
    _parent_for,
)


@pytest.mark.asyncio
async def test_locked_header_and_exact_capability_reject_readiness_drift() -> None:
    current = _legacy_current()
    database = _LegacyCurrentDatabase(current)
    database.readiness = False
    with pytest.raises(ProviderDirectoryDatasetScopedPublicationError) as foreign:
        await publication._validate_locked_header(
            database,
            exact_uhc_dataset_pair(),
            current.variant,
            _header_for(current),
            _parent_for(current),
        )
    assert foreign.value.code == "foreign_current"
    accepted_stale = await lock_exact_current_dataset(
        database,
        pair=exact_uhc_dataset_pair(),
        require_ready=False,
    )
    assert accepted_stale == current
    database.identity_valid = False
    with pytest.raises(ProviderDirectoryDatasetScopedPublicationError) as invalid:
        await lock_exact_current_dataset(
            database,
            pair=exact_uhc_dataset_pair(),
            require_ready=False,
        )
    assert invalid.value.code == "foreign_current"
    malformed_header = _header_for(current)
    malformed_header["semantic_projection_as_of"] = None
    with pytest.raises(ProviderDirectoryDatasetScopedPublicationError) as malformed:
        publication._exact_current_from_header(
            current.variant,
            malformed_header,
            _parent_for(current),
        )
    assert malformed.value.code == "state"


@pytest.mark.asyncio
async def test_supersede_handles_absence_type_success_and_write_drift() -> None:
    current = _legacy_current()
    database = _LegacyCurrentDatabase(current)
    await supersede_exact_current_dataset(database, None)
    with pytest.raises(ProviderDirectoryDatasetScopedPublicationError):
        await supersede_exact_current_dataset(database, object())
    database.readiness = False
    await supersede_exact_current_dataset(database, current)
    assert len(database.status_writes) == 2

    class _FailedWriteDatabase(_LegacyCurrentDatabase):
        async def status(self, statement: str, **parameters: object) -> int:
            await super().status(statement, **parameters)
            return 0

    with pytest.raises(ProviderDirectoryDatasetScopedPublicationError) as failed:
        await supersede_exact_current_dataset(_FailedWriteDatabase(current), current)
    assert failed.value.code == "state"


@pytest.mark.asyncio
@pytest.mark.parametrize("changed", [None, "missing_proof", "current", "input_error"])
async def test_rooted_supersession_rechecks_prevalidated_inputs_without_full_scan(monkeypatch, changed):
    from process import provider_directory_rooted_graph_current_preflight as preflight
    from process import provider_directory_rooted_graph_source_runtime as runtime

    current = _legacy_current()
    database = _LegacyCurrentDatabase(current)
    proof = object()
    original = RuntimeError("predecessor input changed")
    proof_reader = Mock(return_value=proof)
    fence = AsyncMock(return_value=current)
    ordinary = AsyncMock(side_effect=AssertionError("full content scan entered final cutover"))
    monkeypatch.setattr(runtime, "current_rooted_graph_runtime", lambda _: object())
    monkeypatch.setattr(preflight, "current_root_proof", proof_reader)
    monkeypatch.setattr(preflight, "require_current_root_fence", fence)
    monkeypatch.setattr(publication, "lock_exact_current_dataset", ordinary)
    if changed == "missing_proof":
        proof_reader.side_effect = original
    elif changed == "current":
        fence.return_value = replace(current, operation_key="e" * 64)
    elif changed == "input_error":
        fence.side_effect = original
    if changed is None:
        await supersede_exact_current_dataset(database, current)
        assert len(database.status_writes) == 2
    else:
        with pytest.raises((RuntimeError, ProviderDirectoryDatasetScopedPublicationError)) as error:
            await supersede_exact_current_dataset(database, current)
        assert not database.status_writes
        if changed != "current":
            assert error.value is original
    ordinary.assert_not_awaited()
    if changed == "missing_proof":
        fence.assert_not_awaited()
    else:
        fence.assert_awaited_once_with(database, proof)
