# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Drift boundaries for exact Provider Directory current datasets."""

import pytest

from process import provider_directory_dataset_scoped_publication as publication
from process.provider_directory_dataset_scoped_publication import (
    ProviderDirectoryDatasetScopedPublicationError,
    exact_uhc_dataset_pair,
    lock_exact_current_dataset,
    supersede_exact_current_dataset,
)
from tests.test_provider_directory_dataset_scoped_publication_contract import (
    _LegacyCurrentDatabase,
    _header_for,
    _legacy_current,
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
