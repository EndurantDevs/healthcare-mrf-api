# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Cleanup regression proof for projection child retained-read leases."""

from unittest.mock import AsyncMock

import pytest

import process.provider_directory_projection_child_read as child_read
from process.provider_directory_projection_types import (
    ProviderDirectoryProjectionLeaseLost,
)
from tests.test_provider_directory_projection_child_read import _child_lease


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "release_failure",
    (None, ValueError("fixture-release-failed")),
)
async def test_unverified_normal_exit_releases_child_lease(
    monkeypatch,
    release_failure,
) -> None:
    """Verification failure attempts release without replacing its error."""

    lease = _child_lease()
    release_lease = AsyncMock(side_effect=release_failure)
    monkeypatch.setattr(
        child_read,
        "claim_projection_child_read_lease",
        AsyncMock(return_value=lease),
    )
    monkeypatch.setattr(
        child_read,
        "assert_verified_projection_child_read_lease",
        AsyncMock(
            side_effect=ProviderDirectoryProjectionLeaseLost(
                "provider_directory_projection_child_read_not_verified"
            )
        ),
    )
    monkeypatch.setattr(
        child_read,
        "release_projection_child_read_lease",
        release_lease,
    )

    with pytest.raises(
        ProviderDirectoryProjectionLeaseLost,
        match="provider_directory_projection_child_read_not_verified",
    ):
        async with child_read.claimed_projection_child_read_lease(
            lease.shard_claim
        ) as claimed_lease:
            assert claimed_lease == lease

    release_lease.assert_awaited_once_with(
        lease,
        database=child_read.db,
        schema="mrf",
    )
