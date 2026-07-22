# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Public, locator-free child retained-read lease boundary."""

from __future__ import annotations

from contextlib import asynccontextmanager
import logging
from typing import Any, AsyncIterator

from db.connection import db
from process.provider_directory_projection_child_read_contract import (
    validated_child_read_lease,
)
from process.provider_directory_projection_child_read_store import (
    assert_verified_projection_child_read_lease,
    claim_projection_child_read_lease,
    heartbeat_projection_child_read_lease,
    release_projection_child_read_lease,
    verify_projection_child_read_lease,
)
from process.provider_directory_projection_types import (
    ProjectionRetainedChildLease,
    ProjectionShardClaim,
)


LOGGER = logging.getLogger(__name__)


async def _release_child_after_failure(
    lease: ProjectionRetainedChildLease,
    *,
    database: Any,
    schema: str,
) -> None:
    """Best-effort release without replacing the triggering failure."""

    try:
        await release_projection_child_read_lease(
            lease,
            database=database,
            schema=schema,
        )
    except BaseException:
        LOGGER.exception("provider_directory_projection_child_cleanup_failed")


@asynccontextmanager
async def claimed_projection_child_read_lease(
    claim: ProjectionShardClaim,
    *,
    lease_seconds: int = 300,
    database: Any = db,
    schema: str = "mrf",
) -> AsyncIterator[ProjectionRetainedChildLease]:
    """Own one child; verification and shard completion occur before exit."""

    lease = await claim_projection_child_read_lease(
        claim,
        lease_seconds=lease_seconds,
        database=database,
        schema=schema,
    )
    try:
        yield validated_child_read_lease(lease)
    except BaseException:
        await _release_child_after_failure(
            lease,
            database=database,
            schema=schema,
        )
        raise
    else:
        try:
            await assert_verified_projection_child_read_lease(
                lease,
                database=database,
                schema=schema,
            )
        except BaseException:
            await _release_child_after_failure(
                lease,
                database=database,
                schema=schema,
            )
            raise
        await release_projection_child_read_lease(
            lease,
            database=database,
            schema=schema,
        )


__all__ = (
    "assert_verified_projection_child_read_lease",
    "claim_projection_child_read_lease",
    "claimed_projection_child_read_lease",
    "heartbeat_projection_child_read_lease",
    "release_projection_child_read_lease",
    "verify_projection_child_read_lease",
)
