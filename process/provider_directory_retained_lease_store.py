# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Fenced campaign and item leases for retained acquisition."""

from __future__ import annotations

from typing import Any

import asyncpg

from process.provider_directory_retained_artifact_contract import (
    LeaseIdentity,
    RetainedArtifactError,
    require_digest,
    require_safe_id,
)
from process.provider_directory_retained_store_support import (
    assert_campaign_record_identity,
    database_record,
    database_table,
)
from process.provider_directory_retained_identity_contract import (
    assert_planned_item_commitments,
    assert_planned_item_identities,
)


async def acquire_campaign_lease(
    connection: asyncpg.Connection,
    *,
    campaign_id: str,
    owner: str,
    ttl_seconds: int = 300,
) -> LeaseIdentity:
    """Fence one campaign while still permitting independent other campaigns."""

    require_digest(campaign_id, "campaign_id")
    require_safe_id(owner, "lease_owner")
    if isinstance(ttl_seconds, bool) or not 30 <= ttl_seconds <= 3600:
        raise RetainedArtifactError("lease_ttl_invalid")
    async with connection.transaction():
        lease_record_by_field = database_record(
            await connection.fetchrow(
                f"""UPDATE {database_table('provider_directory_retained_artifact_campaign')}
                       SET lease_owner=$2, lease_epoch=lease_epoch + 1,
                           lease_expires_at=now() + ($3 * interval '1 second'),
                           updated_at=now()
                     WHERE campaign_id=$1
                       AND state IN ('planned', 'preflighting', 'downloading',
                                     'sealed_incomplete')
                       AND (lease_owner IS NULL OR lease_expires_at <= now()
                            OR lease_owner=$2)
                     RETURNING *;""",
                campaign_id,
                owner,
                ttl_seconds,
            )
        )
        if not lease_record_by_field:
            raise RetainedArtifactError("campaign_lease_unavailable")
        assert_campaign_record_identity(lease_record_by_field)
    return LeaseIdentity(
        owner=owner,
        epoch=int(lease_record_by_field["lease_epoch"]),
    )


async def renew_campaign_lease(
    connection: asyncpg.Connection,
    *,
    campaign_id: str,
    lease: LeaseIdentity,
    ttl_seconds: int = 300,
) -> None:
    """Extend an active campaign lease without changing its fence epoch."""

    lease.validate()
    if isinstance(ttl_seconds, bool) or not 30 <= ttl_seconds <= 3600:
        raise RetainedArtifactError("lease_ttl_invalid")
    async with connection.transaction():
        campaign_record_by_field = database_record(
            await connection.fetchrow(
                f"""UPDATE {database_table('provider_directory_retained_artifact_campaign')}
                       SET lease_expires_at=now() + ($4 * interval '1 second'),
                           updated_at=now()
                     WHERE campaign_id=$1 AND lease_owner=$2 AND lease_epoch=$3
                       AND lease_expires_at > now()
                     RETURNING *;""",
                campaign_id,
                lease.owner,
                lease.epoch,
                ttl_seconds,
            )
        )
        if not campaign_record_by_field:
            raise RetainedArtifactError("campaign_lease_lost")
        assert_campaign_record_identity(campaign_record_by_field)


async def release_campaign_lease(
    connection: asyncpg.Connection,
    *,
    campaign_id: str,
    lease: LeaseIdentity,
) -> None:
    """Release a campaign only when owner and fence epoch still match."""

    lease.validate()
    await connection.execute(
        f"""UPDATE {database_table('provider_directory_retained_artifact_campaign')}
               SET lease_owner=NULL, lease_expires_at=NULL, updated_at=now()
             WHERE campaign_id=$1 AND lease_owner=$2 AND lease_epoch=$3;""",
        campaign_id,
        lease.owner,
        lease.epoch,
    )


async def acquire_item_lease(
    connection: asyncpg.Connection,
    *,
    campaign_id: str,
    source_item_id: str,
    campaign_lease: LeaseIdentity,
    owner: str,
    ttl_seconds: int = 300,
) -> LeaseIdentity:
    """Lease one item so items may run in parallel without duplicate writes."""

    require_digest(source_item_id, "source_item_id")
    campaign_lease.validate()
    require_safe_id(owner, "item_lease_owner")
    if isinstance(ttl_seconds, bool) or not 30 <= ttl_seconds <= 3600:
        raise RetainedArtifactError("lease_ttl_invalid")
    async with connection.transaction():
        await _require_campaign_lease(connection, campaign_id, campaign_lease)
        lease_record_by_field = database_record(
            await connection.fetchrow(
                f"""UPDATE {database_table('provider_directory_retained_artifact_campaign_item')}
                       SET lease_owner=$3, lease_epoch=lease_epoch + 1,
                           lease_expires_at=now() + ($4 * interval '1 second'),
                           updated_at=now()
                     WHERE campaign_id=$1 AND source_item_id=$2
                       AND status NOT IN ('admitted', 'terminal_zero')
                       AND (lease_owner IS NULL OR lease_expires_at <= now()
                            OR lease_owner=$3)
                     RETURNING *;""",
                campaign_id,
                source_item_id,
                owner,
                ttl_seconds,
            )
        )
        if not lease_record_by_field:
            raise RetainedArtifactError("item_lease_unavailable")
        await _assert_item_record_identity(connection, lease_record_by_field)
    return LeaseIdentity(
        owner=owner,
        epoch=int(lease_record_by_field["lease_epoch"]),
    )


async def renew_item_lease(
    connection: asyncpg.Connection,
    *,
    campaign_id: str,
    source_item_id: str,
    lease: LeaseIdentity,
    ttl_seconds: int = 300,
) -> None:
    """Extend an active item lease without changing its fence epoch."""

    lease.validate()
    if isinstance(ttl_seconds, bool) or not 30 <= ttl_seconds <= 3600:
        raise RetainedArtifactError("lease_ttl_invalid")
    async with connection.transaction():
        item_record_by_field = database_record(
            await connection.fetchrow(
                f"""UPDATE {database_table('provider_directory_retained_artifact_campaign_item')}
                       SET lease_expires_at=now() + ($5 * interval '1 second'),
                           updated_at=now()
                     WHERE campaign_id=$1 AND source_item_id=$2
                       AND lease_owner=$3 AND lease_epoch=$4
                       AND lease_expires_at > now()
                     RETURNING *;""",
                campaign_id,
                source_item_id,
                lease.owner,
                lease.epoch,
                ttl_seconds,
            )
        )
        if not item_record_by_field:
            raise RetainedArtifactError("item_lease_lost")
        await _assert_item_record_identity(connection, item_record_by_field)


async def _assert_item_record_identity(
    connection: asyncpg.Connection,
    item_record_by_field: dict[str, Any],
) -> None:
    assert_planned_item_identities([item_record_by_field])
    commitment_record_by_field = database_record(
        await connection.fetchrow(
            f"""SELECT campaign_id, source_item_id, campaign_ordinal,
                        planned_identity_sha256
                   FROM {database_table('provider_directory_retained_artifact_campaign_item_identity')}
                  WHERE campaign_id=$1 AND source_item_id=$2 FOR SHARE;""",
            item_record_by_field["campaign_id"],
            item_record_by_field["source_item_id"],
        )
    )
    assert_planned_item_commitments(
        [item_record_by_field],
        [commitment_record_by_field] if commitment_record_by_field else [],
    )


async def _require_campaign_lease(
    connection: asyncpg.Connection,
    campaign_id: str,
    lease: LeaseIdentity,
) -> dict[str, Any]:
    lease.validate()
    campaign_record_by_field = database_record(
        await connection.fetchrow(
            f"""SELECT * FROM {database_table('provider_directory_retained_artifact_campaign')}
                  WHERE campaign_id=$1 AND lease_owner=$2 AND lease_epoch=$3
                    AND lease_expires_at > now() FOR UPDATE;""",
            campaign_id,
            lease.owner,
            lease.epoch,
        )
    )
    if not campaign_record_by_field:
        raise RetainedArtifactError("campaign_lease_lost")
    assert_campaign_record_identity(campaign_record_by_field)
    return campaign_record_by_field


async def _require_item_lease(
    connection: asyncpg.Connection,
    campaign_id: str,
    source_item_id: str,
    lease: LeaseIdentity,
) -> dict[str, Any]:
    lease.validate()
    item_record_by_field = database_record(
        await connection.fetchrow(
            f"""SELECT *
                   FROM {database_table('provider_directory_retained_artifact_campaign_item')}
                  WHERE campaign_id=$1 AND source_item_id=$2
                    AND lease_owner=$3 AND lease_epoch=$4
                    AND lease_expires_at > now() FOR UPDATE;""",
            campaign_id,
            source_item_id,
            lease.owner,
            lease.epoch,
        )
    )
    if not item_record_by_field:
        raise RetainedArtifactError("item_lease_lost")
    await _assert_item_record_identity(connection, item_record_by_field)
    return item_record_by_field


async def release_item_lease(
    connection: asyncpg.Connection,
    *,
    campaign_id: str,
    source_item_id: str,
    lease: LeaseIdentity,
) -> None:
    """Release an item only when owner and fence epoch still match."""

    lease.validate()
    await connection.execute(
        f"""UPDATE {database_table('provider_directory_retained_artifact_campaign_item')}
               SET lease_owner=NULL, lease_expires_at=NULL, updated_at=now()
             WHERE campaign_id=$1 AND source_item_id=$2
               AND lease_owner=$3 AND lease_epoch=$4;""",
        campaign_id,
        source_item_id,
        lease.owner,
        lease.epoch,
    )
