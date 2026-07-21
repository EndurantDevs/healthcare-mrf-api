# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Capability-free retained campaign status summaries."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

import asyncpg

from process.provider_directory_retained_artifact_contract import (
    RetainedArtifactError,
    require_digest,
    require_safe_id,
)
from process.provider_directory_retained_store_support import (
    database_record,
    database_record_list,
    database_table,
)


def _status_query_parts(
    campaign_id: str | None = None,
    adapter_id: str | None = None,
    limit: int = 25,
) -> tuple[str, list[Any]]:
    if campaign_id is not None:
        require_digest(campaign_id, "campaign_id")
    if adapter_id is not None:
        require_safe_id(adapter_id, "adapter_id", maximum=64)
    if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= 100:
        raise RetainedArtifactError("campaign_status_limit_invalid")
    filters = []
    parameters: list[Any] = []
    if campaign_id:
        parameters.append(campaign_id)
        filters.append(f"campaign_id=${len(parameters)}")
    if adapter_id:
        parameters.append(adapter_id)
        filters.append(f"adapter_id=${len(parameters)}")
    parameters.append(limit)
    where = " WHERE " + " AND ".join(filters) if filters else ""
    return where, parameters


async def _campaign_census(
    connection: asyncpg.Connection,
    campaign_id: str,
) -> list[dict[str, Any]]:
    return database_record_list(
        await connection.fetch(
            f"""SELECT artifact_kind, family, collection_kind, item_role,
                        status, count(*) AS item_count,
                        COALESCE(sum(observed_byte_count), 0) AS observed_bytes
                   FROM {database_table('provider_directory_retained_artifact_campaign_item')}
                  WHERE campaign_id=$1
                  GROUP BY artifact_kind, family, collection_kind,
                           item_role, status
                  ORDER BY artifact_kind, family, collection_kind,
                           item_role, status;""",
            campaign_id,
        )
    )


async def retained_artifact_campaign_status(
    connection: asyncpg.Connection,
    *,
    campaign_id: str | None = None,
    adapter_id: str | None = None,
    limit: int = 25,
) -> dict[str, Any]:
    """Return only capability/path/validator-free operator summaries."""

    where, parameters = _status_query_parts(campaign_id, adapter_id, limit)
    campaigns = database_record_list(
        await connection.fetch(
            f"""SELECT campaign_id, contract_id, adapter_id, endpoint_id,
                        source_census_sha256, census_mode, state,
                        expected_item_count, expected_stream_count,
                        observed_item_count, expected_byte_count,
                        admitted_item_count, admitted_byte_count,
                        admitted_record_count, terminal_zero_item_count,
                        terminal_stream_count, failed_item_count,
                        unaccounted_item_count, item_census_sha256,
                        observation_set_sha256, campaign_sha256, complete,
                        disk_reserved_bytes, created_at, updated_at, sealed_at
                   FROM {database_table('provider_directory_retained_artifact_campaign')}
                   {where}
                  ORDER BY updated_at DESC, campaign_id DESC
                  LIMIT ${len(parameters)};""",
            *parameters,
        )
    )
    campaign_summaries = []
    for campaign in campaigns:
        campaign_census = await _campaign_census(
            connection,
            campaign["campaign_id"],
        )
        campaign_summaries.append(
            {**_safe_campaign_summary(campaign), "census": campaign_census}
        )
    return {"items": campaign_summaries, "next_cursor": None}


def _safe_campaign_summary(campaign: Mapping[str, Any]) -> dict[str, Any]:
    fields = (
        "campaign_id",
        "contract_id",
        "adapter_id",
        "endpoint_id",
        "source_census_sha256",
        "census_mode",
        "state",
        "expected_item_count",
        "expected_stream_count",
        "observed_item_count",
        "expected_byte_count",
        "admitted_item_count",
        "admitted_byte_count",
        "admitted_record_count",
        "terminal_zero_item_count",
        "terminal_stream_count",
        "failed_item_count",
        "unaccounted_item_count",
        "item_census_sha256",
        "observation_set_sha256",
        "campaign_sha256",
        "complete",
        "disk_reserved_bytes",
        "created_at",
        "updated_at",
        "sealed_at",
    )
    return {field: campaign.get(field) for field in fields}
