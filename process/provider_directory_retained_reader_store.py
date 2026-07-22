# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""O(1) claim and artifact-binding checks for retained byte readers."""

from __future__ import annotations

import asyncpg

from process.provider_directory_retained_artifact_contract import (
    ArtifactLayoutRange,
    RetainedCampaignMismatch,
    SealedRetainedArtifact,
    SealedRetainedCampaign,
)
from process.provider_directory_retained_store_support import (
    database_record,
    database_table,
)


_CLAIM_QUERY = """
SELECT campaign.contract_id,
       campaign.adapter_id,
       campaign.endpoint_id,
       campaign.credential_descriptor_sha256,
       campaign.source_census_sha256,
       campaign.observation_set_sha256,
       campaign.census_mode,
       campaign.state AS campaign_state,
       campaign.complete AS campaign_complete,
       campaign.expected_item_count,
       campaign.admitted_item_count,
       campaign.admitted_byte_count,
       campaign.admitted_record_count,
       campaign.terminal_zero_item_count,
       campaign.campaign_sha256,
       campaign.released_at AS campaign_released_at,
       consumer.claimed_campaign_sha256,
       consumer.claim_generation,
       consumer.released_at AS consumer_released_at
  FROM {campaign_table} AS campaign
  JOIN {consumer_table} AS consumer
    ON consumer.campaign_id=campaign.campaign_id
 WHERE campaign.campaign_id=$1
   AND consumer.consumer_recipe_id=$2;
"""

_BINDING_QUERY = """
SELECT campaign.contract_id,
       campaign.adapter_id,
       campaign.endpoint_id,
       campaign.credential_descriptor_sha256,
       campaign.source_census_sha256,
       campaign.observation_set_sha256,
       campaign.census_mode,
       campaign.state AS campaign_state,
       campaign.complete AS campaign_complete,
       campaign.expected_item_count,
       campaign.admitted_item_count,
       campaign.admitted_byte_count,
       campaign.admitted_record_count,
       campaign.terminal_zero_item_count,
       campaign.campaign_sha256,
       campaign.released_at AS campaign_released_at,
       consumer.claimed_campaign_sha256,
       consumer.claim_generation,
       consumer.released_at AS consumer_released_at,
       reference.artifact_sha256 AS reference_artifact_sha256,
       reference.layout_sha256 AS reference_layout_sha256,
       reference.claim_generation AS reference_claim_generation,
       reference.released_at AS reference_released_at,
       campaign_item.status AS item_status,
       campaign_item.source_entry_sha256,
       campaign_item.artifact_kind,
       campaign_item.family,
       campaign_item.collection_kind,
       campaign_item.partition_metadata_sha256,
       campaign_item.stream_identity_sha256,
       campaign_item.sequence_ordinal,
       campaign_item.item_role,
       campaign_item.acquisition_mode,
       artifact.artifact_byte_count,
       artifact.registry_status AS artifact_registry_status,
       artifact.released_at AS artifact_released_at,
       layout.artifact_record_count,
       layout.layout_contract_id,
       layout.layout_contract_version,
       layout.layout_range_count,
       layout.range_set_sha256,
       layout.canonical_byte_count,
       layout.manifest_sha256,
       layout.registry_status AS layout_registry_status,
       layout.released_at AS layout_released_at,
       layout_range.range_ordinal,
       layout_range.raw_byte_start,
       layout_range.raw_byte_end,
       layout_range.raw_byte_count,
       layout_range.raw_sha256,
       layout_range.record_start,
       layout_range.record_end,
       layout_range.record_count,
       layout_range.canonical_sha256,
       layout_range.canonical_byte_count AS range_canonical_byte_count
  FROM {campaign_table} AS campaign
  JOIN {consumer_table} AS consumer
    ON consumer.campaign_id=campaign.campaign_id
  JOIN {reference_table} AS reference
    ON reference.campaign_id=consumer.campaign_id
   AND reference.consumer_recipe_id=consumer.consumer_recipe_id
  JOIN {item_table} AS campaign_item
    ON campaign_item.campaign_id=reference.campaign_id
   AND campaign_item.source_item_id=reference.source_item_id
  JOIN {artifact_table} AS artifact
    ON artifact.artifact_sha256=reference.artifact_sha256
  JOIN {layout_table} AS layout
    ON layout.layout_sha256=reference.layout_sha256
   AND layout.artifact_sha256=reference.artifact_sha256
  LEFT JOIN {range_table} AS layout_range
    ON layout_range.layout_sha256=layout.layout_sha256
   AND layout_range.range_ordinal=$4
 WHERE campaign.campaign_id=$1
   AND consumer.consumer_recipe_id=$2
   AND reference.source_item_id=$3;
"""


def _reader_tables() -> dict[str, str]:
    return {
        "campaign_table": database_table(
            "provider_directory_retained_artifact_campaign"
        ),
        "consumer_table": database_table(
            "provider_directory_retained_artifact_consumer"
        ),
        "reference_table": database_table(
            "provider_directory_retained_artifact_consumer_reference"
        ),
        "item_table": database_table(
            "provider_directory_retained_artifact_campaign_item"
        ),
        "artifact_table": database_table("provider_directory_retained_artifact"),
        "layout_table": database_table("provider_directory_retained_artifact_layout"),
        "range_table": database_table("provider_directory_retained_artifact_range"),
    }


def _campaign_descriptor(campaign: SealedRetainedCampaign) -> dict[str, object]:
    return {
        "contract_id": campaign.contract_id,
        "adapter_id": campaign.adapter_id,
        "endpoint_id": campaign.endpoint_id,
        "credential_descriptor_sha256": campaign.credential_descriptor_sha256,
        "source_census_sha256": campaign.source_census_sha256,
        "observation_set_sha256": campaign.observation_set_sha256,
        "census_mode": campaign.census_mode,
        "campaign_state": "complete",
        "campaign_complete": True,
        "expected_item_count": campaign.expected_item_count,
        "admitted_item_count": campaign.admitted_item_count,
        "admitted_byte_count": campaign.admitted_byte_count,
        "admitted_record_count": campaign.admitted_record_count,
        "terminal_zero_item_count": campaign.terminal_zero_item_count,
        "campaign_sha256": campaign.campaign_sha256,
        "campaign_released_at": None,
        "claimed_campaign_sha256": campaign.campaign_sha256,
        "claim_generation": campaign.consumer_claim_generation,
        "consumer_released_at": None,
    }


def _artifact_descriptor(
    artifact: SealedRetainedArtifact,
    claim_generation: int,
) -> dict[str, object]:
    return {
        "reference_artifact_sha256": artifact.artifact_sha256,
        "reference_layout_sha256": artifact.layout_sha256,
        "reference_claim_generation": claim_generation,
        "reference_released_at": None,
        "item_status": "admitted",
        "source_entry_sha256": artifact.source_entry_sha256,
        "artifact_kind": artifact.artifact_kind,
        "family": artifact.family,
        "collection_kind": artifact.collection_kind,
        "partition_metadata_sha256": artifact.partition_metadata_sha256,
        "stream_identity_sha256": artifact.stream_identity_sha256,
        "sequence_ordinal": artifact.sequence_ordinal,
        "item_role": artifact.item_role,
        "acquisition_mode": artifact.acquisition_mode,
        "artifact_byte_count": artifact.artifact_byte_count,
        "artifact_registry_status": "verified",
        "artifact_released_at": None,
        "artifact_record_count": artifact.artifact_record_count,
        "layout_contract_id": artifact.layout_contract_id,
        "layout_contract_version": artifact.layout_contract_version,
        "layout_range_count": artifact.layout_range_count,
        "range_set_sha256": artifact.range_set_sha256,
        "canonical_byte_count": artifact.canonical_byte_count,
        "manifest_sha256": artifact.manifest_sha256,
        "layout_registry_status": "verified",
        "layout_released_at": None,
    }


def _range_descriptor(layout_range: ArtifactLayoutRange) -> dict[str, object]:
    return {
        "range_ordinal": layout_range.range_ordinal,
        "raw_byte_start": layout_range.raw_byte_start,
        "raw_byte_end": layout_range.raw_byte_end,
        "raw_byte_count": layout_range.raw_byte_count,
        "raw_sha256": layout_range.raw_sha256,
        "record_start": layout_range.record_start,
        "record_end": layout_range.record_end,
        "record_count": layout_range.record_count,
        "canonical_sha256": layout_range.canonical_sha256,
        "range_canonical_byte_count": layout_range.canonical_byte_count,
    }


def _require_exact_descriptor(
    actual_by_field: dict[str, object],
    expected_by_field: dict[str, object],
) -> None:
    if not actual_by_field or any(
        actual_by_field.get(field_name) != expected_field
        for field_name, expected_field in expected_by_field.items()
    ):
        raise RetainedCampaignMismatch("retained_reader_claim_binding_mismatch")


async def assert_active_reader_claim(
    connection: asyncpg.Connection,
    campaign: SealedRetainedCampaign,
    consumer_recipe_id: str,
) -> None:
    """Revalidate one exact active claim generation in O(1)."""

    query = _CLAIM_QUERY.format(**_reader_tables())
    claim_by_field = database_record(
        await connection.fetchrow(query, campaign.campaign_id, consumer_recipe_id)
    )
    _require_exact_descriptor(claim_by_field, _campaign_descriptor(campaign))


async def assert_active_reader_binding(
    connection: asyncpg.Connection,
    campaign: SealedRetainedCampaign,
    consumer_recipe_id: str,
    artifact: SealedRetainedArtifact,
    layout_range: ArtifactLayoutRange | None,
) -> None:
    """Revalidate one exact claimed artifact or range binding in O(1)."""

    range_ordinal = -1 if layout_range is None else layout_range.range_ordinal
    query = _BINDING_QUERY.format(**_reader_tables())
    binding_by_field = database_record(
        await connection.fetchrow(
            query,
            campaign.campaign_id,
            consumer_recipe_id,
            artifact.source_item_id,
            range_ordinal,
        )
    )
    expected_by_field = _campaign_descriptor(campaign)
    expected_by_field.update(
        _artifact_descriptor(artifact, campaign.consumer_claim_generation)
    )
    if layout_range is not None:
        expected_by_field.update(_range_descriptor(layout_range))
    _require_exact_descriptor(binding_by_field, expected_by_field)


__all__ = ("assert_active_reader_binding", "assert_active_reader_claim")
