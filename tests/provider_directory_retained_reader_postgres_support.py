# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Shared PostgreSQL campaign and claim helpers for retained-reader tests."""

from __future__ import annotations

import hashlib
from dataclasses import replace
from pathlib import Path

from process.provider_directory_retained_artifact_contract import (
    expected_range_set_digest,
)
from process.provider_directory_retained_catalog_store import (
    initialize_retained_artifact_campaign,
)
from process.provider_directory_retained_lease_store import acquire_campaign_lease
from process.provider_directory_retained_seal_store import (
    seal_retained_artifact_campaign,
)
from process.provider_directory_retained_store_support import database_table
from tests.provider_directory_retained_core_postgres_support import (
    admit_campaign_item,
    campaign_item,
    fixed_campaign_plan,
    registry_artifact,
    registry_artifact_payload,
)
from tests.provider_directory_retained_reader_support import (
    write_retained_artifact_blob,
)


READER_RECIPE_ID = "retained_reader_profile_fixture_v1"
OVERLAP_RECIPE_IDS = (
    "retained_reader_overlap_fixture_a_v1",
    "retained_reader_overlap_fixture_b_v1",
)


async def sealed_reader_campaign(
    connection,
    artifact_root: Path,
    label: str,
    *,
    raw_byte_start: int = 0,
):
    """Create one sealed campaign and its matching retained artifact blob."""

    retained_item = campaign_item(label)
    campaign_id = await initialize_retained_artifact_campaign(
        connection,
        plan=fixed_campaign_plan(label, (retained_item,)),
    )
    campaign_lease = await acquire_campaign_lease(
        connection,
        campaign_id=campaign_id,
        owner=f"reader-{label}",
    )
    produced_artifact = registry_artifact(label, retained_item.artifact_kind)
    artifact_bytes = registry_artifact_payload(label)
    if raw_byte_start:
        produced_artifact = _artifact_with_nonzero_range(
            produced_artifact,
            artifact_bytes,
            raw_byte_start,
        )
    written_sha256, blob_path = write_retained_artifact_blob(
        artifact_root,
        artifact_bytes,
    )
    assert written_sha256 == produced_artifact.artifact_sha256
    await admit_campaign_item(
        connection,
        campaign_id,
        retained_item,
        produced_artifact,
    )
    sealed_summary = await seal_retained_artifact_campaign(
        connection,
        campaign_id=campaign_id,
        campaign_lease=campaign_lease,
    )
    assert sealed_summary["complete"] is True
    return retained_item, campaign_id, artifact_bytes, blob_path


def _artifact_with_nonzero_range(
    produced_artifact,
    artifact_bytes: bytes,
    raw_byte_start: int,
):
    """Return fixture metadata for one verified suffix range."""

    assert 0 < raw_byte_start < len(artifact_bytes)
    range_bytes = artifact_bytes[raw_byte_start:]
    range_sha256 = hashlib.sha256(range_bytes).hexdigest()
    layout_range = replace(
        produced_artifact.ranges[0],
        raw_byte_start=raw_byte_start,
        raw_byte_count=len(range_bytes),
        raw_sha256=range_sha256,
        canonical_sha256=range_sha256,
        canonical_byte_count=len(range_bytes),
    )
    provisional_artifact = replace(
        produced_artifact,
        range_set_sha256="0" * 64,
        canonical_byte_count=len(range_bytes),
        ranges=(layout_range,),
    )
    return replace(
        provisional_artifact,
        range_set_sha256=expected_range_set_digest(provisional_artifact),
    )


async def collect_new_stream(byte_stream) -> bytes:
    """Collect a not-yet-started retained byte stream."""

    return b"".join([byte_chunk async for byte_chunk in byte_stream])


async def drain_started_stream(byte_stream) -> bytes:
    """Collect a retained byte stream whose iterator was already started."""

    byte_chunks = []
    while True:
        try:
            byte_chunks.append(await byte_stream.__anext__())
        except StopAsyncIteration:
            return b"".join(byte_chunks)


async def _reader_claim_state(
    connection,
    campaign_id: str,
    consumer_recipe_id: str,
):
    return await connection.fetchrow(
        f"""SELECT consumer.released_at AS consumer_released_at,
                   reference.released_at AS reference_released_at
              FROM {database_table('provider_directory_retained_artifact_consumer')} AS consumer
              JOIN {database_table('provider_directory_retained_artifact_consumer_reference')} AS reference
                ON reference.campaign_id=consumer.campaign_id
               AND reference.consumer_recipe_id=consumer.consumer_recipe_id
             WHERE consumer.campaign_id=$1 AND consumer.consumer_recipe_id=$2""",
        campaign_id,
        consumer_recipe_id,
    )


async def assert_reader_claim_released(
    connection,
    campaign_id: str,
    consumer_recipe_id: str = READER_RECIPE_ID,
) -> None:
    """Assert that both the exact consumer and artifact reference released."""

    release_state = await _reader_claim_state(
        connection,
        campaign_id,
        consumer_recipe_id,
    )
    assert release_state["consumer_released_at"] is not None
    assert release_state["reference_released_at"] is not None


async def assert_reader_claim_active(
    connection,
    campaign_id: str,
    consumer_recipe_id: str,
) -> None:
    """Assert that both the exact consumer and artifact reference are active."""

    claim_state = await _reader_claim_state(
        connection,
        campaign_id,
        consumer_recipe_id,
    )
    assert claim_state["consumer_released_at"] is None
    assert claim_state["reference_released_at"] is None
