# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import hashlib
from dataclasses import replace

import pytest

from process import provider_directory_retained_producer_store as producer_store
from process.provider_directory_retained_artifact_contract import (
    ArtifactLayoutRange,
    ProducedArtifact,
    RetainedArtifactError,
    RetainedCampaignMismatch,
    expected_range_set_digest,
    produced_layout_digest,
)
from process.provider_directory_retained_blob_store import retained_artifact_blob_components
from process.provider_directory_retained_catalog_store import (
    initialize_retained_artifact_campaign,
)
from process.provider_directory_retained_consumer_claim_store import (
    claim_sealed_retained_campaign,
)
from process.provider_directory_retained_lease_store import (
    acquire_campaign_lease,
    acquire_item_lease,
)
from process.provider_directory_retained_reader_store import (
    assert_active_reader_binding,
    assert_active_reader_claim,
)
from process.provider_directory_retained_seal_store import (
    seal_retained_artifact_campaign,
    set_campaign_disk_reservation,
)
from process.provider_directory_retained_store_support import database_table
from tests.provider_directory_retained_core_postgres_support import (
    campaign_item,
    digest,
    fixed_campaign_plan,
    retained_database,
)


def _produced_artifact(
    label: str,
    artifact_kind: str,
    *,
    byte_count: int = 13,
    range_count: int = 1,
) -> ProducedArtifact:
    seed = f"{label}|".encode()
    artifact_bytes = (seed * ((byte_count // len(seed)) + 1))[:byte_count]
    artifact_sha256 = hashlib.sha256(artifact_bytes).hexdigest()
    layout_ranges = []
    for range_ordinal in range(range_count):
        raw_byte_start = byte_count * range_ordinal // range_count
        raw_byte_end = byte_count * (range_ordinal + 1) // range_count
        range_bytes = artifact_bytes[raw_byte_start:raw_byte_end]
        range_sha256 = hashlib.sha256(range_bytes).hexdigest()
        layout_ranges.append(
            ArtifactLayoutRange(
                range_ordinal=range_ordinal,
                raw_byte_start=raw_byte_start,
                raw_byte_end=raw_byte_end,
                raw_byte_count=len(range_bytes),
                raw_sha256=range_sha256,
                record_start=range_ordinal,
                record_end=range_ordinal + 1,
                record_count=1,
                canonical_sha256=range_sha256,
                canonical_byte_count=len(range_bytes),
            )
        )
    manifest_sha256 = digest(f"manifest:{label}:{range_count}")
    provisional = ProducedArtifact(
        artifact_sha256=artifact_sha256,
        artifact_kind=artifact_kind,
        artifact_byte_count=byte_count,
        artifact_record_count=range_count,
        artifact_path=f"fixture://artifact/{artifact_sha256}",
        layout_contract_id="retained-producer-fixture-layout-v1",
        layout_contract_version=1,
        range_set_sha256="0" * 64,
        canonical_byte_count=byte_count,
        manifest_sha256=manifest_sha256,
        manifest_byte_count=64,
        manifest_path=f"fixture://manifest/{manifest_sha256}",
        producer_build_id="retained-producer-fixture-v1",
        ranges=tuple(layout_ranges),
    )
    return replace(
        provisional,
        range_set_sha256=expected_range_set_digest(provisional),
    )


def _with_ranges(
    produced_artifact: ProducedArtifact,
    layout_ranges: tuple[ArtifactLayoutRange, ...],
) -> ProducedArtifact:
    provisional = replace(
        produced_artifact,
        ranges=layout_ranges,
        range_set_sha256="0" * 64,
    )
    return replace(
        provisional,
        range_set_sha256=expected_range_set_digest(provisional),
    )


async def _initialize_campaign(
    connection,
    label: str,
    retained_items,
    *,
    per_item_byte_budget: int = 1024,
    aggregate_byte_budget: int = 8192,
):
    plan = replace(
        fixed_campaign_plan(label, retained_items),
        per_item_byte_budget=per_item_byte_budget,
        aggregate_byte_budget=aggregate_byte_budget,
    ).validate()
    campaign_id = await initialize_retained_artifact_campaign(connection, plan=plan)
    campaign_lease = await acquire_campaign_lease(
        connection,
        campaign_id=campaign_id,
        owner=f"producer-{label}",
    )
    return campaign_id, campaign_lease


async def _acquire_item(connection, campaign_id, retained_item, campaign_lease):
    return await acquire_item_lease(
        connection,
        campaign_id=campaign_id,
        source_item_id=retained_item.source_item_id,
        campaign_lease=campaign_lease,
        owner=f"producer-item-{retained_item.source_item_id[:16]}",
    )


async def _leased_item_campaign(connection, label: str, retained_item):
    campaign_id, campaign_lease = await _initialize_campaign(
        connection, label, (retained_item,)
    )
    item_lease = await _acquire_item(
        connection, campaign_id, retained_item, campaign_lease
    )
    return campaign_id, campaign_lease, item_lease


async def _admit(
    connection,
    campaign_id,
    retained_item,
    campaign_lease,
    item_lease,
    produced_artifact,
):
    return await producer_store.admit_produced_artifact(
        connection,
        campaign_id=campaign_id,
        source_item_id=retained_item.source_item_id,
        campaign_lease=campaign_lease,
        item_lease=item_lease,
        produced_artifact=produced_artifact,
    )


async def _registry_counts(connection) -> tuple[int, int, int]:
    table_names = (
        "provider_directory_retained_artifact",
        "provider_directory_retained_artifact_layout",
        "provider_directory_retained_artifact_range",
    )
    counts = []
    for table_name in table_names:
        counts.append(
            int(
                await connection.fetchval(
                    f"SELECT count(*) FROM {database_table(table_name)}"
                )
            )
        )
    return counts[0], counts[1], counts[2]


@pytest.mark.asyncio
async def test_multi_range_admission_replays_and_seals_for_reader(monkeypatch) -> None:
    async with retained_database(monkeypatch) as (connection, _schema_name):
        retained_item = campaign_item("producer-multi-range")
        produced_artifact = _produced_artifact(
            "producer-multi-range",
            retained_item.artifact_kind,
            range_count=2,
        )
        campaign_id, campaign_lease, item_lease = await _leased_item_campaign(
            connection, "producer-multi-range", retained_item
        )
        expected_layout = produced_layout_digest(produced_artifact)
        admission_args = (
            connection,
            campaign_id,
            retained_item,
            campaign_lease,
            item_lease,
            produced_artifact,
        )
        assert await _admit(*admission_args) == expected_layout
        assert await _admit(*admission_args) == expected_layout
        assert await _registry_counts(connection) == (1, 1, 2)

        await set_campaign_disk_reservation(
            connection,
            campaign_id=campaign_id,
            campaign_lease=campaign_lease,
            reserved_bytes=4096,
        )
        sealed = await seal_retained_artifact_campaign(
            connection,
            campaign_id=campaign_id,
            campaign_lease=campaign_lease,
        )
        assert sealed["complete"] is True
        claimed = await claim_sealed_retained_campaign(
            connection,
            campaign_id=campaign_id,
            consumer_recipe_id="producer_reader_fixture_v1",
        )
        assert len(claimed.artifacts[0].ranges) == 2
        await assert_active_reader_claim(
            connection,
            claimed,
            "producer_reader_fixture_v1",
        )
        for layout_range in claimed.artifacts[0].ranges:
            await assert_active_reader_binding(
                connection,
                claimed,
                "producer_reader_fixture_v1",
                claimed.artifacts[0],
                layout_range,
            )


@pytest.mark.asyncio
async def test_scratch_locator_variation_replays_without_persisting_input(
    monkeypatch,
) -> None:
    """Store only digest-derived locators across equivalent scratch-path replays."""

    async with retained_database(monkeypatch) as (connection, _schema_name):
        retained_item = campaign_item("producer-locator-replay")
        produced_artifact = _produced_artifact(
            "producer-locator-replay",
            retained_item.artifact_kind,
            range_count=2,
        )
        campaign_id, campaign_lease, item_lease = await _leased_item_campaign(
            connection, "producer-locator-replay", retained_item
        )
        admission_args = (
            connection,
            campaign_id,
            retained_item,
            campaign_lease,
            item_lease,
        )
        expected_layout = produced_layout_digest(produced_artifact)
        assert await _admit(*admission_args, produced_artifact) == expected_layout
        replay = replace(
            produced_artifact,
            artifact_path="/ephemeral/run-two/artifact.ndjson",
            manifest_path="/ephemeral/run-two/manifest.json",
        )
        assert await _admit(*admission_args, replay) == expected_layout
        locators = await connection.fetchrow(
            f"""SELECT artifact_locator, manifest_locator
                   FROM {database_table('provider_directory_retained_artifact')}
                   JOIN {database_table('provider_directory_retained_artifact_layout')}
                        USING (artifact_sha256)
                  WHERE artifact_sha256=$1""",
            produced_artifact.artifact_sha256,
        )
        assert dict(locators) == {
            "artifact_locator": "/".join(
                retained_artifact_blob_components(produced_artifact.artifact_sha256)
            ),
            "manifest_locator": "/".join(
                retained_artifact_blob_components(produced_artifact.manifest_sha256)
            ),
        }
        assert "ephemeral" not in " ".join(dict(locators).values())


@pytest.mark.asyncio
async def test_artifact_registry_conflict_rolls_back(monkeypatch) -> None:
    """Reject a conflicting persisted artifact identity without changing admission."""

    async with retained_database(monkeypatch) as (connection, _schema_name):
        retained_item = campaign_item("producer-artifact-conflict")
        produced_artifact = _produced_artifact(
            "producer-artifact-conflict", retained_item.artifact_kind
        )
        campaign_id, campaign_lease, item_lease = await _leased_item_campaign(
            connection, "producer-artifact-conflict", retained_item
        )
        admission_args = (
            connection,
            campaign_id,
            retained_item,
            campaign_lease,
            item_lease,
            produced_artifact,
        )
        await _admit(*admission_args)
        await connection.execute(
            f"""UPDATE {database_table('provider_directory_retained_artifact')}
                   SET artifact_byte_count=artifact_byte_count + 1
                 WHERE artifact_sha256=$1""",
            produced_artifact.artifact_sha256,
        )
        with pytest.raises(
            RetainedCampaignMismatch, match="retained_artifact_registry_mismatch"
        ):
            await _admit(*admission_args)
        assert await _registry_counts(connection) == (1, 1, 1)


@pytest.mark.asyncio
async def test_layout_registry_conflict_rolls_back(monkeypatch) -> None:
    """Reject a second layout identity that conflicts with the registered contract."""

    async with retained_database(monkeypatch) as (connection, _schema_name):
        retained_item = campaign_item("producer-layout-source")
        produced_artifact = _produced_artifact(
            "producer-layout-source", retained_item.artifact_kind, range_count=2
        )
        campaign_id, campaign_lease, item_lease = await _leased_item_campaign(
            connection, "producer-layout-source", retained_item
        )
        await _admit(
            connection,
            campaign_id,
            retained_item,
            campaign_lease,
            item_lease,
            produced_artifact,
        )

        second_item = campaign_item("producer-layout-conflict")
        second_campaign, second_campaign_lease, second_item_lease = (
            await _leased_item_campaign(
                connection, "producer-layout-conflict", second_item
            )
        )
        changed_first_range = replace(
            produced_artifact.ranges[0],
            canonical_sha256=digest("conflicting-canonical-range"),
        )
        conflicting_layout = _with_ranges(
            produced_artifact,
            (changed_first_range, produced_artifact.ranges[1]),
        )
        with pytest.raises(
            RetainedCampaignMismatch,
            match="retained_layout_registry_mismatch",
        ):
            await _admit(
                connection,
                second_campaign,
                second_item,
                second_campaign_lease,
                second_item_lease,
                conflicting_layout,
            )
        assert await _registry_counts(connection) == (1, 1, 2)


@pytest.mark.asyncio
async def test_malformed_proofs_fail_before_registry_mutation(monkeypatch) -> None:
    async with retained_database(monkeypatch) as (connection, _schema_name):
        retained_item = campaign_item("producer-malformed")
        campaign_id, campaign_lease, item_lease = await _leased_item_campaign(
            connection, "producer-malformed", retained_item
        )
        valid_artifact = _produced_artifact(
            "producer-malformed",
            retained_item.artifact_kind,
            range_count=2,
        )
        second_range = valid_artifact.ranges[1]
        gapped_range = replace(
            second_range,
            raw_byte_start=second_range.raw_byte_start + 1,
            raw_byte_count=second_range.raw_byte_count - 1,
        )
        malformed_cases = (
            (
                _with_ranges(valid_artifact, (valid_artifact.ranges[0], gapped_range)),
                "artifact_layout_range_sequence_invalid",
            ),
            (
                replace(valid_artifact, range_set_sha256=digest("forged-range-set")),
                "artifact_range_set_mismatch",
            ),
            (
                replace(valid_artifact, artifact_sha256="invalid"),
                "artifact_sha256_invalid",
            ),
        )
        for malformed_artifact, expected_code in malformed_cases:
            with pytest.raises(RetainedArtifactError, match=expected_code):
                await _admit(
                    connection,
                    campaign_id,
                    retained_item,
                    campaign_lease,
                    item_lease,
                    malformed_artifact,
                )
        mutated_artifact = _produced_artifact(
            "producer-mutated",
            retained_item.artifact_kind,
        )
        object.__setattr__(mutated_artifact, "ranges", ())
        with pytest.raises(RetainedArtifactError, match="artifact_ranges_invalid"):
            await _admit(
                connection,
                campaign_id,
                retained_item,
                campaign_lease,
                item_lease,
                mutated_artifact,
            )
        assert await _registry_counts(connection) == (0, 0, 0)
