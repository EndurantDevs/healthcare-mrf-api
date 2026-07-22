# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import asyncio
import hashlib
from dataclasses import replace
from pathlib import Path

import pytest

from process import provider_directory_retained_reader as reader_module
from process.provider_directory_retained_artifact_contract import (
    RetainedArtifactError,
    RetainedCampaignMismatch,
)
from process.provider_directory_retained_consumer_claim_store import (
    release_retained_campaign_consumer,
)
from process.provider_directory_retained_blob_store import (
    retained_artifact_blob_components,
)
from process.provider_directory_retained_reader import (
    MAX_READER_CHUNK_BYTES,
    RetainedArtifactReader,
    claimed_retained_artifact_reader,
)
from tests import (
    provider_directory_retained_reader_postgres_support as reader_test_support,
)
from tests.provider_directory_retained_core_postgres_support import (
    digest,
    retained_database,
    retained_peer_connection,
)


READER_RECIPE_ID = reader_test_support.READER_RECIPE_ID
_sealed_reader_campaign = reader_test_support.sealed_reader_campaign
_collect_new_stream = reader_test_support.collect_new_stream
_drain_started_stream = reader_test_support.drain_started_stream
_assert_reader_claim_released = reader_test_support.assert_reader_claim_released
pytest_plugins = ("tests.provider_directory_retained_reader_fixtures",)


@pytest.mark.asyncio
async def test_canonical_producer_blob_streams_through_claimed_reader(
    monkeypatch,
    retained_artifact_test_root: Path,
) -> None:
    async with retained_database(monkeypatch) as (connection, _schema_name):
        retained_item, campaign_id, artifact_bytes, blob_path = (
            await _sealed_reader_campaign(
                connection,
                retained_artifact_test_root,
                "canonical-producer-reader",
            )
        )
        artifact_sha256 = hashlib.sha256(artifact_bytes).hexdigest()
        assert blob_path == retained_artifact_test_root.joinpath(
            *retained_artifact_blob_components(artifact_sha256)
        )
        async with claimed_retained_artifact_reader(
            connection,
            campaign_id=campaign_id,
            consumer_recipe_id=READER_RECIPE_ID,
        ) as reader:
            byte_stream = await reader.iter_full(
                retained_item.source_item_id,
                chunk_bytes=3,
            )
            assert await _collect_new_stream(byte_stream) == artifact_bytes
        await _assert_reader_claim_released(connection, campaign_id)


@pytest.mark.asyncio
async def test_reader_detects_exact_generation_release_during_stream(
    monkeypatch,
    retained_artifact_test_root: Path,
) -> None:
    async with retained_database(monkeypatch) as (connection, _schema_name):
        retained_item, campaign_id, _artifact_bytes, _blob_path = (
            await _sealed_reader_campaign(
                connection,
                retained_artifact_test_root,
                "reader-release",
            )
        )
        with pytest.raises(RetainedCampaignMismatch, match="claim_binding_mismatch"):
            async with claimed_retained_artifact_reader(
                connection,
                campaign_id=campaign_id,
                consumer_recipe_id=READER_RECIPE_ID,
            ) as reader:
                byte_stream = await reader.iter_full(
                    retained_item.source_item_id,
                    chunk_bytes=1,
                )
                byte_stream.__aiter__()
                await byte_stream.__anext__()
                async with retained_peer_connection() as peer_connection:
                    await release_retained_campaign_consumer(
                        peer_connection,
                        campaign_id=campaign_id,
                        consumer_recipe_id=READER_RECIPE_ID,
                        claimed_campaign_sha256=reader.campaign.campaign_sha256,
                        consumer_claim_generation=(
                            reader.campaign.consumer_claim_generation
                        ),
                    )
                await _drain_started_stream(byte_stream)
        await _assert_reader_claim_released(connection, campaign_id)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("mutation_kind", "expected_code"),
    (("rewrite", "digest_mismatch"), ("truncate", "blob_truncated")),
)
async def test_reader_detects_mutation_without_second_whole_artifact_hash(
    monkeypatch,
    retained_artifact_test_root: Path,
    mutation_kind: str,
    expected_code: str,
) -> None:
    async with retained_database(monkeypatch) as (connection, _schema_name):
        retained_item, campaign_id, artifact_bytes, blob_path = (
            await _sealed_reader_campaign(
                connection,
                retained_artifact_test_root,
                f"reader-{mutation_kind}",
            )
        )
        with pytest.raises(RetainedArtifactError, match=expected_code):
            async with claimed_retained_artifact_reader(
                connection,
                campaign_id=campaign_id,
                consumer_recipe_id=READER_RECIPE_ID,
            ) as reader:
                byte_stream = await reader.iter_full(
                    retained_item.source_item_id,
                    chunk_bytes=2,
                )
                byte_stream.__aiter__()
                await byte_stream.__anext__()
                if mutation_kind == "rewrite":
                    blob_path.write_bytes(b"X" * len(artifact_bytes))
                else:
                    blob_path.write_bytes(artifact_bytes[:2])
                await _drain_started_stream(byte_stream)
        await _assert_reader_claim_released(connection, campaign_id)


class _ConsumerFailure(RuntimeError):
    """Fixture exception whose identity must survive reader cleanup."""


@pytest.mark.asyncio
async def test_reader_preserves_consumer_exception_and_releases_partial_claim(
    monkeypatch,
    retained_artifact_test_root: Path,
) -> None:
    async with retained_database(monkeypatch) as (connection, _schema_name):
        retained_item, campaign_id, _artifact_bytes, _blob_path = (
            await _sealed_reader_campaign(
                connection,
                retained_artifact_test_root,
                "reader-consumer-error",
            )
        )
        with pytest.raises(_ConsumerFailure, match="consumer failure is primary"):
            async with claimed_retained_artifact_reader(
                connection,
                campaign_id=campaign_id,
                consumer_recipe_id=READER_RECIPE_ID,
            ) as reader:
                byte_stream = await reader.iter_full(
                    retained_item.source_item_id,
                    chunk_bytes=1,
                )
                byte_stream.__aiter__()
                await byte_stream.__anext__()
                raise _ConsumerFailure("consumer failure is primary")
        await _assert_reader_claim_released(connection, campaign_id)


@pytest.mark.asyncio
@pytest.mark.parametrize("read_mode", ("none", "partial"))
async def test_reader_requires_a_completed_verified_read_on_normal_exit(
    monkeypatch,
    retained_artifact_test_root: Path,
    read_mode: str,
) -> None:
    async with retained_database(monkeypatch) as (connection, _schema_name):
        retained_item, campaign_id, _artifact_bytes, _blob_path = (
            await _sealed_reader_campaign(
                connection,
                retained_artifact_test_root,
                f"reader-incomplete-{read_mode}",
            )
        )
        expected_code = (
            "verified_read_required" if read_mode == "none" else "incomplete_read"
        )
        with pytest.raises(RetainedArtifactError, match=expected_code):
            async with claimed_retained_artifact_reader(
                connection,
                campaign_id=campaign_id,
                consumer_recipe_id=READER_RECIPE_ID,
            ) as reader:
                await reader.heartbeat()
                if read_mode == "partial":
                    byte_stream = await reader.iter_full(
                        retained_item.source_item_id,
                        chunk_bytes=1,
                    )
                    byte_stream.__aiter__()
                    await byte_stream.__anext__()
        await _assert_reader_claim_released(connection, campaign_id)


@pytest.mark.asyncio
async def test_reader_rejects_concurrent_and_repeated_iteration(
    monkeypatch,
    retained_artifact_test_root: Path,
) -> None:
    async with retained_database(monkeypatch) as (connection, _schema_name):
        retained_item, campaign_id, artifact_bytes, _blob_path = (
            await _sealed_reader_campaign(
                connection,
                retained_artifact_test_root,
                "reader-concurrency",
            )
        )
        async with claimed_retained_artifact_reader(
            connection,
            campaign_id=campaign_id,
            consumer_recipe_id=READER_RECIPE_ID,
        ) as reader:
            byte_stream = await reader.iter_full(
                retained_item.source_item_id,
                chunk_bytes=len(artifact_bytes),
            )
            byte_stream.__aiter__()
            with pytest.raises(RetainedArtifactError, match="concurrent_iteration"):
                await reader.iter_full(retained_item.source_item_id)
            concurrent_results = await asyncio.gather(
                byte_stream.__anext__(),
                byte_stream.__anext__(),
                return_exceptions=True,
            )
            assert artifact_bytes in concurrent_results
            assert any(
                isinstance(concurrent_result, RetainedArtifactError)
                and "concurrent_iteration" in str(concurrent_result)
                for concurrent_result in concurrent_results
            )
            with pytest.raises(RetainedArtifactError, match="repeated_iteration"):
                byte_stream.__aiter__()
        await _assert_reader_claim_released(connection, campaign_id)


async def _exercise_invalid_and_single_use_requests(
    reader,
    retained_item,
    artifact_bytes: bytes,
) -> None:
    with pytest.raises(RetainedArtifactError, match="source_item_id_invalid"):
        await reader.iter_full("invalid")
    with pytest.raises(RetainedArtifactError, match="artifact_not_claimed"):
        await reader.iter_full(digest("unclaimed-reader-item"))
    with pytest.raises(RetainedArtifactError, match="range_not_claimed"):
        await reader.iter_range(retained_item.source_item_id, 1)
    with pytest.raises(RetainedArtifactError, match="chunk_bytes_invalid"):
        await reader.iter_full(
            retained_item.source_item_id,
            chunk_bytes=MAX_READER_CHUNK_BYTES + 1,
        )
    byte_stream = await reader.iter_full(
        retained_item.source_item_id,
        chunk_bytes=3,
    )
    sealed_artifact = reader.campaign.artifacts[0]
    with pytest.raises(RetainedCampaignMismatch, match="stream_identity"):
        await reader._stream_completed(object(), sealed_artifact, None)
    with pytest.raises(RetainedArtifactError, match="iteration_not_started"):
        await byte_stream.__anext__()
    assert await _collect_new_stream(byte_stream) == artifact_bytes
    await byte_stream.aclose()
    malformed_range = replace(sealed_artifact.ranges[0], range_ordinal=1)
    malformed_artifact = replace(sealed_artifact, ranges=(malformed_range,))
    with pytest.raises(RetainedCampaignMismatch, match="range_identity"):
        reader._range_for_ordinal(malformed_artifact, 0)


async def _raise_fixture_cleanup_failure(_reader) -> None:
    raise RuntimeError("fixture cleanup failure")


async def _assert_detached_reader_guards(
    connection,
    reader,
    monkeypatch,
) -> None:
    sealed_artifact = reader.campaign.artifacts[0]
    duplicate_campaign = replace(
        reader.campaign,
        artifacts=(sealed_artifact, sealed_artifact),
    )
    with pytest.raises(RetainedCampaignMismatch, match="identity_duplicate"):
        RetainedArtifactReader(connection, READER_RECIPE_ID, duplicate_campaign)
    with pytest.raises(RetainedArtifactError, match="reader_closed"):
        await reader.heartbeat()
    detached_reader = RetainedArtifactReader(
        connection,
        READER_RECIPE_ID,
        reader.campaign,
    )
    detached_reader._stream_failed(object())
    assert detached_reader._failed is True
    monkeypatch.setattr(
        reader_module,
        "_release_reader_claim",
        _raise_fixture_cleanup_failure,
    )
    await reader_module._cleanup_reader_after_error(reader, should_abort=False)


@pytest.mark.asyncio
async def test_reader_rejects_unclaimed_invalid_and_malformed_read_requests(
    monkeypatch,
    retained_artifact_test_root: Path,
) -> None:
    async with retained_database(monkeypatch) as (connection, _schema_name):
        retained_item, campaign_id, artifact_bytes, _blob_path = (
            await _sealed_reader_campaign(
                connection,
                retained_artifact_test_root,
                "reader-invalid-requests",
            )
        )
        async with claimed_retained_artifact_reader(
            connection,
            campaign_id=campaign_id,
            consumer_recipe_id=READER_RECIPE_ID,
        ) as reader:
            await _exercise_invalid_and_single_use_requests(
                reader,
                retained_item,
                artifact_bytes,
            )
        await _assert_detached_reader_guards(
            connection,
            reader,
            monkeypatch,
        )
        await _assert_reader_claim_released(connection, campaign_id)


@pytest.mark.asyncio
async def test_reader_poisoned_by_missing_blob_stays_failed_and_releases(
    monkeypatch,
    retained_artifact_test_root: Path,
) -> None:
    async with retained_database(monkeypatch) as (connection, _schema_name):
        retained_item, campaign_id, _artifact_bytes, blob_path = (
            await _sealed_reader_campaign(
                connection,
                retained_artifact_test_root,
                "reader-missing-blob",
            )
        )
        blob_path.unlink()
        with pytest.raises(RetainedArtifactError, match="incomplete_read"):
            async with claimed_retained_artifact_reader(
                connection,
                campaign_id=campaign_id,
                consumer_recipe_id=READER_RECIPE_ID,
            ) as reader:
                with pytest.raises(RetainedArtifactError, match="blob_unavailable"):
                    await reader.iter_full(retained_item.source_item_id)
                with pytest.raises(RetainedArtifactError, match="reader_failed"):
                    await reader.heartbeat()
        await _assert_reader_claim_released(connection, campaign_id)
