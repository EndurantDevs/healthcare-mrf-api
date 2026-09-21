# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Focused contract checks for generic custom-import capture registration."""

from __future__ import annotations

import hashlib

import pytest

from process.custom_import.capture_store import (
    CaptureReceipt,
    CaptureStoreError,
    CaptureStoreTransactionRequired,
    register_capture_bundle,
)
from process.custom_import.snowflake import SnowflakeAcquisitionManifest, SnowflakeResultPartitionManifest


def _digest(label: str) -> str:
    return hashlib.sha256(label.encode("utf-8")).hexdigest()


def _build_receipt(**changes: object) -> CaptureReceipt:
    receipt_values_by_field = {
        "stream_id": "records",
        "source_snapshot_token": "synthetic-snapshot-1",
        "byte_count": 17,
        "content_sha256": _digest("content"),
        "canonical_manifest": '{"connector":"synthetic","ordinal":1}',
        "manifest_sha256": _digest("manifest"),
    }
    receipt_values_by_field.update(changes)
    return CaptureReceipt(**receipt_values_by_field)


def test_capture_receipt_requires_canonical_sealed_manifest_facts():
    receipt = _build_receipt()

    assert receipt.stream_id == "records"
    assert receipt.canonical_manifest == '{"connector":"synthetic","ordinal":1}'
    with pytest.raises(CaptureStoreError, match="canonical JSON"):
        _build_receipt(canonical_manifest='{"ordinal":1,"connector":"synthetic"}')
    with pytest.raises(CaptureStoreError, match="duplicate"):
        _build_receipt(canonical_manifest='{"connector":"synthetic","connector":"other"}')
    with pytest.raises(CaptureStoreError, match="content digest"):
        _build_receipt(content_sha256="not-a-digest")


def test_capture_receipt_preserves_a_verified_connector_domain_seal():
    partition = SnowflakeResultPartitionManifest(1, 17, _digest("partition"))
    manifest = SnowflakeAcquisitionManifest(
        request_sha256=_digest("request"),
        statement_sha256=_digest("statement"),
        source_snapshot_token="synthetic-snapshot-1",
        schema_fingerprint=_digest("schema"),
        result_partitions=(partition,),
        content_sha256=_digest("content"),
    )

    receipt = _build_receipt(
        byte_count=partition.content_bytes,
        content_sha256=manifest.content_sha256,
        canonical_manifest=manifest.canonical_manifest,
        manifest_sha256=manifest.manifest_sha256,
    )

    assert receipt.canonical_manifest == manifest.canonical_manifest
    assert receipt.manifest_sha256 == manifest.manifest_sha256
    assert receipt.manifest_sha256 != hashlib.sha256(receipt.canonical_manifest.encode("utf-8")).hexdigest()


async def test_registration_rejects_receipts_beyond_the_v1_stream_limit_first():
    receipt = _build_receipt()

    with pytest.raises(CaptureStoreError, match="v1 stream limit"):
        await register_capture_bundle(
            _ActiveTransaction(),
            dataset_id=1,
            definition_revision_id=2,
            schema_revision_id=3,
            receipts=(receipt,) * 10,
        )


async def test_registration_rejects_oversized_aggregate_manifests_before_database_work():
    large_manifest = '{"value":"' + ("x" * 1_100_000) + '"}'

    with pytest.raises(CaptureStoreError, match="aggregate byte limit"):
        await register_capture_bundle(
            _ActiveTransaction(),
            dataset_id=1,
            definition_revision_id=2,
            schema_revision_id=3,
            receipts=(
                _build_receipt(canonical_manifest=large_manifest),
                _build_receipt(stream_id="details", canonical_manifest=large_manifest),
            ),
        )


class _NoTransaction:
    def in_transaction(self) -> bool:
        return False


class _ActiveTransaction:
    new = dirty = deleted = ()

    def in_transaction(self) -> bool:
        return True


async def test_registration_requires_a_caller_owned_transaction_before_any_database_work():
    with pytest.raises(CaptureStoreTransactionRequired):
        await register_capture_bundle(
            _NoTransaction(),
            dataset_id=1,
            definition_revision_id=2,
            schema_revision_id=3,
            receipts=(_build_receipt(),),
        )
