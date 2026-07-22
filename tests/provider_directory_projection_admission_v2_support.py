# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Shared pure fixtures for exact projection admission v2 proofs."""

from __future__ import annotations

import hashlib

from process.provider_directory_projection_contract import (
    projection_admission_input_block,
    projection_admission_terminal_zero,
    projection_completeness_manifest,
    projection_input_block,
    projection_input_set_sha256,
    projection_recipe_identity,
)


def _digest(label: str) -> str:
    return hashlib.sha256(label.encode("utf-8")).hexdigest()


CAMPAIGN_ID = _digest("campaign-id")
CAMPAIGN_SHA256 = _digest("campaign-proof")
POSITIVE_PARTITION = _digest("positive-partition")
ZERO_PARTITION = _digest("zero-partition")


def _block():
    return projection_input_block(
        upstream_artifact_id=_digest("artifact"),
        source_object_id=_digest("layout"),
        block_kind="ndjson",
        input_contract_id="test-input.v1",
        record_start=0,
        record_count=2,
        content_sha256=_digest("content"),
        payload_sha256=_digest("payload"),
        payload_bytes=128,
        summary={"resource_count": 2},
    )


def _stream(
    ordinal: int,
    *,
    proof_label: str | None = None,
    terminal_sequence_ordinal: int | None = None,
) -> dict[str, object]:
    return {
        "identity_sha256": _digest(f"stream:{ordinal}"),
        "stream_ordinal": ordinal,
        "terminal_sequence_ordinal": (
            ordinal + 3
            if terminal_sequence_ordinal is None
            else terminal_sequence_ordinal
        ),
        "terminal_proof_sha256": _digest(
            proof_label or f"terminal-proof:{ordinal}"
        ),
    }


def _partition(
    partition_hash: str,
    source_ordinal: int,
    *,
    block_count: int,
    row_count: int,
    byte_count: int,
    retained_stream: dict[str, object] | None = None,
) -> dict[str, object]:
    partition_by_field = {
        "resource_type": "Organization",
        "partition_key_hash": partition_hash,
        "source_partition_ordinal": source_ordinal,
        "terminal_cursor_hmac": _digest(f"cursor:{source_ordinal}"),
        "terminal": True,
        "block_count": block_count,
        "row_count": row_count,
        "byte_count": byte_count,
    }
    if row_count == 0:
        partition_by_field["zero_row_proof_sha256"] = (
            retained_stream["terminal_proof_sha256"]
            if retained_stream is not None
            else _digest(f"zero:{source_ordinal}")
        )
    if retained_stream is not None:
        partition_by_field["retained_stream"] = retained_stream
    return partition_by_field


def _manifest(*partitions):
    return projection_completeness_manifest(
        endpoint_campaign_hash=CAMPAIGN_SHA256,
        partition_strategy_contract_id="test-ordered-fhir.v1",
        selected_resources=("Organization",),
        required_resources=("Organization",),
        terminal_partitions=partitions,
        complete=True,
    )


def _recipe(blocks, manifest):
    return projection_recipe_identity(
        decoder_contract_id="test-decoder.v1",
        acquisition_adapter_id="test-acquisition.v1",
        input_set_sha256=projection_input_set_sha256(
            blocks,
            decoder_contract_id="test-decoder.v1",
        ),
        source_ids=("source-a",),
        transform_contract_id="test-transform.v1",
        scope_contract_id="healthporta.provider-directory.global-scope.v1",
        transform_context={
            "as_of_date": "2026-07-22",
            "time_rule_contract_id": "test-time.v1",
        },
        selected_resources=("Organization",),
        required_resources=("Organization",),
        completeness_manifest=manifest,
    )


def _binding(
    block,
    *,
    source_item: str,
    range_ordinal: int,
    partition_hash: str = POSITIVE_PARTITION,
    source_ordinal: int = 0,
    stream_identity_sha256: str | None = None,
    sequence_ordinal: int = 0,
):
    return projection_admission_input_block(
        block,
        retained_campaign_id=CAMPAIGN_ID,
        retained_campaign_sha256=CAMPAIGN_SHA256,
        retained_source_item_id=_digest(source_item),
        retained_range_ordinal=range_ordinal,
        stream_identity_sha256=(
            stream_identity_sha256 or _digest(f"stream-item:{source_item}")
        ),
        sequence_ordinal=sequence_ordinal,
        resource_type="Organization",
        partition_key_hash=partition_hash,
        source_partition_ordinal=source_ordinal,
    )


def _terminal_zero(
    stream: dict[str, object],
    *,
    source_item: str,
    partition_hash: str,
    source_ordinal: int,
):
    return projection_admission_terminal_zero(
        retained_campaign_id=CAMPAIGN_ID,
        retained_campaign_sha256=CAMPAIGN_SHA256,
        retained_source_item_id=_digest(source_item),
        resource_type="Organization",
        partition_key_hash=partition_hash,
        source_partition_ordinal=source_ordinal,
        retained_stream=stream,
    )
