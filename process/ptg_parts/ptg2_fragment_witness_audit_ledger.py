# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Validate bounded fragment traversal against its sealed witness manifest."""

from __future__ import annotations

from typing import Any, Mapping

from process.ptg_parts.ptg2_source_witness_contract import (
    PTG2_V3_SOURCE_WITNESS_FRAGMENT_BYTES,
    PTG2_V3_SOURCE_WITNESS_MAX_RECONSTRUCTED_BYTES,
    source_witness_manifest_projection,
)

FRAGMENT_WITNESS_IO_FIELDS = frozenset(
    {
        "fragment_decompressions",
        "fragment_sha256_hashes",
        "token_reconstructions",
        "reconstruction_bytes",
        "decoded_evidence_bytes",
    }
)


def _validate_traversal_work(
    witness_io_by_name: Mapping[str, int], manifest_by_field: Mapping[str, Any]
) -> tuple[int, int]:
    """Bound the unique pair traversal without retaining its evidence tokens."""

    token_count = witness_io_by_name["token_reconstructions"] - manifest_by_field["evidence_dictionary_count"]
    fragment_count = (
        witness_io_by_name["fragment_decompressions"]
        - manifest_by_field["fragment_count"]
        - manifest_by_field["recipe_reference_count"]
    )
    byte_count = witness_io_by_name["reconstruction_bytes"] - manifest_by_field["evidence_reconstructed_bytes"]
    reference_count = token_count + witness_io_by_name["evidence_reuse_deliveries"]
    fragment_bytes = PTG2_V3_SOURCE_WITNESS_FRAGMENT_BYTES
    if (
        not manifest_by_field["evidence_dictionary_count"] <= token_count <= 2 * manifest_by_field["record_count"]
        or not manifest_by_field["record_count"] <= reference_count <= 2 * manifest_by_field["record_count"]
        or fragment_count < manifest_by_field["recipe_reference_count"]
        or byte_count < manifest_by_field["evidence_reconstructed_bytes"]
        or not (byte_count + fragment_bytes - 1) // fragment_bytes
        <= fragment_count
        <= (byte_count + token_count * (fragment_bytes - 1)) // fragment_bytes
        or witness_io_by_name["reconstruction_bytes"] > PTG2_V3_SOURCE_WITNESS_MAX_RECONSTRUCTED_BYTES
    ):
        raise ValueError("audit_batch_fragment_witness_work_invalid")
    return token_count, fragment_count


def validate_fragment_witness_io(
    witness_io_by_name: Mapping[str, int], expected_source_witness: Mapping[str, Any]
) -> None:
    """Require truthful initialization and pair traversal counters for v6."""

    manifest_by_field = source_witness_manifest_projection(expected_source_witness)
    token_count, fragment_count = _validate_traversal_work(witness_io_by_name, manifest_by_field)
    unique_fragments = manifest_by_field["fragment_count"]
    unique_tokens = manifest_by_field["evidence_dictionary_count"]
    fragment_references = manifest_by_field["recipe_reference_count"]
    fragment_hashes = unique_fragments + 2 * (fragment_references + fragment_count)
    reconstructions = unique_tokens + token_count
    decompressions = unique_fragments + fragment_references + fragment_count
    expected_io_by_name = {
        "payload_reads": 1,
        "payload_decodes": 1,
        "record_decodes": manifest_by_field["record_count"],
        "unique_evidence_entries": unique_tokens,
        "evidence_decompressions": decompressions,
        "fragment_decompressions": decompressions,
        "fragment_sha256_hashes": fragment_hashes,
        "token_reconstructions": reconstructions,
        "evidence_sha256_hashes": fragment_hashes + reconstructions,
        "evidence_json_parses": reconstructions,
        "decoded_evidence_bytes": (
            manifest_by_field["evidence_dictionary_raw_bytes"] + witness_io_by_name["reconstruction_bytes"]
        ),
        "repeated_evidence_decompressions": decompressions - unique_fragments,
        "repeated_evidence_sha256_hashes": fragment_hashes + reconstructions - unique_fragments - unique_tokens,
        "repeated_evidence_json_parses": token_count,
    }
    if (
        unique_fragments < 1
        or unique_tokens < 1
        or unique_fragments > fragment_references
        or any(witness_io_by_name[key] != expected_counter for key, expected_counter in expected_io_by_name.items())
    ):
        raise ValueError("audit_batch_fragment_witness_io_invalid")
