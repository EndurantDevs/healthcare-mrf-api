# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Strict claim-bound validation for native PostgreSQL COPY summaries."""

from __future__ import annotations

from dataclasses import dataclass
import math
from typing import Any, Mapping

from process.provider_directory_projection_contract import (
    claimed_projection_proof_shard,
)
from process.provider_directory_projection_json import (
    canonical_exact_json,
    decoded_json_object,
    exactly_decoded_object,
    invalid_native_spool,
)
from process.provider_directory_projection_types import (
    HASH_PATTERN,
    PROJECTION_INPUT_BLOCK_MAX_BYTES,
    PROJECTION_MIXED_RESOURCE_TYPE,
    PROJECTION_PROOF_SHARD_CONTRACT_ID,
    ProjectionProofShard,
    ProjectionRetainedChildLease,
    ProjectionShardClaim,
)


NATIVE_COPY_MAGIC = b"HPPDCOPY\0\0\0\x02"
NATIVE_COPY_SPOOL_CONTRACT_ID = (
    "healthporta.provider-directory.native-projection-copy-spool.v2"
)
NATIVE_DECODER_CONTRACT_ID = (
    "healthporta.provider-directory.fhir-json-decoder.v1"
)
NATIVE_TRANSFORM_CONTRACT_ID = (
    "healthporta.provider-directory.fhir-profile-projection.v2"
)
NATIVE_COPY_CONTRACT_ID = "healthporta.postgresql.binary-copy.v1"
NATIVE_COPY_COLUMN_CONTRACT_ID = (
    "healthporta.provider-directory.projection-stage-columns.v1"
)
NATIVE_CANONICAL_ROW_CONTRACT_ID = (
    "healthporta.provider-directory.canonical-projection-row.v2"
)
NATIVE_COPY_SUMMARY_MAX_BYTES = 1024 * 1024
NATIVE_COPY_STREAM_MAX_BYTES = 128 * 1024 * 1024

NATIVE_COPY_SUMMARY_FIELDS = (
    "record_kind",
    "contract_id",
    "decoder_contract_id",
    "transform_contract_id",
    "copy_contract_id",
    "copy_column_contract_id",
    "canonical_row_contract_id",
    "input_framing",
    "recipe_id",
    "partition_id",
    "partition_ordinal",
    "input_byte_count",
    "input_sha256",
    "canonical_input_byte_count",
    "canonical_input_sha256",
    "resource_count",
    "resource_counts",
    "first_identity",
    "last_identity",
    "canonical_row_sha256",
    "copy_byte_count",
    "copy_sha256",
    "timings_seconds",
)
NATIVE_COPY_TIMING_FIELDS = (
    "input_read_seconds",
    "parse_seconds",
    "transform_seconds",
    "sort_seconds",
    "copy_encode_seconds",
    "total_before_stdout_seconds",
)


@dataclass(frozen=True)
class NativeCopySummary:
    """Validated native COPY evidence bound to one exact child generation."""

    summary_map: Mapping[str, Any]
    resource_count_map: Mapping[str, int]
    first_identity: tuple[str, str]
    last_identity: tuple[str, str]

    @property
    def copy_byte_count(self) -> int:
        """Return the declared full PostgreSQL COPY stream size."""

        return int(self.summary_map["copy_byte_count"])

    @property
    def copy_sha256(self) -> str:
        """Return the declared full PostgreSQL COPY stream digest."""

        return str(self.summary_map["copy_sha256"])

    @property
    def resource_count(self) -> int:
        """Return the declared number of projected COPY rows."""

        return int(self.summary_map["resource_count"])


def _has_exact_scalar_shape(summary_map: Mapping[str, Any]) -> bool:
    integer_fields = (
        "partition_ordinal",
        "input_byte_count",
        "canonical_input_byte_count",
        "resource_count",
        "copy_byte_count",
    )
    string_fields = (
        "record_kind",
        "contract_id",
        "decoder_contract_id",
        "transform_contract_id",
        "copy_contract_id",
        "copy_column_contract_id",
        "canonical_row_contract_id",
        "input_framing",
        "recipe_id",
        "partition_id",
        "input_sha256",
        "canonical_input_sha256",
        "canonical_row_sha256",
        "copy_sha256",
    )
    return all(
        type(summary_map.get(field_name)) is int for field_name in integer_fields
    ) and all(
        isinstance(summary_map.get(field_name), str) for field_name in string_fields
    )


def _validated_resource_count_map(
    candidate: Any,
    claim: ProjectionShardClaim,
    resource_count: int,
) -> dict[str, int]:
    if (
        type(candidate) is not dict
        or tuple(candidate) != tuple(sorted(candidate))
        or not candidate
        or any(
            resource_type not in claim.recipe_lease.recipe.selected_resources
            or type(family_count) is not int
            or family_count <= 0
            for resource_type, family_count in candidate.items()
        )
        or sum(candidate.values()) != resource_count
    ):
        raise invalid_native_spool()
    if claim.shard.resource_type != PROJECTION_MIXED_RESOURCE_TYPE and (
        candidate != {claim.shard.resource_type: resource_count}
    ):
        raise invalid_native_spool()
    return dict(candidate)


def _validated_identity(candidate: Any) -> tuple[str, str]:
    if (
        type(candidate) is not list
        or len(candidate) != 2
        or any(not isinstance(coordinate, str) or not coordinate for coordinate in candidate)
        or len(candidate[0]) > 64
        or len(candidate[1]) > 256
    ):
        raise invalid_native_spool()
    return candidate[0], candidate[1]


def _validate_timing_map(candidate: Any) -> None:
    if (
        type(candidate) is not dict
        or tuple(candidate) != NATIVE_COPY_TIMING_FIELDS
        or any(
            type(seconds) not in {int, float}
            or not math.isfinite(seconds)
            or not 0 <= seconds <= 86_400
            for seconds in candidate.values()
        )
    ):
        raise invalid_native_spool()


def _validate_contract_coordinates(
    summary_map: Mapping[str, Any],
    claim: ProjectionShardClaim,
    framing: str,
) -> None:
    recipe = claim.recipe_lease.recipe
    expected_field_map = {
        "record_kind": "provider_directory_projection_copy_summary",
        "contract_id": NATIVE_COPY_SPOOL_CONTRACT_ID,
        "decoder_contract_id": NATIVE_DECODER_CONTRACT_ID,
        "transform_contract_id": NATIVE_TRANSFORM_CONTRACT_ID,
        "copy_contract_id": NATIVE_COPY_CONTRACT_ID,
        "copy_column_contract_id": NATIVE_COPY_COLUMN_CONTRACT_ID,
        "canonical_row_contract_id": NATIVE_CANONICAL_ROW_CONTRACT_ID,
        "input_framing": framing,
        "recipe_id": recipe.recipe_id,
        "partition_id": claim.shard.partition_id,
        "partition_ordinal": claim.shard.partition_ordinal,
    }
    if (
        recipe.decoder_contract_id != NATIVE_DECODER_CONTRACT_ID
        or recipe.transform_contract_id != NATIVE_TRANSFORM_CONTRACT_ID
        or any(
            summary_map.get(field_name) != expected_content
            for field_name, expected_content in expected_field_map.items()
        )
    ):
        raise invalid_native_spool()


def _validate_child_census(
    summary_map: Mapping[str, Any],
    child_lease: ProjectionRetainedChildLease,
    copy_byte_count: int,
) -> None:
    hash_fields = (
        "input_sha256",
        "canonical_input_sha256",
        "canonical_row_sha256",
        "copy_sha256",
    )
    if (
        summary_map.get("input_byte_count") != child_lease.expected_byte_count
        or summary_map.get("input_sha256") != child_lease.expected_payload_sha256
        or (
            child_lease.retained_range_ordinal is not None
            and summary_map.get("canonical_input_sha256") != child_lease.input_sha256
        )
        or summary_map.get("resource_count") != child_lease.expected_record_count
        or summary_map.get("copy_byte_count") != copy_byte_count
        or not 21 <= copy_byte_count <= NATIVE_COPY_STREAM_MAX_BYTES
        or not 3 <= summary_map.get("canonical_input_byte_count", 0) <= (
            PROJECTION_INPUT_BLOCK_MAX_BYTES
        )
        or any(
            HASH_PATTERN.fullmatch(str(summary_map.get(field_name, ""))) is None
            for field_name in hash_fields
        )
    ):
        raise invalid_native_spool()


def validated_native_copy_summary(
    summary_payload: bytes,
    *,
    claim: ProjectionShardClaim,
    child_lease: ProjectionRetainedChildLease,
    framing: str,
    copy_byte_count: int,
) -> NativeCopySummary:
    """Require canonical shape, fixed contracts, lineage, and bounded census."""

    summary_map = decoded_json_object(summary_payload)
    exact_summary_map = exactly_decoded_object(summary_payload)
    if (
        tuple(summary_map) != NATIVE_COPY_SUMMARY_FIELDS
        or canonical_exact_json(exact_summary_map, should_sort_keys=False)
        != summary_payload
        or not _has_exact_scalar_shape(summary_map)
    ):
        raise invalid_native_spool()
    _validate_contract_coordinates(summary_map, claim, framing)
    _validate_child_census(summary_map, child_lease, copy_byte_count)
    _validate_timing_map(summary_map.get("timings_seconds"))
    resource_count = int(summary_map["resource_count"])
    resource_count_map = _validated_resource_count_map(
        summary_map.get("resource_counts"),
        claim,
        resource_count,
    )
    first_identity = _validated_identity(summary_map.get("first_identity"))
    last_identity = _validated_identity(summary_map.get("last_identity"))
    selected_resource_set = set(claim.recipe_lease.recipe.selected_resources)
    if (
        resource_count < 1
        or first_identity > last_identity
        or first_identity[0] not in selected_resource_set
        or last_identity[0] not in selected_resource_set
    ):
        raise invalid_native_spool()
    return NativeCopySummary(
        summary_map,
        resource_count_map,
        first_identity,
        last_identity,
    )


def native_copy_projection_proof(
    summary: NativeCopySummary,
    claim: ProjectionShardClaim,
) -> ProjectionProofShard:
    """Bind validated native COPY evidence to the exact live shard claim."""

    summary_map = summary.summary_map
    resource_count_map = {
        resource_type: summary.resource_count_map.get(resource_type, 0)
        for resource_type in claim.recipe_lease.recipe.selected_resources
    }
    producer_proof_map = {
        field_name: summary_map[field_name]
        for field_name in (
            "contract_id",
            "decoder_contract_id",
            "transform_contract_id",
            "copy_contract_id",
            "copy_column_contract_id",
            "canonical_row_contract_id",
            "input_framing",
            "input_byte_count",
            "input_sha256",
            "canonical_input_byte_count",
            "canonical_input_sha256",
            "copy_byte_count",
            "copy_sha256",
        )
    }
    proof_map = {
        "contract_id": PROJECTION_PROOF_SHARD_CONTRACT_ID,
        "recipe_id": claim.recipe_lease.recipe.recipe_id,
        "attempt": claim.recipe_lease.attempt,
        "partition_attempt": claim.partition_attempt,
        "partition_id": claim.shard.partition_id,
        "partition_ordinal": claim.shard.partition_ordinal,
        "resource_type": claim.shard.resource_type,
        "input_sha256": claim.shard.input_sha256,
        "canonical_row_sha256": summary_map["canonical_row_sha256"],
        "resource_count": summary.resource_count,
        "resource_counts": resource_count_map,
        "first_identity": list(summary.first_identity),
        "last_identity": list(summary.last_identity),
        "producer_proof": producer_proof_map,
    }
    return claimed_projection_proof_shard(proof_map, claim=claim)


__all__ = (
    "NATIVE_CANONICAL_ROW_CONTRACT_ID",
    "NATIVE_COPY_COLUMN_CONTRACT_ID",
    "NATIVE_COPY_CONTRACT_ID",
    "NATIVE_COPY_MAGIC",
    "NATIVE_COPY_SPOOL_CONTRACT_ID",
    "NATIVE_COPY_STREAM_MAX_BYTES",
    "NATIVE_COPY_SUMMARY_FIELDS",
    "NATIVE_COPY_SUMMARY_MAX_BYTES",
    "NATIVE_COPY_TIMING_FIELDS",
    "NATIVE_DECODER_CONTRACT_ID",
    "NATIVE_TRANSFORM_CONTRACT_ID",
    "NativeCopySummary",
    "native_copy_projection_proof",
    "validated_native_copy_summary",
)
