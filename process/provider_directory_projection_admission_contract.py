# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Pure identity contract for acquisition-to-physical admissions."""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
from typing import Any, Mapping, Sequence

from process.provider_directory_projection_contract import (
    projection_admission_input_block,
    projection_admission_terminal_zero,
    projection_input_set_sha256,
    validated_projection_recipe_identity,
)
from process.provider_directory_projection_types import (
    ProjectionAdmissionIdentity,
    ProjectionAdmissionInputBlock,
    ProjectionAdmissionTerminalZero,
    ProjectionInputBlock,
    ProjectionRecipeIdentity,
    ProjectionShardSpec,
    ProviderDirectoryProjectionError,
    required_hash,
    stable_hash,
)
from process.provider_directory_projection_workset import (
    _normalized_workset,
    _validated_block_fields,
)


PROJECTION_ADMISSION_CONTRACT_ID = (
    "healthporta.provider-directory.projection-acquisition-admission.v2"
)
_BINDING_SET_DOMAIN = b"provider-directory-projection-admission-binding-set-v3\x00"
_STREAM_SET_DOMAIN = b"provider-directory-projection-admission-stream-set-v1\x00"


@dataclass(frozen=True)
class _AdmissionBase:
    recipe: ProjectionRecipeIdentity
    retained_campaign_id: str
    retained_campaign_sha256: str
    physical_blocks: tuple[ProjectionInputBlock, ...]
    binding_count: int
    binding_set_sha256: str
    stream_count: int
    stream_set_sha256: str
    retained_consumer_id: str


def admission_binding_fields(
    admission_block: ProjectionAdmissionInputBlock,
) -> dict[str, Any]:
    """Return one immutable retained-to-physical block mapping."""

    return {
        "binding_id": admission_block.binding_id,
        "block_id": admission_block.block.block_id,
        "retained_campaign_id": admission_block.retained_campaign_id,
        "retained_source_item_id": admission_block.retained_source_item_id,
        "retained_artifact_sha256": admission_block.block.upstream_artifact_id,
        "retained_layout_sha256": admission_block.block.source_object_id,
        "retained_range_ordinal": admission_block.retained_range_ordinal,
        "resource_type": admission_block.resource_type,
        "partition_key_hash": admission_block.partition_key_hash,
        "source_partition_ordinal": admission_block.source_partition_ordinal,
    }


def admission_stream_fields(
    terminal_zero: ProjectionAdmissionTerminalZero,
) -> dict[str, Any]:
    """Return one exact retained ordered-stream terminal mapping."""

    return {
        "binding_id": terminal_zero.binding_id,
        "retained_campaign_id": terminal_zero.retained_campaign_id,
        "retained_source_item_id": terminal_zero.retained_source_item_id,
        "resource_type": terminal_zero.resource_type,
        "partition_key_hash": terminal_zero.partition_key_hash,
        "source_partition_ordinal": terminal_zero.source_partition_ordinal,
        "stream_identity_sha256": terminal_zero.stream_identity_sha256,
        "stream_ordinal": terminal_zero.stream_ordinal,
        "terminal_sequence_ordinal": terminal_zero.terminal_sequence_ordinal,
        "terminal_proof_sha256": terminal_zero.terminal_proof_sha256,
    }


def _validated_campaign(
    blocks: Sequence[ProjectionAdmissionInputBlock],
    terminal_zeros: Sequence[ProjectionAdmissionTerminalZero],
) -> tuple[str, str]:
    logical_bindings = (*blocks, *terminal_zeros)
    if not logical_bindings:
        raise ProviderDirectoryProjectionError(
            "provider_directory_projection_admission_bindings_empty"
        )
    campaign_ids = {
        required_hash(binding.retained_campaign_id, "retained_campaign_id")
        for binding in logical_bindings
    }
    campaign_hashes = {
        required_hash(
            binding.retained_campaign_sha256,
            "retained_campaign_sha256",
        )
        for binding in logical_bindings
    }
    if len(campaign_ids) != 1 or len(campaign_hashes) != 1:
        raise ProviderDirectoryProjectionError(
            "provider_directory_projection_admission_campaign_mismatch"
        )
    return next(iter(campaign_ids)), next(iter(campaign_hashes))


def _validated_positive_bindings(
    blocks: Sequence[ProjectionAdmissionInputBlock],
) -> tuple[list[dict[str, Any]], tuple[ProjectionInputBlock, ...]]:
    """Rebuild logical mappings and deduplicate only their physical blocks."""

    if any(type(block) is not ProjectionAdmissionInputBlock for block in blocks):
        raise ProviderDirectoryProjectionError(
            "provider_directory_projection_admission_block_invalid"
        )
    fields: list[dict[str, Any]] = []
    physical_by_id: dict[str, ProjectionInputBlock] = {}
    for admission_block in blocks:
        rebuilt_binding = projection_admission_input_block(
            admission_block.block,
            retained_campaign_id=admission_block.retained_campaign_id,
            retained_campaign_sha256=admission_block.retained_campaign_sha256,
            retained_source_item_id=admission_block.retained_source_item_id,
            retained_range_ordinal=admission_block.retained_range_ordinal,
            resource_type=admission_block.resource_type,
            partition_key_hash=admission_block.partition_key_hash,
            source_partition_ordinal=admission_block.source_partition_ordinal,
        )
        if rebuilt_binding != admission_block:
            raise ProviderDirectoryProjectionError(
                "provider_directory_projection_admission_block_invalid"
            )
        _validated_block_fields(admission_block.block)
        fields.append(admission_binding_fields(admission_block))
        physical_by_id[admission_block.block.block_id] = admission_block.block
    fields.sort(key=lambda binding: binding["binding_id"])
    if len({binding["binding_id"] for binding in fields}) != len(fields):
        raise ProviderDirectoryProjectionError(
            "provider_directory_projection_admission_binding_duplicate"
        )
    return fields, tuple(physical_by_id[key] for key in sorted(physical_by_id))


def _validated_terminal_bindings(
    terminal_zeros: Sequence[ProjectionAdmissionTerminalZero],
) -> list[dict[str, Any]]:
    """Rebuild every terminal-zero mapping so public records cannot lie."""

    if any(
        type(terminal_zero) is not ProjectionAdmissionTerminalZero
        for terminal_zero in terminal_zeros
    ):
        raise ProviderDirectoryProjectionError(
            "provider_directory_projection_admission_stream_invalid"
        )
    fields: list[dict[str, Any]] = []
    for terminal_zero in terminal_zeros:
        rebuilt_binding = projection_admission_terminal_zero(
            retained_campaign_id=terminal_zero.retained_campaign_id,
            retained_campaign_sha256=terminal_zero.retained_campaign_sha256,
            retained_source_item_id=terminal_zero.retained_source_item_id,
            resource_type=terminal_zero.resource_type,
            partition_key_hash=terminal_zero.partition_key_hash,
            source_partition_ordinal=terminal_zero.source_partition_ordinal,
            retained_stream={
                "identity_sha256": terminal_zero.stream_identity_sha256,
                "stream_ordinal": terminal_zero.stream_ordinal,
                "terminal_sequence_ordinal": terminal_zero.terminal_sequence_ordinal,
                "terminal_proof_sha256": terminal_zero.terminal_proof_sha256,
            },
        )
        if rebuilt_binding != terminal_zero:
            raise ProviderDirectoryProjectionError(
                "provider_directory_projection_admission_stream_invalid"
            )
        fields.append(admission_stream_fields(terminal_zero))
    fields.sort(key=lambda binding: binding["binding_id"])
    if len({binding["binding_id"] for binding in fields}) != len(fields):
        raise ProviderDirectoryProjectionError(
            "provider_directory_projection_admission_binding_duplicate"
        )
    return fields


def _line_digest(domain: bytes, records: Sequence[str]) -> str:
    digest = hashlib.sha256()
    digest.update(domain)
    for record_ordinal, record in enumerate(records):
        if record_ordinal:
            digest.update(b"\n")
        digest.update(record.encode("ascii"))
    return digest.hexdigest()


def _binding_set_sha256(
    block_fields: Sequence[Mapping[str, Any]],
    stream_fields: Sequence[Mapping[str, Any]],
) -> str:
    binding_ids = sorted(
        [str(binding["binding_id"]) for binding in block_fields]
        + [str(binding["binding_id"]) for binding in stream_fields]
    )
    if len(set(binding_ids)) != len(binding_ids):
        raise ProviderDirectoryProjectionError(
            "provider_directory_projection_admission_binding_duplicate"
        )
    return _line_digest(_BINDING_SET_DOMAIN, binding_ids)


def _stream_set_sha256(stream_fields: Sequence[Mapping[str, Any]]) -> str:
    ordered_fields = sorted(
        stream_fields,
        key=lambda stream: (
            int(stream["stream_ordinal"]),
            str(stream["stream_identity_sha256"]),
        ),
    )
    stream_records = [
        "|".join(
            str(stream[field_name])
            for field_name in (
                "stream_identity_sha256",
                "stream_ordinal",
                "terminal_sequence_ordinal",
                "terminal_proof_sha256",
            )
        )
        for stream in ordered_fields
    ]
    return _line_digest(_STREAM_SET_DOMAIN, stream_records)


def _terminal_coordinate(terminal: Mapping[str, Any]) -> tuple[str, str, int]:
    return (
        str(terminal["resource_type"]),
        str(terminal["partition_key_hash"]),
        int(terminal["source_partition_ordinal"]),
    )


def _validate_admission_census(
    recipe: ProjectionRecipeIdentity,
    admission_blocks: Sequence[ProjectionAdmissionInputBlock],
) -> None:
    """Require exact logical block, row, and byte coverage at every coordinate."""

    terminal_by_coordinate = {
        _terminal_coordinate(terminal): terminal
        for terminal in recipe.completeness_manifest["terminal_partitions"]
    }
    blocks_by_coordinate: dict[
        tuple[str, str, int], list[ProjectionInputBlock]
    ] = {}
    for admission_block in admission_blocks:
        coordinate = (
            admission_block.resource_type,
            admission_block.partition_key_hash,
            admission_block.source_partition_ordinal,
        )
        blocks_by_coordinate.setdefault(coordinate, []).append(admission_block.block)
    if not set(blocks_by_coordinate).issubset(terminal_by_coordinate):
        raise ProviderDirectoryProjectionError(
            "provider_directory_projection_admission_census_mismatch"
        )
    for coordinate, terminal in terminal_by_coordinate.items():
        physical_blocks = blocks_by_coordinate.get(coordinate, [])
        observed_census = (
            len(physical_blocks),
            sum(block.record_count for block in physical_blocks),
            sum(block.payload_bytes for block in physical_blocks),
        )
        expected_census = (
            terminal["block_count"],
            terminal["row_count"],
            terminal["byte_count"],
        )
        if observed_census != expected_census:
            raise ProviderDirectoryProjectionError(
                "provider_directory_projection_admission_census_mismatch"
            )


def _validate_stream_census(
    recipe: ProjectionRecipeIdentity,
    terminal_zeros: Sequence[ProjectionAdmissionTerminalZero],
) -> None:
    """Bind every ordered terminal descriptor to its exact retained zero item."""

    terminal_by_coordinate = {
        _terminal_coordinate(terminal): terminal
        for terminal in recipe.completeness_manifest["terminal_partitions"]
    }
    stream_by_coordinate = {
        coordinate: terminal["retained_stream"]
        for coordinate, terminal in terminal_by_coordinate.items()
        if "retained_stream" in terminal
    }
    zero_by_coordinate = {
        (
            terminal_zero.resource_type,
            terminal_zero.partition_key_hash,
            terminal_zero.source_partition_ordinal,
        ): terminal_zero
        for terminal_zero in terminal_zeros
    }
    if (
        len(zero_by_coordinate) != len(terminal_zeros)
        or set(zero_by_coordinate) != set(stream_by_coordinate)
    ):
        raise ProviderDirectoryProjectionError(
            "provider_directory_projection_admission_stream_census_mismatch"
        )
    for coordinate, retained_stream in stream_by_coordinate.items():
        terminal_zero = zero_by_coordinate[coordinate]
        observed_stream_by_field = {
            "identity_sha256": terminal_zero.stream_identity_sha256,
            "stream_ordinal": terminal_zero.stream_ordinal,
            "terminal_sequence_ordinal": terminal_zero.terminal_sequence_ordinal,
            "terminal_proof_sha256": terminal_zero.terminal_proof_sha256,
        }
        terminal = terminal_by_coordinate[coordinate]
        if observed_stream_by_field != retained_stream or (
            terminal["row_count"] == 0
            and terminal["zero_row_proof_sha256"]
            != terminal_zero.terminal_proof_sha256
        ):
            raise ProviderDirectoryProjectionError(
                "provider_directory_projection_admission_stream_census_mismatch"
            )


def _admission_base(
    recipe: ProjectionRecipeIdentity,
    blocks: Sequence[ProjectionAdmissionInputBlock],
    terminal_zeros: Sequence[ProjectionAdmissionTerminalZero],
) -> _AdmissionBase:
    """Validate retained evidence and return one canonical admission identity."""

    recipe = validated_projection_recipe_identity(recipe)
    block_fields, physical_blocks = _validated_positive_bindings(blocks)
    stream_fields = _validated_terminal_bindings(terminal_zeros)
    retained_campaign_id, retained_campaign_sha256 = _validated_campaign(
        blocks,
        terminal_zeros,
    )
    if (
        recipe.completeness_manifest.get("endpoint_campaign_hash")
        != retained_campaign_sha256
    ):
        raise ProviderDirectoryProjectionError(
            "provider_directory_projection_admission_campaign_proof_mismatch"
        )
    _validate_admission_census(recipe, blocks)
    _validate_stream_census(recipe, terminal_zeros)
    binding_set_sha256 = _binding_set_sha256(block_fields, stream_fields)
    stream_set_sha256 = _stream_set_sha256(stream_fields)
    input_set_hash = projection_input_set_sha256(
        physical_blocks,
        decoder_contract_id=recipe.decoder_contract_id,
    )
    if input_set_hash != recipe.input_set_sha256:
        raise ProviderDirectoryProjectionError(
            "provider_directory_projection_input_manifest_hash_mismatch"
        )
    retained_consumer_id = stable_hash(
        {
            "contract_id": PROJECTION_ADMISSION_CONTRACT_ID,
            "recipe_id": recipe.recipe_id,
            "acquisition_adapter_id": recipe.acquisition_adapter_id,
            "source_scope_hash": recipe.source_scope_hash,
            "source_ids": list(recipe.source_ids),
            "completeness_manifest_hash": recipe.completeness_manifest_hash,
            "retained_campaign_id": retained_campaign_id,
            "retained_campaign_sha256": retained_campaign_sha256,
            "binding_set_sha256": binding_set_sha256,
            "stream_set_sha256": stream_set_sha256,
            "stream_count": len(stream_fields),
        },
        domain="provider-directory-projection-admission-consumer-id-v2",
    )
    return _AdmissionBase(
        recipe=recipe,
        retained_campaign_id=retained_campaign_id,
        retained_campaign_sha256=retained_campaign_sha256,
        physical_blocks=physical_blocks,
        binding_count=len(block_fields) + len(stream_fields),
        binding_set_sha256=binding_set_sha256,
        stream_count=len(stream_fields),
        stream_set_sha256=stream_set_sha256,
        retained_consumer_id=retained_consumer_id,
    )


def projection_admission_identity(
    recipe: ProjectionRecipeIdentity,
    blocks: Sequence[ProjectionAdmissionInputBlock],
    *,
    claim_generation: int,
    terminal_zeros: Sequence[ProjectionAdmissionTerminalZero] = (),
) -> ProjectionAdmissionIdentity:
    """Build one immutable campaign proof for a source-neutral recipe."""

    if type(claim_generation) is not int or claim_generation < 1:
        raise ProviderDirectoryProjectionError(
            "provider_directory_projection_admission_claim_generation_invalid"
        )
    base = _admission_base(recipe, blocks, terminal_zeros)
    admission_id = stable_hash(
        {
            "contract_id": PROJECTION_ADMISSION_CONTRACT_ID,
            "retained_consumer_recipe_id": base.retained_consumer_id,
            "claim_generation": claim_generation,
        },
        domain="provider-directory-projection-admission-id-v2",
    )
    return ProjectionAdmissionIdentity(
        admission_id=admission_id,
        recipe_id=base.recipe.recipe_id,
        acquisition_adapter_id=base.recipe.acquisition_adapter_id,
        source_scope_hash=base.recipe.source_scope_hash,
        source_ids=base.recipe.source_ids,
        completeness_manifest_hash=base.recipe.completeness_manifest_hash,
        completeness_manifest=dict(base.recipe.completeness_manifest),
        retained_campaign_id=base.retained_campaign_id,
        retained_campaign_sha256=base.retained_campaign_sha256,
        retained_consumer_recipe_id=base.retained_consumer_id,
        claim_generation=claim_generation,
        input_block_set_sha256=base.recipe.input_set_sha256,
        input_block_count=len(base.physical_blocks),
        binding_count=base.binding_count,
        binding_set_sha256=base.binding_set_sha256,
        stream_set_sha256=base.stream_set_sha256,
        stream_count=base.stream_count,
    )


def projection_admission_consumer_id(
    recipe: ProjectionRecipeIdentity,
    blocks: Sequence[ProjectionAdmissionInputBlock],
    *,
    terminal_zeros: Sequence[ProjectionAdmissionTerminalZero] = (),
) -> str:
    """Return the stable retained-consumer ID to claim before registration."""

    return _admission_base(recipe, blocks, terminal_zeros).retained_consumer_id


def mapped_admission_binding_fields(
    identity: ProjectionAdmissionIdentity,
    blocks: Sequence[ProjectionAdmissionInputBlock],
) -> list[dict[str, Any]]:
    """Return exact database mappings for physical admission bindings."""

    return sorted(
        (
            {
                "admission_id": identity.admission_id,
                "recipe_id": identity.recipe_id,
                **admission_binding_fields(block),
                "retained_consumer_recipe_id": (
                    identity.retained_consumer_recipe_id
                ),
                "claim_generation": identity.claim_generation,
            }
            for block in blocks
        ),
        key=lambda binding: binding["binding_id"],
    )


def mapped_admission_stream_fields(
    identity: ProjectionAdmissionIdentity,
    terminal_zeros: Sequence[ProjectionAdmissionTerminalZero],
) -> list[dict[str, Any]]:
    """Return exact database mappings for terminal ordered streams."""

    return sorted(
        (
            {
                "admission_id": identity.admission_id,
                "recipe_id": identity.recipe_id,
                **admission_stream_fields(terminal_zero),
                "claim_generation": identity.claim_generation,
            }
            for terminal_zero in terminal_zeros
        ),
        key=lambda binding: binding["binding_id"],
    )


def admission_registration_inputs(
    recipe: ProjectionRecipeIdentity,
    blocks: Sequence[ProjectionAdmissionInputBlock],
    shards: Sequence[ProjectionShardSpec],
    claim_generation: int,
    lease_seconds: int,
    *,
    terminal_zeros: Sequence[ProjectionAdmissionTerminalZero] = (),
) -> tuple[
    ProjectionAdmissionIdentity,
    list[dict[str, Any]],
    list[dict[str, Any]],
    list[dict[str, Any]],
    list[dict[str, Any]],
]:
    """Build exact admission, core-workset, mapping, and stream inputs."""

    if not 30 <= lease_seconds <= 86_400:
        raise ProviderDirectoryProjectionError(
            "provider_directory_projection_admission_lease_seconds_invalid"
        )
    identity = projection_admission_identity(
        recipe,
        blocks,
        claim_generation=claim_generation,
        terminal_zeros=terminal_zeros,
    )
    physical_by_id = {binding.block.block_id: binding.block for binding in blocks}
    physical_blocks = [physical_by_id[key] for key in sorted(physical_by_id)]
    block_fields, shard_fields, block_set_hash, _shard_set_hash = (
        _normalized_workset(recipe, physical_blocks, shards)
    )
    if block_set_hash != identity.input_block_set_sha256:
        raise ProviderDirectoryProjectionError(
            "provider_directory_projection_input_manifest_hash_mismatch"
        )
    return (
        identity,
        block_fields,
        shard_fields,
        mapped_admission_binding_fields(identity, blocks),
        mapped_admission_stream_fields(identity, terminal_zeros),
    )


__all__ = [
    "PROJECTION_ADMISSION_CONTRACT_ID",
    "admission_binding_fields",
    "admission_registration_inputs",
    "admission_stream_fields",
    "mapped_admission_binding_fields",
    "mapped_admission_stream_fields",
    "projection_admission_consumer_id",
    "projection_admission_identity",
]
