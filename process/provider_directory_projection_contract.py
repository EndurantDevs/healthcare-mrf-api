# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Exact identities and source-neutral proof contracts for projections."""

from __future__ import annotations

import hashlib
import datetime as dt
from typing import Any, Iterable, Mapping, Sequence

from process.provider_directory_projection_types import (
    PROJECTION_CONTENT_HASH_CONTRACT_ID,
    PROJECTION_PROOF_SHARD_CONTRACT_ID,
    PROJECTION_RECIPE_CONTRACT_ID,
    PhysicalProjectionProof,
    ProjectionCompletenessManifest,
    ProjectionInputBlock,
    ProjectionProofShard,
    ProjectionRecipeIdentity,
    ProjectionSemanticOutcomeProof,
    ProjectionShardSpec,
    PROJECTION_MIXED_RESOURCE_TYPE,
    PROJECTION_INPUT_BLOCK_MAX_BYTES,
    PROJECTION_INPUT_BLOCK_MAX_DOCUMENTS,
    PROJECTION_INPUT_BLOCK_MAX_RESOURCES,
    ProviderDirectoryProjectionError,
    required_hash,
    required_text,
    sorted_unique_texts,
    stable_hash,
    stable_json,
)
from process.provider_directory_projection_summary import (
    SEMANTIC_SOURCE_SUMMARY_CONTRACT_ID,
    semantic_source_summary,
    validated_semantic_source_summary,
)
from process.provider_directory_projection_contribution import (
    SEMANTIC_CONTRIBUTION_COLUMNS,
    SEMANTIC_CONTRIBUTION_CONTRACT_ID,
    SEMANTIC_OUTCOME_PROOF_CONTRACT_ID,
    normalized_semantic_contribution,
    validated_semantic_outcome_proof,
)
from process.provider_directory_projection_inline_profile import (
    INLINE_PROFILE_EVIDENCE_CONTRACT_ID,
)


PROJECTION_INPUT_BLOCK_CONTRACT_ID = (
    "healthporta.provider-directory.projection-input-block.v1"
)
PROJECTION_SHARD_SPEC_CONTRACT_ID = (
    "healthporta.provider-directory.projection-shard-spec.v1"
)
PROJECTION_COMPLETENESS_CONTRACT_ID = (
    "healthporta.provider-directory.acquisition-completeness.v1"
)
PROJECTION_REDUCER_PROOF_CONTRACT_ID = (
    "healthporta.provider-directory.projection-reducer.v1"
)
_COMPLETENESS_FIELDS = frozenset(
    {
        "block_count",
        "byte_count",
        "complete",
        "contract_id",
        "endpoint_campaign_hash",
        "partition_count",
        "partition_strategy_contract_id",
        "required_resources",
        "row_count",
        "selected_resources",
        "terminal_partitions",
    }
)
_TERMINAL_PARTITION_FIELDS = frozenset(
    {
        "block_count",
        "byte_count",
        "partition_key_hash",
        "resource_type",
        "row_count",
        "source_partition_ordinal",
        "terminal",
        "terminal_cursor_hmac",
        "zero_row_proof_sha256",
    }
)


def _canonical_hash(candidate: Any, field_name: str) -> str:
    """Require one already-canonical lowercase SHA-256 string."""

    if type(candidate) is not str:
        raise ProviderDirectoryProjectionError(
            f"provider_directory_projection_{field_name}_invalid"
        )
    normalized_hash = required_hash(candidate, field_name)
    if normalized_hash != candidate:
        raise ProviderDirectoryProjectionError(
            f"provider_directory_projection_{field_name}_invalid"
        )
    return normalized_hash


def _canonical_text(candidate: Any, field_name: str, *, limit: int = 128) -> str:
    """Require one already-normalized contract string."""

    if type(candidate) is not str:
        raise ProviderDirectoryProjectionError(
            f"provider_directory_projection_{field_name}_invalid"
        )
    normalized_text = required_text(candidate, field_name, limit=limit)
    if normalized_text != candidate:
        raise ProviderDirectoryProjectionError(
            f"provider_directory_projection_{field_name}_invalid"
        )
    return normalized_text


def _canonical_nonnegative_integer(candidate: Any, field_name: str) -> int:
    """Require an exact nonnegative JSON integer, never bool or text."""

    if type(candidate) is not int or candidate < 0:
        raise ProviderDirectoryProjectionError(
            f"provider_directory_projection_{field_name}_invalid"
        )
    return candidate


def _canonical_resource_list(candidate: Any, field_name: str) -> list[str]:
    """Require a sorted, duplicate-free JSON list of canonical strings."""

    if type(candidate) is not list:
        raise ProviderDirectoryProjectionError(
            "provider_directory_projection_completeness_manifest_invalid"
        )
    canonical_resources = [
        _canonical_text(resource, field_name, limit=64) for resource in candidate
    ]
    if canonical_resources != sorted(set(canonical_resources)):
        raise ProviderDirectoryProjectionError(
            "provider_directory_projection_completeness_manifest_invalid"
        )
    return canonical_resources


def _canonical_partition_counts(candidate: Mapping[str, Any]) -> dict[str, int]:
    """Validate the internally consistent count triplet for one partition."""

    count_by_field = {
        field_name: _canonical_nonnegative_integer(candidate[field_name], field_name)
        for field_name in ("block_count", "row_count", "byte_count")
    }
    if count_by_field["row_count"] == 0:
        if any(count_by_field[field_name] != 0 for field_name in count_by_field):
            raise ProviderDirectoryProjectionError(
                "provider_directory_projection_terminal_partition_invalid"
            )
    elif (
        count_by_field["block_count"] < 1 or count_by_field["byte_count"] < 1
    ):
        raise ProviderDirectoryProjectionError(
            "provider_directory_projection_terminal_partition_invalid"
        )
    return count_by_field


def _canonical_terminal_evidence(
    candidate: Mapping[str, Any],
    row_count: int,
) -> tuple[str | None, str | None]:
    """Validate exact terminal-cursor and zero-row evidence types."""

    if type(candidate["terminal"]) is not bool or candidate["terminal"] is not True:
        raise ProviderDirectoryProjectionError(
            "provider_directory_projection_terminal_partition_invalid"
        )
    terminal_cursor_hmac = candidate["terminal_cursor_hmac"]
    if terminal_cursor_hmac is not None:
        terminal_cursor_hmac = _canonical_hash(
            terminal_cursor_hmac,
            "terminal_cursor_hmac",
        )
    zero_row_proof_sha256 = candidate["zero_row_proof_sha256"]
    if row_count == 0:
        zero_row_proof_sha256 = _canonical_hash(
            zero_row_proof_sha256,
            "zero_row_proof_sha256",
        )
    elif zero_row_proof_sha256 is not None:
        raise ProviderDirectoryProjectionError(
            "provider_directory_projection_terminal_partition_invalid"
        )
    return terminal_cursor_hmac, zero_row_proof_sha256


def _canonical_terminal_partition(candidate: Any) -> dict[str, Any]:
    """Validate one exact terminal-partition census entry."""

    if not isinstance(candidate, Mapping) or set(candidate) != set(
        _TERMINAL_PARTITION_FIELDS
    ):
        raise ProviderDirectoryProjectionError(
            "provider_directory_projection_terminal_partition_invalid"
        )
    count_by_field = _canonical_partition_counts(candidate)
    terminal_cursor_hmac, zero_row_proof_sha256 = _canonical_terminal_evidence(
        candidate,
        count_by_field["row_count"],
    )
    return {
        "resource_type": _canonical_text(
            candidate["resource_type"],
            "resource_type",
            limit=64,
        ),
        "partition_key_hash": _canonical_hash(
            candidate["partition_key_hash"],
            "partition_key_hash",
        ),
        "source_partition_ordinal": _canonical_nonnegative_integer(
            candidate["source_partition_ordinal"],
            "source_partition_ordinal",
        ),
        "terminal_cursor_hmac": terminal_cursor_hmac,
        **count_by_field,
        "zero_row_proof_sha256": zero_row_proof_sha256,
        "terminal": True,
    }


def _canonical_partition_census(candidate: Any) -> list[dict[str, Any]]:
    """Require a sorted terminal census with unique exact coordinates."""

    raw_terminal_partitions = candidate["terminal_partitions"]
    if type(raw_terminal_partitions) is not list:
        raise ProviderDirectoryProjectionError(
            "provider_directory_projection_completeness_manifest_invalid"
        )
    terminal_partitions = [
        _canonical_terminal_partition(partition)
        for partition in raw_terminal_partitions
    ]
    canonical_partitions = sorted(
        terminal_partitions,
        key=lambda partition: (
            partition["resource_type"],
            partition["source_partition_ordinal"],
            partition["partition_key_hash"],
        ),
    )
    coordinates = {
        (
            partition["resource_type"],
            partition["partition_key_hash"],
            partition["source_partition_ordinal"],
        )
        for partition in canonical_partitions
    }
    if terminal_partitions != canonical_partitions or len(coordinates) != len(
        canonical_partitions
    ):
        raise ProviderDirectoryProjectionError(
            "provider_directory_projection_completeness_manifest_invalid"
        )
    return canonical_partitions


def _canonical_completeness_counts(
    candidate: Mapping[str, Any],
    terminal_partitions: Sequence[Mapping[str, Any]],
) -> dict[str, int]:
    """Validate manifest totals against the exact terminal census."""

    count_by_field = {
        field_name: _canonical_nonnegative_integer(candidate[field_name], field_name)
        for field_name in ("partition_count", "block_count", "row_count", "byte_count")
    }
    if (
        count_by_field["partition_count"] != len(terminal_partitions)
        or any(
            count_by_field[field_name]
            != sum(partition[field_name] for partition in terminal_partitions)
            for field_name in ("block_count", "row_count", "byte_count")
        )
    ):
        raise ProviderDirectoryProjectionError(
            "provider_directory_projection_completeness_manifest_invalid"
        )
    return count_by_field


def _canonical_completeness_proof(candidate: Any) -> dict[str, Any]:
    """Validate and canonicalize one complete acquisition manifest proof."""

    if not isinstance(candidate, Mapping) or set(candidate) != set(
        _COMPLETENESS_FIELDS
    ):
        raise ProviderDirectoryProjectionError(
            "provider_directory_projection_completeness_manifest_invalid"
        )
    selected_resources = _canonical_resource_list(
        candidate["selected_resources"],
        "selected_resource",
    )
    required_resources = _canonical_resource_list(
        candidate["required_resources"],
        "required_resource",
    )
    terminal_partitions = _canonical_partition_census(candidate)
    terminal_resource_types = {
        partition["resource_type"] for partition in terminal_partitions
    }
    if (
        candidate["contract_id"] != PROJECTION_COMPLETENESS_CONTRACT_ID
        or type(candidate["complete"]) is not bool
        or candidate["complete"] is not True
        or not selected_resources
        or not set(required_resources).issubset(selected_resources)
        or terminal_resource_types
        not in (set(selected_resources), {PROJECTION_MIXED_RESOURCE_TYPE})
    ):
        raise ProviderDirectoryProjectionError(
            "provider_directory_projection_completeness_manifest_invalid"
        )
    count_by_field = _canonical_completeness_counts(
        candidate,
        terminal_partitions,
    )
    return {
        "contract_id": PROJECTION_COMPLETENESS_CONTRACT_ID,
        "endpoint_campaign_hash": _canonical_hash(
            candidate["endpoint_campaign_hash"],
            "endpoint_campaign_hash",
        ),
        "partition_strategy_contract_id": _canonical_text(
            candidate["partition_strategy_contract_id"],
            "partition_strategy_contract_id",
        ),
        "selected_resources": selected_resources,
        "required_resources": required_resources,
        **count_by_field,
        "terminal_partitions": terminal_partitions,
        "complete": True,
    }


def validated_projection_completeness_manifest(
    manifest: ProjectionCompletenessManifest,
) -> ProjectionCompletenessManifest:
    """Recompute one manifest hash and reject any forged or noncanonical proof."""

    if type(manifest) is not ProjectionCompletenessManifest:
        raise ProviderDirectoryProjectionError(
            "provider_directory_projection_completeness_manifest_invalid"
        )
    canonical_proof = _canonical_completeness_proof(manifest.proof)
    manifest_sha256 = stable_hash(
        canonical_proof,
        domain="provider-directory-projection-completeness-manifest-v1",
    )
    if (
        type(manifest.manifest_sha256) is not str
        or _canonical_hash(
            manifest.manifest_sha256,
            "completeness_manifest_hash",
        )
        != manifest_sha256
        or dict(manifest.proof) != canonical_proof
    ):
        raise ProviderDirectoryProjectionError(
            "provider_directory_projection_completeness_manifest_hash_mismatch"
        )
    return ProjectionCompletenessManifest(manifest_sha256, canonical_proof)


def _recipe_components(
    decoder_contract_id: str,
    acquisition_adapter_id: str,
    input_set_sha256: str,
    source_ids: Iterable[str],
    transform_contract_id: str,
    scope_contract_id: str,
    transform_context: Mapping[str, Any],
    selected_resources: Iterable[str],
    required_resources: Iterable[str],
    completeness_manifest: ProjectionCompletenessManifest,
) -> dict[str, Any]:
    """Normalize exact recipe inputs before source-neutral identity hashing."""

    validated_completeness = validated_projection_completeness_manifest(
        completeness_manifest
    )
    normalized_sources = sorted_unique_texts(source_ids, "source_id", limit=96)
    normalized_selected = sorted_unique_texts(
        selected_resources,
        "selected_resource",
        limit=64,
    )
    normalized_required_resources = tuple(
        sorted(
            {
                required_text(candidate, "required_resource", limit=64)
                for candidate in required_resources
            }
        )
    )
    if not set(normalized_required_resources).issubset(normalized_selected):
        raise ProviderDirectoryProjectionError(
            "provider_directory_projection_required_resource_not_selected"
        )
    completeness_proof_map = dict(validated_completeness.proof)
    if (
        completeness_proof_map.get("selected_resources")
        != list(normalized_selected)
        or completeness_proof_map.get("required_resources")
        != list(normalized_required_resources)
        or completeness_proof_map.get("complete") is not True
    ):
        raise ProviderDirectoryProjectionError(
            "provider_directory_projection_completeness_profile_mismatch"
        )
    normalized_context_map = {
        "as_of_date": required_text(
            transform_context.get("as_of_date"),
            "as_of_date",
            limit=10,
        ),
        "time_rule_contract_id": required_text(
            transform_context.get("time_rule_contract_id"),
            "time_rule_contract_id",
        ),
    }
    try:
        dt.date.fromisoformat(normalized_context_map["as_of_date"])
    except ValueError as error:
        raise ProviderDirectoryProjectionError(
            "provider_directory_projection_as_of_date_invalid"
        ) from error
    if set(transform_context) != set(normalized_context_map):
        raise ProviderDirectoryProjectionError(
            "provider_directory_projection_transform_context_invalid"
        )
    return {
        "decoder_contract_id": required_text(
            decoder_contract_id,
            "decoder_contract_id",
        ),
        "acquisition_adapter_id": required_text(
            acquisition_adapter_id,
            "acquisition_adapter_id",
        ),
        "input_set_sha256": required_hash(input_set_sha256, "input_set_sha256"),
        "source_ids": normalized_sources,
        "transform_contract_id": required_text(
            transform_contract_id,
            "transform_contract_id",
        ),
        "scope_contract_id": required_text(
            scope_contract_id,
            "scope_contract_id",
        ),
        "transform_context": normalized_context_map,
        "selected_resources": normalized_selected,
        "required_resources": normalized_required_resources,
        "completeness_manifest_hash": required_hash(
            validated_completeness.manifest_sha256,
            "completeness_manifest_hash",
        ),
        "completeness_manifest": completeness_proof_map,
    }


def _recipe_payload(components: Mapping[str, Any]) -> dict[str, Any]:
    source_ids = components["source_ids"]
    selected = components["selected_resources"]
    required = components["required_resources"]
    return {
        "contract_id": PROJECTION_RECIPE_CONTRACT_ID,
        "decoder_contract_id": components["decoder_contract_id"],
        "acquisition_adapter_id": components["acquisition_adapter_id"],
        "input_set_sha256": components["input_set_sha256"],
        "source_scope_hash": stable_hash(
            list(source_ids),
            domain="provider-directory-projection-source-scope-v1",
        ),
        "source_ids": list(source_ids),
        "transform_contract_id": components["transform_contract_id"],
        "scope_contract_id": components["scope_contract_id"],
        "transform_context_hash": stable_hash(
            components["transform_context"],
            domain="provider-directory-projection-transform-context-v1",
        ),
        "transform_context": components["transform_context"],
        "resource_profile_hash": stable_hash(
            {
                "selected": list(selected),
                "required": list(required),
                "inline_profile_evidence_contract_id": (
                    INLINE_PROFILE_EVIDENCE_CONTRACT_ID
                ),
                "semantic_contribution_contract_id": (
                    SEMANTIC_CONTRIBUTION_CONTRACT_ID
                ),
                "semantic_outcome_proof_contract_id": (
                    SEMANTIC_OUTCOME_PROOF_CONTRACT_ID
                ),
                "semantic_source_summary_contract_id": (
                    SEMANTIC_SOURCE_SUMMARY_CONTRACT_ID
                ),
                "reducer_proof_contract_id": PROJECTION_REDUCER_PROOF_CONTRACT_ID,
            },
            domain="provider-directory-projection-resource-profile-v1",
        ),
        "selected_resources": list(selected),
        "required_resources": list(required),
        "completeness_manifest_hash": components["completeness_manifest_hash"],
        "completeness_manifest": components["completeness_manifest"],
    }


def _source_neutral_recipe_payload(identity: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "contract_id": identity["contract_id"],
        "decoder_contract_id": identity["decoder_contract_id"],
        "input_set_sha256": identity["input_set_sha256"],
        "transform_contract_id": identity["transform_contract_id"],
        "scope_contract_id": identity["scope_contract_id"],
        "transform_context_hash": identity["transform_context_hash"],
        "transform_context": identity["transform_context"],
        "resource_profile_hash": identity["resource_profile_hash"],
        "selected_resources": identity["selected_resources"],
        "required_resources": identity["required_resources"],
        "completeness_manifest_hash": identity["completeness_manifest_hash"],
    }


def projection_recipe_identity(
    *,
    decoder_contract_id: str,
    acquisition_adapter_id: str,
    input_set_sha256: str,
    source_ids: Iterable[str],
    transform_contract_id: str,
    scope_contract_id: str,
    transform_context: Mapping[str, Any],
    selected_resources: Iterable[str],
    completeness_manifest: ProjectionCompletenessManifest,
    required_resources: Iterable[str] = (),
) -> ProjectionRecipeIdentity:
    """Build the exact pre-decode identity used for physical reuse lookup."""

    components = _recipe_components(
        decoder_contract_id,
        acquisition_adapter_id,
        input_set_sha256,
        source_ids,
        transform_contract_id,
        scope_contract_id,
        transform_context,
        selected_resources,
        required_resources,
        completeness_manifest,
    )
    identity = _recipe_payload(components)
    return ProjectionRecipeIdentity(
        recipe_id=stable_hash(
            _source_neutral_recipe_payload(identity),
            domain="provider-directory-projection-recipe-id-v1",
        ),
        decoder_contract_id=identity["decoder_contract_id"],
        acquisition_adapter_id=identity["acquisition_adapter_id"],
        input_set_sha256=identity["input_set_sha256"],
        source_scope_hash=identity["source_scope_hash"],
        source_ids=tuple(identity["source_ids"]),
        transform_contract_id=identity["transform_contract_id"],
        scope_contract_id=identity["scope_contract_id"],
        transform_context_hash=identity["transform_context_hash"],
        transform_context=dict(identity["transform_context"]),
        resource_profile_hash=identity["resource_profile_hash"],
        selected_resources=tuple(identity["selected_resources"]),
        required_resources=tuple(identity["required_resources"]),
        completeness_manifest_hash=identity["completeness_manifest_hash"],
        completeness_manifest=dict(identity["completeness_manifest"]),
    )


def _validate_recipe_completeness(recipe: ProjectionRecipeIdentity) -> None:
    """Revalidate the shallow recipe manifest at each consuming boundary."""

    validated_projection_completeness_manifest(
        ProjectionCompletenessManifest(
            recipe.completeness_manifest_hash,
            recipe.completeness_manifest,
        )
    )


def projection_input_block(
    *,
    block_ordinal: int,
    upstream_artifact_id: str,
    source_object_id: str,
    block_kind: str,
    input_contract_id: str,
    source_partition_ordinal: int,
    record_start: int,
    record_count: int,
    content_sha256: str,
    payload_sha256: str,
    payload_bytes: int,
    summary: Mapping[str, int],
    retained_campaign_id: str,
    retained_campaign_sha256: str,
    retained_source_item_id: str,
    retained_range_ordinal: int | None,
) -> ProjectionInputBlock:
    """Build one source-neutral immutable retained-block descriptor."""

    integer_fields = (
        block_ordinal,
        source_partition_ordinal,
        record_start,
        record_count,
        payload_bytes,
    )
    if any(
        isinstance(coordinate_value, bool)
        or not isinstance(coordinate_value, int)
        for coordinate_value in integer_fields
    ):
        raise ProviderDirectoryProjectionError(
            "provider_directory_projection_input_block_coordinate_invalid"
        )
    if (
        block_ordinal < 0
        or source_partition_ordinal < 0
        or record_start < 0
        or record_count < 1
        or payload_bytes < 1
    ):
        raise ProviderDirectoryProjectionError(
            "provider_directory_projection_input_block_coordinate_invalid"
        )
    normalized_summary_count_map = {
        required_text(key, "summary_key", limit=64): int(summary_count)
        for key, summary_count in summary.items()
    }
    if any(count < 0 for count in normalized_summary_count_map.values()):
        raise ProviderDirectoryProjectionError(
            "provider_directory_projection_input_block_summary_invalid"
        )
    expanded_resource_count = int(
        normalized_summary_count_map.get("resource_count", record_count)
    )
    if (
        payload_bytes > PROJECTION_INPUT_BLOCK_MAX_BYTES
        or record_count > PROJECTION_INPUT_BLOCK_MAX_DOCUMENTS
        or expanded_resource_count < 1
        or expanded_resource_count > PROJECTION_INPUT_BLOCK_MAX_RESOURCES
    ):
        raise ProviderDirectoryProjectionError(
            "provider_directory_projection_input_block_bound_exceeded"
        )
    if retained_range_ordinal is not None and (
        isinstance(retained_range_ordinal, bool)
        or not isinstance(retained_range_ordinal, int)
        or retained_range_ordinal < 0
    ):
        raise ProviderDirectoryProjectionError(
            "provider_directory_projection_retained_range_ordinal_invalid"
        )
    normalized_retained_campaign_id = required_hash(
        retained_campaign_id,
        "retained_campaign_id",
    )
    normalized_retained_campaign_sha256 = required_hash(
        retained_campaign_sha256,
        "retained_campaign_sha256",
    )
    normalized_retained_source_item_id = required_hash(
        retained_source_item_id,
        "retained_source_item_id",
    )
    block_descriptor_map = {
        "contract_id": PROJECTION_INPUT_BLOCK_CONTRACT_ID,
        "block_ordinal": block_ordinal,
        "upstream_artifact_id": required_hash(
            upstream_artifact_id,
            "upstream_artifact_id",
        ),
        "source_object_id": required_hash(source_object_id, "source_object_id"),
        "block_kind": required_text(block_kind, "block_kind", limit=64),
        "input_contract_id": required_text(
            input_contract_id,
            "input_contract_id",
        ),
        "source_partition_ordinal": source_partition_ordinal,
        "record_start": record_start,
        "record_count": record_count,
        "content_sha256": required_hash(content_sha256, "content_sha256"),
        "payload_sha256": required_hash(payload_sha256, "payload_sha256"),
        "payload_bytes": payload_bytes,
        "summary": normalized_summary_count_map,
    }
    block_proof_sha256 = stable_hash(
        block_descriptor_map,
        domain="provider-directory-projection-input-block-v1",
    )
    return ProjectionInputBlock(
        block_id=block_proof_sha256,
        block_ordinal=block_ordinal,
        upstream_artifact_id=block_descriptor_map["upstream_artifact_id"],
        source_object_id=block_descriptor_map["source_object_id"],
        block_kind=block_descriptor_map["block_kind"],
        input_contract_id=block_descriptor_map["input_contract_id"],
        source_partition_ordinal=source_partition_ordinal,
        record_start=record_start,
        record_count=record_count,
        content_sha256=block_descriptor_map["content_sha256"],
        payload_sha256=block_descriptor_map["payload_sha256"],
        payload_bytes=payload_bytes,
        summary=normalized_summary_count_map,
        retained_campaign_id=normalized_retained_campaign_id,
        retained_campaign_sha256=normalized_retained_campaign_sha256,
        retained_source_item_id=normalized_retained_source_item_id,
        retained_range_ordinal=retained_range_ordinal,
        block_proof_sha256=block_proof_sha256,
    )


def projection_input_set_sha256(
    blocks: Iterable[ProjectionInputBlock],
    *,
    decoder_contract_id: str,
    completeness_manifest: ProjectionCompletenessManifest,
) -> str:
    """Recompute physical input identity without retained campaign bindings."""

    validated_completeness = validated_projection_completeness_manifest(
        completeness_manifest
    )
    normalized_blocks = sorted(
        (
            {
                "block_id": block.block_id,
                "block_ordinal": block.block_ordinal,
                "upstream_artifact_id": block.upstream_artifact_id,
                "source_object_id": block.source_object_id,
                "block_kind": block.block_kind,
                "input_contract_id": block.input_contract_id,
                "source_partition_ordinal": block.source_partition_ordinal,
                "record_start": block.record_start,
                "record_count": block.record_count,
                "content_sha256": block.content_sha256,
                "payload_sha256": block.payload_sha256,
                "payload_bytes": block.payload_bytes,
                "summary": dict(block.summary),
                "block_proof_sha256": block.block_proof_sha256,
            }
            for block in blocks
        ),
        key=lambda block: (block["block_ordinal"], block["block_id"]),
    )
    if len({block["block_id"] for block in normalized_blocks}) != len(
        normalized_blocks
    ):
        raise ProviderDirectoryProjectionError(
            "provider_directory_projection_input_manifest_invalid"
        )
    return stable_hash(
        {
            "contract_id": "healthporta.provider-directory.input-manifest.v1",
            "decoder_contract_id": required_text(
                decoder_contract_id,
                "decoder_contract_id",
            ),
            "completeness_manifest_hash": required_hash(
                validated_completeness.manifest_sha256,
                "completeness_manifest_hash",
            ),
            "blocks": normalized_blocks,
        },
        domain="provider-directory-projection-input-manifest-v1",
    )


def projection_completeness_manifest(
    *,
    endpoint_campaign_hash: str,
    partition_strategy_contract_id: str,
    selected_resources: Iterable[str],
    required_resources: Iterable[str],
    terminal_partitions: Iterable[Mapping[str, Any]],
    complete: bool,
) -> ProjectionCompletenessManifest:
    """Seal terminal cursor and count evidence for every acquisition partition."""

    selected = sorted_unique_texts(selected_resources, "selected_resource", limit=64)
    required_resource_types = tuple(
        sorted(
            {
                required_text(resource, "required_resource", limit=64)
                for resource in required_resources
            }
        )
    )
    if complete is not True or not set(required_resource_types).issubset(selected):
        raise ProviderDirectoryProjectionError(
            "provider_directory_projection_completeness_incomplete"
        )
    normalized_partitions = []
    for partition in terminal_partitions:
        if not isinstance(partition, Mapping) or not set(partition).issubset(
            _TERMINAL_PARTITION_FIELDS
        ):
            raise ProviderDirectoryProjectionError(
                "provider_directory_projection_terminal_partition_invalid"
            )
        resource_type = _canonical_text(
            partition.get("resource_type"),
            "resource_type",
            limit=64,
        )
        partition_key_hash = _canonical_hash(
            partition.get("partition_key_hash"),
            "partition_key_hash",
        )
        source_partition_ordinal = partition.get(
            "source_partition_ordinal",
            0,
        )
        source_partition_ordinal = _canonical_nonnegative_integer(
            source_partition_ordinal,
            "source_partition_ordinal",
        )
        cursor_hmac = partition.get("terminal_cursor_hmac")
        if cursor_hmac is not None:
            cursor_hmac = _canonical_hash(
                cursor_hmac,
                "terminal_cursor_hmac",
            )
        partition_count_by_field = {
            field_name: _canonical_nonnegative_integer(
                partition.get(field_name),
                field_name,
            )
            for field_name in ("block_count", "row_count", "byte_count")
        }
        if (
            (
                resource_type != PROJECTION_MIXED_RESOURCE_TYPE
                and resource_type not in selected
            )
            or type(partition.get("terminal")) is not bool
            or partition.get("terminal") is not True
        ):
            raise ProviderDirectoryProjectionError(
                "provider_directory_projection_terminal_partition_invalid"
            )
        zero_row_proof = partition.get("zero_row_proof_sha256")
        if partition_count_by_field["row_count"] == 0:
            zero_row_proof = _canonical_hash(
                zero_row_proof,
                "zero_row_proof_sha256",
            )
            if any(partition_count_by_field.values()):
                raise ProviderDirectoryProjectionError(
                    "provider_directory_projection_terminal_partition_invalid"
                )
        elif zero_row_proof is not None:
            raise ProviderDirectoryProjectionError(
                "provider_directory_projection_terminal_partition_invalid"
            )
        elif (
            partition_count_by_field["block_count"] < 1
            or partition_count_by_field["byte_count"] < 1
        ):
            raise ProviderDirectoryProjectionError(
                "provider_directory_projection_terminal_partition_invalid"
            )
        normalized_partitions.append(
            {
                "resource_type": resource_type,
                "partition_key_hash": partition_key_hash,
                "source_partition_ordinal": source_partition_ordinal,
                "terminal_cursor_hmac": cursor_hmac,
                **partition_count_by_field,
                "zero_row_proof_sha256": zero_row_proof,
                "terminal": True,
            }
        )
    normalized_partitions.sort(
        key=lambda partition: (
            partition["resource_type"],
            partition["source_partition_ordinal"],
            partition["partition_key_hash"],
        )
    )
    coordinates = {
        (
            partition["resource_type"],
            partition["partition_key_hash"],
            partition["source_partition_ordinal"],
        )
        for partition in normalized_partitions
    }
    terminal_resource_types = {
        partition["resource_type"] for partition in normalized_partitions
    }
    if (
        not normalized_partitions
        or len(coordinates) != len(normalized_partitions)
        or terminal_resource_types
        not in (set(selected), {PROJECTION_MIXED_RESOURCE_TYPE})
    ):
        raise ProviderDirectoryProjectionError(
            "provider_directory_projection_partition_census_invalid"
        )
    completeness_proof_map = {
        "contract_id": PROJECTION_COMPLETENESS_CONTRACT_ID,
        "endpoint_campaign_hash": required_hash(
            endpoint_campaign_hash,
            "endpoint_campaign_hash",
        ),
        "partition_strategy_contract_id": required_text(
            partition_strategy_contract_id,
            "partition_strategy_contract_id",
        ),
        "selected_resources": list(selected),
        "required_resources": list(required_resource_types),
        "partition_count": len(normalized_partitions),
        "block_count": sum(
            partition["block_count"] for partition in normalized_partitions
        ),
        "row_count": sum(partition["row_count"] for partition in normalized_partitions),
        "byte_count": sum(
            partition["byte_count"] for partition in normalized_partitions
        ),
        "terminal_partitions": normalized_partitions,
        "complete": True,
    }
    manifest_hash = stable_hash(
        completeness_proof_map,
        domain="provider-directory-projection-completeness-manifest-v1",
    )
    return validated_projection_completeness_manifest(
        ProjectionCompletenessManifest(manifest_hash, completeness_proof_map)
    )


def projection_shard_spec(
    *,
    recipe: ProjectionRecipeIdentity,
    partition_ordinal: int,
    partition_key: str,
    input_block: ProjectionInputBlock,
    resource_type: str,
    input_sha256: str | None = None,
) -> ProjectionShardSpec:
    """Build the stable pre-transform identity used for shard leasing."""

    _validate_recipe_completeness(recipe)
    if partition_ordinal < 0:
        raise ProviderDirectoryProjectionError(
            "provider_directory_projection_partition_coordinate_invalid"
        )
    normalized_type = required_text(resource_type, "resource_type", limit=64)
    if (
        normalized_type != PROJECTION_MIXED_RESOURCE_TYPE
        and normalized_type not in recipe.selected_resources
    ):
        raise ProviderDirectoryProjectionError(
            "provider_directory_projection_partition_resource_mismatch"
        )
    normalized_input_hash = required_hash(
        input_sha256 or input_block.content_sha256,
        "input_sha256",
    )
    shard_descriptor_map = {
        "contract_id": PROJECTION_SHARD_SPEC_CONTRACT_ID,
        "recipe_id": recipe.recipe_id,
        "partition_ordinal": partition_ordinal,
        "partition_key": required_hash(partition_key, "partition_key"),
        "input_block_id": required_hash(input_block.block_id, "input_block_id"),
        "resource_type": normalized_type,
        "input_sha256": normalized_input_hash,
    }
    return ProjectionShardSpec(
        partition_id=stable_hash(
            shard_descriptor_map,
            domain="provider-directory-projection-partition-id-v2",
        ),
        partition_ordinal=partition_ordinal,
        partition_key=shard_descriptor_map["partition_key"],
        input_block_id=shard_descriptor_map["input_block_id"],
        resource_type=normalized_type,
        input_sha256=normalized_input_hash,
    )


def canonical_row_line(resource: Mapping[str, Any]) -> bytes:
    """Frame one normalized physical row for partition hashing."""

    resource_type = required_text(
        resource.get("resource_type"),
        "resource_type",
        limit=64,
    )
    resource_id = required_text(
        resource.get("resource_id"),
        "resource_id",
        limit=256,
    )
    payload_hash = required_hash(resource.get("payload_hash"), "payload_hash")
    source_rank = required_text(resource.get("source_rank"), "source_rank", limit=512)
    contribution = normalized_semantic_contribution(resource)
    return stable_json(
        [
            resource_type,
            resource_id,
            payload_hash,
            source_rank,
            *(contribution[field_name] for field_name in SEMANTIC_CONTRIBUTION_COLUMNS),
        ]
    ).encode("utf-8")


def canonical_row_digest(
    resources: Iterable[Mapping[str, Any]],
) -> tuple[str, int]:
    """Hash strictly ordered physical identities with newline framing."""

    digest = hashlib.sha256()
    resource_count = 0
    previous_identity: tuple[str, str, str, str] | None = None
    for resource in resources:
        identity = (
            required_text(resource.get("resource_type"), "resource_type", limit=64),
            required_text(resource.get("resource_id"), "resource_id", limit=256),
            required_text(resource.get("source_rank"), "source_rank", limit=512),
            required_hash(resource.get("payload_hash"), "payload_hash"),
        )
        if previous_identity is not None and identity <= previous_identity:
            raise ProviderDirectoryProjectionError(
                "provider_directory_projection_rows_not_strictly_sorted"
            )
        if resource_count:
            digest.update(b"\n")
        digest.update(canonical_row_line(resource))
        previous_identity = identity
        resource_count += 1
    if not resource_count:
        raise ProviderDirectoryProjectionError(
            "provider_directory_projection_partition_empty"
        )
    return digest.hexdigest(), resource_count


def _ordered_partition_resources(
    resources: Iterable[Mapping[str, Any]],
    resource_type: str,
    selected_resources: Sequence[str],
) -> list[dict[str, Any]]:
    resources_by_identity: dict[tuple[str, str, str, str], dict[str, Any]] = {}
    for resource in resources:
        resource_dict = dict(resource)
        identity = (
            str(resource_dict.get("resource_type")),
            str(resource_dict.get("resource_id")),
            str(resource_dict.get("source_rank")),
            str(resource_dict.get("payload_hash")),
        )
        existing_resource = resources_by_identity.get(identity)
        if existing_resource is not None:
            if existing_resource != resource_dict:
                raise ProviderDirectoryProjectionError(
                    "provider_directory_projection_partition_duplicate_conflict"
                )
            continue
        resources_by_identity[identity] = resource_dict
    ordered_resources = [
        resources_by_identity[identity] for identity in sorted(resources_by_identity)
    ]
    observed_types = {
        str(resource.get("resource_type") or "").strip()
        for resource in ordered_resources
    }
    if (
        resource_type == PROJECTION_MIXED_RESOURCE_TYPE
        and (not observed_types or not observed_types.issubset(set(selected_resources)))
    ) or (
        resource_type != PROJECTION_MIXED_RESOURCE_TYPE
        and observed_types != {resource_type}
    ):
        raise ProviderDirectoryProjectionError(
            "provider_directory_projection_partition_resource_mismatch"
        )
    return ordered_resources


def prepare_projection_proof_shard(
    resources: Iterable[Mapping[str, Any]],
    *,
    recipe: ProjectionRecipeIdentity,
    attempt: int,
    partition_ordinal: int,
    resource_type: str,
    input_sha256: str,
    partition_id: str | None = None,
    partition_attempt: int = 1,
    producer_proof: Mapping[str, Any] | None = None,
) -> tuple[ProjectionProofShard, list[dict[str, Any]]]:
    """Build one shard proof and return its single normalized ordered row set."""

    _validate_recipe_completeness(recipe)
    if attempt < 1 or partition_attempt < 1 or partition_ordinal < 0:
        raise ProviderDirectoryProjectionError(
            "provider_directory_projection_partition_coordinate_invalid"
        )
    normalized_type = required_text(resource_type, "resource_type", limit=64)
    ordered_resources = _ordered_partition_resources(
        resources,
        normalized_type,
        recipe.selected_resources,
    )
    row_hash, resource_count = canonical_row_digest(ordered_resources)
    resource_count_by_type = {
        selected_resource: sum(
            1
            for resource in ordered_resources
            if resource["resource_type"] == selected_resource
        )
        for selected_resource in recipe.selected_resources
    }
    proof_dict = {
        "contract_id": PROJECTION_PROOF_SHARD_CONTRACT_ID,
        "recipe_id": recipe.recipe_id,
        "attempt": attempt,
        "partition_attempt": partition_attempt,
        "partition_ordinal": partition_ordinal,
        "resource_type": normalized_type,
        "input_sha256": required_hash(input_sha256, "input_sha256"),
        "canonical_row_sha256": row_hash,
        "resource_count": resource_count,
        "resource_counts": resource_count_by_type,
        "first_identity": [
            ordered_resources[0]["resource_type"],
            ordered_resources[0]["resource_id"],
        ],
        "last_identity": [
            ordered_resources[-1]["resource_type"],
            ordered_resources[-1]["resource_id"],
        ],
    }
    if producer_proof is not None:
        proof_dict["producer_proof"] = dict(producer_proof)
    normalized_partition_id = (
        required_hash(partition_id, "partition_id")
        if partition_id is not None
        else stable_hash(
            proof_dict,
            domain="provider-directory-projection-partition-id-v1",
        )
    )
    shard = ProjectionProofShard(
        recipe_id=recipe.recipe_id,
        attempt=attempt,
        partition_attempt=partition_attempt,
        partition_id=normalized_partition_id,
        partition_ordinal=partition_ordinal,
        resource_type=normalized_type,
        input_sha256=proof_dict["input_sha256"],
        canonical_row_sha256=row_hash,
        resource_count=resource_count,
        first_identity=tuple(proof_dict["first_identity"]),
        last_identity=tuple(proof_dict["last_identity"]),
        proof={**proof_dict, "partition_id": normalized_partition_id},
    )
    return shard, ordered_resources


def projection_proof_shard(
    resources: Iterable[Mapping[str, Any]],
    *,
    recipe: ProjectionRecipeIdentity,
    attempt: int,
    partition_ordinal: int,
    resource_type: str,
    input_sha256: str,
    partition_id: str | None = None,
    partition_attempt: int = 1,
    producer_proof: Mapping[str, Any] | None = None,
) -> ProjectionProofShard:
    """Build one source-neutral, bounded, immutable partition attestation."""

    shard, _ordered_resources = prepare_projection_proof_shard(
        resources,
        recipe=recipe,
        attempt=attempt,
        partition_ordinal=partition_ordinal,
        resource_type=resource_type,
        input_sha256=input_sha256,
        partition_id=partition_id,
        partition_attempt=partition_attempt,
        producer_proof=producer_proof,
    )
    return shard


def _resource_counts(
    recipe: ProjectionRecipeIdentity,
    shards: Sequence[ProjectionProofShard],
) -> dict[str, int]:
    partition_ids: set[str] = set()
    coordinates: set[tuple[str, int]] = set()
    counts_by_resource: dict[str, int] = {
        resource_type: 0 for resource_type in recipe.selected_resources
    }
    for shard in shards:
        coordinate = shard.resource_type, shard.partition_ordinal
        if shard.recipe_id != recipe.recipe_id:
            raise ProviderDirectoryProjectionError(
                "provider_directory_projection_shard_recipe_mismatch"
            )
        if (
            shard.resource_type != PROJECTION_MIXED_RESOURCE_TYPE
            and shard.resource_type not in counts_by_resource
        ):
            raise ProviderDirectoryProjectionError(
                "provider_directory_projection_selected_resource_mismatch"
            )
        if shard.partition_id in partition_ids or coordinate in coordinates:
            raise ProviderDirectoryProjectionError(
                "provider_directory_projection_shard_duplicate"
            )
        partition_ids.add(shard.partition_id)
        coordinates.add(coordinate)
        raw_shard_counts = shard.proof.get("resource_counts")
        if not isinstance(raw_shard_counts, Mapping):
            raise ProviderDirectoryProjectionError(
                "provider_directory_projection_shard_resource_counts_missing"
            )
        shard_count_by_resource = {
            str(resource_type): int(count)
            for resource_type, count in raw_shard_counts.items()
        }
        if (
            set(shard_count_by_resource) != set(recipe.selected_resources)
            or any(count < 0 for count in shard_count_by_resource.values())
            or sum(shard_count_by_resource.values()) != shard.resource_count
            or (
                shard.resource_type != PROJECTION_MIXED_RESOURCE_TYPE
                and shard_count_by_resource.get(shard.resource_type)
                != shard.resource_count
            )
        ):
            raise ProviderDirectoryProjectionError(
                "provider_directory_projection_shard_resource_counts_invalid"
            )
        for resource_type, count in shard_count_by_resource.items():
            counts_by_resource[resource_type] += count
    if any(counts_by_resource.get(name, 0) < 1 for name in recipe.required_resources):
        raise ProviderDirectoryProjectionError(
            "provider_directory_projection_required_resource_empty"
        )
    return counts_by_resource


def _physical_content_identity(
    recipe: ProjectionRecipeIdentity,
    canonical_row_sha256: str,
    dataset_hash: str,
    counts_by_resource: Mapping[str, int],
) -> dict[str, Any]:
    return {
        "contract_id": PROJECTION_CONTENT_HASH_CONTRACT_ID,
        "decoder_contract_id": recipe.decoder_contract_id,
        "input_set_sha256": recipe.input_set_sha256,
        "transform_contract_id": recipe.transform_contract_id,
        "scope_contract_id": recipe.scope_contract_id,
        "transform_context_hash": recipe.transform_context_hash,
        "transform_context": dict(recipe.transform_context),
        "completeness_manifest_hash": recipe.completeness_manifest_hash,
        "canonical_row_sha256": canonical_row_sha256,
        "dataset_hash": dataset_hash,
        "resource_profile_hash": recipe.resource_profile_hash,
        "selected_resources": list(recipe.selected_resources),
        "required_resources": list(recipe.required_resources),
        "resource_count": sum(counts_by_resource.values()),
        "resource_counts": dict(counts_by_resource),
    }


def _basic_physical_projection_proof(
    recipe: ProjectionRecipeIdentity,
    shards: Sequence[ProjectionProofShard],
    *,
    dataset_hash: str,
) -> PhysicalProjectionProof:
    """Build a non-publishable raw-shard proof for internal contract tests."""

    _validate_recipe_completeness(recipe)
    if not shards:
        raise ProviderDirectoryProjectionError(
            "provider_directory_projection_proof_shards_empty"
        )
    ordered_shards = sorted(
        shards,
        key=lambda shard: (
            shard.resource_type,
            shard.partition_ordinal,
            shard.partition_id,
        ),
    )
    counts_by_resource = _resource_counts(recipe, ordered_shards)
    descriptors = [shard.descriptor for shard in ordered_shards]
    row_hash = stable_hash(
        descriptors,
        domain="provider-directory-projection-canonical-shard-set-v1",
    )
    identity = _physical_content_identity(
        recipe,
        row_hash,
        required_hash(dataset_hash, "dataset_hash"),
        counts_by_resource,
    )
    # The exact retained-layout recipe is the pre-COPY storage identity.
    # Output hashes remain mandatory attestations and detect nondeterminism.
    projection_id = recipe.recipe_id
    return PhysicalProjectionProof(
        physical_projection_id=projection_id,
        canonical_row_sha256=row_hash,
        dataset_hash=identity["dataset_hash"],
        resource_count=identity["resource_count"],
        resource_counts=dict(counts_by_resource),
        proof={
            **identity,
            "physical_projection_id": projection_id,
            "shards": descriptors,
        },
    )


def _validated_reducer_proof_map(
    reducer_proof: Mapping[str, Any],
    canonical_row_sha256: str,
    resource_count_by_type: Mapping[str, int],
    outcome_proof: ProjectionSemanticOutcomeProof,
) -> dict[str, Any]:
    if not isinstance(reducer_proof, Mapping):
        raise ProviderDirectoryProjectionError(
            "provider_directory_projection_reducer_proof_invalid"
        )
    raw_resource_count_by_type = reducer_proof.get("resource_counts")
    if not isinstance(raw_resource_count_by_type, Mapping):
        raise ProviderDirectoryProjectionError(
            "provider_directory_projection_reducer_proof_invalid"
        )
    reducer_resource_count_by_type = {
        required_text(resource_type, "resource_type", limit=64): count
        for resource_type, count in raw_resource_count_by_type.items()
        if type(count) is int
    }
    total_resource_count = sum(resource_count_by_type.values())
    if (
        len(reducer_resource_count_by_type) != len(raw_resource_count_by_type)
        or reducer_proof.get("contract_id")
        != PROJECTION_REDUCER_PROOF_CONTRACT_ID
        or required_hash(
            reducer_proof.get("canonical_row_sha256"),
            "reducer_canonical_row_sha256",
        )
        != canonical_row_sha256
        or type(reducer_proof.get("resource_count")) is not int
        or reducer_proof.get("resource_count") != total_resource_count
        or reducer_resource_count_by_type != resource_count_by_type
        or required_hash(
            reducer_proof.get("semantic_outcome_proof_sha256"),
            "reducer_semantic_outcome_proof_sha256",
        )
        != outcome_proof.proof_sha256
    ):
        raise ProviderDirectoryProjectionError(
            "provider_directory_projection_reducer_proof_invalid"
        )
    return dict(reducer_proof)


def _physical_source_summary_map(
    recipe: ProjectionRecipeIdentity,
    dataset_hash: str,
    canonical_row_sha256: str,
    resource_count_by_type: Mapping[str, int],
    outcome_proof: ProjectionSemanticOutcomeProof,
) -> dict[str, Any]:
    semantic_summary = semantic_source_summary(
        semantic_adapter_contract_id=recipe.decoder_contract_id,
        transform_contract_id=recipe.transform_contract_id,
        dataset_hash=dataset_hash,
        outcome_proof=outcome_proof,
    )
    return {
        "contract_id": "healthporta.provider-directory.physical-source-summary.v1",
        "canonical_row_sha256": canonical_row_sha256,
        "dataset_hash": dataset_hash,
        "resource_count": sum(resource_count_by_type.values()),
        "resource_counts": dict(sorted(resource_count_by_type.items())),
        "semantic_summary": validated_semantic_source_summary(
            semantic_summary,
            expected_semantic_adapter_contract_id=recipe.decoder_contract_id,
            expected_transform_contract_id=recipe.transform_contract_id,
            expected_dataset_hash=dataset_hash,
            expected_outcome_proof=outcome_proof,
        ),
    }


def reduced_physical_projection_proof(
    recipe: ProjectionRecipeIdentity,
    raw_shards: Sequence[ProjectionProofShard],
    *,
    dataset_hash: str,
    canonical_row_sha256: str,
    resource_counts: Mapping[str, int],
    reducer_proof: Mapping[str, Any],
    membership_proof: Mapping[str, Any],
    outcome_proof: ProjectionSemanticOutcomeProof,
) -> PhysicalProjectionProof:
    """Seal deterministic reducer output while retaining raw shard lineage."""

    _validate_recipe_completeness(recipe)
    if not isinstance(resource_counts, Mapping):
        raise ProviderDirectoryProjectionError(
            "provider_directory_projection_reduced_resource_mismatch"
        )
    normalized_count_by_resource: dict[str, int] = {}
    for resource_type, count in resource_counts.items():
        if type(count) is not int:
            raise ProviderDirectoryProjectionError(
                "provider_directory_projection_reduced_resource_mismatch"
            )
        normalized_count_by_resource[
            required_text(resource_type, "resource_type", limit=64)
        ] = count
    if (
        set(normalized_count_by_resource) != set(recipe.selected_resources)
        or any(count < 0 for count in normalized_count_by_resource.values())
        or sum(normalized_count_by_resource.values()) < 1
        or any(
            normalized_count_by_resource.get(resource_type, 0) < 1
            for resource_type in recipe.required_resources
        )
    ):
        raise ProviderDirectoryProjectionError(
            "provider_directory_projection_reduced_resource_mismatch"
        )
    ordered_shards = sorted(
        raw_shards,
        key=lambda shard: (
            shard.resource_type,
            shard.partition_ordinal,
            shard.partition_id,
        ),
    )
    _resource_counts(recipe, ordered_shards)
    normalized_row_hash = required_hash(
        canonical_row_sha256,
        "canonical_row_sha256",
    )
    validated_outcome_proof = validated_semantic_outcome_proof(outcome_proof)
    positive_count_by_resource = {
        resource_type: count
        for resource_type, count in normalized_count_by_resource.items()
        if count > 0
    }
    if (
        validated_outcome_proof.canonical_row_sha256 != normalized_row_hash
        or validated_outcome_proof.resource_count
        != sum(normalized_count_by_resource.values())
        or validated_outcome_proof.resource_counts != positive_count_by_resource
    ):
        raise ProviderDirectoryProjectionError(
            "provider_directory_projection_semantic_outcome_reducer_mismatch"
        )
    normalized_reducer_proof_map = _validated_reducer_proof_map(
        reducer_proof,
        normalized_row_hash,
        normalized_count_by_resource,
        validated_outcome_proof,
    )
    identity = _physical_content_identity(
        recipe,
        normalized_row_hash,
        required_hash(dataset_hash, "dataset_hash"),
        normalized_count_by_resource,
    )
    normalized_dataset_hash = required_hash(dataset_hash, "dataset_hash")
    normalized_source_summary_map = _physical_source_summary_map(
        recipe,
        normalized_dataset_hash,
        normalized_row_hash,
        normalized_count_by_resource,
        validated_outcome_proof,
    )
    partition_count_by_id = {
        required_hash(partition_id, "membership_partition_id"): int(count)
        for partition_id, count in dict(
            membership_proof.get("membership_partition_resource_counts") or {}
        ).items()
    }
    membership_edge_count = int(membership_proof.get("membership_edge_count") or 0)
    if (
        membership_proof.get("contract_id")
        != "healthporta.provider-directory.semantic-membership.v1"
        or membership_proof.get("present") is not bool(membership_edge_count)
        or int(membership_proof.get("membership_partition_count") or 0)
        != len(partition_count_by_id)
        or any(count < 1 for count in partition_count_by_id.values())
        or sum(partition_count_by_id.values()) != membership_edge_count
    ):
        raise ProviderDirectoryProjectionError(
            "provider_directory_projection_membership_proof_invalid"
        )
    normalized_membership_proof_map = {
        "contract_id": membership_proof["contract_id"],
        "present": bool(membership_edge_count),
        "membership_edge_count": membership_edge_count,
        "membership_sha256": required_hash(
            membership_proof.get("membership_sha256"),
            "membership_sha256",
        ),
        "membership_partition_count": len(partition_count_by_id),
        "membership_partition_resource_counts": dict(
            sorted(partition_count_by_id.items())
        ),
    }
    projection_id = recipe.recipe_id
    return PhysicalProjectionProof(
        physical_projection_id=projection_id,
        canonical_row_sha256=normalized_row_hash,
        dataset_hash=identity["dataset_hash"],
        resource_count=sum(normalized_count_by_resource.values()),
        resource_counts=normalized_count_by_resource,
        proof={
            **identity,
            "physical_projection_id": projection_id,
            "raw_shards": [shard.descriptor for shard in ordered_shards],
            "reducer": normalized_reducer_proof_map,
            "semantic_membership": normalized_membership_proof_map,
            "source_summary": normalized_source_summary_map,
        },
    )
