# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Valid source-free projection identities for native materializer tests."""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
from typing import Any, Mapping, Sequence

from process.provider_directory_projection_contract import (
    projection_completeness_manifest,
    projection_input_block,
    projection_input_set_sha256,
    projection_recipe_identity,
    projection_shard_spec,
)
from process.provider_directory_projection_copy_summary import (
    NATIVE_DECODER_CONTRACT_ID,
    NATIVE_TRANSFORM_CONTRACT_ID,
)
from process.provider_directory_projection_types import (
    ProjectionLease,
    ProjectionInputBlock,
    ProjectionRetainedChildLease,
    ProjectionShardClaim,
    ProjectionStage,
)
from tests.provider_directory_projection_materializer_support import (
    SyntheticFHIRFixture,
    fhir_fixture,
)
from tests.provider_directory_projection_native_copy_support import (
    canonical_input_bytes,
)


@dataclass(frozen=True)
class SyntheticProjectionContext:
    """One exact native input bound to locator-free projection identities."""

    fixture: SyntheticFHIRFixture
    input_block: ProjectionInputBlock
    claim: ProjectionShardClaim
    child_lease: ProjectionRetainedChildLease
    stage: ProjectionStage


def _digest(label: str) -> str:
    return hashlib.sha256(label.encode("utf-8")).hexdigest()


def _terminal_partitions(fixture, resource_types, label):
    return tuple(
        {
            "resource_type": resource_type,
            "partition_key_hash": _digest(f"{label}:partition:{resource_type}"),
            "source_partition_ordinal": resource_ordinal,
            "terminal_cursor_hmac": _digest(f"{label}:cursor:{resource_type}"),
            "terminal": True,
            "block_count": 1,
            "row_count": sum(
                resource["resourceType"] == resource_type
                for resource in fixture.resources
            ),
            "byte_count": len(fixture.encoded),
            "zero_row_proof_sha256": None,
        }
        for resource_ordinal, resource_type in enumerate(resource_types)
    )


def _projection_recipe(block, fixture, resource_types, label):
    completeness = projection_completeness_manifest(
        endpoint_campaign_hash=_digest(f"{label}:campaign-proof"),
        partition_strategy_contract_id="synthetic-offline-partitions.v1",
        selected_resources=resource_types,
        required_resources=resource_types,
        terminal_partitions=_terminal_partitions(fixture, resource_types, label),
        complete=True,
    )
    return projection_recipe_identity(
        decoder_contract_id=NATIVE_DECODER_CONTRACT_ID,
        acquisition_adapter_id="synthetic-offline-retained.v1",
        input_set_sha256=projection_input_set_sha256(
            (block,),
            decoder_contract_id=NATIVE_DECODER_CONTRACT_ID,
        ),
        source_ids=("synthetic-offline",),
        transform_contract_id=NATIVE_TRANSFORM_CONTRACT_ID,
        scope_contract_id="healthporta.provider-directory.global-scope.v1",
        transform_context={
            "as_of_date": "2026-07-22",
            "time_rule_contract_id": "synthetic-offline-time-rules.v1",
        },
        selected_resources=resource_types,
        required_resources=resource_types,
        completeness_manifest=completeness,
    )


def _projection_input(fixture, label: str, content_sha256: str):
    physical_document_count = (
        1 if fixture.framing == "bundle" else len(fixture.resources)
    )
    return projection_input_block(
        upstream_artifact_id=_digest(f"{label}:artifact"),
        source_object_id=_digest(f"{label}:layout"),
        block_kind=fixture.framing,
        input_contract_id="synthetic-offline-fhir-input.v1",
        record_start=0,
        record_count=physical_document_count,
        content_sha256=content_sha256,
        payload_sha256=hashlib.sha256(fixture.encoded).hexdigest(),
        payload_bytes=len(fixture.encoded),
        summary={"resource_count": len(fixture.resources)},
    )


def _claimed_shard(recipe, block, label: str):
    recipe_lease = ProjectionLease(
        recipe=recipe.physical,
        attempt=1,
        lease_token=_digest(f"{label}:recipe-lease"),
    )
    shard = projection_shard_spec(
        recipe=recipe,
        partition_ordinal=0,
        input_block=block,
    )
    return ProjectionShardClaim(
        recipe_lease=recipe_lease,
        admission_id=_digest(f"{label}:admission"),
        shard=shard,
        partition_attempt=1,
        lease_token=_digest(f"{label}:shard-lease"),
    )


def _retained_child(
    fixture,
    claim,
    block,
    label: str,
    retained_range_ordinal: int | None,
):
    return ProjectionRetainedChildLease(
        shard_claim=claim,
        binding_id=_digest(f"{label}:binding"),
        block_id=block.block_id,
        retained_campaign_id=_digest(f"{label}:campaign"),
        retained_campaign_sha256=_digest(f"{label}:campaign-proof"),
        retained_consumer_recipe_id="synthetic-offline-consumer",
        retained_claim_generation=1,
        retained_source_item_id=_digest(f"{label}:source-item"),
        retained_artifact_sha256=block.upstream_artifact_id,
        retained_layout_sha256=block.source_object_id,
        retained_range_ordinal=retained_range_ordinal,
        artifact_byte_count=len(fixture.encoded),
        raw_byte_start=0,
        expected_byte_count=len(fixture.encoded),
        expected_record_count=len(fixture.resources),
        input_sha256=block.content_sha256,
        expected_payload_sha256=block.payload_sha256,
        child_generation=1,
        child_lease_token=_digest(f"{label}:child-lease"),
    )


def synthetic_projection_context(
    framing: str,
    *,
    groups: int = 1,
    label: str = "synthetic-offline",
    resources: Sequence[Mapping[str, Any]] | None = None,
    block_content_sha256: str | None = None,
    retained_range_ordinal: int | None = None,
) -> SyntheticProjectionContext:
    """Bind synthetic bytes to a valid mixed projection child lease."""

    fixture = fhir_fixture(framing, resources, groups=groups)
    return projection_context_for_fixture(
        fixture,
        label=label,
        block_content_sha256=block_content_sha256,
        retained_range_ordinal=retained_range_ordinal,
    )


def projection_context_for_fixture(
    fixture: SyntheticFHIRFixture,
    *,
    label: str = "synthetic-offline",
    block_content_sha256: str | None = None,
    retained_range_ordinal: int | None = None,
) -> SyntheticProjectionContext:
    """Bind an exactly encoded synthetic fixture to projection identities."""

    canonical_input = canonical_input_bytes(fixture)
    resource_types = tuple(
        sorted({str(resource["resourceType"]) for resource in fixture.resources})
    )
    block = _projection_input(
        fixture,
        label,
        block_content_sha256 or hashlib.sha256(canonical_input).hexdigest(),
    )
    recipe = _projection_recipe(block, fixture, resource_types, label)
    claim = _claimed_shard(recipe, block, label)
    child_lease = _retained_child(
        fixture,
        claim,
        block,
        label,
        retained_range_ordinal,
    )
    return SyntheticProjectionContext(
        fixture=fixture,
        input_block=block,
        claim=claim,
        child_lease=child_lease,
        stage=ProjectionStage("synthetic_stage", "projection_rows", 1),
    )


__all__ = (
    "SyntheticProjectionContext",
    "projection_context_for_fixture",
    "synthetic_projection_context",
)
