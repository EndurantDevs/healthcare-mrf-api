# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""PostgreSQL proof for locator-free projection identity and work leasing."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
import copy
from dataclasses import dataclass, replace
import hashlib
import importlib.util

import pytest
from sqlalchemy.exc import DBAPIError

import process.provider_directory_projection_admission as projection_admission_store
from process.provider_directory_retained_artifact_contract import (
    ArtifactLayoutRange,
    ProducedArtifact,
    RetainedCampaignItem,
    TERMINAL_ZERO,
    expected_range_set_digest,
    produced_layout_digest,
)
from process.provider_directory_retained_catalog_store import (
    initialize_retained_artifact_campaign,
)
from process.provider_directory_retained_consumer_claim_store import (
    claim_sealed_retained_campaign,
)
from process.provider_directory_retained_lease_store import acquire_campaign_lease
from process.provider_directory_retained_seal_store import (
    seal_retained_artifact_campaign,
)
from process.provider_directory_retained_stream_store import (
    append_ordered_stream_item,
)
from process.provider_directory_retained_store_support import database_table
from process.provider_directory_projection_db import set_local_projection_action
from process.provider_directory_projection_contract import (
    projection_admission_terminal_zero,
)
from process.provider_directory_projection_types import (
    ProjectionAdmissionInputBlock,
    ProjectionAdmissionTerminalZero,
    ProjectionRecipeIdentity,
    ProjectionShardSpec,
    ProviderDirectoryProjectionError,
    stable_json,
)
from process.provider_directory_physical_projection import (
    claim_projection_recipe,
    claim_projection_shard,
    projection_completeness_manifest,
    projection_admission_consumer_id,
    projection_admission_identity,
    projection_admission_input_block,
    projection_input_block,
    projection_input_set_sha256,
    projection_recipe_identity,
    projection_shard_spec,
    register_projection_workset,
    register_projection_admission,
)
from tests.provider_directory_projection_foundation_postgres_support import (
    MIGRATION_PATH,
    PROJECTION_RELATIONS,
    projection_foundation_postgres,
)
from tests.provider_directory_retained_core_postgres_support import (
    admit_campaign_item,
    campaign_item,
    fixed_campaign_plan,
    ordered_campaign_plan,
    registry_artifact,
)


def _digest(label: str) -> str:
    return hashlib.sha256(label.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class _RetainedBinding:
    campaign_id: str
    campaign_sha256: str
    source_item_id: str
    consumer_claim_generation: int
    family: str
    partition_metadata_sha256: str
    stream_identity_sha256: str
    sequence_ordinal: int


@dataclass(frozen=True)
class _OrderedStreamItems:
    positive_stream: str
    zero_stream: str
    payload_item: RetainedCampaignItem
    positive_terminal: RetainedCampaignItem
    zero_terminal: RetainedCampaignItem

    @property
    def retained_items(self) -> tuple[RetainedCampaignItem, ...]:
        return (self.payload_item, self.positive_terminal, self.zero_terminal)


@dataclass(frozen=True)
class _OrderedAdmissionContext:
    recipe: ProjectionRecipeIdentity
    admission_block: ProjectionAdmissionInputBlock
    shard: ProjectionShardSpec
    terminal_zeros: tuple[ProjectionAdmissionTerminalZero, ...]
    claim_generation: int


@dataclass(frozen=True)
class _AllZeroAdmissionContext:
    recipe: ProjectionRecipeIdentity
    terminal_zeros: tuple[ProjectionAdmissionTerminalZero, ...]
    claim_generation: int


def _fixture_input_block(
    produced_artifact: ProducedArtifact,
    retained_range: ArtifactLayoutRange,
):
    """Build one campaign-blind projection block for a retained range."""

    return projection_input_block(
        upstream_artifact_id=produced_artifact.artifact_sha256,
        source_object_id=produced_layout_digest(produced_artifact),
        block_kind="ndjson",
        input_contract_id="fixture-input.v1",
        record_start=retained_range.record_start,
        record_count=retained_range.record_count,
        content_sha256=retained_range.canonical_sha256,
        payload_sha256=retained_range.raw_sha256,
        payload_bytes=retained_range.raw_byte_count,
        summary={"resource_count": retained_range.record_count},
    )


def _fixture_projection_recipe(input_blocks, resource_type, completeness):
    """Build the common source-neutral recipe used by PostgreSQL fixtures."""

    input_set_sha256 = projection_input_set_sha256(
        input_blocks,
        decoder_contract_id="fixture-fhir-r4-decoder.v1",
    )
    return projection_recipe_identity(
        decoder_contract_id="fixture-fhir-r4-decoder.v1",
        acquisition_adapter_id="fixture-fhir-rest.v1",
        input_set_sha256=input_set_sha256,
        source_ids=("fixture-source",),
        transform_contract_id="fixture-normalization.v1",
        scope_contract_id="healthporta.provider-directory.global-scope.v1",
        transform_context={
            "as_of_date": "2026-07-22",
            "time_rule_contract_id": "fixture-time-rules.v1",
        },
        selected_resources=(resource_type,),
        required_resources=(resource_type,),
        completeness_manifest=completeness,
    )


def _fixture_shards(recipe, input_blocks):
    return tuple(
        projection_shard_spec(
            recipe=recipe,
            partition_ordinal=partition_ordinal,
            input_block=input_block,
        )
        for partition_ordinal, input_block in enumerate(input_blocks)
    )


def _projection_fixture(
    *,
    retained_binding: _RetainedBinding | None = None,
    artifact_label: str = "projection-shared-artifact",
    completeness_row_delta: int = 0,
):
    """Build one locator-free recipe, input block, and shard fixture."""

    produced_artifact = registry_artifact(artifact_label, "bulk_ndjson")
    layout_sha256 = produced_layout_digest(produced_artifact)
    retained_range = produced_artifact.ranges[0]
    retained_campaign_id = (
        retained_binding.campaign_id
        if retained_binding
        else _digest("provisional-retained-campaign")
    )
    retained_campaign_sha256 = (
        retained_binding.campaign_sha256
        if retained_binding
        else _digest("provisional-retained-campaign-proof")
    )
    retained_source_item_id = (
        retained_binding.source_item_id
        if retained_binding
        else _digest("provisional-retained-source-item")
    )
    stream_identity_sha256 = (
        retained_binding.stream_identity_sha256
        if retained_binding
        else _digest("provisional-retained-stream")
    )
    sequence_ordinal = retained_binding.sequence_ordinal if retained_binding else 0
    resource_type = (
        retained_binding.family if retained_binding else "PractitionerRole"
    )
    partition_key_hash = (
        retained_binding.partition_metadata_sha256
        if retained_binding
        else campaign_item("provisional-retained-item").partition_metadata_sha256
    )
    completeness = projection_completeness_manifest(
        endpoint_campaign_hash=retained_campaign_sha256,
        partition_strategy_contract_id="fixture-partitions.v1",
        selected_resources=(resource_type,),
        required_resources=(resource_type,),
        terminal_partitions=(
            {
                "resource_type": resource_type,
                "partition_key_hash": partition_key_hash,
                "source_partition_ordinal": 0,
                "terminal_cursor_hmac": _digest("terminal-cursor"),
                "terminal": True,
                "block_count": 1,
                "row_count": retained_range.record_count
                + completeness_row_delta,
                "byte_count": retained_range.raw_byte_count,
                "zero_row_proof_sha256": None,
            },
        ),
        complete=True,
    )
    input_block = projection_input_block(
        upstream_artifact_id=produced_artifact.artifact_sha256,
        source_object_id=layout_sha256,
        block_kind="ndjson",
        input_contract_id="fixture-input.v1",
        record_start=retained_range.record_start,
        record_count=retained_range.record_count,
        content_sha256=retained_range.canonical_sha256,
        payload_sha256=retained_range.raw_sha256,
        payload_bytes=retained_range.raw_byte_count,
        summary={"resource_count": retained_range.record_count},
    )
    admission_block = projection_admission_input_block(
        input_block,
        retained_campaign_id=retained_campaign_id,
        retained_campaign_sha256=retained_campaign_sha256,
        retained_source_item_id=retained_source_item_id,
        retained_range_ordinal=retained_range.range_ordinal,
        stream_identity_sha256=stream_identity_sha256,
        sequence_ordinal=sequence_ordinal,
        resource_type=resource_type,
        partition_key_hash=partition_key_hash,
        source_partition_ordinal=0,
    )
    input_set_sha256 = projection_input_set_sha256(
        (input_block,),
        decoder_contract_id="fixture-fhir-r4-decoder.v1",
    )
    recipe = projection_recipe_identity(
        decoder_contract_id="fixture-fhir-r4-decoder.v1",
        acquisition_adapter_id="fixture-fhir-rest.v1",
        input_set_sha256=input_set_sha256,
        source_ids=("fixture-source",),
        transform_contract_id="fixture-normalization.v1",
        scope_contract_id="healthporta.provider-directory.global-scope.v1",
        transform_context={
            "as_of_date": "2026-07-22",
            "time_rule_contract_id": "fixture-time-rules.v1",
        },
        selected_resources=(resource_type,),
        required_resources=(resource_type,),
        completeness_manifest=completeness,
    )
    shard = projection_shard_spec(
        recipe=recipe, partition_ordinal=0, input_block=input_block
    )
    return recipe, admission_block, shard


async def _admit_existing_artifact(
    connection,
    campaign_id: str,
    retained_item,
    produced_artifact,
    layout_sha256: str,
) -> None:
    """Bind a second campaign member to an already verified physical artifact."""

    await connection.execute(
        f"""
        UPDATE {database_table('provider_directory_retained_artifact_campaign_item')}
           SET status = 'admitted', observed_byte_count = $3,
               acquisition_mode = 'producer_verified',
               validator_kind = 'producer_proof', validator_sha256 = NULL,
               immutable_identity_sha256 = $4, committed_byte_count = $3,
               downloaded_artifact_sha256 = $4, artifact_sha256 = $4,
               layout_sha256 = $5, admitted_at = now(), updated_at = now()
         WHERE campaign_id = $1 AND source_item_id = $2;
        """,
        campaign_id,
        retained_item.source_item_id,
        produced_artifact.artifact_byte_count,
        produced_artifact.artifact_sha256,
        layout_sha256,
    )


async def _admit_produced_artifact(
    connection,
    campaign_id: str,
    retained_item: RetainedCampaignItem,
    produced_artifact: ProducedArtifact,
) -> str:
    """Register all fixture layout ranges, not only the legacy first range."""

    if len(produced_artifact.ranges) == 1:
        return await admit_campaign_item(
            connection,
            campaign_id,
            retained_item,
            produced_artifact,
        )
    layout_sha256 = produced_layout_digest(produced_artifact)
    await connection.execute(
        f"""
        INSERT INTO {database_table('provider_directory_retained_artifact')} (
            artifact_sha256, artifact_byte_count, artifact_locator,
            registry_status, verified_at, created_at
        ) VALUES ($1, $2, $3, 'verified', now(), now());
        """,
        produced_artifact.artifact_sha256,
        produced_artifact.artifact_byte_count,
        f"fixture://artifact/{produced_artifact.artifact_sha256}",
    )
    await connection.execute(
        f"""
        INSERT INTO {database_table('provider_directory_retained_artifact_layout')} (
            layout_sha256, artifact_sha256, artifact_record_count,
            layout_contract_id, layout_contract_version, layout_range_count,
            range_set_sha256, canonical_byte_count, manifest_sha256,
            manifest_byte_count, manifest_locator, producer_build_id,
            registry_status, verified_at, created_at
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12,
                  'verified', now(), now());
        """,
        layout_sha256,
        produced_artifact.artifact_sha256,
        produced_artifact.artifact_record_count,
        produced_artifact.layout_contract_id,
        produced_artifact.layout_contract_version,
        len(produced_artifact.ranges),
        produced_artifact.range_set_sha256,
        produced_artifact.canonical_byte_count,
        produced_artifact.manifest_sha256,
        produced_artifact.manifest_byte_count,
        f"fixture://manifest/{produced_artifact.manifest_sha256}",
        produced_artifact.producer_build_id,
    )
    range_table = database_table("provider_directory_retained_artifact_range")
    for retained_range in produced_artifact.ranges:
        await connection.execute(
            f"""
            INSERT INTO {range_table} (
                layout_sha256, artifact_sha256, layout_contract_version,
                layout_range_count, range_ordinal, raw_byte_start, raw_byte_end,
                raw_byte_count, raw_sha256, record_start, record_end,
                record_count, canonical_sha256, canonical_byte_count, verified_at
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12,
                      $13, $14, now());
            """,
            layout_sha256,
            produced_artifact.artifact_sha256,
            produced_artifact.layout_contract_version,
            len(produced_artifact.ranges),
            retained_range.range_ordinal,
            retained_range.raw_byte_start,
            retained_range.raw_byte_end,
            retained_range.raw_byte_count,
            retained_range.raw_sha256,
            retained_range.record_start,
            retained_range.record_end,
            retained_range.record_count,
            retained_range.canonical_sha256,
            retained_range.canonical_byte_count,
        )
    await connection.execute(
        f"""
        UPDATE {database_table('provider_directory_retained_artifact_campaign_item')}
           SET status='admitted', observed_byte_count=$3,
               acquisition_mode='producer_verified',
               validator_kind='producer_proof', validator_sha256=NULL,
               immutable_identity_sha256=$4, committed_byte_count=$3,
               downloaded_artifact_sha256=$4, artifact_sha256=$4,
               layout_sha256=$5, admitted_at=now(), updated_at=now()
         WHERE campaign_id=$1 AND source_item_id=$2;
        """,
        campaign_id,
        retained_item.source_item_id,
        produced_artifact.artifact_byte_count,
        produced_artifact.artifact_sha256,
        layout_sha256,
    )
    return layout_sha256


def _shared_item(label: str, partition_label: str):
    """Build distinct retained items with one exact logical partition."""

    base_item = campaign_item(label)
    return RetainedCampaignItem(
        source_item_id=base_item.source_item_id,
        source_entry_sha256=base_item.source_entry_sha256,
        artifact_kind=base_item.artifact_kind,
        family=base_item.family,
        collection_kind=base_item.collection_kind,
        partition_metadata={"shared_partition": partition_label},
        stream_identity_sha256=base_item.stream_identity_sha256,
        sequence_ordinal=base_item.sequence_ordinal,
        item_role=base_item.item_role,
        source_locator=f"fixture://{base_item.source_item_id}",
        declared_byte_count=base_item.declared_byte_count,
        terminal_proof_sha256=base_item.terminal_proof_sha256,
    ).validate()


def _shared_block_fixture(
    bindings: Sequence[_RetainedBinding],
    produced_artifact: ProducedArtifact,
):
    """Map many logical retained items to one content-identical core block."""

    assert bindings
    assert len({binding.family for binding in bindings}) == 1
    assert len({binding.partition_metadata_sha256 for binding in bindings}) == 1
    resource_type = bindings[0].family
    partition_key_hash = bindings[0].partition_metadata_sha256
    retained_range = produced_artifact.ranges[0]
    input_block = _fixture_input_block(produced_artifact, retained_range)
    admission_blocks = tuple(
        projection_admission_input_block(
            input_block,
            retained_campaign_id=binding.campaign_id,
            retained_campaign_sha256=binding.campaign_sha256,
            retained_source_item_id=binding.source_item_id,
            retained_range_ordinal=retained_range.range_ordinal,
            stream_identity_sha256=binding.stream_identity_sha256,
            sequence_ordinal=binding.sequence_ordinal,
            resource_type=resource_type,
            partition_key_hash=partition_key_hash,
            source_partition_ordinal=0,
        )
        for binding in bindings
    )
    completeness = projection_completeness_manifest(
        endpoint_campaign_hash=bindings[0].campaign_sha256,
        partition_strategy_contract_id="fixture-partitions.v1",
        selected_resources=(resource_type,),
        required_resources=(resource_type,),
        terminal_partitions=(
            {
                "resource_type": resource_type,
                "partition_key_hash": partition_key_hash,
                "source_partition_ordinal": 0,
                "terminal_cursor_hmac": _digest("shared-terminal-cursor"),
                "terminal": True,
                "block_count": len(admission_blocks),
                "row_count": (
                    retained_range.record_count * len(admission_blocks)
                ),
                "byte_count": (
                    retained_range.raw_byte_count * len(admission_blocks)
                ),
                "zero_row_proof_sha256": None,
            },
        ),
        complete=True,
    )
    recipe = _fixture_projection_recipe((input_block,), resource_type, completeness)
    shard = projection_shard_spec(
        recipe=recipe,
        partition_ordinal=0,
        input_block=input_block,
    )
    return recipe, admission_blocks, shard


async def _sealed_shared_artifact_bindings(
    postgres,
    *,
    campaign_label: str,
    item_count: int,
    consumer_recipe_id,
    produced_artifact: ProducedArtifact | None = None,
) -> tuple[tuple[_RetainedBinding, ...], ProducedArtifact]:
    """Seal and claim multiple logical items backed by one physical artifact."""

    connection = postgres.retained_connection
    retained_items = tuple(
        _shared_item(f"{campaign_label}-item-{ordinal}", campaign_label)
        for ordinal in range(item_count)
    )
    campaign_id = await initialize_retained_artifact_campaign(
        connection,
        plan=fixed_campaign_plan(campaign_label, retained_items),
    )
    campaign_lease = await acquire_campaign_lease(
        connection,
        campaign_id=campaign_id,
        owner=f"projection-{campaign_label}",
    )
    produced_artifact = produced_artifact or registry_artifact(
        f"{campaign_label}-shared-artifact",
        retained_items[0].artifact_kind,
    )
    layout_sha256 = await _admit_produced_artifact(
        connection,
        campaign_id,
        retained_items[0],
        produced_artifact,
    )
    for retained_item in retained_items[1:]:
        await _admit_existing_artifact(
            connection,
            campaign_id,
            retained_item,
            produced_artifact,
            layout_sha256,
        )
    sealed_summary = await seal_retained_artifact_campaign(
        connection,
        campaign_id=campaign_id,
        campaign_lease=campaign_lease,
    )
    provisional_bindings = tuple(
        _RetainedBinding(
            campaign_id=campaign_id,
            campaign_sha256=sealed_summary["campaign_sha256"],
            source_item_id=retained_item.source_item_id,
            consumer_claim_generation=1,
            family=retained_item.family,
            partition_metadata_sha256=retained_item.partition_metadata_sha256,
            stream_identity_sha256=retained_item.stream_identity_sha256,
            sequence_ordinal=retained_item.sequence_ordinal,
        )
        for retained_item in retained_items
    )
    if callable(consumer_recipe_id):
        consumer_recipe_id = consumer_recipe_id(provisional_bindings, produced_artifact)
    claimed_campaign = await claim_sealed_retained_campaign(
        connection,
        campaign_id=campaign_id,
        consumer_recipe_id=consumer_recipe_id,
    )
    claimed_bindings = tuple(
        replace(
            binding,
            consumer_claim_generation=claimed_campaign.consumer_claim_generation,
        )
        for binding in provisional_bindings
    )
    return claimed_bindings, produced_artifact


def _two_range_artifact(label: str, artifact_kind: str) -> ProducedArtifact:
    """Return one verified fixture layout split across two retained ranges."""

    payloads = (
        f'{{"id":"{label}-0"}}\n'.encode(),
        f'{{"id":"{label}-1"}}\n'.encode(),
    )
    artifact_payload = b"".join(payloads)
    artifact_sha256 = hashlib.sha256(artifact_payload).hexdigest()
    first_size = len(payloads[0])
    ranges = tuple(
        ArtifactLayoutRange(
            range_ordinal=ordinal,
            raw_byte_start=0 if ordinal == 0 else first_size,
            raw_byte_end=first_size if ordinal == 0 else len(artifact_payload),
            raw_byte_count=len(range_payload),
            raw_sha256=hashlib.sha256(range_payload).hexdigest(),
            record_start=ordinal,
            record_end=ordinal + 1,
            record_count=1,
            canonical_sha256=hashlib.sha256(range_payload).hexdigest(),
            canonical_byte_count=len(range_payload),
        )
        for ordinal, range_payload in enumerate(payloads)
    )
    provisional = ProducedArtifact(
        artifact_sha256=artifact_sha256,
        artifact_kind=artifact_kind,
        artifact_byte_count=len(artifact_payload),
        artifact_record_count=2,
        artifact_path="fixture://two-range-artifact",
        layout_contract_id="retained-core-fixture-layout-v1",
        layout_contract_version=1,
        range_set_sha256="0" * 64,
        canonical_byte_count=len(artifact_payload),
        manifest_sha256=_digest(f"manifest:{label}"),
        manifest_byte_count=64,
        manifest_path="fixture://two-range-manifest",
        producer_build_id="retained-core-fixture-v1",
        ranges=ranges,
    )
    return replace(
        provisional,
        range_set_sha256=expected_range_set_digest(provisional),
    )


def _range_subset_fixture(
    binding: _RetainedBinding,
    produced_artifact: ProducedArtifact,
    retained_range_ordinals: Sequence[int],
):
    """Build a valid logical recipe over a chosen subset of retained ranges."""

    selected_ranges = tuple(
        produced_artifact.ranges[range_ordinal]
        for range_ordinal in retained_range_ordinals
    )
    input_blocks = tuple(
        _fixture_input_block(produced_artifact, retained_range)
        for retained_range in selected_ranges
    )
    admission_blocks = tuple(
        projection_admission_input_block(
            input_block,
            retained_campaign_id=binding.campaign_id,
            retained_campaign_sha256=binding.campaign_sha256,
            retained_source_item_id=binding.source_item_id,
            retained_range_ordinal=retained_range.range_ordinal,
            stream_identity_sha256=binding.stream_identity_sha256,
            sequence_ordinal=binding.sequence_ordinal,
            resource_type=binding.family,
            partition_key_hash=binding.partition_metadata_sha256,
            source_partition_ordinal=0,
        )
        for input_block, retained_range in zip(
            input_blocks,
            selected_ranges,
            strict=True,
        )
    )
    completeness = projection_completeness_manifest(
        endpoint_campaign_hash=binding.campaign_sha256,
        partition_strategy_contract_id="fixture-partitions.v1",
        selected_resources=(binding.family,),
        required_resources=(binding.family,),
        terminal_partitions=(
            {
                "resource_type": binding.family,
                "partition_key_hash": binding.partition_metadata_sha256,
                "source_partition_ordinal": 0,
                "terminal_cursor_hmac": _digest("range-terminal-cursor"),
                "terminal": True,
                "block_count": len(input_blocks),
                "row_count": sum(block.record_count for block in input_blocks),
                "byte_count": sum(block.payload_bytes for block in input_blocks),
                "zero_row_proof_sha256": None,
            },
        ),
        complete=True,
    )
    recipe = _fixture_projection_recipe(input_blocks, binding.family, completeness)
    shards = _fixture_shards(recipe, input_blocks)
    return recipe, admission_blocks, shards


def _ordered_item(
    label: str,
    *,
    stream_identity: str,
    sequence_ordinal: int,
    item_role: str,
    partition_label: str,
) -> RetainedCampaignItem:
    base_item = campaign_item(
        label,
        stream_identity=stream_identity,
        sequence_ordinal=sequence_ordinal,
        item_role=item_role,
    )
    is_terminal = item_role == TERMINAL_ZERO
    return RetainedCampaignItem(
        source_item_id=base_item.source_item_id,
        source_entry_sha256=base_item.source_entry_sha256,
        artifact_kind=base_item.artifact_kind,
        family=base_item.family,
        collection_kind=base_item.collection_kind,
        partition_metadata={"ordered_partition": partition_label},
        stream_identity_sha256=stream_identity,
        sequence_ordinal=sequence_ordinal,
        item_role=item_role,
        source_locator=(
            None if is_terminal else f"fixture://{base_item.source_item_id}"
        ),
        declared_byte_count=0 if is_terminal else base_item.declared_byte_count,
        terminal_proof_sha256=base_item.terminal_proof_sha256,
    ).validate()


def _retained_stream_descriptor(
    terminal_item: RetainedCampaignItem,
    stream_ordinal: int,
) -> dict[str, object]:
    return {
        "identity_sha256": terminal_item.stream_identity_sha256,
        "stream_ordinal": stream_ordinal,
        "terminal_sequence_ordinal": terminal_item.sequence_ordinal,
        "terminal_proof_sha256": terminal_item.terminal_proof_sha256,
    }


def _ordered_terminal_zero_evidence(
    campaign_id: str,
    campaign_sha256: str,
    positive_terminal: RetainedCampaignItem,
    zero_terminal: RetainedCampaignItem,
    stream_ordinal_by_identity: Mapping[str, int],
):
    positive_stream = _retained_stream_descriptor(
        positive_terminal,
        stream_ordinal_by_identity[positive_terminal.stream_identity_sha256],
    )
    zero_stream = _retained_stream_descriptor(
        zero_terminal,
        stream_ordinal_by_identity[zero_terminal.stream_identity_sha256],
    )
    return (
        projection_admission_terminal_zero(
            retained_campaign_id=campaign_id,
            retained_campaign_sha256=campaign_sha256,
            retained_source_item_id=positive_terminal.source_item_id,
            resource_type=positive_terminal.family,
            partition_key_hash=positive_terminal.partition_metadata_sha256,
            source_partition_ordinal=0,
            retained_stream=positive_stream,
        ),
        projection_admission_terminal_zero(
            retained_campaign_id=campaign_id,
            retained_campaign_sha256=campaign_sha256,
            retained_source_item_id=zero_terminal.source_item_id,
            resource_type=zero_terminal.family,
            partition_key_hash=zero_terminal.partition_metadata_sha256,
            source_partition_ordinal=1,
            retained_stream=zero_stream,
        ),
    )


def _ordered_terminal_partitions(
    payload_item: RetainedCampaignItem,
    positive_terminal: RetainedCampaignItem,
    zero_terminal: RetainedCampaignItem,
    retained_range: ArtifactLayoutRange,
    stream_ordinal_by_identity: Mapping[str, int],
) -> tuple[dict[str, object], dict[str, object]]:
    positive_stream = _retained_stream_descriptor(
        positive_terminal,
        stream_ordinal_by_identity[positive_terminal.stream_identity_sha256],
    )
    zero_stream = _retained_stream_descriptor(
        zero_terminal,
        stream_ordinal_by_identity[zero_terminal.stream_identity_sha256],
    )
    return (
        {
            "resource_type": payload_item.family,
            "partition_key_hash": payload_item.partition_metadata_sha256,
            "source_partition_ordinal": 0,
            "terminal_cursor_hmac": _digest("positive-stream-cursor"),
            "terminal": True,
            "block_count": 1,
            "row_count": retained_range.record_count,
            "byte_count": retained_range.raw_byte_count,
            "zero_row_proof_sha256": None,
            "retained_stream": positive_stream,
        },
        {
            "resource_type": zero_terminal.family,
            "partition_key_hash": zero_terminal.partition_metadata_sha256,
            "source_partition_ordinal": 1,
            "terminal_cursor_hmac": _digest("zero-stream-cursor"),
            "terminal": True,
            "block_count": 0,
            "row_count": 0,
            "byte_count": 0,
            "zero_row_proof_sha256": zero_terminal.terminal_proof_sha256,
            "retained_stream": zero_stream,
        },
    )


def _ordered_stream_items(campaign_label: str) -> _OrderedStreamItems:
    positive_stream = _digest(f"{campaign_label}:positive-stream")
    zero_stream = _digest(f"{campaign_label}:zero-stream")
    return _OrderedStreamItems(
        positive_stream=positive_stream,
        zero_stream=zero_stream,
        payload_item=_ordered_item(
            f"{campaign_label}-payload",
            stream_identity=positive_stream,
            sequence_ordinal=0,
            item_role="payload",
            partition_label="positive",
        ),
        positive_terminal=_ordered_item(
            f"{campaign_label}-positive-terminal",
            stream_identity=positive_stream,
            sequence_ordinal=1,
            item_role=TERMINAL_ZERO,
            partition_label="positive",
        ),
        zero_terminal=_ordered_item(
            f"{campaign_label}-zero-terminal",
            stream_identity=zero_stream,
            sequence_ordinal=0,
            item_role=TERMINAL_ZERO,
            partition_label="zero",
        ),
    )


def _stream_ordinal_by_identity(
    positive_terminal: RetainedCampaignItem,
    zero_terminal: RetainedCampaignItem,
) -> dict[str, int]:
    """Assign deterministic ordinals to both ordered fixture streams."""

    stream_identities = sorted(
        (
            positive_terminal.stream_identity_sha256,
            zero_terminal.stream_identity_sha256,
        )
    )
    return {
        stream_identity: stream_ordinal
        for stream_ordinal, stream_identity in enumerate(stream_identities)
    }


def _ordered_zero_stream_fixture(
    *,
    campaign_id: str,
    campaign_sha256: str,
    payload_item: RetainedCampaignItem,
    positive_terminal: RetainedCampaignItem,
    zero_terminal: RetainedCampaignItem,
    produced_artifact: ProducedArtifact,
):
    """Build exact positive and zero-only admission evidence."""

    retained_range = produced_artifact.ranges[0]
    stream_ordinal_by_identity = _stream_ordinal_by_identity(
        positive_terminal,
        zero_terminal,
    )
    input_block = _fixture_input_block(produced_artifact, retained_range)
    admission_block = projection_admission_input_block(
        input_block,
        retained_campaign_id=campaign_id,
        retained_campaign_sha256=campaign_sha256,
        retained_source_item_id=payload_item.source_item_id,
        retained_range_ordinal=retained_range.range_ordinal,
        stream_identity_sha256=payload_item.stream_identity_sha256,
        sequence_ordinal=payload_item.sequence_ordinal,
        resource_type=payload_item.family,
        partition_key_hash=payload_item.partition_metadata_sha256,
        source_partition_ordinal=0,
    )
    terminal_zeros = _ordered_terminal_zero_evidence(
        campaign_id, campaign_sha256, positive_terminal, zero_terminal,
        stream_ordinal_by_identity,
    )
    completeness = projection_completeness_manifest(
        endpoint_campaign_hash=campaign_sha256,
        partition_strategy_contract_id="fixture-ordered-partitions.v1",
        selected_resources=(payload_item.family,),
        required_resources=(payload_item.family,),
        terminal_partitions=_ordered_terminal_partitions(
            payload_item,
            positive_terminal,
            zero_terminal,
            retained_range,
            stream_ordinal_by_identity,
        ),
        complete=True,
    )
    recipe = _fixture_projection_recipe(
        (input_block,), payload_item.family, completeness
    )
    shard = projection_shard_spec(
        recipe=recipe, partition_ordinal=0, input_block=input_block
    )
    return recipe, admission_block, shard, terminal_zeros


async def _ordered_fixture_campaign(
    connection,
    campaign_label: str,
    stream_items: _OrderedStreamItems,
) -> tuple[str, object]:
    """Initialize and append every item in one ordered retained campaign."""

    plan = replace(
        ordered_campaign_plan(campaign_label, stream_items.positive_stream),
        expected_stream_identities=tuple(
            sorted((stream_items.positive_stream, stream_items.zero_stream))
        ),
    ).validate()
    campaign_id = await initialize_retained_artifact_campaign(connection, plan=plan)
    campaign_lease = await acquire_campaign_lease(
        connection,
        campaign_id=campaign_id,
        owner=f"projection-{campaign_label}",
    )
    for retained_item in stream_items.retained_items:
        await append_ordered_stream_item(
            connection,
            campaign_id=campaign_id,
            campaign_lease=campaign_lease,
            item=retained_item,
        )
    return campaign_id, campaign_lease


async def _ordered_zero_stream_context(postgres, campaign_label: str):
    """Seal and claim one positive plus one terminal-zero-only stream."""

    connection = postgres.retained_connection
    stream_items = _ordered_stream_items(campaign_label)
    campaign_id, campaign_lease = await _ordered_fixture_campaign(
        connection,
        campaign_label,
        stream_items,
    )
    produced_artifact = registry_artifact(
        f"{campaign_label}-payload",
        stream_items.payload_item.artifact_kind,
    )
    await admit_campaign_item(
        connection,
        campaign_id,
        stream_items.payload_item,
        produced_artifact,
    )
    sealed_summary = await seal_retained_artifact_campaign(
        connection,
        campaign_id=campaign_id,
        campaign_lease=campaign_lease,
    )
    fixture = _ordered_zero_stream_fixture(
        campaign_id=campaign_id,
        campaign_sha256=sealed_summary["campaign_sha256"],
        payload_item=stream_items.payload_item,
        positive_terminal=stream_items.positive_terminal,
        zero_terminal=stream_items.zero_terminal,
        produced_artifact=produced_artifact,
    )
    recipe, admission_block, shard, terminal_zeros = fixture
    consumer_id = projection_admission_consumer_id(
        recipe,
        (admission_block,),
        terminal_zeros=terminal_zeros,
    )
    claimed_campaign = await claim_sealed_retained_campaign(
        connection,
        campaign_id=campaign_id,
        consumer_recipe_id=consumer_id,
    )
    return _OrderedAdmissionContext(
        recipe=recipe,
        admission_block=admission_block,
        shard=shard,
        terminal_zeros=terminal_zeros,
        claim_generation=claimed_campaign.consumer_claim_generation,
    )


def _all_zero_ordered_recipe(
    campaign_sha256: str,
    terminal_items: Sequence[RetainedCampaignItem],
    stream_ordinal_by_identity: Mapping[str, int],
):
    """Build a no-input recipe and exact terminal evidence for zero streams."""

    terminal_partitions = tuple(
        {
            "resource_type": terminal_item.family,
            "partition_key_hash": terminal_item.partition_metadata_sha256,
            "source_partition_ordinal": source_partition_ordinal,
            "terminal_cursor_hmac": _digest(
                f"all-zero-cursor:{source_partition_ordinal}"
            ),
            "terminal": True,
            "block_count": 0,
            "row_count": 0,
            "byte_count": 0,
            "zero_row_proof_sha256": terminal_item.terminal_proof_sha256,
            "retained_stream": _retained_stream_descriptor(
                terminal_item,
                stream_ordinal_by_identity[
                    terminal_item.stream_identity_sha256
                ],
            ),
        }
        for source_partition_ordinal, terminal_item in enumerate(terminal_items)
    )
    completeness = projection_completeness_manifest(
        endpoint_campaign_hash=campaign_sha256,
        partition_strategy_contract_id="fixture-all-zero-ordered.v1",
        selected_resources=(terminal_items[0].family,),
        required_resources=(terminal_items[0].family,),
        terminal_partitions=terminal_partitions,
        complete=True,
    )
    return _fixture_projection_recipe(
        (), terminal_items[0].family, completeness
    )


def _all_zero_terminal_evidence(
    campaign_id: str,
    campaign_sha256: str,
    terminal_items: Sequence[RetainedCampaignItem],
    stream_ordinal_by_identity: Mapping[str, int],
):
    return tuple(
        projection_admission_terminal_zero(
            retained_campaign_id=campaign_id,
            retained_campaign_sha256=campaign_sha256,
            retained_source_item_id=terminal_item.source_item_id,
            resource_type=terminal_item.family,
            partition_key_hash=terminal_item.partition_metadata_sha256,
            source_partition_ordinal=source_partition_ordinal,
            retained_stream=_retained_stream_descriptor(
                terminal_item,
                stream_ordinal_by_identity[
                    terminal_item.stream_identity_sha256
                ],
            ),
        )
        for source_partition_ordinal, terminal_item in enumerate(terminal_items)
    )


def _all_zero_stream_items(
    campaign_label: str,
) -> tuple[tuple[str, ...], tuple[RetainedCampaignItem, ...]]:
    """Build deterministic terminal-only items for two ordered streams."""

    stream_identities = tuple(
        sorted(_digest(f"{campaign_label}:stream:{ordinal}") for ordinal in range(2))
    )
    terminal_items = tuple(
        _ordered_item(
            f"{campaign_label}-terminal-{ordinal}",
            stream_identity=stream_identity,
            sequence_ordinal=0,
            item_role=TERMINAL_ZERO,
            partition_label=f"zero-{ordinal}",
        )
        for ordinal, stream_identity in enumerate(stream_identities)
    )
    return stream_identities, terminal_items


async def _all_zero_ordered_context(postgres, campaign_label: str):
    """Seal, describe, and claim a campaign containing only terminal-zero items."""

    connection = postgres.retained_connection
    stream_identities, terminal_items = _all_zero_stream_items(campaign_label)
    plan = replace(
        ordered_campaign_plan(campaign_label, stream_identities[0]),
        expected_stream_identities=stream_identities,
    ).validate()
    campaign_id = await initialize_retained_artifact_campaign(connection, plan=plan)
    campaign_lease = await acquire_campaign_lease(
        connection,
        campaign_id=campaign_id,
        owner=f"projection-{campaign_label}",
    )
    for terminal_item in terminal_items:
        await append_ordered_stream_item(
            connection,
            campaign_id=campaign_id,
            campaign_lease=campaign_lease,
            item=terminal_item,
        )
    sealed_summary = await seal_retained_artifact_campaign(
        connection,
        campaign_id=campaign_id,
        campaign_lease=campaign_lease,
    )
    stream_ordinal_by_identity = {
        stream_identity: stream_ordinal
        for stream_ordinal, stream_identity in enumerate(stream_identities)
    }
    recipe = _all_zero_ordered_recipe(
        sealed_summary["campaign_sha256"],
        terminal_items,
        stream_ordinal_by_identity,
    )
    terminal_zeros = _all_zero_terminal_evidence(
        campaign_id,
        sealed_summary["campaign_sha256"],
        terminal_items,
        stream_ordinal_by_identity,
    )
    consumer_id = projection_admission_consumer_id(
        recipe,
        (),
        terminal_zeros=terminal_zeros,
    )
    claimed_campaign = await claim_sealed_retained_campaign(
        connection,
        campaign_id=campaign_id,
        consumer_recipe_id=consumer_id,
    )
    return _AllZeroAdmissionContext(
        recipe=recipe,
        terminal_zeros=terminal_zeros,
        claim_generation=claimed_campaign.consumer_claim_generation,
    )


async def _register_ordered_workset(
    postgres,
    context: _OrderedAdmissionContext,
) -> None:
    """Register the physical half of an ordered admission fixture."""

    projection_claim = await claim_projection_recipe(
        context.recipe,
        database=postgres.database,
        schema=postgres.schema,
    )
    assert projection_claim.lease is not None
    await register_projection_workset(
        projection_claim.lease,
        (context.admission_block.block,),
        (context.shard,),
        database=postgres.database,
        schema=postgres.schema,
    )


def _rebound_ordered_payload(
    context: _OrderedAdmissionContext,
    stream_identity_sha256: str,
    sequence_ordinal: int,
) -> ProjectionAdmissionInputBlock:
    """Rebind one retained payload to a deliberately supplied stream coordinate."""

    admission_block = context.admission_block
    return projection_admission_input_block(
        admission_block.block,
        retained_campaign_id=admission_block.retained_campaign_id,
        retained_campaign_sha256=admission_block.retained_campaign_sha256,
        retained_source_item_id=admission_block.retained_source_item_id,
        retained_range_ordinal=admission_block.retained_range_ordinal,
        stream_identity_sha256=stream_identity_sha256,
        sequence_ordinal=sequence_ordinal,
        resource_type=admission_block.resource_type,
        partition_key_hash=admission_block.partition_key_hash,
        source_partition_ordinal=admission_block.source_partition_ordinal,
    )


def _forged_ordered_payloads(
    context: _OrderedAdmissionContext,
) -> tuple[ProjectionAdmissionInputBlock, ProjectionAdmissionInputBlock]:
    """Return wrong-stream and fabricated-sequence variants of one payload."""

    return (
        _rebound_ordered_payload(
            context,
            context.terminal_zeros[1].stream_identity_sha256,
            context.admission_block.sequence_ordinal,
        ),
        _rebound_ordered_payload(
            context,
            context.admission_block.stream_identity_sha256,
            context.admission_block.sequence_ordinal + 100,
        ),
    )


async def _assert_forged_mapping_rejected(
    postgres,
    monkeypatch,
    context: _OrderedAdmissionContext,
    forged_binding: ProjectionAdmissionInputBlock,
    insert_mappings,
) -> None:
    """Replace one persisted mapping and require the SQL fence to reject it."""

    async def insert_forged_mapping(database, schema, mappings):
        forged_mapping = dict(mappings[0])
        forged_mapping.update(
            binding_id=forged_binding.binding_id,
            stream_identity_sha256=forged_binding.stream_identity_sha256,
            sequence_ordinal=forged_binding.sequence_ordinal,
        )
        await insert_mappings(database, schema, (forged_mapping,))

    monkeypatch.setattr(
        projection_admission_store,
        "_insert_admission_mappings",
        insert_forged_mapping,
    )
    with pytest.raises(
        DBAPIError,
        match="provider_directory_projection_admission_map_unfenced",
    ):
        await register_projection_admission(
            context.recipe,
            (context.admission_block,),
            (context.shard,),
            claim_generation=context.claim_generation,
            terminal_zeros=context.terminal_zeros,
            database=postgres.database,
            schema=postgres.schema,
        )


async def _register_ordered_admission(
    postgres,
    context: _OrderedAdmissionContext,
) -> str:
    """Register one untampered ordered admission fixture."""

    return await register_projection_admission(
        context.recipe,
        (context.admission_block,),
        (context.shard,),
        claim_generation=context.claim_generation,
        terminal_zeros=context.terminal_zeros,
        database=postgres.database,
        schema=postgres.schema,
    )


async def _stored_payload_coordinate(postgres, admission_id: str) -> tuple[str, int]:
    """Read the stream identity and sequence fenced for one payload mapping."""

    stored_coordinate = await postgres.database.first(
        f"""
        SELECT stream_identity_sha256, sequence_ordinal
          FROM "{postgres.schema}".
               provider_directory_projection_admission_input_block
         WHERE admission_id = :admission_id;
        """,
        admission_id=admission_id,
    )
    return tuple(stored_coordinate)


async def _register_all_zero_admission(
    postgres,
    context: _AllZeroAdmissionContext,
) -> str:
    """Register an exact no-input admission from terminal-only streams."""

    return await register_projection_admission(
        context.recipe,
        (),
        (),
        claim_generation=context.claim_generation,
        terminal_zeros=context.terminal_zeros,
        database=postgres.database,
        schema=postgres.schema,
    )


async def _assert_mixed_no_input_descriptor_rejected(
    postgres,
    monkeypatch,
    context: _AllZeroAdmissionContext,
    identity,
) -> None:
    """Require SQL to reject a no-input manifest missing one stream descriptor."""

    insert_admission = projection_admission_store._insert_admission
    mixed_manifest = copy.deepcopy(dict(identity.completeness_manifest))
    mixed_manifest["terminal_partitions"][1].pop("retained_stream")
    mixed_descriptor_identity = replace(
        identity,
        completeness_manifest=mixed_manifest,
    )

    async def insert_mixed_descriptor_admission(
        database,
        table,
        _identity,
        lease_token,
        lease_seconds,
    ):
        await insert_admission(
            database,
            table,
            mixed_descriptor_identity,
            lease_token,
            lease_seconds,
        )

    monkeypatch.setattr(
        projection_admission_store,
        "_insert_admission",
        insert_mixed_descriptor_admission,
    )
    with pytest.raises(
        DBAPIError,
        match="provider_directory_projection_admission_insert_invalid",
    ):
        await _register_all_zero_admission(postgres, context)
    monkeypatch.setattr(
        projection_admission_store,
        "_insert_admission",
        insert_admission,
    )


async def _assert_incomplete_no_input_streams_rejected(
    postgres,
    monkeypatch,
    context: _AllZeroAdmissionContext,
) -> None:
    """Require SQL to reject a no-input admission missing one terminal stream."""

    insert_streams = projection_admission_store._insert_admission_streams

    async def insert_incomplete_streams(database, schema, streams):
        await insert_streams(database, schema, streams[:-1])

    monkeypatch.setattr(
        projection_admission_store,
        "_insert_admission_streams",
        insert_incomplete_streams,
    )
    with pytest.raises(
        DBAPIError,
        match="provider_directory_projection_admission_transition_invalid",
    ):
        await _register_all_zero_admission(postgres, context)
    monkeypatch.setattr(
        projection_admission_store,
        "_insert_admission_streams",
        insert_streams,
    )


async def _assert_stored_no_input_outcome(
    postgres,
    context: _AllZeroAdmissionContext,
    admission_id: str,
) -> None:
    """Verify the sealed outcome and absence of all physical recipe rows."""

    stored_outcome = await postgres.database.first(
        f"""
        SELECT status, outcome_kind, recipe_id, planned_recipe_id,
               input_block_count, stream_count
          FROM "{postgres.schema}".
               provider_directory_projection_admission
         WHERE admission_id = :admission_id;
        """,
        admission_id=admission_id,
    )
    assert tuple(stored_outcome) == (
        "sealed",
        "no_input",
        None,
        context.recipe.recipe_id,
        0,
        2,
    )
    physical_work_count = await postgres.database.scalar(
        f"""
        SELECT
            (SELECT count(*) FROM "{postgres.schema}".
                provider_directory_projection_recipe
             WHERE recipe_id = :recipe_id)
          + (SELECT count(*) FROM "{postgres.schema}".
                provider_directory_projection_input_block
             WHERE recipe_id = :recipe_id)
          + (SELECT count(*) FROM "{postgres.schema}".
                provider_directory_projection_proof_shard
             WHERE recipe_id = :recipe_id);
        """,
        recipe_id=context.recipe.recipe_id,
    )
    assert physical_work_count == 0


async def _assert_no_input_recipe_guards(
    postgres,
    context: _AllZeroAdmissionContext,
) -> None:
    """Verify physical claiming and migration downgrade both fail closed."""

    with pytest.raises(
        DBAPIError,
        match="provider_directory_projection_no_input_recipe_conflict",
    ):
        await claim_projection_recipe(
            context.recipe,
            database=postgres.database,
            schema=postgres.schema,
        )
    with pytest.raises(
        DBAPIError,
        match="provider_directory_projection_downgrade_has_live_state",
    ):
        await postgres.downgrade()


def _forged_all_zero_recipe(
    terminal_zeros: Sequence[ProjectionAdmissionTerminalZero],
) -> ProjectionRecipeIdentity:
    """Describe an all-zero recipe over a campaign that retained a payload."""

    terminal_partitions = tuple(
        {
            "resource_type": terminal.resource_type,
            "partition_key_hash": terminal.partition_key_hash,
            "source_partition_ordinal": terminal.source_partition_ordinal,
            "terminal_cursor_hmac": _digest(
                f"zero-forged-cursor:{terminal.source_partition_ordinal}"
            ),
            "terminal": True,
            "block_count": 0,
            "row_count": 0,
            "byte_count": 0,
            "zero_row_proof_sha256": terminal.terminal_proof_sha256,
            "retained_stream": {
                "identity_sha256": terminal.stream_identity_sha256,
                "stream_ordinal": terminal.stream_ordinal,
                "terminal_sequence_ordinal": terminal.terminal_sequence_ordinal,
                "terminal_proof_sha256": terminal.terminal_proof_sha256,
            },
        }
        for terminal in terminal_zeros
    )
    completeness = projection_completeness_manifest(
        endpoint_campaign_hash=terminal_zeros[0].retained_campaign_sha256,
        partition_strategy_contract_id="fixture-forged-all-zero.v1",
        selected_resources=(terminal_zeros[0].resource_type,),
        required_resources=(terminal_zeros[0].resource_type,),
        terminal_partitions=terminal_partitions,
        complete=True,
    )
    return _fixture_projection_recipe(
        (),
        terminal_zeros[0].resource_type,
        completeness,
    )


async def _sealed_retained_binding(
    postgres,
    *,
    campaign_label: str,
    artifact_label: str,
    consumer_recipe_id,
    reuse_verified_artifact: bool = False,
) -> _RetainedBinding:
    """Create and claim one real sealed retained campaign binding."""

    connection = postgres.retained_connection
    retained_item = campaign_item(campaign_label)
    campaign_id = await initialize_retained_artifact_campaign(
        connection,
        plan=fixed_campaign_plan(campaign_label, (retained_item,)),
    )
    campaign_lease = await acquire_campaign_lease(
        connection,
        campaign_id=campaign_id,
        owner=f"projection-{campaign_label}",
    )
    produced_artifact = registry_artifact(
        artifact_label,
        retained_item.artifact_kind,
    )
    layout_sha256 = produced_layout_digest(produced_artifact)
    if reuse_verified_artifact:
        await _admit_existing_artifact(
            connection,
            campaign_id,
            retained_item,
            produced_artifact,
            layout_sha256,
        )
    else:
        admitted_layout = await admit_campaign_item(
            connection,
            campaign_id,
            retained_item,
            produced_artifact,
        )
        assert admitted_layout == layout_sha256
    sealed_summary = await seal_retained_artifact_campaign(
        connection,
        campaign_id=campaign_id,
        campaign_lease=campaign_lease,
    )
    assert sealed_summary["complete"] is True
    if callable(consumer_recipe_id):
        consumer_recipe_id = consumer_recipe_id(
            _RetainedBinding(
                campaign_id=campaign_id,
                campaign_sha256=sealed_summary["campaign_sha256"],
                source_item_id=retained_item.source_item_id,
                consumer_claim_generation=1,
                family=retained_item.family,
                partition_metadata_sha256=retained_item.partition_metadata_sha256,
                stream_identity_sha256=retained_item.stream_identity_sha256,
                sequence_ordinal=retained_item.sequence_ordinal,
            )
        )
    claimed_campaign = await claim_sealed_retained_campaign(
        connection,
        campaign_id=campaign_id,
        consumer_recipe_id=consumer_recipe_id,
    )
    return _RetainedBinding(
        campaign_id=campaign_id,
        campaign_sha256=claimed_campaign.campaign_sha256,
        source_item_id=retained_item.source_item_id,
        consumer_claim_generation=claimed_campaign.consumer_claim_generation,
        family=retained_item.family,
        partition_metadata_sha256=retained_item.partition_metadata_sha256,
        stream_identity_sha256=retained_item.stream_identity_sha256,
        sequence_ordinal=retained_item.sequence_ordinal,
    )


async def _registered_projection_context(
    postgres,
    campaign_label: str,
    *,
    completeness_row_delta: int = 0,
    reuse_verified_artifact: bool = False,
    artifact_label: str = "projection-shared-artifact",
):
    def admission_consumer_id(binding):
        bound_recipe, bound_block, _ = _projection_fixture(
            retained_binding=binding,
            artifact_label=artifact_label,
            completeness_row_delta=completeness_row_delta,
        )
        return projection_admission_consumer_id(bound_recipe, (bound_block,))

    retained_binding = await _sealed_retained_binding(
        postgres,
        campaign_label=campaign_label,
        artifact_label=artifact_label,
        consumer_recipe_id=admission_consumer_id,
        reuse_verified_artifact=reuse_verified_artifact,
    )
    recipe, admission_block, shard = _projection_fixture(
        retained_binding=retained_binding,
        artifact_label=artifact_label,
        completeness_row_delta=completeness_row_delta,
    )
    projection_claim = await claim_projection_recipe(
        recipe,
        database=postgres.database,
        schema=postgres.schema,
    )
    assert projection_claim.lease is not None
    await register_projection_workset(
        projection_claim.lease,
        (admission_block.block,),
        (shard,),
        database=postgres.database,
        schema=postgres.schema,
    )
    admission_id = await register_projection_admission(
        recipe,
        (admission_block,),
        (shard,),
        claim_generation=retained_binding.consumer_claim_generation,
        database=postgres.database,
        schema=postgres.schema,
    )
    return (
        projection_claim.lease,
        retained_binding,
        admission_block,
        shard,
        admission_id,
    )


async def _registered_projection_lease(postgres, campaign_label: str):
    context = await _registered_projection_context(postgres, campaign_label)
    return context[0]


async def _prepare_additional_projection_admission(
    postgres,
    expected_recipe,
    campaign_label: str,
    *,
    completeness_row_delta: int = 0,
):
    def admission_consumer_id(binding):
        recipe, block, _ = _projection_fixture(
            retained_binding=binding,
            completeness_row_delta=completeness_row_delta,
        )
        assert recipe.recipe_id == expected_recipe.recipe_id
        return projection_admission_consumer_id(recipe, (block,))

    retained_binding = await _sealed_retained_binding(
        postgres,
        campaign_label=campaign_label,
        artifact_label="projection-shared-artifact",
        consumer_recipe_id=admission_consumer_id,
        reuse_verified_artifact=True,
    )
    recipe, block, shard = _projection_fixture(
        retained_binding=retained_binding,
        completeness_row_delta=completeness_row_delta,
    )
    assert recipe.recipe_id == expected_recipe.recipe_id
    return retained_binding, recipe, block, shard


async def _register_additional_projection_admission(
    postgres,
    expected_recipe,
    campaign_label: str,
    *,
    completeness_row_delta: int = 0,
):
    retained_binding, recipe, block, shard = (
        await _prepare_additional_projection_admission(
            postgres,
            expected_recipe,
            campaign_label,
            completeness_row_delta=completeness_row_delta,
        )
    )
    admission_id = await register_projection_admission(
        recipe,
        (block,),
        (shard,),
        claim_generation=retained_binding.consumer_claim_generation,
        database=postgres.database,
        schema=postgres.schema,
    )
    return retained_binding, block, shard, admission_id


def _migration_module():
    module_spec = importlib.util.spec_from_file_location(
        "projection_foundation_identity_migration",
        MIGRATION_PATH,
    )
    assert module_spec is not None and module_spec.loader is not None
    migration_module = importlib.util.module_from_spec(module_spec)
    module_spec.loader.exec_module(migration_module)
    return migration_module


def _physical_projection_proof(
    physical_projection_id: str,
    raw_shard: Mapping[str, object],
) -> dict:
    canonical_row_sha256 = _digest("canonical-row")
    dataset_hash = _digest("dataset")
    shard_resource_type = str(raw_shard["resource_type"])
    resource_type = str(raw_shard["first_identity"][0])
    resource_count_by_type = {resource_type: 1}
    source_summary_map = {
        "contract_id": "healthporta.provider-directory.physical-source-summary.v1",
        "canonical_row_sha256": canonical_row_sha256,
        "dataset_hash": dataset_hash,
        "resource_count": 1,
        "resource_counts": resource_count_by_type,
        "semantic_summary": {
            "contract_id": (
                "healthporta.provider-directory."
                "physical-source-semantic-summary.v1"
            ),
            "complete": True,
        },
    }
    proof_map = {
        "physical_projection_id": physical_projection_id,
        "canonical_row_sha256": canonical_row_sha256,
        "dataset_hash": dataset_hash,
        "resource_count": 1,
        "resource_counts": resource_count_by_type,
        "raw_shards": [dict(raw_shard)],
        "source_summary": source_summary_map,
    }
    assert shard_resource_type == "__mixed__"
    assert set(raw_shard) == {
        "canonical_row_sha256",
        "first_identity",
        "input_sha256",
        "last_identity",
        "partition_id",
        "partition_ordinal",
        "resource_count",
        "resource_type",
    }
    return proof_map


async def _complete_registered_projection_shard(
    database,
    schema: str,
    lease,
) -> dict[str, object]:
    """Complete the registered fixture shard and return its reduced descriptor."""

    completed_row = await database.first(
        f"""
        SELECT partition_id, partition_ordinal, resource_type, input_sha256,
               canonical_row_sha256, resource_count,
               first_identity_json, last_identity_json
          FROM "{schema}".provider_directory_projection_proof_shard
         WHERE recipe_id = :recipe_id AND attempt = :attempt
           AND status = 'complete';
        """,
        recipe_id=lease.recipe.recipe_id,
        attempt=lease.attempt,
    )
    if completed_row is not None:
        return {
            "partition_id": completed_row.partition_id,
            "partition_ordinal": completed_row.partition_ordinal,
            "resource_type": completed_row.resource_type,
            "input_sha256": completed_row.input_sha256,
            "canonical_row_sha256": completed_row.canonical_row_sha256,
            "resource_count": completed_row.resource_count,
            "first_identity": list(completed_row.first_identity_json),
            "last_identity": list(completed_row.last_identity_json),
        }
    admission_id = await database.scalar(
        f"""
        SELECT admission_id
          FROM "{schema}".provider_directory_projection_admission
         WHERE recipe_id = :recipe_id AND status = 'sealed'
         ORDER BY created_at
         LIMIT 1;
        """,
        recipe_id=lease.recipe.recipe_id,
    )
    assert admission_id is not None
    shard_claim = await claim_projection_shard(
        lease,
        admission_id=admission_id,
        database=database,
        schema=schema,
    )
    assert shard_claim is not None
    canonical_row_sha256 = _digest(
        f"completed-partition:{shard_claim.shard.partition_id}"
    )
    resource_type = shard_claim.shard.resource_type
    selected_resource = lease.recipe.selected_resources[0]
    resource_identity_parts = [selected_resource, "fixture-1"]
    resource_count_by_type = {
        resource_name: int(resource_name == selected_resource)
        for resource_name in lease.recipe.selected_resources
    }
    expanded_proof_map = {
        "contract_id": (
            "healthporta.provider-directory.projection-proof-shard.v1"
        ),
        "recipe_id": lease.recipe.recipe_id,
        "attempt": lease.attempt,
        "partition_attempt": shard_claim.partition_attempt,
        "partition_id": shard_claim.shard.partition_id,
        "partition_ordinal": shard_claim.shard.partition_ordinal,
        "resource_type": resource_type,
        "input_sha256": shard_claim.shard.input_sha256,
        "canonical_row_sha256": canonical_row_sha256,
        "resource_count": 1,
        "resource_counts": resource_count_by_type,
        "first_identity": resource_identity_parts,
        "last_identity": resource_identity_parts,
    }
    async with database.transaction():
        await set_local_projection_action(
            database,
            "shard_complete",
            recipe_id=lease.recipe.recipe_id,
            recipe_attempt=lease.attempt,
            recipe_lease_token=lease.lease_token,
            partition_id=shard_claim.shard.partition_id,
            partition_attempt=shard_claim.partition_attempt,
            shard_lease_token=shard_claim.lease_token,
        )
        await database.status(
            f"""
            UPDATE "{schema}".provider_directory_projection_proof_shard
               SET status = 'complete', recipe_lease_token = NULL,
                   lease_token = NULL, lease_expires_at = NULL,
                   lease_heartbeat_at = NULL,
                   canonical_row_sha256 = :canonical_row_sha256,
                   resource_count = 1,
                   first_identity_json = CAST(:first_identity AS jsonb),
                   last_identity_json = CAST(:last_identity AS jsonb),
                   proof_json = CAST(:proof_json AS jsonb),
                   completed_at = clock_timestamp()
             WHERE recipe_id = :recipe_id AND attempt = :attempt
               AND partition_id = :partition_id;
            """,
            canonical_row_sha256=canonical_row_sha256,
            first_identity=stable_json(resource_identity_parts),
            last_identity=stable_json(resource_identity_parts),
            proof_json=stable_json(expanded_proof_map),
            recipe_id=lease.recipe.recipe_id,
            attempt=lease.attempt,
            partition_id=shard_claim.shard.partition_id,
        )
    return {
        descriptor_field: expanded_proof_map[descriptor_field]
        for descriptor_field in (
            "partition_id",
            "partition_ordinal",
            "resource_type",
            "input_sha256",
            "canonical_row_sha256",
            "resource_count",
            "first_identity",
            "last_identity",
        )
    }


@dataclass(frozen=True)
class _StageBinding:
    schema: str
    relation: str
    relation_oid: int
    trigger_oid: int | None


async def _create_projection_stage(
    database,
    schema: str,
    *,
    label: str,
    physical_projection_id: str,
    raw_shard: Mapping[str, object],
    install_trigger: bool,
) -> _StageBinding:
    """Create a logged fixture stage with exact serving shape and indexes."""

    relation = f"projection_stage_{_digest(label)[:16]}"
    await database.status(
        f"""
        CREATE TABLE "{schema}"."{relation}" (
            LIKE "{schema}".provider_directory_physical_projection_resource
            INCLUDING DEFAULTS INCLUDING CONSTRAINTS INCLUDING STORAGE
        );
        """
    )
    await database.status(
        f"""
        ALTER TABLE "{schema}"."{relation}"
            ALTER COLUMN physical_projection_id
            SET DEFAULT '{physical_projection_id}';
        """
    )
    await database.status(
        f"""
        ALTER TABLE "{schema}"."{relation}"
            ADD CONSTRAINT provider_directory_projection_partition_bound
            CHECK (physical_projection_id = '{physical_projection_id}');
        """
    )
    resource_type = str(raw_shard["first_identity"][0])
    resource_id = str(raw_shard["first_identity"][1])
    await database.status(
        f"""
        INSERT INTO "{schema}"."{relation}" (
            physical_projection_id, resource_type, resource_id,
            proof_partition_id, payload_hash, payload_json, source_rank,
            summary_npi, summary_address_count,
            summary_addressed_location, summary_geocoded_location,
            summary_network_link_count, summary_affiliation_link_count,
            active, effective_start, effective_end, observed_at,
            profile_evidence_json
        ) VALUES (
            :physical_projection_id, :resource_type, :resource_id,
            :proof_partition_id, :payload_hash,
            jsonb_build_object('id', CAST(:payload_resource_id AS text)),
            'fixture-source-rank', NULL, 0, false, false, 0, 0,
            true, NULL, NULL, NULL, NULL
        );
        """,
        physical_projection_id=physical_projection_id,
        resource_type=resource_type,
        resource_id=resource_id,
        payload_resource_id=resource_id,
        proof_partition_id=raw_shard["partition_id"],
        payload_hash=_digest("fixture-stage-payload"),
    )
    await database.status(
        f"""
        CREATE UNIQUE INDEX "{relation}_key"
            ON "{schema}"."{relation}" (
                physical_projection_id, resource_type, resource_id
            );
        """
    )
    await database.status(
        f"""
        CREATE INDEX "{relation}_identity"
            ON "{schema}"."{relation}" (
                resource_type, resource_id, physical_projection_id
            );
        """
    )
    relation_oid = int(
        await database.scalar(
            "SELECT to_regclass(:qualified_relation)::oid::bigint;",
            qualified_relation=f"{schema}.{relation}",
        )
    )
    stage = _StageBinding(schema, relation, relation_oid, None)
    if not install_trigger:
        return stage
    return await _install_projection_stage_trigger(database, stage)


async def _install_projection_stage_trigger(
    database,
    stage: _StageBinding,
) -> _StageBinding:
    await database.status(
        f"""
        CREATE TRIGGER provider_directory_projection_stage_immutable
            BEFORE INSERT OR UPDATE OR DELETE
            ON "{stage.schema}"."{stage.relation}"
            FOR EACH ROW EXECUTE FUNCTION
                "{stage.schema}".
                reject_provider_directory_projection_stage_mutation();
        """
    )
    trigger_oid = await database.scalar(
        """
        SELECT trigger_record.oid::bigint
          FROM pg_trigger AS trigger_record
         WHERE trigger_record.tgrelid = CAST(:relation_oid AS oid)
           AND trigger_record.tgname =
               'provider_directory_projection_stage_immutable'
           AND NOT trigger_record.tgisinternal;
        """,
        relation_oid=stage.relation_oid,
    )
    assert trigger_oid is not None
    return replace(stage, trigger_oid=int(trigger_oid))


async def _bind_projection_stage(database, schema: str, lease, stage) -> None:
    async with database.transaction():
        await set_local_projection_action(
            database,
            "stage_bind",
            recipe_id=lease.recipe.recipe_id,
            recipe_attempt=lease.attempt,
            recipe_lease_token=lease.lease_token,
        )
        await database.status(
            f"""
            UPDATE "{schema}".provider_directory_projection_recipe
               SET stage_schema = :stage_schema,
                   stage_relation = :stage_relation,
                   stage_relation_oid = :stage_relation_oid,
                   updated_at = clock_timestamp()
             WHERE recipe_id = :recipe_id;
            """,
            stage_schema=stage.schema,
            stage_relation=stage.relation,
            stage_relation_oid=stage.relation_oid,
            recipe_id=lease.recipe.recipe_id,
        )


async def _mark_projection_proof_ready(
    database,
    schema: str,
    lease,
    projection_proof: Mapping[str, object],
) -> None:
    async with database.transaction():
        await set_local_projection_action(
            database,
            "proof_ready",
            recipe_id=lease.recipe.recipe_id,
            recipe_attempt=lease.attempt,
            recipe_lease_token=lease.lease_token,
        )
        await database.status(
            f"""
            UPDATE "{schema}".provider_directory_projection_recipe
               SET status = 'proof_ready',
                   prepared_proof_json = CAST(:prepared_proof_json AS jsonb),
                   updated_at = clock_timestamp()
             WHERE recipe_id = :recipe_id;
            """,
            prepared_proof_json=stable_json(projection_proof),
            recipe_id=lease.recipe.recipe_id,
        )


def _partition_records(projection_proof: Mapping[str, object]) -> list[dict]:
    return [
        {
            "proof_partition_id": raw_shard["partition_id"],
            "partition_ordinal": raw_shard["partition_ordinal"],
            "resource_type": raw_shard["resource_type"],
            "canonical_row_sha256": raw_shard["canonical_row_sha256"],
            "resource_count": raw_shard["resource_count"],
            "proof_json": copy.deepcopy(raw_shard),
        }
        for raw_shard in projection_proof["raw_shards"]
    ]


async def _seal_physical_projection(
    database,
    schema: str,
    lease,
    stage: _StageBinding,
    projection_proof: Mapping[str, object],
    partition_records: Sequence[Mapping[str, object]],
    *,
    retain_seconds: int,
    attach_stage: bool = True,
    set_action: bool = True,
) -> str:
    """Seal one fixture projection through the exact guarded write order."""

    physical_projection_id = lease.recipe.recipe_id
    assert stage.trigger_oid is not None
    source_summary = projection_proof["source_summary"]
    async with database.transaction():
        if set_action:
            await set_local_projection_action(
                database,
                "seal",
                recipe_id=lease.recipe.recipe_id,
                recipe_attempt=lease.attempt,
                recipe_lease_token=lease.lease_token,
                physical_projection_id=physical_projection_id,
            )
        await database.status(
            f"""
            INSERT INTO "{schema}".provider_directory_physical_projection (
                physical_projection_id, canonical_row_sha256,
                content_hash_contract_id, decoder_contract_id, input_set_sha256,
                transform_contract_id, scope_contract_id, transform_context_hash,
                transform_context_json, dataset_hash, resource_profile_hash,
                selected_resources_json, required_resources_json, resource_count,
                resource_counts_json, proof_json, storage_schema, storage_relation,
                storage_relation_oid, storage_trigger_oid, status, created_at,
                sealed_at, retain_until
            ) VALUES (
                :physical_projection_id, :canonical_row_sha256,
                'fixture-content-hash.v1', :decoder_contract_id,
                :input_set_sha256, :transform_contract_id, :scope_contract_id,
                :transform_context_hash, CAST(:transform_context_json AS jsonb),
                :dataset_hash, :resource_profile_hash,
                CAST(:selected_resources_json AS jsonb),
                CAST(:required_resources_json AS jsonb), :resource_count,
                CAST(:resource_counts_json AS jsonb), CAST(:proof_json AS jsonb),
                :storage_schema, :storage_relation, :storage_relation_oid,
                :storage_trigger_oid, 'sealed', now(), now(),
                now() + CAST(:retain_seconds AS integer) * interval '1 second'
            );
            """,
            physical_projection_id=physical_projection_id,
            canonical_row_sha256=projection_proof["canonical_row_sha256"],
            decoder_contract_id=lease.recipe.decoder_contract_id,
            input_set_sha256=lease.recipe.input_set_sha256,
            transform_contract_id=lease.recipe.transform_contract_id,
            scope_contract_id=lease.recipe.scope_contract_id,
            transform_context_hash=lease.recipe.transform_context_hash,
            transform_context_json=stable_json(lease.recipe.transform_context),
            dataset_hash=projection_proof["dataset_hash"],
            resource_profile_hash=lease.recipe.resource_profile_hash,
            selected_resources_json=stable_json(lease.recipe.selected_resources),
            required_resources_json=stable_json(lease.recipe.required_resources),
            resource_count=projection_proof["resource_count"],
            resource_counts_json=stable_json(projection_proof["resource_counts"]),
            proof_json=stable_json(projection_proof),
            storage_schema=stage.schema,
            storage_relation=stage.relation,
            storage_relation_oid=stage.relation_oid,
            storage_trigger_oid=stage.trigger_oid,
            retain_seconds=retain_seconds,
        )
        await database.status(
            f"""
            INSERT INTO
                "{schema}".provider_directory_physical_projection_source_summary (
                physical_projection_id, canonical_row_sha256, dataset_hash,
                resource_count, resource_counts_json, proof_json, created_at
            ) VALUES (
                :physical_projection_id, :canonical_row_sha256, :dataset_hash,
                :resource_count, CAST(:resource_counts_json AS jsonb),
                CAST(:proof_json AS jsonb), now()
            );
            """,
            physical_projection_id=physical_projection_id,
            canonical_row_sha256=source_summary["canonical_row_sha256"],
            dataset_hash=source_summary["dataset_hash"],
            resource_count=source_summary["resource_count"],
            resource_counts_json=stable_json(source_summary["resource_counts"]),
            proof_json=stable_json(source_summary),
        )
        for partition_record in partition_records:
            await database.status(
                f"""
                INSERT INTO
                    "{schema}".provider_directory_physical_projection_partition (
                    physical_projection_id, proof_partition_id, partition_ordinal,
                    resource_type, canonical_row_sha256, resource_count,
                    proof_json, created_at
                ) VALUES (
                    :physical_projection_id, :proof_partition_id,
                    :partition_ordinal, :resource_type,
                    :canonical_row_sha256, :resource_count,
                    CAST(:proof_json AS jsonb), now()
                );
                """,
                physical_projection_id=physical_projection_id,
                proof_partition_id=partition_record["proof_partition_id"],
                partition_ordinal=partition_record["partition_ordinal"],
                resource_type=partition_record["resource_type"],
                canonical_row_sha256=partition_record["canonical_row_sha256"],
                resource_count=partition_record["resource_count"],
                proof_json=stable_json(partition_record["proof_json"]),
            )
        if attach_stage:
            await database.status(
                f"""
                ALTER TABLE
                    "{schema}".provider_directory_physical_projection_resource
                ATTACH PARTITION "{stage.schema}"."{stage.relation}"
                FOR VALUES IN ('{physical_projection_id}');
                """
            )
        await database.status(
            f"""
            UPDATE "{schema}".provider_directory_projection_recipe
               SET status = 'sealed',
                   physical_projection_id = :physical_projection_id,
                   lease_token = NULL,
                   lease_expires_at = NULL,
                   lease_heartbeat_at = NULL,
                   sealed_at = now(),
                   updated_at = clock_timestamp()
             WHERE recipe_id = :recipe_id;
            """,
            physical_projection_id=physical_projection_id,
            recipe_id=lease.recipe.recipe_id,
        )
    return physical_projection_id


@pytest.mark.asyncio
async def test_projection_migration_adopts_in_place_and_downgrades_empty(
    monkeypatch,
) -> None:
    migration_module = _migration_module()
    assert migration_module.revision == (
        "20260721170000_provider_directory_physical_projection"
    )
    assert migration_module.down_revision == (
        "20260721160000_provider_directory_retained_artifact_acquisition"
    )

    async with projection_foundation_postgres(monkeypatch) as postgres:
        predecessor_catalog = await postgres.catalog_snapshot()
        assert all(
            predecessor_catalog[catalog_kind]
            for catalog_kind in predecessor_catalog
        )
        await postgres.upgrade(schema_on_search_path=True)
        first_oids = await postgres.relation_oids()
        first_catalog = await postgres.catalog_snapshot()
        assert set(first_oids) == set(PROJECTION_RELATIONS)
        assert all(relation_oid is not None for relation_oid in first_oids.values())
        assert all(first_catalog[catalog_kind] for catalog_kind in first_catalog)

        await postgres.upgrade(schema_on_search_path=True)
        assert await postgres.relation_oids() == first_oids
        assert await postgres.catalog_snapshot() == first_catalog

        await postgres.downgrade()
        assert all(
            relation_oid is None
            for relation_oid in (await postgres.relation_oids()).values()
        )
        assert await postgres.catalog_snapshot() == predecessor_catalog


@pytest.mark.asyncio
async def test_source_neutral_recipe_reuses_immutable_campaign_admissions(
    monkeypatch,
) -> None:
    """Bind independent campaigns to one source-neutral physical workset."""

    async with projection_foundation_postgres(monkeypatch) as postgres:
        await postgres.upgrade()
        database = postgres.database
        schema = postgres.schema
        (
            lease,
            original_binding,
            input_block,
            _shard,
            original_admission_id,
        ) = await _registered_projection_context(
            postgres,
            "source-neutral-original-campaign",
        )
        assert lease is not None
        core_columns = {
                column_record.column_name
                for column_record in await database.all(
                """
                SELECT column_name
                  FROM information_schema.columns
                 WHERE table_schema = :schema
                   AND table_name =
                       'provider_directory_projection_input_block';
                """,
                schema=schema,
            )
        }
        assert not {
            "retained_campaign_id",
            "retained_campaign_sha256",
            "retained_source_item_id",
            "retained_range_ordinal",
        } & core_columns

        with pytest.raises(
            DBAPIError,
            match="provider_directory_projection_admission_identity_immutable",
        ):
            await database.status(
                f"""
                UPDATE "{schema}".provider_directory_projection_admission
                   SET retained_campaign_sha256 = :forged_hash
                 WHERE admission_id = :admission_id;
                """,
                forged_hash=_digest("forged-campaign-hash"),
                admission_id=original_admission_id,
            )
        with pytest.raises(
            DBAPIError,
            match="provider_directory_projection_admission_block_immutable",
        ):
            await database.status(
                f"""
                UPDATE "{schema}".
                       provider_directory_projection_admission_input_block
                   SET retained_source_item_id = :forged_item
                 WHERE admission_id = :admission_id;
                """,
                forged_item=_digest("forged-source-item"),
                admission_id=original_admission_id,
            )

        (
            rebound_binding,
            rebound_block,
            _rebound_shard,
            rebound_admission_id,
        ) = await _register_additional_projection_admission(
            postgres,
            lease.recipe,
            "source-neutral-rebound-campaign",
        )
        assert rebound_admission_id != original_admission_id
        assert rebound_block.block.block_id == input_block.block.block_id
        assert rebound_binding.campaign_id != original_binding.campaign_id
        assert await database.scalar(
            f'SELECT count(*) FROM "{schema}".'
            'provider_directory_projection_recipe;'
        ) == 1
        assert await database.scalar(
            f"""
            SELECT count(*)
              FROM "{schema}".provider_directory_projection_admission
             WHERE recipe_id = :recipe_id AND status = 'sealed';
            """,
            recipe_id=lease.recipe.recipe_id,
        ) == 2
        with pytest.raises(
            ProviderDirectoryProjectionError,
            match="provider_directory_projection_admission_census_mismatch",
        ):
            await _prepare_additional_projection_admission(
                postgres,
                lease.recipe,
                "source-neutral-bad-census-campaign",
                completeness_row_delta=1,
            )


@pytest.mark.asyncio
async def test_admission_rejects_an_omitted_second_retained_item(
    monkeypatch,
) -> None:
    """A self-consistent subset cannot seal over a larger claimed campaign."""

    async with projection_foundation_postgres(monkeypatch) as postgres:
        await postgres.upgrade()

        def subset_consumer_id(bindings, produced_artifact):
            recipe, admission_blocks, _shard = _shared_block_fixture(
                bindings[:1],
                produced_artifact,
            )
            return projection_admission_consumer_id(recipe, admission_blocks)

        bindings, produced_artifact = await _sealed_shared_artifact_bindings(
            postgres,
            campaign_label="reverse-scope-omission",
            item_count=2,
            consumer_recipe_id=subset_consumer_id,
        )
        recipe, admission_blocks, shard = _shared_block_fixture(
            bindings[:1],
            produced_artifact,
        )
        projection_claim = await claim_projection_recipe(
            recipe,
            database=postgres.database,
            schema=postgres.schema,
        )
        assert projection_claim.lease is not None
        await register_projection_workset(
            projection_claim.lease,
            (admission_blocks[0].block,),
            (shard,),
            database=postgres.database,
            schema=postgres.schema,
        )
        with pytest.raises(
            DBAPIError,
            match="provider_directory_projection_admission_transition_invalid",
        ):
            await register_projection_admission(
                recipe,
                admission_blocks,
                (shard,),
                claim_generation=bindings[0].consumer_claim_generation,
                database=postgres.database,
                schema=postgres.schema,
            )


@pytest.mark.asyncio
async def test_admission_maps_two_items_to_one_content_block(monkeypatch) -> None:
    """Logical item coverage remains many-to-one over physical content."""

    async with projection_foundation_postgres(monkeypatch) as postgres:
        await postgres.upgrade()

        def shared_consumer_id(bindings, produced_artifact):
            recipe, admission_blocks, _shard = _shared_block_fixture(
                bindings,
                produced_artifact,
            )
            return projection_admission_consumer_id(recipe, admission_blocks)

        bindings, produced_artifact = await _sealed_shared_artifact_bindings(
            postgres,
            campaign_label="shared-content-success",
            item_count=2,
            consumer_recipe_id=shared_consumer_id,
        )
        recipe, admission_blocks, shard = _shared_block_fixture(
            bindings,
            produced_artifact,
        )
        assert admission_blocks[0].block.block_id == admission_blocks[1].block.block_id
        projection_claim = await claim_projection_recipe(
            recipe,
            database=postgres.database,
            schema=postgres.schema,
        )
        assert projection_claim.lease is not None
        await register_projection_workset(
            projection_claim.lease,
            (admission_blocks[0].block,),
            (shard,),
            database=postgres.database,
            schema=postgres.schema,
        )
        admission_id = await register_projection_admission(
            recipe,
            admission_blocks,
            (shard,),
            claim_generation=bindings[0].consumer_claim_generation,
            database=postgres.database,
            schema=postgres.schema,
        )
        stored_counts = await postgres.database.first(
            f"""
            SELECT admission.input_block_count, admission.binding_count,
                   admission.stream_count,
                   count(mapping.binding_id)::integer AS mapping_count,
                   count(DISTINCT mapping.block_id)::integer AS core_count
              FROM "{postgres.schema}".provider_directory_projection_admission
                   AS admission
              JOIN "{postgres.schema}".
                   provider_directory_projection_admission_input_block AS mapping
                ON mapping.admission_id = admission.admission_id
             WHERE admission.admission_id = :admission_id
             GROUP BY admission.input_block_count, admission.binding_count,
                      admission.stream_count;
            """,
            admission_id=admission_id,
        )
        assert tuple(stored_counts) == (1, 2, 0, 2, 1)


@pytest.mark.asyncio
async def test_admission_rejects_a_missing_layout_range(monkeypatch) -> None:
    """One valid mapped range cannot stand in for an entire retained layout."""

    async with projection_foundation_postgres(monkeypatch) as postgres:
        await postgres.upgrade()
        produced_artifact = _two_range_artifact(
            "missing-layout-range",
            campaign_item("missing-layout-range").artifact_kind,
        )
        assert len(produced_artifact.ranges) == 2

        def partial_range_consumer_id(bindings, shared_artifact):
            recipe, admission_blocks, _shards = _range_subset_fixture(
                bindings[0],
                shared_artifact,
                (0,),
            )
            return projection_admission_consumer_id(recipe, admission_blocks)

        bindings, produced_artifact = await _sealed_shared_artifact_bindings(
            postgres,
            campaign_label="missing-layout-range",
            item_count=1,
            consumer_recipe_id=partial_range_consumer_id,
            produced_artifact=produced_artifact,
        )
        assert len(produced_artifact.ranges) == 2
        recipe, admission_blocks, shards = _range_subset_fixture(
            bindings[0],
            produced_artifact,
            (0,),
        )
        assert await postgres.database.scalar(
            f"""
            SELECT layout_range_count
              FROM "{postgres.schema}".provider_directory_retained_artifact_layout
             WHERE layout_sha256 = :layout_sha256;
            """,
            layout_sha256=produced_layout_digest(produced_artifact),
        ) == 2
        projection_claim = await claim_projection_recipe(
            recipe,
            database=postgres.database,
            schema=postgres.schema,
        )
        assert projection_claim.lease is not None
        await register_projection_workset(
            projection_claim.lease,
            tuple(binding.block for binding in admission_blocks),
            shards,
            database=postgres.database,
            schema=postgres.schema,
        )
        with pytest.raises(
            DBAPIError,
            match="provider_directory_projection_admission_transition_invalid",
        ):
            await register_projection_admission(
                recipe,
                admission_blocks,
                shards,
                claim_generation=bindings[0].consumer_claim_generation,
                database=postgres.database,
                schema=postgres.schema,
            )


@pytest.mark.asyncio
async def test_admission_seals_zero_stream_and_rejects_terminal_omission(
    monkeypatch,
) -> None:
    """Zero-only ordered streams require their exact retained terminal record."""

    async with projection_foundation_postgres(monkeypatch) as postgres:
        await postgres.upgrade()
        context = await _ordered_zero_stream_context(
            postgres,
            "ordered-zero-stream",
        )
        await _register_ordered_workset(postgres, context)
        insert_streams = projection_admission_store._insert_admission_streams

        async def insert_incomplete_streams(database, schema, streams):
            await insert_streams(database, schema, streams[:1])

        monkeypatch.setattr(
            projection_admission_store,
            "_insert_admission_streams",
            insert_incomplete_streams,
        )
        with pytest.raises(
            DBAPIError,
            match="provider_directory_projection_admission_transition_invalid",
        ):
            await _register_ordered_admission(postgres, context)
        monkeypatch.setattr(
            projection_admission_store,
            "_insert_admission_streams",
            insert_streams,
        )
        admission_id = await _register_ordered_admission(postgres, context)
        stored_counts = await postgres.database.first(
            f"""
            SELECT admission.binding_count, admission.stream_count,
                   count(stream.binding_id)::integer AS persisted_streams,
                   count(*) FILTER (
                       WHERE stream.source_partition_ordinal = 1
                   )::integer AS zero_streams
              FROM "{postgres.schema}".provider_directory_projection_admission
                   AS admission
              JOIN "{postgres.schema}".
                   provider_directory_projection_admission_stream AS stream
                ON stream.admission_id = admission.admission_id
             WHERE admission.admission_id = :admission_id
             GROUP BY admission.binding_count, admission.stream_count;
            """,
            admission_id=admission_id,
        )
        assert tuple(stored_counts) == (3, 2, 2, 1)


@pytest.mark.asyncio
async def test_ordered_payload_mapping_rejects_forged_stream_coordinates(
    monkeypatch,
) -> None:
    """SQL rechecks stream identity and sequence after binding recomputation."""

    async with projection_foundation_postgres(monkeypatch) as postgres:
        await postgres.upgrade()
        context = await _ordered_zero_stream_context(
            postgres,
            "ordered-swapped-payload",
        )
        await _register_ordered_workset(postgres, context)
        insert_mappings = projection_admission_store._insert_admission_mappings
        for forged_binding in _forged_ordered_payloads(context):
            await _assert_forged_mapping_rejected(
                postgres,
                monkeypatch,
                context,
                forged_binding,
                insert_mappings,
            )
        monkeypatch.setattr(
            projection_admission_store,
            "_insert_admission_mappings",
            insert_mappings,
        )
        admission_id = await _register_ordered_admission(postgres, context)
        assert await _stored_payload_coordinate(postgres, admission_id) == (
            context.admission_block.stream_identity_sha256,
            context.admission_block.sequence_ordinal,
        )


@pytest.mark.asyncio
async def test_all_zero_ordered_admission_seals_without_physical_recipe(
    monkeypatch,
) -> None:
    """An exact all-zero campaign seals before and excludes recipe claiming."""

    async with projection_foundation_postgres(monkeypatch) as postgres:
        await postgres.upgrade()
        context = await _all_zero_ordered_context(postgres, "all-zero-ordered")
        identity = projection_admission_identity(
            context.recipe,
            (),
            claim_generation=context.claim_generation,
            terminal_zeros=context.terminal_zeros,
        )
        assert identity.outcome_kind == "no_input"
        assert identity.recipe_id is None
        await _assert_mixed_no_input_descriptor_rejected(
            postgres,
            monkeypatch,
            context,
            identity,
        )
        await _assert_incomplete_no_input_streams_rejected(
            postgres,
            monkeypatch,
            context,
        )
        admission_id = await _register_all_zero_admission(postgres, context)
        await _assert_stored_no_input_outcome(postgres, context, admission_id)
        await _assert_no_input_recipe_guards(postgres, context)


@pytest.mark.asyncio
async def test_no_input_admission_rejects_retained_positive_payload(
    monkeypatch,
) -> None:
    """A zero manifest cannot hide an admitted payload item in its campaign."""

    async with projection_foundation_postgres(monkeypatch) as postgres:
        await postgres.upgrade()
        context = await _ordered_zero_stream_context(
            postgres,
            "zero-manifest-positive-payload",
        )
        terminal_zeros = context.terminal_zeros
        recipe = _forged_all_zero_recipe(terminal_zeros)
        consumer_id = projection_admission_consumer_id(
            recipe,
            (),
            terminal_zeros=terminal_zeros,
        )
        claimed_campaign = await claim_sealed_retained_campaign(
            postgres.retained_connection,
            campaign_id=terminal_zeros[0].retained_campaign_id,
            consumer_recipe_id=consumer_id,
        )

        with pytest.raises(
            DBAPIError,
            match="provider_directory_projection_admission_transition_invalid",
        ):
            await register_projection_admission(
                recipe,
                (),
                (),
                claim_generation=claimed_campaign.consumer_claim_generation,
                terminal_zeros=terminal_zeros,
                database=postgres.database,
                schema=postgres.schema,
            )
        assert await postgres.database.scalar(
            f"""
            SELECT count(*)
              FROM "{postgres.schema}".
                   provider_directory_projection_admission
             WHERE planned_recipe_id = :planned_recipe_id;
            """,
            planned_recipe_id=recipe.recipe_id,
        ) == 0


@pytest.mark.asyncio
async def test_failed_recipe_requires_classification_and_quarantines_work(
    monkeypatch,
) -> None:
    """Failed native work cannot be retried without a classified policy."""

    async with projection_foundation_postgres(monkeypatch) as postgres:
        await postgres.upgrade()
        database = postgres.database
        schema = postgres.schema
        primary_context = await _registered_projection_context(
            postgres,
            "retry-primary-campaign",
        )
        sibling_context = await _registered_projection_context(
            postgres,
            "retry-sibling-campaign",
            artifact_label="projection-sibling-artifact",
        )
        primary_lease = primary_context[0]
        sibling_lease = sibling_context[0]
        assert primary_lease is not None and sibling_lease is not None
        assert primary_lease.recipe.recipe_id != sibling_lease.recipe.recipe_id

        completed_shard = await _complete_registered_projection_shard(
            database,
            schema,
            primary_lease,
        )
        proof = _physical_projection_proof(
            primary_lease.recipe.recipe_id,
            completed_shard,
        )
        stage = await _create_projection_stage(
            database,
            schema,
            label="retry-primary-stage",
            physical_projection_id=primary_lease.recipe.recipe_id,
            raw_shard=completed_shard,
            install_trigger=True,
        )
        await _bind_projection_stage(
            database,
            schema,
            primary_lease,
            stage,
        )
        before_retry = await database.first(
            f"""
            SELECT recipe.attempt, recipe.stage_relation_oid,
                   recipe.input_block_count, recipe.partition_count,
                   proof_shard.partition_id, proof_shard.status,
                   proof_shard.canonical_row_sha256
              FROM "{schema}".provider_directory_projection_recipe AS recipe
              JOIN "{schema}".provider_directory_projection_proof_shard
                   AS proof_shard
                ON proof_shard.recipe_id = recipe.recipe_id
               AND proof_shard.attempt = recipe.attempt
             WHERE recipe.recipe_id = :recipe_id;
            """,
            recipe_id=primary_lease.recipe.recipe_id,
        )
        sibling_before = await database.first(
            f"""
            SELECT attempt, status, stage_relation_oid,
                   input_block_count, partition_count, lease_token
              FROM "{schema}".provider_directory_projection_recipe
             WHERE recipe_id = :recipe_id;
            """,
            recipe_id=sibling_lease.recipe.recipe_id,
        )
        async with database.transaction():
            await set_local_projection_action(
                database,
                "fail",
                recipe_id=primary_lease.recipe.recipe_id,
                recipe_attempt=primary_lease.attempt,
                recipe_lease_token=primary_lease.lease_token,
            )
            await database.status(
                f"""
                UPDATE "{schema}".provider_directory_projection_recipe
                   SET status = 'failed', lease_token = NULL,
                       lease_expires_at = NULL, lease_heartbeat_at = NULL,
                       updated_at = now()
                 WHERE recipe_id = :recipe_id;
                """,
                recipe_id=primary_lease.recipe.recipe_id,
            )
        with pytest.raises(
            ProviderDirectoryProjectionError,
            match="provider_directory_projection_failure_classification_required",
        ):
            await claim_projection_recipe(
                primary_lease.recipe,
                database=database,
                schema=schema,
            )
        with pytest.raises(
            DBAPIError,
            match="provider_directory_projection_failure_classification_required",
        ):
            async with database.transaction():
                replacement_token = _digest("unclassified-retry-token")
                for setting_name, setting_value in (
                    ("action", "recipe_retry"),
                    ("recipe_id", primary_lease.recipe.recipe_id),
                    ("recipe_attempt", str(primary_lease.attempt)),
                    ("recipe_lease_token", replacement_token),
                ):
                    assert await database.scalar(
                        "SELECT set_config("
                        "'healthporta.provider_directory_projection_"
                        f"{setting_name}', :setting_value, true);",
                        setting_value=setting_value,
                    ) == setting_value
                await database.status(
                    f"""
                    UPDATE "{schema}".provider_directory_projection_recipe
                       SET status = 'building', lease_token = :lease_token,
                           lease_expires_at = now() + interval '15 minutes',
                           lease_heartbeat_at = now(), updated_at = now()
                     WHERE recipe_id = :recipe_id;
                    """,
                    lease_token=replacement_token,
                    recipe_id=primary_lease.recipe.recipe_id,
                )
        after_failure = await database.first(
            f"""
            SELECT recipe.attempt, recipe.stage_relation_oid,
                   recipe.input_block_count, recipe.partition_count,
                   proof_shard.partition_id, proof_shard.status,
                   proof_shard.canonical_row_sha256
              FROM "{schema}".provider_directory_projection_recipe AS recipe
              JOIN "{schema}".provider_directory_projection_proof_shard
                   AS proof_shard
                ON proof_shard.recipe_id = recipe.recipe_id
               AND proof_shard.attempt = recipe.attempt
             WHERE recipe.recipe_id = :recipe_id;
            """,
            recipe_id=primary_lease.recipe.recipe_id,
        )
        assert tuple(after_failure) == tuple(before_retry)
        assert proof["raw_shards"][0]["partition_id"] == after_failure.partition_id
        assert await database.scalar(
            f"""
            SELECT status
              FROM "{schema}".provider_directory_projection_recipe
             WHERE recipe_id = :recipe_id;
            """,
            recipe_id=primary_lease.recipe.recipe_id,
        ) == "failed"
        sibling_after = await database.first(
            f"""
            SELECT attempt, status, stage_relation_oid,
                   input_block_count, partition_count, lease_token
              FROM "{schema}".provider_directory_projection_recipe
             WHERE recipe_id = :recipe_id;
            """,
            recipe_id=sibling_lease.recipe.recipe_id,
        )
        assert tuple(sibling_after) == tuple(sibling_before)


@pytest.mark.asyncio
async def test_projection_migration_rejects_bypassed_trigger_adoption(
    monkeypatch,
) -> None:
    async with projection_foundation_postgres(monkeypatch) as postgres:
        await postgres.upgrade(schema_on_search_path=True)
        schema = postgres.schema
        await postgres.database.status(
            f"""
            DROP TRIGGER provider_directory_projection_input_block_guard
                ON "{schema}".provider_directory_projection_input_block;
            """
        )
        await postgres.database.status(
            f"""
            CREATE TRIGGER provider_directory_projection_input_block_guard
                BEFORE UPDATE OR DELETE
                ON "{schema}".provider_directory_projection_input_block
                FOR EACH ROW WHEN (false)
                EXECUTE FUNCTION
                    "{schema}".guard_provider_directory_projection_input_block();
            """
        )

        with pytest.raises(
            RuntimeError,
            match="existing_schema_trigger_mismatch",
        ):
            await postgres.upgrade(schema_on_search_path=True)


@pytest.mark.asyncio
async def test_projection_migration_recreates_a_missing_trigger(
    monkeypatch,
) -> None:
    async with projection_foundation_postgres(monkeypatch) as postgres:
        await postgres.upgrade(schema_on_search_path=True)
        schema = postgres.schema
        trigger_name = "provider_directory_projection_source_summary_guard"
        table_name = "provider_directory_physical_projection_source_summary"
        trigger_key = f"{table_name}.{trigger_name}"
        original_trigger_oid = (await postgres.catalog_snapshot())["triggers"][
            trigger_key
        ][0]
        await postgres.database.status(
            f"""
            DROP TRIGGER {trigger_name}
                ON "{schema}".{table_name};
            """
        )

        await postgres.upgrade(schema_on_search_path=True)

        repaired_trigger_oid = (await postgres.catalog_snapshot())["triggers"][
            trigger_key
        ][0]
        assert repaired_trigger_oid != original_trigger_oid


@pytest.mark.asyncio
async def test_projection_downgrade_refuses_live_foundation_state(monkeypatch) -> None:
    async with projection_foundation_postgres(monkeypatch) as postgres:
        await postgres.upgrade()
        await _registered_projection_lease(
            postgres,
            "projection-downgrade-campaign",
        )

        with pytest.raises(
            DBAPIError,
            match="provider_directory_projection_downgrade_has_live_state",
        ):
            await postgres.downgrade()


@pytest.mark.asyncio
async def test_stage_bind_requires_real_identity_and_freezes_after_first_bind(
    monkeypatch,
) -> None:
    """Bind only an exact physical stage and freeze its first catalog identity."""

    async with projection_foundation_postgres(monkeypatch) as postgres:
        await postgres.upgrade()
        database = postgres.database
        schema = postgres.schema
        lease = await _registered_projection_lease(
            postgres,
            "stage-bind-campaign",
        )
        completed_shard = await _complete_registered_projection_shard(
            database,
            schema,
            lease,
        )
        projection_proof = _physical_projection_proof(
            lease.recipe.recipe_id,
            completed_shard,
        )

        nonexistent_stage = _StageBinding(
            schema,
            "missing_projection_stage",
            2_147_483_647,
            None,
        )
        with pytest.raises(
            DBAPIError,
            match="provider_directory_projection_recipe_transition_invalid",
        ):
            await _bind_projection_stage(
                database,
                schema,
                lease,
                nonexistent_stage,
            )

        arbitrary_relation = f"arbitrary_stage_{_digest('one-column')[:12]}"
        await database.status(
            f'CREATE TABLE "{schema}"."{arbitrary_relation}" '
            "(stage_row_id bigint);"
        )
        arbitrary_oid = int(
            await database.scalar(
                "SELECT to_regclass(:qualified_relation)::oid::bigint;",
                qualified_relation=f"{schema}.{arbitrary_relation}",
            )
        )
        with pytest.raises(
            DBAPIError,
            match="provider_directory_projection_recipe_transition_invalid",
        ):
            await _bind_projection_stage(
                database,
                schema,
                lease,
                _StageBinding(
                    schema,
                    arbitrary_relation,
                    arbitrary_oid,
                    None,
                ),
            )

        stage = await _create_projection_stage(
            database,
            schema,
            label="bind-once-stage",
            physical_projection_id=lease.recipe.recipe_id,
            raw_shard=completed_shard,
            install_trigger=False,
        )
        await _bind_projection_stage(database, schema, lease, stage)
        await _bind_projection_stage(database, schema, lease, stage)

        replacement_stage = await _create_projection_stage(
            database,
            schema,
            label="replacement-stage",
            physical_projection_id=lease.recipe.recipe_id,
            raw_shard=completed_shard,
            install_trigger=False,
        )
        with pytest.raises(
            DBAPIError,
            match="provider_directory_projection_recipe_transition_invalid",
        ):
            await _bind_projection_stage(
                database,
                schema,
                lease,
                replacement_stage,
            )

        with pytest.raises(
            ProviderDirectoryProjectionError,
            match="provider_directory_projection_action_invalid",
        ):
            await _mark_projection_proof_ready(
                database,
                schema,
                lease,
                projection_proof,
            )

        immutable_stage = await _install_projection_stage_trigger(database, stage)
        with pytest.raises(
            DBAPIError,
            match="provider_directory_projection_native_attestation_required",
        ):
            async with database.transaction():
                await database.status(
                    "SET LOCAL healthporta.provider_directory_projection_action "
                    "= 'proof_ready';"
                )
                await database.status(
                    f"""
                    UPDATE "{schema}".provider_directory_projection_recipe
                       SET status = 'proof_ready',
                           prepared_proof_json =
                               CAST(:prepared_proof_json AS jsonb),
                           updated_at = clock_timestamp()
                     WHERE recipe_id = :recipe_id;
                    """,
                    prepared_proof_json=stable_json(projection_proof),
                    recipe_id=lease.recipe.recipe_id,
                )
        stored_stage = await database.first(
            f"""
            SELECT stage_schema, stage_relation, stage_relation_oid, status
              FROM "{schema}".provider_directory_projection_recipe
             WHERE recipe_id = :recipe_id;
            """,
            recipe_id=lease.recipe.recipe_id,
        )
        assert tuple(stored_stage) == (
            schema,
            immutable_stage.relation,
            immutable_stage.relation_oid,
            "building",
        )
        assert immutable_stage.trigger_oid == await database.scalar(
            """
            SELECT oid::bigint FROM pg_trigger
             WHERE tgrelid = CAST(:relation_oid AS oid)
               AND tgname = 'provider_directory_projection_stage_immutable'
               AND NOT tgisinternal;
            """,
            relation_oid=immutable_stage.relation_oid,
        )

@pytest.mark.asyncio
async def test_foundation_rejects_unattested_physical_storage(monkeypatch) -> None:
    async with projection_foundation_postgres(monkeypatch) as postgres:
        await postgres.upgrade()
        database = postgres.database
        schema = postgres.schema
        lease = await _registered_projection_lease(
            postgres,
            "unattested-physical-storage-campaign",
        )
        completed_shard = await _complete_registered_projection_shard(
            database,
            schema,
            lease,
        )
        projection_proof = _physical_projection_proof(
            lease.recipe.recipe_id,
            completed_shard,
        )
        stage = await _create_projection_stage(
            database,
            schema,
            label="unattested-physical-storage",
            physical_projection_id=lease.recipe.recipe_id,
            raw_shard=completed_shard,
            install_trigger=True,
        )
        await _bind_projection_stage(database, schema, lease, stage)
        with pytest.raises(
            DBAPIError,
            match="provider_directory_projection_native_attestation_required",
        ):
            await _seal_physical_projection(
                database,
                schema,
                lease,
                stage,
                projection_proof,
                _partition_records(projection_proof),
                retain_seconds=0,
                attach_stage=False,
                set_action=False,
            )
