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

from process.provider_directory_retained_artifact_contract import (
    produced_layout_digest,
)
from process.provider_directory_retained_catalog_store import (
    initialize_retained_artifact_campaign,
)
from process.provider_directory_retained_consumer_claim_store import (
    claim_sealed_retained_campaign,
    release_retained_campaign_consumer,
)
from process.provider_directory_retained_lease_store import acquire_campaign_lease
from process.provider_directory_retained_seal_store import (
    seal_retained_artifact_campaign,
)
from process.provider_directory_retained_store_support import database_table
from process.provider_directory_projection_db import set_local_projection_action
from process.provider_directory_projection_types import stable_json
from process.provider_directory_physical_projection import (
    ProjectionLease,
    ProviderDirectoryProjectionError,
    ProviderDirectoryProjectionLeaseLost,
    claim_projection_recipe,
    claim_projection_shard,
    heartbeat_projection_lease,
    heartbeat_projection_shard,
    load_projection_input_block,
    projection_completeness_manifest,
    projection_input_block,
    projection_input_set_sha256,
    projection_recipe_identity,
    projection_shard_spec,
    register_projection_workset,
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


def _projection_fixture(
    *,
    retained_binding: _RetainedBinding | None = None,
    artifact_label: str = "projection-shared-artifact",
):
    """Build one locator-free recipe, input block, and shard fixture."""

    produced_artifact = registry_artifact(artifact_label, "bulk_ndjson")
    layout_sha256 = produced_layout_digest(produced_artifact)
    retained_range = produced_artifact.ranges[0]
    completeness = projection_completeness_manifest(
        endpoint_campaign_hash=_digest("endpoint-campaign"),
        partition_strategy_contract_id="fixture-partitions.v1",
        selected_resources=("Organization",),
        required_resources=("Organization",),
        terminal_partitions=(
            {
                "resource_type": "Organization",
                "partition_key_hash": _digest("organization-partition"),
                "terminal_cursor_hmac": _digest("terminal-cursor"),
                "terminal": True,
                "block_count": 1,
                "row_count": retained_range.record_count,
                "byte_count": retained_range.raw_byte_count,
            },
        ),
        complete=True,
    )
    input_block = projection_input_block(
        block_ordinal=0,
        upstream_artifact_id=produced_artifact.artifact_sha256,
        source_object_id=layout_sha256,
        block_kind="ndjson",
        input_contract_id="fixture-input.v1",
        source_partition_ordinal=0,
        record_start=retained_range.record_start,
        record_count=retained_range.record_count,
        content_sha256=retained_range.canonical_sha256,
        payload_sha256=retained_range.raw_sha256,
        payload_bytes=retained_range.raw_byte_count,
        summary={"resource_count": retained_range.record_count},
        retained_campaign_id=(
            retained_binding.campaign_id
            if retained_binding
            else _digest("provisional-retained-campaign")
        ),
        retained_campaign_sha256=(
            retained_binding.campaign_sha256
            if retained_binding
            else _digest("provisional-retained-campaign-proof")
        ),
        retained_source_item_id=(
            retained_binding.source_item_id
            if retained_binding
            else _digest("provisional-retained-source-item")
        ),
        retained_range_ordinal=retained_range.range_ordinal,
    )
    input_set_sha256 = projection_input_set_sha256(
        (input_block,),
        decoder_contract_id="fixture-fhir-r4-decoder.v1",
        completeness_manifest=completeness,
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
        selected_resources=("Organization",),
        required_resources=("Organization",),
        completeness_manifest=completeness,
    )
    shard = projection_shard_spec(
        recipe=recipe,
        partition_ordinal=0,
        partition_key=_digest("organization-partition"),
        input_block=input_block,
        resource_type="Organization",
    )
    return recipe, input_block, shard


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


async def _sealed_retained_binding(
    postgres,
    *,
    campaign_label: str,
    artifact_label: str,
    consumer_recipe_id: str,
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
    )


async def _insert_projection_input_block(
    database,
    schema: str,
    recipe_id: str,
    input_block,
) -> None:
    await database.status(
        f"""
        INSERT INTO "{schema}".provider_directory_projection_input_block (
            recipe_id, block_id, block_ordinal, upstream_artifact_id,
            source_object_id, block_kind, input_contract_id,
            source_partition_ordinal, record_start, record_count,
            content_sha256, payload_sha256, payload_bytes, summary_json,
            retained_campaign_id, retained_campaign_sha256,
            retained_source_item_id, retained_range_ordinal,
            block_proof_sha256, created_at
        ) VALUES (
            :recipe_id, :block_id, :block_ordinal, :upstream_artifact_id,
            :source_object_id, :block_kind, :input_contract_id,
            :source_partition_ordinal, :record_start, :record_count,
            :content_sha256, :payload_sha256, :payload_bytes,
            CAST(:summary_json AS jsonb), :retained_campaign_id,
            :retained_campaign_sha256, :retained_source_item_id,
            :retained_range_ordinal, :block_proof_sha256, now()
        );
        """,
        recipe_id=recipe_id,
        block_id=input_block.block_id,
        block_ordinal=input_block.block_ordinal,
        upstream_artifact_id=input_block.upstream_artifact_id,
        source_object_id=input_block.source_object_id,
        block_kind=input_block.block_kind,
        input_contract_id=input_block.input_contract_id,
        source_partition_ordinal=input_block.source_partition_ordinal,
        record_start=input_block.record_start,
        record_count=input_block.record_count,
        content_sha256=input_block.content_sha256,
        payload_sha256=input_block.payload_sha256,
        payload_bytes=input_block.payload_bytes,
        summary_json='{"resource_count":1}',
        retained_campaign_id=input_block.retained_campaign_id,
        retained_campaign_sha256=input_block.retained_campaign_sha256,
        retained_source_item_id=input_block.retained_source_item_id,
        retained_range_ordinal=input_block.retained_range_ordinal,
        block_proof_sha256=input_block.block_proof_sha256,
    )


async def _registered_projection_lease(postgres, campaign_label: str):
    provisional_recipe, _, _ = _projection_fixture()
    retained_binding = await _sealed_retained_binding(
        postgres,
        campaign_label=campaign_label,
        artifact_label="projection-shared-artifact",
        consumer_recipe_id=provisional_recipe.recipe_id,
    )
    recipe, input_block, shard = _projection_fixture(
        retained_binding=retained_binding
    )
    projection_claim = await claim_projection_recipe(
        recipe,
        database=postgres.database,
        schema=postgres.schema,
    )
    assert projection_claim.lease is not None
    await register_projection_workset(
        projection_claim.lease,
        (input_block,),
        (shard,),
        database=postgres.database,
        schema=postgres.schema,
    )
    return projection_claim.lease


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
    resource_type = str(raw_shard["resource_type"])
    resource_count_by_type = {"Organization": 1}
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
    assert resource_type == "Organization"
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
    shard_claim = await claim_projection_shard(
        lease,
        database=database,
        schema=schema,
    )
    assert shard_claim is not None
    canonical_row_sha256 = _digest(
        f"completed-partition:{shard_claim.shard.partition_id}"
    )
    resource_type = shard_claim.shard.resource_type
    resource_identity_parts = [resource_type, "o-1"]
    resource_count_by_type = {
        selected_resource: int(selected_resource == resource_type)
        for selected_resource in lease.recipe.selected_resources
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
    resource_type = str(raw_shard["resource_type"])
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
            :physical_projection_id, :resource_type, 'o-1',
            :proof_partition_id, :payload_hash, '{{"id":"o-1"}}'::jsonb,
            'fixture-source-rank', NULL, 0, false, false, 0, 0,
            true, NULL, NULL, NULL, NULL
        );
        """,
        physical_projection_id=physical_projection_id,
        resource_type=resource_type,
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
) -> str:
    """Seal one fixture projection through the exact guarded write order."""

    physical_projection_id = lease.recipe.recipe_id
    assert stage.trigger_oid is not None
    source_summary = projection_proof["source_summary"]
    async with database.transaction():
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
                transform_context_json, completeness_manifest_hash,
                completeness_manifest_json, dataset_hash, resource_profile_hash,
                selected_resources_json, required_resources_json, resource_count,
                resource_counts_json, proof_json, storage_schema, storage_relation,
                storage_relation_oid, storage_trigger_oid, status, created_at,
                sealed_at, retain_until
            ) VALUES (
                :physical_projection_id, :canonical_row_sha256,
                'fixture-content-hash.v1', :decoder_contract_id,
                :input_set_sha256, :transform_contract_id, :scope_contract_id,
                :transform_context_hash, CAST(:transform_context_json AS jsonb),
                :completeness_manifest_hash,
                CAST(:completeness_manifest_json AS jsonb), :dataset_hash,
                :resource_profile_hash, CAST(:selected_resources_json AS jsonb),
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
            completeness_manifest_hash=lease.recipe.completeness_manifest_hash,
            completeness_manifest_json=stable_json(
                lease.recipe.completeness_manifest
            ),
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


async def _insert_physical_projection(
    database,
    schema: str,
    lease,
    *,
    projection_proof: Mapping[str, object] | None = None,
    partition_records: Sequence[Mapping[str, object]] | None = None,
    retain_seconds: int = 86_400,
) -> str:
    completed_shard = await _complete_registered_projection_shard(
        database,
        schema,
        lease,
    )
    prepared_proof = (
        _physical_projection_proof(
            lease.recipe.recipe_id,
            completed_shard,
        )
        if projection_proof is None
        else projection_proof
    )
    stage = await _create_projection_stage(
        database,
        schema,
        label=lease.recipe.recipe_id,
        physical_projection_id=lease.recipe.recipe_id,
        raw_shard=prepared_proof["raw_shards"][0],
        install_trigger=True,
    )
    await _bind_projection_stage(database, schema, lease, stage)
    await _mark_projection_proof_ready(
        database,
        schema,
        lease,
        prepared_proof,
    )
    sealed_partitions = (
        _partition_records(prepared_proof)
        if partition_records is None
        else partition_records
    )
    return await _seal_physical_projection(
        database,
        schema,
        lease,
        stage,
        prepared_proof,
        sealed_partitions,
        retain_seconds=retain_seconds,
    )


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
        await postgres.upgrade()
        first_oids = await postgres.relation_oids()
        first_catalog = await postgres.catalog_snapshot()
        assert set(first_oids) == set(PROJECTION_RELATIONS)
        assert all(relation_oid is not None for relation_oid in first_oids.values())
        assert all(first_catalog[catalog_kind] for catalog_kind in first_catalog)

        await postgres.upgrade()
        assert await postgres.relation_oids() == first_oids
        assert await postgres.catalog_snapshot() == first_catalog

        await postgres.downgrade()
        assert all(
            relation_oid is None
            for relation_oid in (await postgres.relation_oids()).values()
        )
        assert await postgres.catalog_snapshot() == predecessor_catalog


@pytest.mark.asyncio
async def test_locator_free_workset_rebinds_and_fences_exact_generations(
    monkeypatch,
) -> None:
    """Prove rebind-only replay plus recipe and shard generation fences."""

    async with projection_foundation_postgres(monkeypatch) as postgres:
        await postgres.upgrade()
        database = postgres.database
        schema = postgres.schema
        provisional_recipe, provisional_block, provisional_shard = (
            _projection_fixture()
        )
        original_binding = await _sealed_retained_binding(
            postgres,
            campaign_label="projection-original-campaign",
            artifact_label="projection-shared-artifact",
            consumer_recipe_id=provisional_recipe.recipe_id,
        )
        recipe, input_block, shard = _projection_fixture(
            retained_binding=original_binding
        )
        assert recipe.recipe_id == provisional_recipe.recipe_id
        assert input_block.block_id == provisional_block.block_id
        assert input_block.block_proof_sha256 == provisional_block.block_proof_sha256
        assert shard == provisional_shard

        projection_claim = await claim_projection_recipe(
            recipe,
            database=database,
            schema=schema,
        )
        assert projection_claim.lease is not None
        lease = projection_claim.lease
        with pytest.raises(
            DBAPIError,
            match="provider_directory_projection_recipe_identity_immutable",
        ):
            await database.status(
                f"""
                UPDATE "{schema}".provider_directory_projection_recipe
                   SET decoder_contract_id = 'tampered-decoder.v1'
                 WHERE recipe_id = :recipe_id;
                """,
                recipe_id=recipe.recipe_id,
            )

        with pytest.raises(
            DBAPIError,
            match="provider_directory_projection_workset_write_unfenced",
        ):
            async with database.transaction():
                await set_local_projection_action(
                    database,
                    "workset_register",
                    recipe_id=recipe.recipe_id,
                    recipe_attempt=lease.attempt,
                    recipe_lease_token=lease.lease_token,
                )
                await _insert_projection_input_block(
                    database,
                    schema,
                    recipe.recipe_id,
                    input_block,
                )
                await database.status(
                    f"""
                    INSERT INTO "{schema}".provider_directory_projection_proof_shard (
                        recipe_id, attempt, partition_id, partition_ordinal,
                        partition_key, input_block_id, resource_type,
                        input_sha256, status, partition_attempt,
                        canonical_row_sha256, resource_count,
                        first_identity_json, last_identity_json, proof_json,
                        created_at, completed_at
                    ) VALUES (
                        :recipe_id, :attempt, :partition_id, 0,
                        :partition_key, :input_block_id, 'Organization',
                        :input_sha256, 'complete', 1,
                        :canonical_row_sha256, 1,
                        '["Organization", "o-1"]'::jsonb,
                        '["Organization", "o-1"]'::jsonb,
                        '{{"complete": true}}'::jsonb, now(), now()
                    );
                    """,
                    recipe_id=recipe.recipe_id,
                    attempt=lease.attempt,
                    partition_id=shard.partition_id,
                    partition_key=shard.partition_key,
                    input_block_id=shard.input_block_id,
                    input_sha256=shard.input_sha256,
                    canonical_row_sha256=_digest("direct-complete-shard"),
                )

        nonexistent_binding = replace(
            original_binding,
            campaign_id=_digest("nonexistent-retained-campaign"),
            source_item_id=_digest("nonexistent-retained-source-item"),
        )
        _, nonexistent_block, nonexistent_shard = _projection_fixture(
            retained_binding=nonexistent_binding
        )
        with pytest.raises(
            DBAPIError,
            match="provider_directory_projection_retained_binding_invalid",
        ):
            await register_projection_workset(
                lease,
                (nonexistent_block,),
                (nonexistent_shard,),
                database=database,
                schema=schema,
            )

        stale_binding = replace(
            original_binding,
            campaign_sha256=_digest("stale-retained-campaign-proof"),
        )
        _, stale_block, stale_shard = _projection_fixture(
            retained_binding=stale_binding
        )
        with pytest.raises(
            DBAPIError,
            match="provider_directory_projection_retained_binding_invalid",
        ):
            await register_projection_workset(
                lease,
                (stale_block,),
                (stale_shard,),
                database=database,
                schema=schema,
            )

        await register_projection_workset(
            lease,
            (input_block,),
            (shard,),
            database=database,
            schema=schema,
        )
        for post_registration_insert in ("input", "shard"):
            with pytest.raises(
                DBAPIError,
                match="provider_directory_projection_workset_write_unfenced",
            ):
                async with database.transaction():
                    await set_local_projection_action(
                        database,
                        "workset_register",
                        recipe_id=recipe.recipe_id,
                        recipe_attempt=lease.attempt,
                        recipe_lease_token=lease.lease_token,
                    )
                    if post_registration_insert == "input":
                        await database.status(
                            f"""
                            INSERT INTO
                                "{schema}".provider_directory_projection_input_block
                            SELECT recipe_id, :block_id, 1,
                                   upstream_artifact_id, source_object_id,
                                   block_kind, input_contract_id, 1,
                                   record_start, record_count, content_sha256,
                                   payload_sha256, payload_bytes, summary_json,
                                   retained_campaign_id, retained_campaign_sha256,
                                   retained_source_item_id, retained_range_ordinal,
                                   :block_proof_sha256, now()
                              FROM "{schema}".provider_directory_projection_input_block
                             WHERE recipe_id = :recipe_id;
                            """,
                            recipe_id=recipe.recipe_id,
                            block_id=_digest("post-registration-block"),
                            block_proof_sha256=_digest(
                                "post-registration-block-proof"
                            ),
                        )
                    else:
                        await database.status(
                            f"""
                            INSERT INTO
                                "{schema}".provider_directory_projection_proof_shard
                            SELECT recipe_id, attempt, :partition_id, 1,
                                   :partition_key, input_block_id,
                                   resource_type, :input_sha256,
                                   'pending', 0, NULL, NULL, NULL, NULL,
                                   NULL, NULL, NULL, NULL, NULL, created_at, NULL
                              FROM "{schema}".provider_directory_projection_proof_shard
                             WHERE recipe_id = :recipe_id;
                            """,
                            recipe_id=recipe.recipe_id,
                            partition_id=_digest("post-registration-shard"),
                            partition_key=_digest(
                                "post-registration-shard-partition"
                            ),
                            input_sha256=_digest(
                                "post-registration-shard-input"
                            ),
                        )
        with pytest.raises(
            DBAPIError,
            match="provider_directory_projection_recipe_identity_immutable",
        ):
            await database.status(
                f"""
                UPDATE "{schema}".provider_directory_projection_recipe
                   SET transform_contract_id = 'tampered-transform.v1'
                 WHERE recipe_id = :recipe_id;
                """,
                recipe_id=recipe.recipe_id,
            )
        recipe_mutations = (
            "attempt = attempt + 1",
            f"lease_token = '{_digest('bypassed-recipe-lease')}'",
            "stage_schema = 'fixture', stage_relation = 'bypass', "
            "stage_relation_oid = 99",
            "native_campaign_proof_json = '{\"bypassed\": true}'::jsonb",
            "status = 'proof_ready', "
            "prepared_proof_json = '{\"complete\": true}'::jsonb",
        )
        for recipe_mutation in recipe_mutations:
            with pytest.raises(
                DBAPIError,
                match=(
                    "provider_directory_projection_recipe_"
                    "(write_unfenced|transition_invalid)"
                ),
            ):
                await database.status(
                    f"""
                    UPDATE "{schema}".provider_directory_projection_recipe
                       SET {recipe_mutation}
                     WHERE recipe_id = :recipe_id;
                    """,
                    recipe_id=recipe.recipe_id,
                )

        rebound_binding = await _sealed_retained_binding(
            postgres,
            campaign_label="projection-rebound-campaign",
            artifact_label="projection-shared-artifact",
            consumer_recipe_id=recipe.recipe_id,
            reuse_verified_artifact=True,
        )
        rebound_recipe, rebound_block, rebound_shard = _projection_fixture(
            retained_binding=rebound_binding
        )
        assert rebound_recipe.recipe_id == recipe.recipe_id
        assert rebound_block.block_id == input_block.block_id
        assert rebound_shard == shard
        await register_projection_workset(
            lease,
            (rebound_block,),
            (rebound_shard,),
            database=database,
            schema=schema,
        )

        recipe_count = await database.scalar(
            f'SELECT count(*) FROM "{schema}".'
            'provider_directory_projection_recipe;'
        )
        stored_binding = await database.first(
            f"""
            SELECT block_id, block_proof_sha256, retained_campaign_id,
                   retained_campaign_sha256, retained_source_item_id,
                   retained_range_ordinal
              FROM "{schema}".provider_directory_projection_input_block
             WHERE recipe_id = :recipe_id;
            """,
            recipe_id=recipe.recipe_id,
        )
        assert recipe_count == 1
        assert stored_binding.block_id == input_block.block_id
        assert stored_binding.block_proof_sha256 == input_block.block_proof_sha256
        assert stored_binding.retained_campaign_id == rebound_block.retained_campaign_id
        assert (
            stored_binding.retained_campaign_sha256
            == rebound_block.retained_campaign_sha256
        )
        assert (
            stored_binding.retained_source_item_id
            == rebound_block.retained_source_item_id
        )
        assert stored_binding.retained_range_ordinal == 0

        await release_retained_campaign_consumer(
            postgres.retained_connection,
            campaign_id=original_binding.campaign_id,
            consumer_recipe_id=recipe.recipe_id,
            claimed_campaign_sha256=original_binding.campaign_sha256,
            consumer_claim_generation=original_binding.consumer_claim_generation,
        )
        with pytest.raises(
            DBAPIError,
            match="provider_directory_projection_retained_binding_invalid",
        ):
            await register_projection_workset(
                lease,
                (input_block,),
                (shard,),
                database=database,
                schema=schema,
            )

        with pytest.raises(
            DBAPIError,
            match=(
                "provider_directory_projection_shard_"
                "(write_unfenced|transition_invalid)"
            ),
        ):
            await database.status(
                f"""
                UPDATE "{schema}".provider_directory_projection_proof_shard
                   SET status = 'complete',
                       canonical_row_sha256 = :canonical_row_sha256,
                       resource_count = 1,
                       first_identity_json = '["Organization", "o-1"]'::jsonb,
                       last_identity_json = '["Organization", "o-1"]'::jsonb,
                       proof_json = '{{"complete": true}}'::jsonb,
                       completed_at = now()
                 WHERE recipe_id = :recipe_id;
                """,
                canonical_row_sha256=_digest("bypassed-shard"),
                recipe_id=recipe.recipe_id,
            )

        shard_claim = await claim_projection_shard(
            lease,
            database=database,
            schema=schema,
        )
        assert shard_claim is not None
        loaded_block = await load_projection_input_block(
            shard_claim,
            database=database,
            schema=schema,
        )
        assert loaded_block["retained_campaign_id"] == (
            rebound_block.retained_campaign_id
        )
        assert "storage_locator_json" not in loaded_block
        assert all("path" not in field_name for field_name in loaded_block)

        await heartbeat_projection_lease(
            lease,
            database=database,
            schema=schema,
        )
        with pytest.raises(
            ProviderDirectoryProjectionError,
            match="provider_directory_projection_lease_seconds_invalid",
        ):
            await heartbeat_projection_lease(
                lease,
                lease_seconds=29,
                database=database,
                schema=schema,
            )
        await heartbeat_projection_shard(
            shard_claim,
            database=database,
            schema=schema,
        )

        stale_lease = replace(lease, lease_token=_digest("stale-recipe-lease"))
        with pytest.raises(
            ProviderDirectoryProjectionLeaseLost,
            match="provider_directory_projection_lease_lost",
        ):
            await heartbeat_projection_lease(
                stale_lease,
                database=database,
                schema=schema,
            )
        stale_shard_claim = replace(
            shard_claim,
            partition_attempt=shard_claim.partition_attempt + 1,
        )
        with pytest.raises(
            ProviderDirectoryProjectionLeaseLost,
            match="provider_directory_projection_shard_lease_lost",
        ):
            await heartbeat_projection_shard(
                stale_shard_claim,
                database=database,
                schema=schema,
            )

        with pytest.raises(
            DBAPIError,
            match="provider_directory_projection_input_block_immutable",
        ):
            await database.status(
                f"""
                UPDATE "{schema}".provider_directory_projection_input_block
                   SET retained_campaign_id = :campaign_id
                 WHERE recipe_id = :recipe_id;
                """,
                campaign_id=_digest("ungated-binding"),
                recipe_id=recipe.recipe_id,
            )
        with pytest.raises(
            DBAPIError,
            match="provider_directory_projection_input_block_immutable",
        ):
            async with database.transaction():
                await database.status(
                    "SET LOCAL "
                    "healthporta.provider_directory_projection_retained_binding_refresh "
                    "= 'on';"
                )
                await database.status(
                    f"""
                    UPDATE "{schema}".provider_directory_projection_input_block
                       SET payload_bytes = payload_bytes + 1
                     WHERE recipe_id = :recipe_id;
                    """,
                    recipe_id=recipe.recipe_id,
                )

        assert isinstance(lease, ProjectionLease)


@pytest.mark.asyncio
async def test_projection_migration_rejects_bypassed_trigger_adoption(
    monkeypatch,
) -> None:
    async with projection_foundation_postgres(monkeypatch) as postgres:
        await postgres.upgrade()
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
            await postgres.upgrade()


@pytest.mark.asyncio
async def test_projection_downgrade_refuses_lone_physical_state(monkeypatch) -> None:
    async with projection_foundation_postgres(monkeypatch) as postgres:
        await postgres.upgrade()
        lease = await _registered_projection_lease(
            postgres,
            "projection-downgrade-campaign",
        )
        await _insert_physical_projection(
            postgres.database,
            postgres.schema,
            lease,
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
            DBAPIError,
            match="provider_directory_projection_recipe_transition_invalid",
        ):
            await _mark_projection_proof_ready(
                database,
                schema,
                lease,
                projection_proof,
            )

        immutable_stage = await _install_projection_stage_trigger(database, stage)
        await _mark_projection_proof_ready(
            database,
            schema,
            lease,
            projection_proof,
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
            "proof_ready",
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


@pytest.mark.parametrize(
    "stage_forgery",
    ("empty", "extra_row", "missing_index", "unlogged"),
)
@pytest.mark.asyncio
async def test_proof_ready_rejects_incomplete_physical_storage(
    monkeypatch,
    stage_forgery,
) -> None:
    """Reject empty, overfull, unindexed, and non-durable final storage."""

    async with projection_foundation_postgres(monkeypatch) as postgres:
        await postgres.upgrade()
        database = postgres.database
        schema = postgres.schema
        lease = await _registered_projection_lease(
            postgres,
            f"stage-forgery-{stage_forgery}-campaign",
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
            label=f"stage-forgery-{stage_forgery}",
            physical_projection_id=lease.recipe.recipe_id,
            raw_shard=completed_shard,
            install_trigger=False,
        )
        stage_ref = f'"{schema}"."{stage.relation}"'
        if stage_forgery == "empty":
            await database.status(f"DELETE FROM {stage_ref};")
        elif stage_forgery == "extra_row":
            await database.status(
                f"""
                INSERT INTO {stage_ref}
                SELECT physical_projection_id, resource_type, 'o-2',
                       proof_partition_id, :payload_hash,
                       '{{"id":"o-2"}}'::jsonb, source_rank,
                       summary_npi, summary_address_count,
                       summary_addressed_location,
                       summary_geocoded_location,
                       summary_network_link_count,
                       summary_affiliation_link_count, active,
                       effective_start, effective_end, observed_at,
                       profile_evidence_json
                  FROM {stage_ref};
                """,
                payload_hash=_digest("extra-stage-payload"),
            )
        elif stage_forgery == "missing_index":
            await database.status(
                f'DROP INDEX "{schema}"."{stage.relation}_identity";'
            )
        else:
            await database.status(f"ALTER TABLE {stage_ref} SET UNLOGGED;")
        await _bind_projection_stage(database, schema, lease, stage)
        await _install_projection_stage_trigger(database, stage)
        with pytest.raises(
            DBAPIError,
            match="provider_directory_projection_recipe_transition_invalid",
        ):
            await _mark_projection_proof_ready(
                database,
                schema,
                lease,
                projection_proof,
            )


@pytest.mark.asyncio
async def test_proof_ready_requires_completed_registered_shards(
    monkeypatch,
) -> None:
    async with projection_foundation_postgres(monkeypatch) as postgres:
        await postgres.upgrade()
        database = postgres.database
        schema = postgres.schema
        lease = await _registered_projection_lease(
            postgres,
            "pending-shard-proof-campaign",
        )
        pending_shard = await database.first(
            f"""
            SELECT partition_id, partition_ordinal, resource_type, input_sha256
              FROM "{schema}".provider_directory_projection_proof_shard
             WHERE recipe_id = :recipe_id;
            """,
            recipe_id=lease.recipe.recipe_id,
        )
        raw_shard_map = {
            "partition_id": pending_shard.partition_id,
            "partition_ordinal": pending_shard.partition_ordinal,
            "resource_type": pending_shard.resource_type,
            "input_sha256": pending_shard.input_sha256,
            "canonical_row_sha256": _digest("pending-shard-row"),
            "resource_count": 1,
            "first_identity": ["Organization", "o-1"],
            "last_identity": ["Organization", "o-1"],
        }
        projection_proof = _physical_projection_proof(
            lease.recipe.recipe_id,
            raw_shard_map,
        )
        stage = await _create_projection_stage(
            database,
            schema,
            label="pending-shard-stage",
            physical_projection_id=lease.recipe.recipe_id,
            raw_shard=raw_shard_map,
            install_trigger=True,
        )
        await _bind_projection_stage(database, schema, lease, stage)
        with pytest.raises(
            DBAPIError,
            match="provider_directory_projection_recipe_transition_invalid",
        ):
            await _mark_projection_proof_ready(
                database,
                schema,
                lease,
                projection_proof,
            )


@pytest.mark.asyncio
async def test_final_seal_rejects_missing_partition(monkeypatch) -> None:
    async with projection_foundation_postgres(monkeypatch) as postgres:
        await postgres.upgrade()
        database = postgres.database
        schema = postgres.schema
        lease = await _registered_projection_lease(
            postgres,
            "missing-partition-campaign",
        )
        with pytest.raises(
            DBAPIError,
            match="provider_directory_projection_recipe_transition_invalid",
        ):
            await _insert_physical_projection(
                database,
                schema,
                lease,
                partition_records=(),
            )
        assert await database.scalar(
            f"""
            SELECT count(*)
              FROM "{schema}".provider_directory_physical_projection;
            """
        ) == 0
        assert await database.scalar(
            f"""
            SELECT status
              FROM "{schema}".provider_directory_projection_recipe
             WHERE recipe_id = :recipe_id;
            """,
            recipe_id=lease.recipe.recipe_id,
        ) == "proof_ready"


@pytest.mark.parametrize("forgery", ("extra_key", "column_mismatch"))
@pytest.mark.asyncio
async def test_partition_seal_rejects_forged_descriptor_or_column(
    monkeypatch,
    forgery,
) -> None:
    async with projection_foundation_postgres(monkeypatch) as postgres:
        await postgres.upgrade()
        database = postgres.database
        schema = postgres.schema
        lease = await _registered_projection_lease(
            postgres,
            f"forged-partition-{forgery}-campaign",
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
        if forgery == "extra_key":
            projection_proof["raw_shards"][0]["forged"] = True
            partition_records = _partition_records(projection_proof)
        else:
            partition_records = _partition_records(projection_proof)
            partition_records[0]["canonical_row_sha256"] = _digest(
                "forged-column-value"
            )
        with pytest.raises(
            DBAPIError,
            match="provider_directory_projection_partition_immutable",
        ):
            await _insert_physical_projection(
                database,
                schema,
                lease,
                projection_proof=projection_proof,
                partition_records=partition_records,
            )


@pytest.mark.asyncio
async def test_partition_seal_rejects_undeclared_resource(
    monkeypatch,
) -> None:
    async with projection_foundation_postgres(monkeypatch) as postgres:
        await postgres.upgrade()
        database = postgres.database
        schema = postgres.schema
        lease = await _registered_projection_lease(
            postgres,
            "undeclared-resource-campaign",
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
        projection_proof["resource_counts"] = {"Practitioner": 1}
        projection_proof["source_summary"]["resource_counts"] = {
            "Practitioner": 1
        }
        raw_shard = projection_proof["raw_shards"][0]
        raw_shard["resource_type"] = "Practitioner"
        raw_shard["first_identity"] = ["Practitioner", "p-1"]
        raw_shard["last_identity"] = ["Practitioner", "p-1"]
        with pytest.raises(
            DBAPIError,
            match="provider_directory_projection_recipe_transition_invalid",
        ):
            await _insert_physical_projection(
                database,
                schema,
                lease,
                projection_proof=projection_proof,
            )


async def _set_gc_action(database, lease) -> None:
    await set_local_projection_action(
        database,
        "gc",
        recipe_id=lease.recipe.recipe_id,
        recipe_attempt=lease.attempt,
        physical_projection_id=lease.recipe.recipe_id,
    )


async def _retire_physical_projection(database, schema: str, lease) -> None:
    async with database.transaction():
        await _set_gc_action(database, lease)
        await database.status(
            f"""
            UPDATE "{schema}".provider_directory_physical_projection
               SET status = 'retiring', retiring_at = clock_timestamp()
             WHERE physical_projection_id = :physical_projection_id;
            """,
            physical_projection_id=lease.recipe.recipe_id,
        )


async def _retire_projection_recipe(database, schema: str, lease) -> None:
    async with database.transaction():
        await _set_gc_action(database, lease)
        await database.status(
            f"""
            UPDATE "{schema}".provider_directory_projection_recipe
               SET status = 'retired', physical_projection_id = NULL,
                   stage_schema = NULL, stage_relation = NULL,
                   stage_relation_oid = NULL,
                   membership_stage_schema = NULL,
                   membership_stage_relation = NULL,
                   membership_stage_relation_oid = NULL,
                   profile_contribution_stage_schema = NULL,
                   profile_contribution_stage_relation = NULL,
                   profile_contribution_stage_relation_oid = NULL,
                   updated_at = clock_timestamp()
             WHERE recipe_id = :recipe_id;
            """,
            recipe_id=lease.recipe.recipe_id,
        )


async def _insert_projection_reference(database, schema: str, lease) -> None:
    reference_identity_hash = _digest("projection-reference")
    reference_lease_token = _digest("projection-reference-lease")
    async with database.transaction():
        await set_local_projection_action(
            database,
            "reference_insert",
            recipe_id=lease.recipe.recipe_id,
            recipe_attempt=lease.attempt,
            physical_projection_id=lease.recipe.recipe_id,
            reference_owner_kind="dataset",
            reference_owner_id="fixture-dataset",
            reference_identity_hash=reference_identity_hash,
            reference_lease_token=reference_lease_token,
        )
        await database.status(
            f"""
            INSERT INTO
                "{schema}".provider_directory_physical_projection_reference (
                physical_projection_id, owner_kind, owner_id,
                reference_identity_hash, reference_proof_json, lease_token,
                created_at, lease_expires_at, lease_heartbeat_at
            ) VALUES (
                :physical_projection_id, 'dataset', 'fixture-dataset',
                :reference_identity_hash, '{{"complete": true}}'::jsonb,
                :lease_token, now(), now() + interval '1 day', now()
            );
            """,
            physical_projection_id=lease.recipe.recipe_id,
            reference_identity_hash=reference_identity_hash,
            lease_token=reference_lease_token,
        )


async def _release_projection_reference(database, schema: str, lease) -> None:
    async with database.transaction():
        await set_local_projection_action(
            database,
            "reference_release",
            recipe_id=lease.recipe.recipe_id,
            recipe_attempt=lease.attempt,
            physical_projection_id=lease.recipe.recipe_id,
            reference_owner_kind="dataset",
            reference_owner_id="fixture-dataset",
            reference_identity_hash=_digest("projection-reference"),
            reference_lease_token=_digest("projection-reference-lease"),
        )
        await database.status(
            f"""
            UPDATE "{schema}".provider_directory_physical_projection_reference
               SET released_at = clock_timestamp(),
                   lease_expires_at = clock_timestamp(),
                   lease_heartbeat_at = clock_timestamp()
             WHERE physical_projection_id = :physical_projection_id;
            """,
            physical_projection_id=lease.recipe.recipe_id,
        )


async def _set_reference_action(
    database,
    lease,
    action: str,
    lease_token: str,
    *,
    previous_lease_token: str | None = None,
) -> None:
    await set_local_projection_action(
        database,
        action,
        recipe_id=lease.recipe.recipe_id,
        recipe_attempt=lease.attempt,
        physical_projection_id=lease.recipe.recipe_id,
        reference_owner_kind="dataset",
        reference_owner_id="fixture-dataset",
        reference_identity_hash=_digest("projection-reference"),
        reference_lease_token=lease_token,
        previous_reference_lease_token=previous_lease_token,
    )


@pytest.mark.asyncio
async def test_projection_reference_requires_exact_lease_generations(
    monkeypatch,
) -> None:
    """Fence reference acquisition, heartbeat, release, reclaim, and replay."""

    async with projection_foundation_postgres(monkeypatch) as postgres:
        await postgres.upgrade()
        database = postgres.database
        schema = postgres.schema
        lease = await _registered_projection_lease(
            postgres,
            "reference-generation-campaign",
        )
        await _insert_physical_projection(
            database,
            schema,
            lease,
            retain_seconds=0,
        )
        with pytest.raises(
            DBAPIError,
            match="provider_directory_projection_reference_insert_unfenced",
        ):
            await database.status(
                f"""
                INSERT INTO
                    "{schema}".provider_directory_physical_projection_reference (
                    physical_projection_id, owner_kind, owner_id,
                    reference_identity_hash, reference_proof_json,
                    lease_token, created_at, lease_expires_at,
                    lease_heartbeat_at
                ) VALUES (
                    :physical_projection_id, 'dataset', 'fixture-dataset',
                    :identity_hash, '{{}}'::jsonb, :lease_token,
                    now(), now() + interval '1 day', now()
                );
                """,
                physical_projection_id=lease.recipe.recipe_id,
                identity_hash=_digest("projection-reference"),
                lease_token=_digest("projection-reference-lease"),
            )
        await _insert_projection_reference(database, schema, lease)
        original_token = _digest("projection-reference-lease")
        stale_token = _digest("stale-projection-reference-lease")
        reclaimed_token = _digest("reclaimed-projection-reference-lease")

        with pytest.raises(
            DBAPIError,
            match="provider_directory_projection_reference_immutable",
        ):
            async with database.transaction():
                await database.status(
                    "SET LOCAL "
                    "healthporta.provider_directory_projection_reference_lease "
                    "= 'on';"
                )
                await database.status(
                    f"""
                    UPDATE
                        "{schema}".
                        provider_directory_physical_projection_reference
                       SET lease_heartbeat_at = clock_timestamp()
                     WHERE physical_projection_id = :physical_projection_id;
                    """,
                    physical_projection_id=lease.recipe.recipe_id,
                )

        with pytest.raises(
            DBAPIError,
            match="provider_directory_projection_reference_immutable",
        ):
            async with database.transaction():
                await _set_reference_action(
                    database,
                    lease,
                    "reference_release",
                    stale_token,
                )
                await database.status(
                    f"""
                    UPDATE
                        "{schema}".
                        provider_directory_physical_projection_reference
                       SET released_at = clock_timestamp(),
                           lease_expires_at = clock_timestamp(),
                           lease_heartbeat_at = clock_timestamp()
                     WHERE physical_projection_id = :physical_projection_id;
                    """,
                    physical_projection_id=lease.recipe.recipe_id,
                )

        async with database.transaction():
            await _set_reference_action(
                database,
                lease,
                "reference_heartbeat",
                original_token,
            )
            await database.status(
                f"""
                UPDATE
                    "{schema}".provider_directory_physical_projection_reference
                   SET lease_expires_at = clock_timestamp() + interval '1 day',
                       lease_heartbeat_at = clock_timestamp()
                 WHERE physical_projection_id = :physical_projection_id;
                """,
                physical_projection_id=lease.recipe.recipe_id,
            )

        with pytest.raises(
            DBAPIError,
            match="provider_directory_projection_reference_immutable",
        ):
            async with database.transaction():
                await _set_reference_action(
                    database,
                    lease,
                    "reference_reclaim",
                    reclaimed_token,
                    previous_lease_token=original_token,
                )
                await database.status(
                    f"""
                    UPDATE
                        "{schema}".
                        provider_directory_physical_projection_reference
                       SET lease_token = :lease_token, released_at = NULL,
                           lease_expires_at =
                               clock_timestamp() + interval '1 day',
                           lease_heartbeat_at = clock_timestamp()
                     WHERE physical_projection_id = :physical_projection_id;
                    """,
                    lease_token=reclaimed_token,
                    physical_projection_id=lease.recipe.recipe_id,
                )

        await _release_projection_reference(database, schema, lease)
        async with database.transaction():
            await _set_reference_action(
                database,
                lease,
                "reference_reclaim",
                reclaimed_token,
                previous_lease_token=original_token,
            )
            await database.status(
                f"""
                UPDATE
                    "{schema}".provider_directory_physical_projection_reference
                   SET lease_token = :lease_token, released_at = NULL,
                       lease_expires_at =
                           clock_timestamp() + interval '1 day',
                       lease_heartbeat_at = clock_timestamp()
                 WHERE physical_projection_id = :physical_projection_id;
                """,
                lease_token=reclaimed_token,
                physical_projection_id=lease.recipe.recipe_id,
            )
        async with database.transaction():
            await _set_reference_action(
                database,
                lease,
                "reference_release",
                reclaimed_token,
            )
            await database.status(
                f"""
                UPDATE
                    "{schema}".provider_directory_physical_projection_reference
                   SET released_at = clock_timestamp(),
                       lease_expires_at = clock_timestamp(),
                       lease_heartbeat_at = clock_timestamp()
                 WHERE physical_projection_id = :physical_projection_id;
                """,
                physical_projection_id=lease.recipe.recipe_id,
            )
        await _retire_physical_projection(database, schema, lease)
        async with database.transaction():
            await _set_reference_action(
                database,
                lease,
                "reference_release",
                reclaimed_token,
            )
            replay_count = await database.status(
                f"""
                UPDATE
                    "{schema}".provider_directory_physical_projection_reference
                   SET released_at = released_at
                 WHERE physical_projection_id = :physical_projection_id;
                """,
                physical_projection_id=lease.recipe.recipe_id,
            )
        assert replay_count == 1


@pytest.mark.asyncio
async def test_projection_gc_rejects_legacy_boolean_and_retention_bypass(
    monkeypatch,
) -> None:
    async with projection_foundation_postgres(monkeypatch) as postgres:
        await postgres.upgrade()
        database = postgres.database
        schema = postgres.schema
        lease = await _registered_projection_lease(
            postgres,
            "retention-fence-campaign",
        )
        physical_projection_id = await _insert_physical_projection(
            database,
            schema,
            lease,
            retain_seconds=60,
        )
        with pytest.raises(
            DBAPIError,
            match="provider_directory_projection_partition_immutable",
        ):
            async with database.transaction():
                await database.status(
                    "SET LOCAL "
                    "healthporta.provider_directory_projection_gc = 'on';"
                )
                await database.status(
                    f"""
                    DELETE FROM
                        "{schema}".
                        provider_directory_physical_projection_partition
                     WHERE physical_projection_id = :physical_projection_id;
                    """,
                    physical_projection_id=physical_projection_id,
                )
        with pytest.raises(
            DBAPIError,
            match="provider_directory_physical_projection_immutable",
        ):
            await _retire_physical_projection(database, schema, lease)


@pytest.mark.asyncio
async def test_projection_gc_obeys_reference_and_exact_delete_order(
    monkeypatch,
) -> None:
    """Delete references, proofs, storage, physical state, and recipe in order."""

    async with projection_foundation_postgres(monkeypatch) as postgres:
        await postgres.upgrade()
        database = postgres.database
        schema = postgres.schema
        lease = await _registered_projection_lease(
            postgres,
            "ordered-gc-campaign",
        )
        physical_projection_id = await _insert_physical_projection(
            database,
            schema,
            lease,
            retain_seconds=0,
        )
        physical_stage = await database.first(
            f"""
            SELECT storage_relation, storage_relation_oid
              FROM "{schema}".provider_directory_physical_projection
             WHERE physical_projection_id = :physical_projection_id;
            """,
            physical_projection_id=physical_projection_id,
        )

        with pytest.raises(
            DBAPIError,
            match="provider_directory_projection_recipe_transition_invalid",
        ):
            await _retire_projection_recipe(database, schema, lease)

        await _insert_projection_reference(database, schema, lease)
        with pytest.raises(
            DBAPIError,
            match="provider_directory_physical_projection_immutable",
        ):
            await _retire_physical_projection(database, schema, lease)
        await _release_projection_reference(database, schema, lease)
        await _retire_physical_projection(database, schema, lease)
        assert tuple(
            await database.first(
                f"""
                SELECT physical.status, recipe.status
                  FROM "{schema}".provider_directory_physical_projection
                       AS physical
                  JOIN "{schema}".provider_directory_projection_recipe AS recipe
                    ON recipe.recipe_id = physical.physical_projection_id
                 WHERE physical.physical_projection_id = :physical_projection_id;
                """,
                physical_projection_id=physical_projection_id,
            )
        ) == ("retiring", "sealed")

        with pytest.raises(
            DBAPIError,
            match="provider_directory_projection_partition_immutable",
        ):
            async with database.transaction():
                await _set_gc_action(database, lease)
                await database.status(
                    f"""
                    DELETE FROM
                        "{schema}".
                        provider_directory_physical_projection_partition
                     WHERE physical_projection_id = :physical_projection_id;
                    """,
                    physical_projection_id=physical_projection_id,
                )
        with pytest.raises(
            DBAPIError,
            match="provider_directory_projection_proof_shard_immutable",
        ):
            async with database.transaction():
                await _set_gc_action(database, lease)
                await database.status(
                    f"""
                    DELETE FROM
                        "{schema}".provider_directory_projection_proof_shard
                     WHERE recipe_id = :recipe_id;
                    """,
                    recipe_id=lease.recipe.recipe_id,
                )

        await _retire_projection_recipe(database, schema, lease)
        assert await database.scalar(
            f"""
            SELECT status
              FROM "{schema}".provider_directory_projection_recipe
             WHERE recipe_id = :recipe_id;
            """,
            recipe_id=lease.recipe.recipe_id,
        ) == "retired"

        async with database.transaction():
            await _set_gc_action(database, lease)
            await database.status(
                f"""
                DELETE FROM
                    "{schema}".provider_directory_physical_projection_reference
                 WHERE physical_projection_id = :physical_projection_id;
                """,
                physical_projection_id=physical_projection_id,
            )
            await database.status(
                f"""
                DELETE FROM
                    "{schema}".provider_directory_physical_projection_partition
                 WHERE physical_projection_id = :physical_projection_id;
                """,
                physical_projection_id=physical_projection_id,
            )
            await database.status(
                f"""
                DELETE FROM
                    "{schema}".
                    provider_directory_physical_projection_source_summary
                 WHERE physical_projection_id = :physical_projection_id;
                """,
                physical_projection_id=physical_projection_id,
            )

        with pytest.raises(
            DBAPIError,
            match="provider_directory_physical_projection_immutable",
        ):
            async with database.transaction():
                await _set_gc_action(database, lease)
                await database.status(
                    f"""
                    DELETE FROM "{schema}".provider_directory_physical_projection
                     WHERE physical_projection_id = :physical_projection_id;
                    """,
                    physical_projection_id=physical_projection_id,
                )

        async with database.transaction():
            await _set_gc_action(database, lease)
            await database.status(
                f"""
                DELETE FROM "{schema}".provider_directory_projection_proof_shard
                 WHERE recipe_id = :recipe_id;
                """,
                recipe_id=lease.recipe.recipe_id,
            )
            await database.status(
                f"""
                DELETE FROM "{schema}".provider_directory_projection_input_block
                 WHERE recipe_id = :recipe_id;
                """,
                recipe_id=lease.recipe.recipe_id,
            )

        with pytest.raises(
            DBAPIError,
            match="provider_directory_physical_projection_immutable",
        ):
            async with database.transaction():
                await _set_gc_action(database, lease)
                await database.status(
                    f"""
                    DELETE FROM "{schema}".provider_directory_physical_projection
                     WHERE physical_projection_id = :physical_projection_id;
                    """,
                    physical_projection_id=physical_projection_id,
                )

        async with database.transaction():
            await _set_gc_action(database, lease)
            await database.status(
                f'DROP TABLE "{schema}"."{physical_stage.storage_relation}";'
            )
            await database.status(
                f"""
                DELETE FROM "{schema}".provider_directory_physical_projection
                 WHERE physical_projection_id = :physical_projection_id;
                """,
                physical_projection_id=physical_projection_id,
            )
            await database.status(
                f"""
                DELETE FROM "{schema}".provider_directory_projection_recipe
                 WHERE recipe_id = :recipe_id;
                """,
                recipe_id=lease.recipe.recipe_id,
            )

        assert await database.scalar(
            "SELECT to_regclass(:qualified_relation);",
            qualified_relation=f"{schema}.{physical_stage.storage_relation}",
        ) is None
        for relation in (
            "provider_directory_physical_projection_reference",
            "provider_directory_physical_projection_partition",
            "provider_directory_physical_projection_source_summary",
            "provider_directory_physical_projection",
            "provider_directory_projection_proof_shard",
            "provider_directory_projection_input_block",
            "provider_directory_projection_recipe",
        ):
            assert await database.scalar(
                f'SELECT count(*) FROM "{schema}"."{relation}";'
            ) == 0
