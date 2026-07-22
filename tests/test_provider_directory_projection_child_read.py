# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Pure contract proofs for locator-free projection child read leases."""
from __future__ import annotations
from dataclasses import fields, replace
import hashlib
from unittest.mock import AsyncMock, MagicMock
import pytest
import process.provider_directory_projection_child_read as child_read
import process.provider_directory_projection_child_read_contract as child_contract
import process.provider_directory_projection_child_read_lock as child_lock
import process.provider_directory_projection_child_read_persistence as child_persistence
from process.provider_directory_physical_projection import (
    claim_projection_recipe,
    claim_projection_shard,
    projection_admission_input_block,
    register_projection_admission,
    register_projection_workset,
)
from process.provider_directory_projection_child_read_contract import (
    child_lease_database_identity,
    child_row_matches_lease,
    validated_child_read_lease,
    validated_child_shard_claim,
)
from process.provider_directory_projection_db import (
    set_local_projection_action,
    shared_active_recipe,
)
from process.provider_directory_projection_contract import (
    projection_completeness_manifest,
    projection_input_block,
    projection_input_set_sha256,
    projection_recipe_identity,
    projection_shard_spec,
)
from process.provider_directory_projection_types import (
    ProjectionLease,
    ProjectionRetainedChildLease,
    ProjectionShardClaim,
    ProviderDirectoryProjectionError,
    ProviderDirectoryProjectionLeaseLost,
    stable_json,
)
from process.provider_directory_retained_artifact_contract import (
    produced_layout_digest,
)
from tests.provider_directory_retained_core_postgres_support import (
    registry_artifact,
)
from tests.test_provider_directory_projection_foundation_postgres import (
    _fixture_projection_recipe,
    _registered_projection_context,
)
def _digest(label: str) -> str:
    return hashlib.sha256(label.encode("utf-8")).hexdigest()
def _unit_database(*, first_result=None, scalar_result=True, status_result=1):
    database = MagicMock()
    database.first = AsyncMock(return_value=first_result)
    database.scalar = AsyncMock(return_value=scalar_result)
    database.status = AsyncMock(return_value=status_result)
    return database
def _child_input_block():
    """Build the canonical retained input block used by pure contract tests."""
    return projection_input_block(
        upstream_artifact_id=_digest("artifact"),
        source_object_id=_digest("layout"),
        block_kind="ndjson",
        input_contract_id="fixture-input.v1",
        record_start=0,
        record_count=2,
        content_sha256=_digest("canonical-input"),
        payload_sha256=_digest("raw-input"),
        payload_bytes=20,
        summary={"resource_count": 2},
    )
def _child_completeness_manifest():
    """Build the complete single-partition acquisition proof."""
    return projection_completeness_manifest(
        endpoint_campaign_hash=_digest("campaign-proof"),
        partition_strategy_contract_id="fixture-partitions.v1",
        selected_resources=("Practitioner",),
        required_resources=("Practitioner",),
        terminal_partitions=(
            {
                "resource_type": "Practitioner",
                "partition_key_hash": _digest("partition-key"),
                "source_partition_ordinal": 0,
                "terminal_cursor_hmac": _digest("cursor"),
                "terminal": True,
                "block_count": 1,
                "row_count": 2,
                "byte_count": 20,
                "zero_row_proof_sha256": None,
            },
        ),
        complete=True,
    )
def _child_recipe(block, completeness_manifest):
    """Build the physical recipe that consumes the canonical test block."""
    return projection_recipe_identity(
        decoder_contract_id="fixture-fhir-r4-decoder.v1",
        acquisition_adapter_id="fixture-fhir-rest.v1",
        input_set_sha256=projection_input_set_sha256(
            (block,),
            decoder_contract_id="fixture-fhir-r4-decoder.v1",
        ),
        source_ids=("fixture-source",),
        transform_contract_id="fixture-normalization.v1",
        scope_contract_id="healthporta.provider-directory.global-scope.v1",
        transform_context={
            "as_of_date": "2026-07-22",
            "time_rule_contract_id": "fixture-time-rules.v1",
        },
        selected_resources=("Practitioner",),
        required_resources=("Practitioner",),
        completeness_manifest=completeness_manifest,
    )
def _child_shard_claim(recipe, block) -> ProjectionShardClaim:
    """Build a claimed shard for the canonical test recipe."""
    shard = projection_shard_spec(
        recipe=recipe,
        partition_ordinal=0,
        input_block=block,
    )
    return ProjectionShardClaim(
        recipe_lease=ProjectionLease(
            recipe=recipe.physical,
            attempt=1,
            lease_token=_digest("recipe-lease"),
        ),
        admission_id=_digest("admission"),
        shard=shard,
        partition_attempt=1,
        lease_token=_digest("shard-lease"),
    )
def _child_lease() -> ProjectionRetainedChildLease:
    """Build one internally canonical locator-free child lease."""
    block = _child_input_block()
    recipe = _child_recipe(block, _child_completeness_manifest())
    claim = _child_shard_claim(recipe, block)
    return ProjectionRetainedChildLease(
        shard_claim=claim,
        binding_id=_digest("binding"),
        block_id=block.block_id,
        retained_campaign_id=_digest("campaign"),
        retained_campaign_sha256=_digest("campaign-proof"),
        retained_consumer_recipe_id="projection-child-test",
        retained_claim_generation=1,
        retained_source_item_id=_digest("source-item"),
        retained_artifact_sha256=_digest("artifact"),
        retained_layout_sha256=_digest("layout"),
        retained_range_ordinal=0,
        artifact_byte_count=100,
        raw_byte_start=10,
        expected_byte_count=20,
        expected_record_count=2,
        input_sha256=block.content_sha256,
        expected_payload_sha256=block.payload_sha256,
        child_generation=1,
        child_lease_token=_digest("child-lease"),
    )
def _completion_proof(claim, canonical_row_sha256: str, identity_list: list[str]):
    """Build the exact completion proof stored for one shard."""
    selected_resource = claim.recipe_lease.recipe.selected_resources[0]
    return {
        "contract_id": "healthporta.provider-directory.projection-proof-shard.v1",
        "recipe_id": claim.recipe_lease.recipe.recipe_id,
        "attempt": claim.recipe_lease.attempt,
        "partition_attempt": claim.partition_attempt,
        "partition_id": claim.shard.partition_id,
        "partition_ordinal": claim.shard.partition_ordinal,
        "resource_type": claim.shard.resource_type,
        "input_sha256": claim.shard.input_sha256,
        "canonical_row_sha256": canonical_row_sha256,
        "resource_count": 1,
        "resource_counts": {
            resource_type: int(resource_type == selected_resource)
            for resource_type in claim.recipe_lease.recipe.selected_resources
        },
        "first_identity": identity_list,
        "last_identity": identity_list,
    }
async def _complete_claimed_shard(postgres, claim) -> None:
    """Complete one exact shard through the recipe and child guards."""
    selected_resource = claim.recipe_lease.recipe.selected_resources[0]
    canonical_row_sha256 = _digest(f"child-completed:{claim.shard.partition_id}")
    identity_list = [selected_resource, "fixture-1"]
    database = postgres.database
    async with database.transaction():
        await shared_active_recipe(
            claim.recipe_lease,
            database=database,
            schema=postgres.schema,
        )
        await set_local_projection_action(
            database,
            "shard_complete",
            recipe_id=claim.recipe_lease.recipe.recipe_id,
            recipe_attempt=claim.recipe_lease.attempt,
            recipe_lease_token=claim.recipe_lease.lease_token,
            partition_id=claim.shard.partition_id,
            partition_attempt=claim.partition_attempt,
            shard_lease_token=claim.lease_token,
        )
        await database.status(
            f"""
            UPDATE "{postgres.schema}".provider_directory_projection_proof_shard
               SET status = 'complete', recipe_lease_token = NULL,
                   lease_token = NULL, lease_expires_at = NULL,
                   lease_heartbeat_at = NULL,
                   canonical_row_sha256 = :canonical_row_sha256,
                   resource_count = 1,
                   first_identity_json = CAST(:identity AS jsonb),
                   last_identity_json = CAST(:identity AS jsonb),
                   proof_json = CAST(:proof AS jsonb),
                   completed_at = clock_timestamp()
             WHERE recipe_id = :recipe_id AND attempt = :attempt
               AND partition_id = :partition_id;
            """,
            canonical_row_sha256=canonical_row_sha256,
            identity=stable_json(identity_list),
            proof=stable_json(
                _completion_proof(claim, canonical_row_sha256, identity_list)
            ),
            recipe_id=claim.recipe_lease.recipe.recipe_id,
            attempt=claim.recipe_lease.attempt,
            partition_id=claim.shard.partition_id,
        )
def _full_artifact_completeness(retained_binding, produced_artifact):
    """Build completeness for an unpartitioned retained artifact."""
    return projection_completeness_manifest(
        endpoint_campaign_hash=retained_binding.campaign_sha256,
        partition_strategy_contract_id="fixture-full-artifact.v1",
        selected_resources=(retained_binding.family,),
        required_resources=(retained_binding.family,),
        terminal_partitions=(
            {
                "resource_type": retained_binding.family,
                "partition_key_hash": retained_binding.partition_metadata_sha256,
                "source_partition_ordinal": 0,
                "terminal_cursor_hmac": _digest("full-artifact-cursor"),
                "terminal": True,
                "block_count": 1,
                "row_count": produced_artifact.artifact_record_count,
                "byte_count": produced_artifact.artifact_byte_count,
                "zero_row_proof_sha256": None,
            },
        ),
        complete=True,
    )
def _full_artifact_fixture(retained_binding, artifact_label: str):
    """Build one exact full-artifact admission fixture."""
    produced_artifact = registry_artifact(artifact_label, "bulk_ndjson")
    layout_sha256 = produced_layout_digest(produced_artifact)
    input_block = projection_input_block(
        upstream_artifact_id=produced_artifact.artifact_sha256,
        source_object_id=layout_sha256,
        block_kind="ndjson",
        input_contract_id="fixture-input.v1",
        record_start=0,
        record_count=produced_artifact.artifact_record_count,
        content_sha256=produced_artifact.manifest_sha256,
        payload_sha256=produced_artifact.artifact_sha256,
        payload_bytes=produced_artifact.artifact_byte_count,
        summary={"resource_count": produced_artifact.artifact_record_count},
    )
    recipe = _fixture_projection_recipe(
        (input_block,),
        retained_binding.family,
        _full_artifact_completeness(retained_binding, produced_artifact),
    )
    admission_block = projection_admission_input_block(
        input_block,
        retained_campaign_id=retained_binding.campaign_id,
        retained_campaign_sha256=retained_binding.campaign_sha256,
        retained_source_item_id=retained_binding.source_item_id,
        retained_range_ordinal=None,
        stream_identity_sha256=retained_binding.stream_identity_sha256,
        sequence_ordinal=retained_binding.sequence_ordinal,
        resource_type=retained_binding.family,
        partition_key_hash=retained_binding.partition_metadata_sha256,
        source_partition_ordinal=0,
    )
    shard = projection_shard_spec(
        recipe=recipe,
        partition_ordinal=0,
        input_block=input_block,
    )
    return produced_artifact, recipe, admission_block, shard
async def _claim_projection_children(
    postgres,
    recipe,
    admission_blocks,
    shards,
    claim_generation: int,
):
    """Register a workset and claim each shard with its retained child."""
    database = postgres.database
    projection_claim = await claim_projection_recipe(
        recipe,
        database=database,
        schema=postgres.schema,
    )
    await register_projection_workset(
        projection_claim.lease,
        tuple(binding.block for binding in admission_blocks),
        shards,
        database=database,
        schema=postgres.schema,
    )
    admission_id = await register_projection_admission(
        recipe,
        admission_blocks,
        shards,
        claim_generation=claim_generation,
        database=database,
        schema=postgres.schema,
    )
    shard_claims = []
    child_leases = []
    for shard in shards:
        shard_claim = await claim_projection_shard(
            projection_claim.lease,
            admission_id=admission_id,
            partition_id=shard.partition_id,
            database=database,
            schema=postgres.schema,
        )
        assert shard_claim is not None
        shard_claims.append(shard_claim)
        child_leases.append(
            await child_read.claim_projection_child_read_lease(
                shard_claim,
                database=database,
                schema=postgres.schema,
            )
        )
    return tuple(shard_claims), tuple(child_leases)
async def _claimed_child_context(postgres, label: str):
    """Claim one projection shard and its exact retained child lease."""
    context = await _registered_projection_context(postgres, label)
    lease, retained_binding, admission_block, _shard, admission_id = context
    shard_claim = await claim_projection_shard(
        lease,
        admission_id=admission_id,
        database=postgres.database,
        schema=postgres.schema,
    )
    assert shard_claim is not None
    child_lease = await child_read.claim_projection_child_read_lease(
        shard_claim,
        database=postgres.database,
        schema=postgres.schema,
    )
    return retained_binding, admission_block, shard_claim, child_lease
def test_child_contract_rejects_noncanonical_shard_identity() -> None:
    lease = _child_lease()
    forged_shard = replace(
        lease.shard_claim.shard,
        partition_key=_digest("forged-partition-key"),
    )
    with pytest.raises(
        ProviderDirectoryProjectionError,
        match="provider_directory_projection_child_shard_claim_invalid",
    ):
        validated_child_shard_claim(
            replace(lease.shard_claim, shard=forged_shard)
        )
    for invalid_claim in (object(), replace(lease.shard_claim, recipe_lease=object())):
        with pytest.raises(ProviderDirectoryProjectionError):
            validated_child_shard_claim(invalid_claim)
def test_child_contract_is_exact_and_locator_free() -> None:
    lease = _child_lease()
    assert validated_child_read_lease(lease) == lease
    forbidden_fragments = {"credential", "locator", "path", "secret", "url"}
    exposed_fields = {
        field.name.lower() for field in fields(ProjectionRetainedChildLease)
    }
    assert not any(
        fragment in field_name
        for field_name in exposed_fields
        for fragment in forbidden_fragments
    )
    assert "release_retained_campaign_consumer" not in child_read.__dict__
    with pytest.raises(
        ProviderDirectoryProjectionError,
        match="provider_directory_projection_child_read_lease_invalid",
    ):
        validated_child_read_lease(
            replace(lease, retained_consumer_recipe_id=" projection-child-test ")
        )
    invalid_leases = (
        object(),
        replace(lease, child_generation=0),
        replace(lease, raw_byte_start=-1),
        replace(lease, binding_id=lease.binding_id.upper()),
    )
    for invalid_lease in invalid_leases:
        with pytest.raises(ProviderDirectoryProjectionError):
            validated_child_read_lease(invalid_lease)
def test_child_database_identity_rejects_stale_generation() -> None:
    lease = _child_lease()
    row = child_lease_database_identity(lease)
    assert child_row_matches_lease(row, lease)
    assert not child_row_matches_lease(
        {**row, "child_generation": lease.child_generation + 1},
        lease,
    )
@pytest.mark.asyncio
async def test_child_context_releases_only_child_on_normal_and_error_exit(
    monkeypatch,
) -> None:
    lease = _child_lease()
    released_leases, verified_leases = [], []
    async def fake_release(released_lease, **_kwargs):
        if not released_leases:
            assert verified_leases == [lease]
        released_leases.append(released_lease)
    monkeypatch.setattr(
        child_read, "claim_projection_child_read_lease", AsyncMock(return_value=lease)
    )
    monkeypatch.setattr(
        child_read,
        "assert_verified_projection_child_read_lease",
        AsyncMock(side_effect=lambda verified_lease, **_kwargs:
                  verified_leases.append(verified_lease)),
    )
    monkeypatch.setattr(child_read, "release_projection_child_read_lease", fake_release)
    async with child_read.claimed_projection_child_read_lease(
        lease.shard_claim
    ) as claimed:
        assert claimed == lease
    assert released_leases == [lease]
    assert verified_leases == [lease]
    with pytest.raises(RuntimeError, match="fixture-body-failed"):
        async with child_read.claimed_projection_child_read_lease(
            lease.shard_claim
        ):
            raise RuntimeError("fixture-body-failed")
    assert released_leases == [lease, lease]
    assert verified_leases == [lease]
    monkeypatch.setattr(
        child_read,
        "release_projection_child_read_lease",
        AsyncMock(side_effect=ValueError("cleanup-failed")),
    )
    with pytest.raises(RuntimeError, match="body-error-preserved"):
        async with child_read.claimed_projection_child_read_lease(lease.shard_claim):
            raise RuntimeError("body-error-preserved")
@pytest.mark.asyncio
async def test_child_dependency_and_action_failures_are_fenced() -> None:
    claim = _child_lease().shard_claim
    admission_by_field = {
        "retained_campaign_id": _digest("campaign"),
        "retained_consumer_recipe_id": "projection-child-test",
        "retained_campaign_sha256": _digest("campaign-proof"),
        "claim_generation": 1,
    }
    binding_by_field = {
        "retained_source_item_id": _digest("source-item"),
        "retained_artifact_sha256": _digest("artifact"),
        "retained_layout_sha256": _digest("layout"),
        "retained_range_ordinal": None,
    }
    missing_row_operations = (
        child_lock._lock_recipe(_unit_database(), "fixture", claim, False),
        child_lock._lock_admission(_unit_database(), "fixture", claim),
        child_lock._lock_shard(_unit_database(), "fixture", claim, False),
        child_lock._lock_binding(_unit_database(), "fixture", claim),
        child_lock._lock_campaign_and_parent(
            _unit_database(), "fixture", admission_by_field, binding_by_field
        ),
        child_lock._lock_artifact_layout(
            _unit_database(), "fixture", binding_by_field
        ),
    )
    for missing_row_operation in missing_row_operations:
        with pytest.raises(ProviderDirectoryProjectionLeaseLost):
            await missing_row_operation
    with pytest.raises(ProviderDirectoryProjectionLeaseLost):
        child_lock._validate_block_mapping(
            {"expected_byte_count": 20, "expected_record_count": 2,
             "expected_payload_sha256": _digest("raw-input")},
            {"payload_bytes": 21, "record_count": 2, "decoded_resource_count": 2,
             "payload_sha256": _digest("raw-input"),
             "content_sha256": _digest("canonical-input")},
            {"manifest_sha256": _digest("canonical-input"),
             "artifact_record_count": 2}, {}, True,
        )
    with pytest.raises(ProviderDirectoryProjectionError):
        child_persistence._validated_lease_seconds(29)
    with pytest.raises(ProviderDirectoryProjectionError):
        await child_persistence._set_child_action(
            _unit_database(), "unknown", claim, 1, _digest("child-lease")
        )
    with pytest.raises(ProviderDirectoryProjectionError):
        await child_persistence._set_child_action(
            _unit_database(scalar_result="wrong"),
            "claim", claim, 1, _digest("child-lease"),
        )
