# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Pure adversarial proofs for physical worksets and campaign admissions."""

from __future__ import annotations

from copy import deepcopy
from dataclasses import replace
import hashlib

import pytest

from process.provider_directory_projection_admission import (
    projection_admission_consumer_id,
    projection_admission_identity,
)
from process.provider_directory_projection_contract import (
    projection_admission_input_block,
    projection_completeness_manifest,
    projection_input_block,
    projection_input_set_sha256,
    projection_recipe_identity,
    projection_shard_spec,
    validated_physical_projection_recipe_identity,
    validated_projection_completeness_manifest,
    validated_projection_recipe_identity,
)
from process.provider_directory_projection_types import (
    PROJECTION_MIXED_RESOURCE_TYPE,
    ProjectionCompletenessManifest,
    ProjectionLease,
    ProviderDirectoryProjectionError,
    stable_hash,
)
from process.provider_directory_projection_workset import _normalized_workset


def _digest(label: str) -> str:
    return hashlib.sha256(label.encode("utf-8")).hexdigest()


CAMPAIGN_ID = _digest("campaign-id")
CAMPAIGN_SHA256 = _digest("campaign-proof")
PARTITION_KEY_HASH = _digest("partition")


def _manifest(
    *,
    endpoint_campaign_hash: str = CAMPAIGN_SHA256,
    partition_key: str = PARTITION_KEY_HASH,
    source_partition_ordinal: int = 0,
    block_count: int = 1,
    row_count: int = 2,
    byte_count: int = 128,
    terminal_partitions=None,
):
    terminal_census = terminal_partitions or (
        {
            "resource_type": "Organization",
            "partition_key_hash": partition_key,
            "source_partition_ordinal": source_partition_ordinal,
            "terminal_cursor_hmac": _digest("terminal-cursor"),
            "terminal": True,
            "block_count": block_count,
            "row_count": row_count,
            "byte_count": byte_count,
        },
    )
    return projection_completeness_manifest(
        endpoint_campaign_hash=endpoint_campaign_hash,
        partition_strategy_contract_id="test-partitions.v1",
        selected_resources=("Organization",),
        required_resources=("Organization",),
        terminal_partitions=terminal_census,
        complete=True,
    )


def _physical_block(
    label: str = "one",
    *,
    record_start: int = 0,
    record_count: int = 2,
    payload_bytes: int = 128,
):
    return projection_input_block(
        upstream_artifact_id=_digest(f"artifact:{label}"),
        source_object_id=_digest(f"object:{label}"),
        block_kind="ndjson",
        input_contract_id="test-input.v1",
        record_start=record_start,
        record_count=record_count,
        content_sha256=_digest(f"canonical-content:{label}"),
        payload_sha256=_digest(f"retained-payload:{label}"),
        payload_bytes=payload_bytes,
        summary={"resource_count": record_count},
    )


def _admission_block(
    block,
    *,
    campaign_id: str = CAMPAIGN_ID,
    campaign_sha256: str = CAMPAIGN_SHA256,
    source_item_id: str | None = None,
    retained_range_ordinal: int | None = 0,
    resource_type: str = "Organization",
    partition_key_hash: str = PARTITION_KEY_HASH,
    source_partition_ordinal: int = 0,
):
    return projection_admission_input_block(
        block,
        retained_campaign_id=campaign_id,
        retained_campaign_sha256=campaign_sha256,
        retained_source_item_id=source_item_id or _digest(
            f"source-item:{block.block_id}"
        ),
        retained_range_ordinal=retained_range_ordinal,
        resource_type=resource_type,
        partition_key_hash=partition_key_hash,
        source_partition_ordinal=source_partition_ordinal,
    )


def _recipe(
    blocks,
    completeness,
    *,
    acquisition_adapter_id: str = "test-acquisition.v1",
    source_ids=("source-a",),
):
    return projection_recipe_identity(
        decoder_contract_id="test-decoder.v1",
        acquisition_adapter_id=acquisition_adapter_id,
        input_set_sha256=projection_input_set_sha256(
            blocks,
            decoder_contract_id="test-decoder.v1",
        ),
        source_ids=source_ids,
        transform_contract_id="test-transform.v1",
        scope_contract_id="healthporta.provider-directory.global-scope.v1",
        transform_context={
            "as_of_date": "2026-07-22",
            "time_rule_contract_id": "test-time-rules.v1",
        },
        selected_resources=("Organization",),
        required_resources=("Organization",),
        completeness_manifest=completeness,
    )


def _shards(recipe, blocks):
    return tuple(
        projection_shard_spec(
            recipe=recipe.physical,
            partition_ordinal=ordinal,
            input_block=block,
        )
        for ordinal, block in enumerate(sorted(blocks, key=lambda item: item.block_id))
    )


def _workset(*, manifest=None):
    block = _physical_block()
    completeness = manifest or _manifest()
    recipe = _recipe((block,), completeness)
    admission_block = _admission_block(block)
    shard = _shards(recipe, (block,))[0]
    lease = ProjectionLease(
        recipe=recipe.physical,
        attempt=1,
        lease_token=_digest("lease"),
    )
    return recipe, lease, block, admission_block, shard


def _multi_block_workset():
    first_block = _physical_block()
    second_block = _physical_block(
        "two",
        record_start=2,
        record_count=3,
        payload_bytes=192,
    )
    blocks = (second_block, first_block)
    completeness = _manifest(block_count=2, row_count=5, byte_count=320)
    recipe = _recipe(blocks, completeness)
    admissions = (
        _admission_block(
            second_block,
            source_item_id=_digest("source-item:two"),
            retained_range_ordinal=1,
        ),
        _admission_block(
            first_block,
            source_item_id=_digest("source-item:one"),
            retained_range_ordinal=0,
        ),
    )
    return recipe, blocks, admissions, _shards(recipe, blocks)


def _rehash(proof):
    return stable_hash(
        proof,
        domain="provider-directory-projection-completeness-manifest-v1",
    )


def test_manifest_hash_is_recomputed_instead_of_trusted():
    completeness = _manifest()
    forged = replace(completeness, manifest_sha256=_digest("forged"))

    with pytest.raises(
        ProviderDirectoryProjectionError,
        match="completeness_manifest_hash_mismatch",
    ):
        validated_projection_completeness_manifest(forged)


@pytest.mark.parametrize(
    ("path", "forged_value"),
    (
        (("complete",), "true"),
        (("block_count",), "1"),
        (("terminal_partitions", 0, "terminal"), "true"),
        (("terminal_partitions", 0, "row_count"), "2"),
    ),
)
def test_manifest_rejects_string_booleans_and_counts(path, forged_value):
    proof = deepcopy(_manifest().proof)
    target = proof
    for key in path[:-1]:
        target = target[key]
    target[path[-1]] = forged_value
    forged = ProjectionCompletenessManifest(_rehash(proof), proof)

    with pytest.raises(ProviderDirectoryProjectionError):
        validated_projection_completeness_manifest(forged)


@pytest.mark.parametrize("mutation", ("extra", "missing"))
def test_manifest_rejects_noncanonical_fields_even_with_matching_hash(mutation):
    proof = deepcopy(_manifest().proof)
    if mutation == "extra":
        proof["unbound_claim"] = True
    else:
        del proof["partition_count"]
    forged = ProjectionCompletenessManifest(_rehash(proof), proof)

    with pytest.raises(
        ProviderDirectoryProjectionError,
        match="completeness_manifest_invalid",
    ):
        validated_projection_completeness_manifest(forged)


def test_workset_is_campaign_blind_and_uses_block_derived_mixed_shard():
    recipe, lease, block, _admission, shard = _workset()

    block_fields, shard_fields, input_hash, shard_hash = _normalized_workset(
        lease.recipe,
        (block,),
        (shard,),
    )

    assert lease.recipe == recipe.physical
    assert block_fields[0]["block_id"] == block.block_id
    assert block_fields[0]["block_ordinal"] == 0
    assert shard_fields[0]["partition_key"] == block.block_id
    assert shard_fields[0]["resource_type"] == PROJECTION_MIXED_RESOURCE_TYPE
    assert input_hash == lease.recipe.input_set_sha256
    assert len(shard_hash) == 64


def test_workset_sorts_blocks_and_shards_by_content_identity():
    recipe, blocks, _admissions, shards = _multi_block_workset()

    block_fields, shard_fields, input_hash, _shard_hash = _normalized_workset(
        recipe.physical,
        blocks,
        tuple(reversed(shards)),
    )

    expected_ids = sorted(block.block_id for block in blocks)
    assert [block["block_id"] for block in block_fields] == expected_ids
    assert [block["block_ordinal"] for block in block_fields] == [0, 1]
    assert [shard["input_block_id"] for shard in shard_fields] == expected_ids
    assert [shard["partition_ordinal"] for shard in shard_fields] == [0, 1]
    assert input_hash == recipe.input_set_sha256


def test_workset_rejects_missing_or_duplicate_physical_identity():
    recipe, blocks, _admissions, shards = _multi_block_workset()

    with pytest.raises(ProviderDirectoryProjectionError, match="workset_invalid"):
        _normalized_workset(recipe.physical, blocks, shards[:1])
    with pytest.raises(ProviderDirectoryProjectionError, match="workset_invalid"):
        _normalized_workset(recipe.physical, blocks[:1], shards)
    with pytest.raises(ProviderDirectoryProjectionError, match="workset_invalid"):
        _normalized_workset(
            recipe.physical,
            (blocks[0], blocks[0]),
            (shards[0],),
        )


def test_workset_rejects_forged_shard_ordinal():
    recipe, blocks, _admissions, shards = _multi_block_workset()
    forged = replace(shards[1], partition_ordinal=shards[0].partition_ordinal)

    with pytest.raises(ProviderDirectoryProjectionError, match="workset_invalid"):
        _normalized_workset(recipe.physical, blocks, (shards[0], forged))


def test_campaign_census_is_enforced_by_admission_not_physical_workset():
    recipe, blocks, admissions, shards = _multi_block_workset()
    orphaned_manifest = _manifest(
        endpoint_campaign_hash=CAMPAIGN_SHA256,
        terminal_partitions=(
            recipe.completeness_manifest["terminal_partitions"][0],
            {
                "resource_type": "Organization",
                "partition_key_hash": _digest("orphan-partition"),
                "source_partition_ordinal": 1,
                "terminal": True,
                "block_count": 1,
                "row_count": 1,
                "byte_count": 64,
            },
        ),
    )
    orphaned_recipe = _recipe(blocks, orphaned_manifest)

    _normalized_workset(orphaned_recipe.physical, blocks, shards)
    with pytest.raises(
        ProviderDirectoryProjectionError,
        match="admission_census_mismatch",
    ):
        projection_admission_identity(
            orphaned_recipe,
            admissions,
            claim_generation=1,
        )


@pytest.mark.parametrize(
    "manifest",
    (
        _manifest(source_partition_ordinal=1),
        _manifest(block_count=2),
        _manifest(row_count=3),
        _manifest(byte_count=129),
        _manifest(partition_key=_digest("other-terminal-coordinate")),
    ),
)
def test_admission_rejects_terminal_coordinate_or_count_mismatch(manifest):
    recipe, _lease, _block, admission, _shard = _workset(manifest=manifest)

    with pytest.raises(
        ProviderDirectoryProjectionError,
        match="admission_census_mismatch",
    ):
        projection_admission_identity(recipe, (admission,), claim_generation=1)


def test_admission_accepts_exact_multi_block_terminal_census():
    recipe, _blocks, admissions, _shards = _multi_block_workset()

    identity = projection_admission_identity(
        recipe,
        admissions,
        claim_generation=1,
    )

    assert identity.input_block_count == 2
    assert identity.retained_campaign_sha256 == CAMPAIGN_SHA256


def test_admission_rejects_endpoint_campaign_proof_mismatch():
    recipe, _lease, _block, admission, _shard = _workset(
        manifest=_manifest(endpoint_campaign_hash=_digest("different-campaign"))
    )

    with pytest.raises(
        ProviderDirectoryProjectionError,
        match="admission_campaign_proof_mismatch",
    ):
        projection_admission_identity(recipe, (admission,), claim_generation=1)


def test_workset_rejects_forged_shard_input_hash():
    _recipe_value, lease, block, _admission, shard = _workset()
    forged_shard = replace(shard, input_sha256=_digest("different-input"))

    with pytest.raises(ProviderDirectoryProjectionError, match="workset_invalid"):
        _normalized_workset(lease.recipe, (block,), (forged_shard,))


@pytest.mark.parametrize(
    ("forged_field", "replacement"),
    (
        ("block_id", _digest("forged-block-id")),
        ("upstream_artifact_id", _digest("forged-artifact")),
        ("source_object_id", _digest("forged-object")),
        ("block_kind", "forged-kind"),
        ("input_contract_id", "forged-input.v1"),
        ("record_start", 1),
        ("record_count", 3),
        ("content_sha256", _digest("forged-content")),
        ("payload_sha256", _digest("forged-payload")),
        ("payload_bytes", 129),
        ("summary", {"resource_count": 3}),
        ("summary", {"resource_count": True}),
        ("block_proof_sha256", _digest("forged-block-proof")),
    ),
)
def test_workset_rejects_forged_public_input_block_fields(
    forged_field,
    replacement,
):
    _recipe_value, lease, block, _admission, shard = _workset()
    forged_block = replace(block, **{forged_field: replacement})

    with pytest.raises(ProviderDirectoryProjectionError):
        _normalized_workset(lease.recipe, (forged_block,), (shard,))


@pytest.mark.parametrize(
    ("forged_field", "replacement"),
    (
        ("partition_id", _digest("forged-partition-id")),
        ("partition_ordinal", 1),
        ("partition_key", _digest("forged-partition-key")),
        ("input_block_id", _digest("forged-input-block")),
        ("resource_type", "Organization"),
        ("input_sha256", _digest("forged-shard-input")),
    ),
)
def test_workset_rejects_forged_public_shard_fields(forged_field, replacement):
    _recipe_value, lease, block, _admission, shard = _workset()
    forged_shard = replace(shard, **{forged_field: replacement})

    with pytest.raises(ProviderDirectoryProjectionError):
        _normalized_workset(lease.recipe, (block,), (forged_shard,))


@pytest.mark.parametrize(
    "forged_field",
    (
        "recipe_id",
        "decoder_contract_id",
        "input_set_sha256",
        "transform_contract_id",
        "scope_contract_id",
        "transform_context_hash",
        "transform_context",
        "resource_profile_hash",
        "selected_resources",
        "required_resources",
    ),
)
def test_physical_recipe_rejects_forged_public_dataclass_fields(forged_field):
    recipe, _lease, _block, _admission, _shard = _workset()
    physical = recipe.physical
    forged_transform_context = deepcopy(physical.transform_context)
    forged_transform_context["as_of_date"] = "2026-07-23"
    replacement_by_field = {
        "recipe_id": _digest("forged:recipe_id"),
        "decoder_contract_id": "forged-decoder.v1",
        "input_set_sha256": _digest("forged:input_set_sha256"),
        "transform_contract_id": "forged-transform.v1",
        "scope_contract_id": "forged-scope.v1",
        "transform_context_hash": _digest("forged:transform_context_hash"),
        "transform_context": forged_transform_context,
        "resource_profile_hash": _digest("forged:resource_profile_hash"),
        "selected_resources": ("Organization", "Practitioner"),
        "required_resources": (),
    }
    forged_recipe = replace(
        physical,
        **{forged_field: replacement_by_field[forged_field]},
    )

    with pytest.raises(ProviderDirectoryProjectionError):
        validated_physical_projection_recipe_identity(forged_recipe)


def test_combined_recipe_rejects_forged_admission_context():
    recipe, _lease, _block, _admission, _shard = _workset()
    forged_manifest = deepcopy(recipe.completeness_manifest)
    forged_manifest["endpoint_campaign_hash"] = _digest("forged-campaign")
    forged = replace(recipe, completeness_manifest=forged_manifest)

    with pytest.raises(ProviderDirectoryProjectionError):
        validated_projection_recipe_identity(forged)


def test_shard_boundary_rejects_forged_physical_recipe_id():
    recipe, _lease, block, _admission, shard = _workset()
    forged_recipe = replace(recipe.physical, recipe_id="0" * 64)

    with pytest.raises(
        ProviderDirectoryProjectionError,
        match="physical_recipe_mismatch",
    ):
        projection_shard_spec(
            recipe=forged_recipe,
            partition_ordinal=shard.partition_ordinal,
            input_block=block,
        )


def test_campaign_evidence_does_not_change_physical_recipe_identity():
    first_recipe, _lease, block, _admission, _shard = _workset()
    second_manifest = _manifest(
        endpoint_campaign_hash=_digest("second-campaign-proof")
    )
    second_recipe = _recipe(
        (block,),
        second_manifest,
        acquisition_adapter_id="different-acquisition-adapter.v2",
        source_ids=("source-b",),
    )

    assert second_recipe.physical == first_recipe.physical
    assert second_recipe.recipe_id == first_recipe.recipe_id
    assert (
        second_recipe.completeness_manifest_hash
        != first_recipe.completeness_manifest_hash
    )


@pytest.mark.parametrize(
    ("forged_field", "replacement"),
    (
        ("retained_campaign_id", "not-a-digest"),
        ("retained_campaign_sha256", "not-a-digest"),
        ("retained_source_item_id", "not-a-digest"),
        ("retained_range_ordinal", True),
        ("resource_type", ""),
        ("partition_key_hash", "not-a-digest"),
        ("source_partition_ordinal", True),
    ),
)
def test_admission_rejects_forged_binding_fields(forged_field, replacement):
    recipe, _lease, _block, admission, _shard = _workset()
    forged = replace(admission, **{forged_field: replacement})

    with pytest.raises(ProviderDirectoryProjectionError):
        projection_admission_identity(recipe, (forged,), claim_generation=1)


def test_campaign_and_retained_bindings_change_admission_not_physical_identity():
    first_recipe, _lease, block, first_block, _shard = _workset()
    first_admission = projection_admission_identity(
        first_recipe,
        (first_block,),
        claim_generation=1,
    )
    replacement_sha = _digest("replacement-campaign-proof")
    rebound_block = _admission_block(
        block,
        campaign_id=_digest("replacement-campaign"),
        campaign_sha256=replacement_sha,
        source_item_id=_digest("replacement-source-item"),
    )
    rebound_recipe = _recipe(
        (block,),
        _manifest(endpoint_campaign_hash=replacement_sha),
        acquisition_adapter_id="replacement-acquisition.v1",
        source_ids=("replacement-source",),
    )
    rebound_admission = projection_admission_identity(
        rebound_recipe,
        (rebound_block,),
        claim_generation=1,
    )

    assert rebound_recipe.physical == first_recipe.physical
    assert rebound_admission.admission_id != first_admission.admission_id
    assert rebound_admission.binding_set_sha256 != first_admission.binding_set_sha256
    assert (
        first_admission.retained_consumer_recipe_id
        == projection_admission_consumer_id(first_recipe, (first_block,))
    )
    assert first_admission.retained_consumer_recipe_id != first_admission.admission_id


def test_claim_generation_changes_admission_but_not_preclaim_consumer_identity():
    recipe, _lease, _block, admission_block, _shard = _workset()
    stable_consumer_id = projection_admission_consumer_id(
        recipe,
        (admission_block,),
    )
    first_admission = projection_admission_identity(
        recipe,
        (admission_block,),
        claim_generation=1,
    )
    reclaimed_admission = projection_admission_identity(
        recipe,
        (admission_block,),
        claim_generation=2,
    )

    assert first_admission.retained_consumer_recipe_id == stable_consumer_id
    assert reclaimed_admission.retained_consumer_recipe_id == stable_consumer_id
    assert reclaimed_admission.admission_id != first_admission.admission_id
    assert reclaimed_admission.binding_set_sha256 == first_admission.binding_set_sha256


@pytest.mark.parametrize("claim_generation", (0, -1, True, "1"))
def test_admission_rejects_nonpositive_or_coerced_claim_generation(
    claim_generation,
):
    recipe, _lease, _block, admission_block, _shard = _workset()

    with pytest.raises(
        ProviderDirectoryProjectionError,
        match="admission_claim_generation_invalid",
    ):
        projection_admission_identity(
            recipe,
            (admission_block,),
            claim_generation=claim_generation,
        )
