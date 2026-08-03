# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Adversarial proofs for forged physical and admission workset fields."""

from copy import deepcopy
from dataclasses import replace

import pytest

from process.provider_directory_projection_contract import (
    projection_shard_spec,
    validated_physical_projection_recipe_identity,
    validated_projection_recipe_identity,
)
from process.provider_directory_projection_admission import (
    projection_admission_consumer_id,
    projection_admission_identity,
)
from process.provider_directory_projection_types import (
    ProviderDirectoryProjectionError,
)
from process.provider_directory_projection_workset import _normalized_workset
from tests.test_provider_directory_projection_workset_contract import (
    _admission_block,
    _digest,
    _manifest,
    _recipe,
    _workset,
)

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
