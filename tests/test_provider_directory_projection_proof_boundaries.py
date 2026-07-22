# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Adversarial shard, reducer, and physical-proof boundary tests."""

from __future__ import annotations

from copy import deepcopy
from dataclasses import replace

import pytest

from process.provider_directory_projection_contract import (
    _basic_physical_projection_proof,
    _resource_counts,
    _validated_reducer_proof_map,
    canonical_row_digest,
    prepare_projection_proof_shard,
    projection_reducer_proof,
    projection_shard_spec,
    reduced_physical_projection_proof,
)
from process.provider_directory_projection_types import (
    ProviderDirectoryProjectionError,
    sorted_unique_texts,
)
from tests.provider_directory_projection_semantic_support import (
    digest,
    semantic_pair,
)
from tests.test_provider_directory_projection_reduced_contract import (
    _candidate_inputs,
)
from tests.test_provider_directory_projection_workset_contract import _workset


def test_shard_and_row_boundaries_reject_invalid_or_ambiguous_work():
    recipe, _lease, block, _admission, _shard = _workset()
    with pytest.raises(ProviderDirectoryProjectionError, match="coordinate_invalid"):
        projection_shard_spec(
            recipe=recipe.physical,
            partition_ordinal=-1,
            input_block=block,
        )
    resource, _contribution = semantic_pair("Organization", "o-1")
    with pytest.raises(ProviderDirectoryProjectionError, match="partition_empty"):
        canonical_row_digest(())
    with pytest.raises(ProviderDirectoryProjectionError, match="strictly_sorted"):
        canonical_row_digest((resource, resource))


def test_proof_shard_rejects_conflicts_and_resource_scope_drift():
    recipe, _lease, block, _admission, _shard = _workset()
    organization, _contribution = semantic_pair("Organization", "o-1")
    duplicate = deepcopy(organization)
    duplicate["payload_json"] = {"resourceType": "Organization", "id": "changed"}
    with pytest.raises(ProviderDirectoryProjectionError, match="duplicate_conflict"):
        prepare_projection_proof_shard(
            (organization, duplicate),
            recipe=recipe,
            attempt=1,
            partition_ordinal=0,
            resource_type="Organization",
            input_sha256=block.content_sha256,
        )
    practitioner, _contribution = semantic_pair("Practitioner", "p-1")
    with pytest.raises(ProviderDirectoryProjectionError, match="resource_mismatch"):
        prepare_projection_proof_shard(
            (practitioner,),
            recipe=recipe,
            attempt=1,
            partition_ordinal=0,
            resource_type="Organization",
            input_sha256=block.content_sha256,
        )
    with pytest.raises(ProviderDirectoryProjectionError, match="coordinate_invalid"):
        prepare_projection_proof_shard(
            (organization,),
            recipe=recipe,
            attempt=0,
            partition_ordinal=0,
            resource_type="Organization",
            input_sha256=block.content_sha256,
        )
    shard, ordered_resources = prepare_projection_proof_shard(
        (organization, deepcopy(organization)),
        recipe=recipe,
        attempt=1,
        partition_ordinal=0,
        resource_type="Organization",
        input_sha256=block.content_sha256,
        producer_proof={"worker": "rust-v1"},
    )
    assert len(ordered_resources) == 1
    assert shard.proof["producer_proof"] == {"worker": "rust-v1"}


def test_physical_proof_rejects_empty_duplicate_or_cross_recipe_shards():
    recipe, shard, _outcome, _counts, _reducer = _candidate_inputs()
    with pytest.raises(ProviderDirectoryProjectionError, match="shards_empty"):
        _basic_physical_projection_proof(
            recipe,
            (),
            dataset_hash=digest("dataset"),
        )
    with pytest.raises(ProviderDirectoryProjectionError, match="shard_duplicate"):
        _resource_counts(recipe.physical, (shard, shard))
    with pytest.raises(ProviderDirectoryProjectionError, match="recipe_mismatch"):
        _resource_counts(
            recipe.physical,
            (replace(shard, recipe_id=digest("other-recipe")),),
        )
    with pytest.raises(ProviderDirectoryProjectionError, match="counts_missing"):
        _resource_counts(recipe.physical, (replace(shard, proof={}),))
    with pytest.raises(ProviderDirectoryProjectionError, match="selected_resource"):
        _resource_counts(
            recipe.physical,
            (replace(shard, resource_type="UnsupportedResource"),),
        )


def test_physical_proof_requires_selected_and_required_resource_counts():
    recipe, shard, _outcome, _counts, _reducer = _candidate_inputs()
    invalid_proof_map = dict(shard.proof)
    invalid_proof_map["resource_counts"] = {"Unknown": shard.resource_count}
    with pytest.raises(ProviderDirectoryProjectionError, match="counts_invalid"):
        _resource_counts(recipe.physical, (replace(shard, proof=invalid_proof_map),))

    one_recipe, _lease, block, _admission, _shard = _workset()
    organization, _contribution = semantic_pair("Organization", "o-1")
    one_shard, _ordered_resources = prepare_projection_proof_shard(
        (organization,),
        recipe=one_recipe,
        attempt=1,
        partition_ordinal=0,
        resource_type="Organization",
        input_sha256=block.content_sha256,
    )
    empty_proof_map = dict(one_shard.proof)
    empty_proof_map["resource_counts"] = {"Organization": 0}
    with pytest.raises(ProviderDirectoryProjectionError, match="required_resource_empty"):
        _resource_counts(
            one_recipe.physical,
            (replace(one_shard, resource_count=0, proof=empty_proof_map),),
        )


def test_reducer_and_reduced_projection_reject_untyped_or_inconsistent_counts():
    recipe, shard, outcome_proof, resource_count_by_type, reducer_proof_map = (
        _candidate_inputs()
    )
    with pytest.raises(ProviderDirectoryProjectionError, match="reducer_proof_invalid"):
        projection_reducer_proof(outcome_proof, ())
    with pytest.raises(ProviderDirectoryProjectionError, match="reducer_proof_invalid"):
        projection_reducer_proof(outcome_proof, {"Organization": True})
    with pytest.raises(ProviderDirectoryProjectionError, match="reducer_proof_invalid"):
        projection_reducer_proof(outcome_proof, {"Organization": 1})
    with pytest.raises(ProviderDirectoryProjectionError, match="resource_mismatch"):
        reduced_physical_projection_proof(
            recipe,
            (shard,),
            dataset_hash=digest("dataset"),
            canonical_row_sha256=outcome_proof.canonical_row_sha256,
            resource_counts=(),
            reducer_proof=reducer_proof_map,
            outcome_proof=outcome_proof,
        )

    with pytest.raises(ProviderDirectoryProjectionError, match="reducer_proof_invalid"):
        _validated_reducer_proof_map(
            None,
            outcome_proof.canonical_row_sha256,
            resource_count_by_type,
            outcome_proof,
        )
    with pytest.raises(ProviderDirectoryProjectionError, match="reducer_proof_invalid"):
        _validated_reducer_proof_map(
            {"resource_counts": []},
            outcome_proof.canonical_row_sha256,
            resource_count_by_type,
            outcome_proof,
        )
    invalid_count_by_resource = dict(resource_count_by_type)
    invalid_count_by_resource["Organization"] = True
    with pytest.raises(ProviderDirectoryProjectionError, match="resource_mismatch"):
        reduced_physical_projection_proof(
            recipe,
            (shard,),
            dataset_hash=digest("dataset"),
            canonical_row_sha256=outcome_proof.canonical_row_sha256,
            resource_counts=invalid_count_by_resource,
            reducer_proof=reducer_proof_map,
            outcome_proof=outcome_proof,
        )
    with pytest.raises(ProviderDirectoryProjectionError, match="resource_mismatch"):
        reduced_physical_projection_proof(
            recipe,
            (shard,),
            dataset_hash=digest("dataset"),
            canonical_row_sha256=outcome_proof.canonical_row_sha256,
            resource_counts={"Organization": 1},
            reducer_proof=reducer_proof_map,
            outcome_proof=outcome_proof,
        )


def test_empty_selected_resource_set_is_never_a_valid_recipe_profile():
    with pytest.raises(ProviderDirectoryProjectionError, match="selected_resource_empty"):
        sorted_unique_texts((), "selected_resource", limit=64)
