# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Pure adversarial proofs for projection completeness and workset binding."""

from __future__ import annotations

from copy import deepcopy
from dataclasses import replace
import hashlib

import pytest

from process.provider_directory_projection_contract import (
    projection_completeness_manifest,
    projection_input_block,
    projection_input_set_sha256,
    projection_recipe_identity,
    projection_shard_spec,
)
from process.provider_directory_projection_types import (
    ProjectionCompletenessManifest,
    ProjectionLease,
    ProviderDirectoryProjectionError,
    stable_hash,
)
from process.provider_directory_projection_workset import _normalized_workset


def _digest(label: str) -> str:
    return hashlib.sha256(label.encode("utf-8")).hexdigest()


def _manifest(
    *,
    partition_key: str | None = None,
    source_partition_ordinal: int = 0,
    row_count: int = 2,
):
    return projection_completeness_manifest(
        endpoint_campaign_hash=_digest("endpoint-campaign"),
        partition_strategy_contract_id="test-partitions.v1",
        selected_resources=("Organization",),
        required_resources=("Organization",),
        terminal_partitions=(
            {
                "resource_type": "Organization",
                "partition_key_hash": partition_key or _digest("partition"),
                "source_partition_ordinal": source_partition_ordinal,
                "terminal_cursor_hmac": _digest("terminal-cursor"),
                "terminal": True,
                "block_count": 1,
                "row_count": row_count,
                "byte_count": 128,
            },
        ),
        complete=True,
    )


def _workset(*, manifest=None, shard_partition_key: str | None = None):
    completeness = manifest or _manifest()
    block = projection_input_block(
        block_ordinal=0,
        upstream_artifact_id=_digest("artifact"),
        source_object_id=_digest("object"),
        block_kind="ndjson",
        input_contract_id="test-input.v1",
        source_partition_ordinal=0,
        record_start=0,
        record_count=2,
        content_sha256=_digest("canonical-content"),
        payload_sha256=_digest("retained-payload"),
        payload_bytes=128,
        summary={"resource_count": 2},
        retained_campaign_id=_digest("campaign-id"),
        retained_campaign_sha256=_digest("campaign-proof"),
        retained_source_item_id=_digest("source-item"),
        retained_range_ordinal=0,
    )
    input_set_sha256 = projection_input_set_sha256(
        (block,),
        decoder_contract_id="test-decoder.v1",
        completeness_manifest=completeness,
    )
    recipe = projection_recipe_identity(
        decoder_contract_id="test-decoder.v1",
        acquisition_adapter_id="test-acquisition.v1",
        input_set_sha256=input_set_sha256,
        source_ids=("source-a",),
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
    shard = projection_shard_spec(
        recipe=recipe,
        partition_ordinal=0,
        partition_key=shard_partition_key or _digest("partition"),
        input_block=block,
        resource_type="Organization",
    )
    lease = ProjectionLease(recipe=recipe, attempt=1, lease_token=_digest("lease"))
    return lease, block, shard


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
        projection_input_set_sha256(
            (),
            decoder_contract_id="test-decoder.v1",
            completeness_manifest=forged,
        )


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
        projection_input_set_sha256(
            (),
            decoder_contract_id="test-decoder.v1",
            completeness_manifest=forged,
        )


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
        projection_input_set_sha256(
            (),
            decoder_contract_id="test-decoder.v1",
            completeness_manifest=forged,
        )


def test_workset_binds_exact_terminal_partition_and_counts():
    lease, block, shard = _workset()

    block_fields, shard_fields, input_hash, shard_hash = _normalized_workset(
        lease,
        (block,),
        (shard,),
    )

    assert block_fields[0]["block_id"] == block.block_id
    assert shard_fields[0]["partition_id"] == shard.partition_id
    assert input_hash == lease.recipe.input_set_sha256
    assert len(shard_hash) == 64


@pytest.mark.parametrize(
    "manifest",
    (
        _manifest(source_partition_ordinal=1),
        _manifest(row_count=3),
    ),
)
def test_workset_rejects_terminal_source_ordinal_or_count_mismatch(manifest):
    lease, block, shard = _workset(manifest=manifest)

    with pytest.raises(
        ProviderDirectoryProjectionError,
        match="workset_partition_census_mismatch",
    ):
        _normalized_workset(lease, (block,), (shard,))


def test_workset_rejects_a_valid_shard_from_a_different_partition():
    lease, block, shard = _workset(shard_partition_key=_digest("other-partition"))

    with pytest.raises(
        ProviderDirectoryProjectionError,
        match="workset_partition_census_mismatch",
    ):
        _normalized_workset(lease, (block,), (shard,))


def test_workset_rejects_forged_shard_input_hash():
    lease, block, shard = _workset()
    forged_shard = replace(shard, input_sha256=_digest("different-input"))

    with pytest.raises(
        ProviderDirectoryProjectionError,
        match="workset_invalid",
    ):
        _normalized_workset(lease, (block,), (forged_shard,))


def test_workset_rejects_forged_block_proof_hash():
    lease, block, shard = _workset()
    forged_block = replace(block, block_proof_sha256=_digest("forged-block"))

    with pytest.raises(
        ProviderDirectoryProjectionError,
        match="workset_invalid",
    ):
        _normalized_workset(lease, (forged_block,), (shard,))
