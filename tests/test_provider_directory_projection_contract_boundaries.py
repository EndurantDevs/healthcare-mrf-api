# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Adversarial boundary coverage for source-neutral projection contracts."""

from __future__ import annotations

from copy import deepcopy
from dataclasses import replace

import pytest

from process.provider_directory_projection_contract import (
    _canonical_completeness_counts,
    _canonical_hash,
    _canonical_partition_census,
    _canonical_partition_counts,
    _canonical_resource_list,
    _canonical_retained_stream,
    _canonical_terminal_evidence,
    _canonical_terminal_partition,
    _canonical_text,
    projection_admission_input_block,
    projection_admission_terminal_zero,
    projection_completeness_manifest,
    projection_input_block,
    projection_input_set_sha256,
    projection_recipe_identity,
    validated_physical_projection_recipe_identity,
    validated_projection_completeness_manifest,
    validated_projection_recipe_identity,
)
from process.provider_directory_projection_types import (
    PROJECTION_INPUT_BLOCK_MAX_BYTES,
    ProviderDirectoryProjectionError,
)
from tests.provider_directory_projection_semantic_support import digest
from tests.test_provider_directory_projection_workset_contract import (
    CAMPAIGN_ID,
    CAMPAIGN_SHA256,
    PARTITION_KEY_HASH,
    _manifest,
    _physical_block,
    _workset,
)


@pytest.mark.parametrize("candidate", (None, True, 7))
def test_canonical_hash_rejects_non_string_values(candidate):
    with pytest.raises(ProviderDirectoryProjectionError, match="digest_invalid"):
        _canonical_hash(candidate, "digest")


def test_canonical_hash_and_text_reject_values_that_need_normalization():
    with pytest.raises(ProviderDirectoryProjectionError, match="digest_invalid"):
        _canonical_hash(digest("value").upper(), "digest")
    with pytest.raises(ProviderDirectoryProjectionError, match="label_invalid"):
        _canonical_text(True, "label")
    with pytest.raises(ProviderDirectoryProjectionError, match="label_invalid"):
        _canonical_text(" padded ", "label")


def test_canonical_resource_and_partition_guards_fail_closed():
    with pytest.raises(ProviderDirectoryProjectionError, match="manifest_invalid"):
        _canonical_resource_list(("Organization",), "resource")
    with pytest.raises(ProviderDirectoryProjectionError, match="manifest_invalid"):
        _canonical_resource_list(["Practitioner", "Organization"], "resource")
    with pytest.raises(ProviderDirectoryProjectionError, match="partition_invalid"):
        _canonical_partition_counts(
            {"block_count": 1, "row_count": 0, "byte_count": 0}
        )
    with pytest.raises(ProviderDirectoryProjectionError, match="partition_invalid"):
        _canonical_partition_counts(
            {"block_count": 0, "row_count": 1, "byte_count": 0}
        )
    with pytest.raises(ProviderDirectoryProjectionError, match="partition_invalid"):
        _canonical_terminal_evidence(
            {
                "terminal": True,
                "terminal_cursor_hmac": None,
                "zero_row_proof_sha256": digest("unexpected-zero"),
            },
            1,
        )


def test_canonical_manifest_shape_guards_reject_partial_or_inconsistent_proofs():
    with pytest.raises(ProviderDirectoryProjectionError, match="stream_invalid"):
        _canonical_retained_stream({})
    with pytest.raises(ProviderDirectoryProjectionError, match="partition_invalid"):
        _canonical_terminal_partition({})
    with pytest.raises(ProviderDirectoryProjectionError, match="manifest_invalid"):
        _canonical_partition_census({"terminal_partitions": ()})
    with pytest.raises(ProviderDirectoryProjectionError, match="manifest_invalid"):
        _canonical_completeness_counts(
            {
                "partition_count": 2,
                "block_count": 1,
                "row_count": 1,
                "byte_count": 1,
            },
            [{"block_count": 1, "row_count": 1, "byte_count": 1}],
        )
    with pytest.raises(ProviderDirectoryProjectionError, match="manifest_invalid"):
        validated_projection_completeness_manifest({})


def test_canonical_zero_terminal_stream_proof_must_match_zero_proof():
    terminal_proof = digest("terminal-proof")
    partition_map = {
        "resource_type": "Organization",
        "partition_key_hash": digest("partition"),
        "source_partition_ordinal": 0,
        "terminal_cursor_hmac": None,
        "terminal": True,
        "block_count": 0,
        "row_count": 0,
        "byte_count": 0,
        "zero_row_proof_sha256": digest("different-zero-proof"),
        "retained_stream": {
            "identity_sha256": digest("stream"),
            "stream_ordinal": 0,
            "terminal_sequence_ordinal": 0,
            "terminal_proof_sha256": terminal_proof,
        },
    }
    with pytest.raises(ProviderDirectoryProjectionError, match="partition_invalid"):
        _canonical_partition_census({"terminal_partitions": [partition_map]})


def _recipe_arguments(manifest):
    return {
        "decoder_contract_id": "test-decoder.v1",
        "acquisition_adapter_id": "test-acquisition.v1",
        "input_set_sha256": digest("input-set"),
        "source_ids": ("source-a",),
        "transform_contract_id": "test-transform.v1",
        "scope_contract_id": "test-scope.v1",
        "transform_context": {
            "as_of_date": "2026-07-22",
            "time_rule_contract_id": "test-time.v1",
        },
        "selected_resources": ("Organization",),
        "required_resources": ("Organization",),
        "completeness_manifest": manifest,
    }


def test_recipe_rejects_manifest_profile_drift_and_invalid_time_context():
    arguments = _recipe_arguments(_manifest())
    arguments["selected_resources"] = ("Practitioner",)
    arguments["required_resources"] = ()
    with pytest.raises(ProviderDirectoryProjectionError, match="profile_mismatch"):
        projection_recipe_identity(**arguments)

    arguments = _recipe_arguments(_manifest())
    arguments["transform_context"] = {
        "as_of_date": "not-a-date",
        "time_rule_contract_id": "test-time.v1",
    }
    with pytest.raises(ProviderDirectoryProjectionError, match="as_of_date_invalid"):
        projection_recipe_identity(**arguments)

    arguments["transform_context"] = {
        "as_of_date": "2026-07-22",
        "time_rule_contract_id": "test-time.v1",
        "unbound_timezone": "UTC",
    }
    with pytest.raises(ProviderDirectoryProjectionError, match="context_invalid"):
        projection_recipe_identity(**arguments)


def test_public_recipe_validators_rebuild_types_and_dates():
    recipe, _lease, _block, _admission, _shard = _workset()
    with pytest.raises(ProviderDirectoryProjectionError, match="identity_invalid"):
        validated_projection_recipe_identity(recipe.physical)
    with pytest.raises(ProviderDirectoryProjectionError, match="identity_mismatch"):
        validated_projection_recipe_identity(
            replace(recipe, recipe_id=digest("forged-recipe"))
        )
    with pytest.raises(ProviderDirectoryProjectionError, match="physical_recipe_invalid"):
        validated_physical_projection_recipe_identity(object())
    invalid_context_map = dict(recipe.physical.transform_context)
    invalid_context_map["as_of_date"] = "2026-99-99"
    with pytest.raises(ProviderDirectoryProjectionError, match="as_of_date_invalid"):
        validated_physical_projection_recipe_identity(
            replace(recipe.physical, transform_context=invalid_context_map)
        )


def _input_block_arguments():
    return {
        "upstream_artifact_id": digest("artifact"),
        "source_object_id": digest("object"),
        "block_kind": "ndjson",
        "input_contract_id": "test-input.v1",
        "record_start": 0,
        "record_count": 1,
        "content_sha256": digest("content"),
        "payload_sha256": digest("payload"),
        "payload_bytes": 16,
        "summary": {"resource_count": 1},
    }


@pytest.mark.parametrize(
    ("field_name", "invalid_value", "expected_error"),
    (
        ("record_start", True, "coordinate_invalid"),
        ("record_count", 0, "coordinate_invalid"),
        ("payload_bytes", 0, "coordinate_invalid"),
        ("payload_bytes", PROJECTION_INPUT_BLOCK_MAX_BYTES + 1, "bound_exceeded"),
    ),
)
def test_input_block_enforces_exact_integer_and_capacity_bounds(
    field_name,
    invalid_value,
    expected_error,
):
    arguments = _input_block_arguments()
    arguments[field_name] = invalid_value
    with pytest.raises(ProviderDirectoryProjectionError, match=expected_error):
        projection_input_block(**arguments)


def test_input_block_rejects_negative_or_zero_expanded_resource_counts():
    arguments = _input_block_arguments()
    arguments["summary"] = {"resource_count": -1}
    with pytest.raises(ProviderDirectoryProjectionError, match="summary_invalid"):
        projection_input_block(**arguments)
    arguments["summary"] = {"resource_count": 0}
    with pytest.raises(ProviderDirectoryProjectionError, match="bound_exceeded"):
        projection_input_block(**arguments)


def test_admission_builders_require_exact_coordinates_and_sequence_types():
    block = _physical_block()
    with pytest.raises(TypeError, match="requires resource_type"):
        projection_admission_input_block(
            block,
            retained_campaign_id=CAMPAIGN_ID,
            retained_campaign_sha256=CAMPAIGN_SHA256,
            retained_source_item_id=digest("item"),
            retained_range_ordinal=0,
            stream_identity_sha256=digest("stream"),
            sequence_ordinal=0,
        )
    with pytest.raises(ProviderDirectoryProjectionError, match="coordinate_invalid"):
        projection_admission_input_block(
            block,
            retained_campaign_id=CAMPAIGN_ID,
            retained_campaign_sha256=CAMPAIGN_SHA256,
            retained_source_item_id=digest("item"),
            retained_range_ordinal=0,
            stream_identity_sha256=digest("stream"),
            sequence_ordinal=True,
            resource_type="Organization",
            partition_key_hash=PARTITION_KEY_HASH,
            source_partition_ordinal=0,
        )
    with pytest.raises(ProviderDirectoryProjectionError, match="coordinate_invalid"):
        projection_admission_terminal_zero(
            retained_campaign_id=CAMPAIGN_ID,
            retained_campaign_sha256=CAMPAIGN_SHA256,
            retained_source_item_id=digest("zero-item"),
            resource_type="Organization",
            partition_key_hash=PARTITION_KEY_HASH,
            source_partition_ordinal=True,
            retained_stream={
                "identity_sha256": digest("stream"),
                "stream_ordinal": 0,
                "terminal_sequence_ordinal": 0,
                "terminal_proof_sha256": digest("terminal"),
            },
        )


def test_input_manifest_rejects_duplicate_blocks_and_non_block_values():
    block = _physical_block()
    with pytest.raises(ProviderDirectoryProjectionError, match="manifest_invalid"):
        projection_input_set_sha256(
            (block, block),
            decoder_contract_id="test-decoder.v1",
        )
    with pytest.raises(ProviderDirectoryProjectionError, match="block_invalid"):
        projection_input_set_sha256(
            (object(),),
            decoder_contract_id="test-decoder.v1",
        )
    forged = replace(block, block_id=digest("forged-block-id"))
    with pytest.raises(ProviderDirectoryProjectionError, match="block_invalid"):
        projection_input_set_sha256(
            (forged,),
            decoder_contract_id="test-decoder.v1",
        )


@pytest.mark.parametrize(
    "partition_map",
    (
        {"unknown": True},
        {
            "resource_type": "Practitioner",
            "partition_key_hash": PARTITION_KEY_HASH,
            "terminal": True,
            "block_count": 1,
            "row_count": 1,
            "byte_count": 1,
        },
        {
            "resource_type": "Organization",
            "partition_key_hash": PARTITION_KEY_HASH,
            "terminal": False,
            "block_count": 1,
            "row_count": 1,
            "byte_count": 1,
        },
        {
            "resource_type": "Organization",
            "partition_key_hash": PARTITION_KEY_HASH,
            "terminal": True,
            "block_count": 1,
            "row_count": 0,
            "byte_count": 0,
            "zero_row_proof_sha256": digest("zero"),
        },
        {
            "resource_type": "Organization",
            "partition_key_hash": PARTITION_KEY_HASH,
            "terminal": True,
            "block_count": 1,
            "row_count": 1,
            "byte_count": 1,
            "zero_row_proof_sha256": digest("unexpected-zero"),
        },
        {
            "resource_type": "Organization",
            "partition_key_hash": PARTITION_KEY_HASH,
            "terminal": True,
            "block_count": 0,
            "row_count": 1,
            "byte_count": 0,
        },
    ),
)
def test_completeness_builder_rejects_malformed_terminal_census(partition_map):
    with pytest.raises(ProviderDirectoryProjectionError):
        projection_completeness_manifest(
            endpoint_campaign_hash=CAMPAIGN_SHA256,
            partition_strategy_contract_id="test-partitions.v1",
            selected_resources=("Organization",),
            required_resources=("Organization",),
            terminal_partitions=(partition_map,),
            complete=True,
        )


def test_completeness_builder_requires_complete_nonempty_unique_census():
    with pytest.raises(ProviderDirectoryProjectionError, match="incomplete"):
        projection_completeness_manifest(
            endpoint_campaign_hash=CAMPAIGN_SHA256,
            partition_strategy_contract_id="test-partitions.v1",
            selected_resources=("Organization",),
            required_resources=("Organization",),
            terminal_partitions=(),
            complete=False,
        )
    with pytest.raises(ProviderDirectoryProjectionError, match="census_invalid"):
        projection_completeness_manifest(
            endpoint_campaign_hash=CAMPAIGN_SHA256,
            partition_strategy_contract_id="test-partitions.v1",
            selected_resources=("Organization",),
            required_resources=("Organization",),
            terminal_partitions=(),
            complete=True,
        )
    terminal = deepcopy(_manifest().proof["terminal_partitions"][0])
    with pytest.raises(ProviderDirectoryProjectionError, match="census_invalid"):
        projection_completeness_manifest(
            endpoint_campaign_hash=CAMPAIGN_SHA256,
            partition_strategy_contract_id="test-partitions.v1",
            selected_resources=("Organization",),
            required_resources=("Organization",),
            terminal_partitions=(terminal, terminal),
            complete=True,
        )
