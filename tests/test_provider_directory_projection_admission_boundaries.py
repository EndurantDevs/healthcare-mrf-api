# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Adversarial logical-admission census and stream boundary tests."""

from __future__ import annotations

from dataclasses import replace

import pytest

import process.provider_directory_projection_admission_contract as admission_contract
from process.provider_directory_projection_admission_contract import (
    _admission_outcome_kind,
    _binding_set_sha256,
    admission_registration_inputs,
    projection_admission_identity,
)
from process.provider_directory_projection_contract import (
    projection_admission_input_block,
    projection_input_block,
    projection_shard_spec,
)
from process.provider_directory_projection_types import (
    ProviderDirectoryProjectionError,
)
from tests.provider_directory_projection_admission_v2_support import (
    CAMPAIGN_ID,
    CAMPAIGN_SHA256,
    POSITIVE_PARTITION,
    ZERO_PARTITION,
    _binding,
    _block,
    _digest,
    _manifest,
    _partition,
    _recipe,
    _stream,
    _terminal_zero,
)
from tests.test_provider_directory_projection_admission_v2 import (
    _ordered_admission,
)


def test_admission_requires_at_least_one_logical_binding():
    block = _block()
    recipe = _recipe(
        (block,),
        _manifest(
            _partition(
                POSITIVE_PARTITION,
                0,
                block_count=1,
                row_count=2,
                byte_count=128,
            )
        ),
    )
    with pytest.raises(ProviderDirectoryProjectionError, match="bindings_empty"):
        projection_admission_identity(recipe, (), claim_generation=1)


def test_admission_rejects_cross_campaign_logical_bindings():
    block = _block()
    recipe = _recipe(
        (block,),
        _manifest(
            _partition(
                POSITIVE_PARTITION,
                0,
                block_count=2,
                row_count=4,
                byte_count=256,
            )
        ),
    )
    first = _binding(block, source_item="first", range_ordinal=0)
    second = projection_admission_input_block(
        block,
        retained_campaign_id=_digest("other-campaign"),
        retained_campaign_sha256=CAMPAIGN_SHA256,
        retained_source_item_id=_digest("second"),
        retained_range_ordinal=1,
        stream_identity_sha256=_digest("stream:second"),
        sequence_ordinal=0,
        resource_type="Organization",
        partition_key_hash=POSITIVE_PARTITION,
        source_partition_ordinal=0,
    )
    with pytest.raises(ProviderDirectoryProjectionError, match="campaign_mismatch"):
        projection_admission_identity(
            recipe,
            (first, second),
            claim_generation=1,
        )


def test_admission_rejects_non_dataclass_positive_and_terminal_bindings():
    block, recipe, binding, terminals = _ordered_admission()
    with pytest.raises(ProviderDirectoryProjectionError, match="block_invalid"):
        projection_admission_identity(
            recipe,
            (object(),),
            claim_generation=1,
            terminal_zeros=terminals,
        )
    with pytest.raises(ProviderDirectoryProjectionError, match="stream_invalid"):
        projection_admission_identity(
            recipe,
            (binding,),
            claim_generation=1,
            terminal_zeros=(object(),),
        )
    assert block.record_count == 2


def test_terminal_stream_bindings_are_unique_across_one_admission():
    _block_value, recipe, binding, terminals = _ordered_admission()
    with pytest.raises(ProviderDirectoryProjectionError, match="binding_duplicate"):
        projection_admission_identity(
            recipe,
            (binding,),
            claim_generation=1,
            terminal_zeros=(terminals[0], terminals[0], terminals[1]),
        )

    duplicate_id = _digest("same-binding")
    with pytest.raises(ProviderDirectoryProjectionError, match="binding_duplicate"):
        _binding_set_sha256(
            ({"binding_id": duplicate_id},),
            ({"binding_id": duplicate_id},),
        )


def test_ordered_stream_census_requires_every_exact_terminal_coordinate():
    _block_value, recipe, binding, terminals = _ordered_admission()
    with pytest.raises(ProviderDirectoryProjectionError, match="stream_census_mismatch"):
        projection_admission_identity(
            recipe,
            (binding,),
            claim_generation=1,
            terminal_zeros=(terminals[0],),
        )

    wrong_stream = _stream(7)
    mismatched_terminal = _terminal_zero(
        wrong_stream,
        source_item="wrong-positive-terminal",
        partition_hash=POSITIVE_PARTITION,
        source_ordinal=0,
    )
    with pytest.raises(ProviderDirectoryProjectionError, match="stream_census_mismatch"):
        projection_admission_identity(
            recipe,
            (binding,),
            claim_generation=1,
            terminal_zeros=(mismatched_terminal, terminals[1]),
        )


def test_no_input_outcome_requires_zero_counts_and_streams_for_every_partition():
    block = _block()
    recipe = _recipe(
        (block,),
        _manifest(
            _partition(
                POSITIVE_PARTITION,
                0,
                block_count=1,
                row_count=2,
                byte_count=128,
            )
        ),
    )
    with pytest.raises(ProviderDirectoryProjectionError, match="no_input_outcome_invalid"):
        _admission_outcome_kind(recipe, (), ())


def test_admission_input_hash_is_recomputed_from_bound_physical_blocks():
    expected_block = _block()
    manifest = _manifest(
        _partition(
            POSITIVE_PARTITION,
            0,
            block_count=1,
            row_count=2,
            byte_count=128,
        )
    )
    recipe = _recipe((expected_block,), manifest)
    substituted_block = projection_input_block(
        upstream_artifact_id=_digest("substitute-artifact"),
        source_object_id=_digest("substitute-layout"),
        block_kind="ndjson",
        input_contract_id="test-input.v1",
        record_start=0,
        record_count=2,
        content_sha256=_digest("substitute-content"),
        payload_sha256=_digest("substitute-payload"),
        payload_bytes=128,
        summary={"resource_count": 2},
    )
    substituted_binding = _binding(
        substituted_block,
        source_item="substitute",
        range_ordinal=0,
    )
    with pytest.raises(ProviderDirectoryProjectionError, match="manifest_hash_mismatch"):
        projection_admission_identity(
            recipe,
            (substituted_binding,),
            claim_generation=1,
        )


def _all_zero_admission():
    streams = (
        _stream(0, terminal_sequence_ordinal=0),
        _stream(1, terminal_sequence_ordinal=0),
    )
    manifest = _manifest(
        _partition(
            POSITIVE_PARTITION,
            0,
            block_count=0,
            row_count=0,
            byte_count=0,
            retained_stream=streams[0],
        ),
        _partition(
            ZERO_PARTITION,
            1,
            block_count=0,
            row_count=0,
            byte_count=0,
            retained_stream=streams[1],
        ),
    )
    recipe = _recipe((), manifest)
    terminals = tuple(
        _terminal_zero(
            stream,
            source_item=f"zero-terminal-{ordinal}",
            partition_hash=(POSITIVE_PARTITION, ZERO_PARTITION)[ordinal],
            source_ordinal=ordinal,
        )
        for ordinal, stream in enumerate(streams)
    )
    return recipe, terminals


def test_registration_validates_lease_bounds_before_persistence():
    block = _block()
    recipe = _recipe(
        (block,),
        _manifest(
            _partition(
                POSITIVE_PARTITION,
                0,
                block_count=1,
                row_count=2,
                byte_count=128,
            )
        ),
    )
    binding = _binding(block, source_item="item", range_ordinal=0)
    shard = projection_shard_spec(
        recipe=recipe.physical,
        partition_ordinal=0,
        input_block=block,
    )
    with pytest.raises(ProviderDirectoryProjectionError, match="lease_seconds_invalid"):
        admission_registration_inputs(recipe, (binding,), (shard,), 1, 29)


def test_registration_cross_checks_normalized_workset_input_hash(monkeypatch):
    block, recipe, binding, terminals = _ordered_admission()
    shard = projection_shard_spec(
        recipe=recipe.physical,
        partition_ordinal=0,
        input_block=block,
    )

    def mismatched_workset(*_args, **_kwargs):
        return [], [], _digest("wrong-input-set"), _digest("shard-set")

    monkeypatch.setattr(admission_contract, "_normalized_workset", mismatched_workset)
    with pytest.raises(ProviderDirectoryProjectionError, match="manifest_hash_mismatch"):
        admission_registration_inputs(
            recipe,
            (binding,),
            (shard,),
            1,
            900,
            terminal_zeros=terminals,
        )


def test_no_input_registration_rejects_any_physical_shard():
    recipe, terminals = _all_zero_admission()
    unrelated_block = _block()
    unrelated_recipe = _recipe(
        (unrelated_block,),
        _manifest(
            _partition(
                POSITIVE_PARTITION,
                0,
                block_count=1,
                row_count=2,
                byte_count=128,
            )
        ),
    )
    unrelated_shard = projection_shard_spec(
        recipe=unrelated_recipe.physical,
        partition_ordinal=0,
        input_block=unrelated_block,
    )
    with pytest.raises(ProviderDirectoryProjectionError, match="no_input_outcome_invalid"):
        admission_registration_inputs(
            recipe,
            (),
            (unrelated_shard,),
            1,
            900,
            terminal_zeros=terminals,
        )


def test_forged_terminal_dataclass_is_rebuilt_before_stream_census_use():
    _block_value, recipe, binding, terminals = _ordered_admission()
    forged = replace(terminals[0], stream_ordinal=9)
    with pytest.raises(ProviderDirectoryProjectionError, match="stream_invalid"):
        projection_admission_identity(
            recipe,
            (binding,),
            claim_generation=1,
            terminal_zeros=(forged, terminals[1]),
        )
