# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Exact logical-census and retained-stream proofs for projection admission v2."""

from __future__ import annotations

from dataclasses import replace

import pytest

from process.provider_directory_projection_admission_contract import (
    admission_registration_inputs,
    projection_admission_identity,
)
from process.provider_directory_projection_contract import projection_shard_spec
from process.provider_directory_projection_types import (
    ProviderDirectoryProjectionError,
)
from tests.provider_directory_projection_admission_v2_support import (
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


def test_distinct_logical_bindings_can_share_one_physical_block():
    block = _block()
    manifest = _manifest(
        _partition(
            POSITIVE_PARTITION,
            0,
            block_count=2,
            row_count=4,
            byte_count=256,
        )
    )
    recipe = _recipe((block,), manifest)
    bindings = (
        _binding(block, source_item="item-a", range_ordinal=0),
        _binding(block, source_item="item-b", range_ordinal=1),
    )
    shard = projection_shard_spec(
        recipe=recipe.physical,
        partition_ordinal=0,
        input_block=block,
    )

    identity, physical, shards, mappings, streams = admission_registration_inputs(
        recipe,
        bindings,
        (shard,),
        1,
        900,
    )

    assert bindings[0].binding_id != bindings[1].binding_id
    assert identity.input_block_count == 1
    assert identity.binding_count == 2
    assert identity.stream_count == 0
    assert len(identity.stream_set_sha256) == 64
    assert len(physical) == len(shards) == 1
    assert len(mappings) == 2
    assert streams == []


def test_zero_terminal_proof_must_match_retained_stream_terminal_proof():
    retained_stream = _stream(0)
    partition_by_field = _partition(
        ZERO_PARTITION,
        0,
        block_count=0,
        row_count=0,
        byte_count=0,
        retained_stream=retained_stream,
    )
    partition_by_field["zero_row_proof_sha256"] = _digest("different-zero-proof")

    with pytest.raises(
        ProviderDirectoryProjectionError,
        match="terminal_partition_invalid",
    ):
        _manifest(partition_by_field)


def test_exact_duplicate_logical_binding_is_rejected():
    block = _block()
    binding = _binding(block, source_item="item-a", range_ordinal=0)
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

    with pytest.raises(
        ProviderDirectoryProjectionError,
        match="admission_binding_duplicate",
    ):
        projection_admission_identity(
            recipe,
            (binding, binding),
            claim_generation=1,
        )


def _ordered_admission(*, positive_proof_label: str = "positive-proof"):
    block = _block()
    positive_stream = _stream(0, proof_label=positive_proof_label)
    zero_stream = _stream(1)
    manifest = _manifest(
        _partition(
            POSITIVE_PARTITION,
            0,
            block_count=1,
            row_count=2,
            byte_count=128,
            retained_stream=positive_stream,
        ),
        _partition(
            ZERO_PARTITION,
            1,
            block_count=0,
            row_count=0,
            byte_count=0,
            retained_stream=zero_stream,
        ),
    )
    recipe = _recipe((block,), manifest)
    binding = _binding(
        block,
        source_item="positive-item",
        range_ordinal=0,
        stream_identity_sha256=str(positive_stream["identity_sha256"]),
        sequence_ordinal=0,
    )
    terminals = (
        _terminal_zero(
            positive_stream,
            source_item="positive-terminal",
            partition_hash=POSITIVE_PARTITION,
            source_ordinal=0,
        ),
        _terminal_zero(
            zero_stream,
            source_item="zero-terminal",
            partition_hash=ZERO_PARTITION,
            source_ordinal=1,
        ),
    )
    return block, recipe, binding, terminals


def test_zero_terminal_is_admitted_without_a_physical_block():
    _block_value, recipe, binding, terminals = _ordered_admission()

    identity = projection_admission_identity(
        recipe,
        (binding,),
        claim_generation=1,
        terminal_zeros=terminals,
    )

    assert identity.input_block_count == 1
    assert identity.binding_count == 3
    assert identity.stream_count == 2
    assert len(identity.stream_set_sha256) == 64
    assert all(terminal.binding_id != binding.binding_id for terminal in terminals)


def test_positive_coordinate_cannot_be_omitted():
    _block_value, recipe, _binding_value, terminals = _ordered_admission()

    with pytest.raises(
        ProviderDirectoryProjectionError,
        match="admission_census_mismatch",
    ):
        projection_admission_identity(
            recipe,
            (),
            claim_generation=1,
            terminal_zeros=terminals,
        )


def test_zero_coordinate_rejects_a_physical_block():
    block, recipe, binding, terminals = _ordered_admission()
    zero_binding = _binding(
        block,
        source_item="forged-zero-block",
        range_ordinal=1,
        partition_hash=ZERO_PARTITION,
        source_ordinal=1,
    )

    with pytest.raises(
        ProviderDirectoryProjectionError,
        match="admission_census_mismatch",
    ):
        projection_admission_identity(
            recipe,
            (binding, zero_binding),
            claim_generation=1,
            terminal_zeros=terminals,
        )


def test_stream_proof_mutation_changes_admission_but_not_physical_recipe():
    _block_one, first_recipe, first_binding, first_terminals = _ordered_admission(
        positive_proof_label="first-proof"
    )
    _block_two, second_recipe, second_binding, second_terminals = _ordered_admission(
        positive_proof_label="second-proof"
    )
    first = projection_admission_identity(
        first_recipe,
        (first_binding,),
        claim_generation=1,
        terminal_zeros=first_terminals,
    )
    second = projection_admission_identity(
        second_recipe,
        (second_binding,),
        claim_generation=1,
        terminal_zeros=second_terminals,
    )

    assert first_recipe.physical == second_recipe.physical
    assert first_recipe.recipe_id == second_recipe.recipe_id
    assert first.stream_set_sha256 != second.stream_set_sha256
    assert first.admission_id != second.admission_id


def test_forged_binding_id_and_stream_terminal_proof_fail_closed():
    _block_value, recipe, binding, terminals = _ordered_admission()

    with pytest.raises(ProviderDirectoryProjectionError, match="block_invalid"):
        projection_admission_identity(
            recipe,
            (replace(binding, binding_id=_digest("forged-binding")),),
            claim_generation=1,
            terminal_zeros=terminals,
        )
    with pytest.raises(ProviderDirectoryProjectionError, match="stream_invalid"):
        projection_admission_identity(
            recipe,
            (binding,),
            claim_generation=1,
            terminal_zeros=(
                replace(
                    terminals[0],
                    terminal_proof_sha256=_digest("forged-terminal-proof"),
                ),
                terminals[1],
            ),
        )


@pytest.mark.parametrize(
    ("stream_identity_sha256", "sequence_ordinal"),
    (
        (str(_stream(1)["identity_sha256"]), 0),
        (_digest("fabricated-stream"), 0),
        (str(_stream(0)["identity_sha256"]), 3),
    ),
)
def test_ordered_payload_rejects_swapped_or_fabricated_stream_coordinates(
    stream_identity_sha256,
    sequence_ordinal,
):
    block, recipe, _binding_value, terminals = _ordered_admission()
    forged_binding = _binding(
        block,
        source_item="positive-item",
        range_ordinal=0,
        stream_identity_sha256=stream_identity_sha256,
        sequence_ordinal=sequence_ordinal,
    )

    with pytest.raises(
        ProviderDirectoryProjectionError,
        match="admission_payload_stream_mismatch",
    ):
        projection_admission_identity(
            recipe,
            (forged_binding,),
            claim_generation=1,
            terminal_zeros=terminals,
        )


def test_ordered_manifest_rejects_mixed_missing_stream_descriptors():
    with pytest.raises(
        ProviderDirectoryProjectionError,
        match="completeness_manifest_invalid",
    ):
        _manifest(
            _partition(
                POSITIVE_PARTITION,
                0,
                block_count=0,
                row_count=0,
                byte_count=0,
                retained_stream=_stream(0, terminal_sequence_ordinal=0),
            ),
            _partition(
                ZERO_PARTITION,
                1,
                block_count=0,
                row_count=0,
                byte_count=0,
            ),
        )


def test_all_zero_ordered_admission_has_no_physical_workset():
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
    terminal_zeros = tuple(
        _terminal_zero(
            stream,
            source_item=f"zero-terminal-{ordinal}",
            partition_hash=(POSITIVE_PARTITION, ZERO_PARTITION)[ordinal],
            source_ordinal=ordinal,
        )
        for ordinal, stream in enumerate(streams)
    )

    identity, blocks, shards, mappings, stored_streams = (
        admission_registration_inputs(
            recipe,
            (),
            (),
            1,
            900,
            terminal_zeros=terminal_zeros,
        )
    )

    assert identity.outcome_kind == "no_input"
    assert identity.planned_recipe_id == recipe.recipe_id
    assert identity.recipe_id is None
    assert identity.input_block_count == 0
    assert blocks == shards == mappings == []
    assert len(stored_streams) == 2
