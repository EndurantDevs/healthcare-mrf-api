# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Exact logical-census and retained-stream proofs for projection admission v2."""

from __future__ import annotations

from dataclasses import replace
import hashlib

import pytest

from process.provider_directory_projection_admission_contract import (
    admission_registration_inputs,
    projection_admission_identity,
)
from process.provider_directory_projection_contract import (
    projection_admission_input_block,
    projection_admission_terminal_zero,
    projection_completeness_manifest,
    projection_input_block,
    projection_input_set_sha256,
    projection_recipe_identity,
    projection_shard_spec,
)
from process.provider_directory_projection_types import (
    ProviderDirectoryProjectionError,
)


def _digest(label: str) -> str:
    return hashlib.sha256(label.encode("utf-8")).hexdigest()


CAMPAIGN_ID = _digest("campaign-id")
CAMPAIGN_SHA256 = _digest("campaign-proof")
POSITIVE_PARTITION = _digest("positive-partition")
ZERO_PARTITION = _digest("zero-partition")


def _block():
    return projection_input_block(
        upstream_artifact_id=_digest("artifact"),
        source_object_id=_digest("layout"),
        block_kind="ndjson",
        input_contract_id="test-input.v1",
        record_start=0,
        record_count=2,
        content_sha256=_digest("content"),
        payload_sha256=_digest("payload"),
        payload_bytes=128,
        summary={"resource_count": 2},
    )


def _stream(
    ordinal: int,
    *,
    proof_label: str | None = None,
) -> dict[str, object]:
    return {
        "identity_sha256": _digest(f"stream:{ordinal}"),
        "stream_ordinal": ordinal,
        "terminal_sequence_ordinal": ordinal + 3,
        "terminal_proof_sha256": _digest(
            proof_label or f"terminal-proof:{ordinal}"
        ),
    }


def _partition(
    partition_hash: str,
    source_ordinal: int,
    *,
    block_count: int,
    row_count: int,
    byte_count: int,
    retained_stream: dict[str, object] | None = None,
) -> dict[str, object]:
    partition_by_field = {
        "resource_type": "Organization",
        "partition_key_hash": partition_hash,
        "source_partition_ordinal": source_ordinal,
        "terminal_cursor_hmac": _digest(f"cursor:{source_ordinal}"),
        "terminal": True,
        "block_count": block_count,
        "row_count": row_count,
        "byte_count": byte_count,
    }
    if row_count == 0:
        partition_by_field["zero_row_proof_sha256"] = (
            retained_stream["terminal_proof_sha256"]
            if retained_stream is not None
            else _digest(f"zero:{source_ordinal}")
        )
    if retained_stream is not None:
        partition_by_field["retained_stream"] = retained_stream
    return partition_by_field


def _manifest(*partitions):
    return projection_completeness_manifest(
        endpoint_campaign_hash=CAMPAIGN_SHA256,
        partition_strategy_contract_id="test-ordered-fhir.v1",
        selected_resources=("Organization",),
        required_resources=("Organization",),
        terminal_partitions=partitions,
        complete=True,
    )


def _recipe(blocks, manifest):
    return projection_recipe_identity(
        decoder_contract_id="test-decoder.v1",
        acquisition_adapter_id="test-acquisition.v1",
        input_set_sha256=projection_input_set_sha256(
            blocks,
            decoder_contract_id="test-decoder.v1",
        ),
        source_ids=("source-a",),
        transform_contract_id="test-transform.v1",
        scope_contract_id="healthporta.provider-directory.global-scope.v1",
        transform_context={
            "as_of_date": "2026-07-22",
            "time_rule_contract_id": "test-time.v1",
        },
        selected_resources=("Organization",),
        required_resources=("Organization",),
        completeness_manifest=manifest,
    )


def _binding(
    block,
    *,
    source_item: str,
    range_ordinal: int,
    partition_hash: str = POSITIVE_PARTITION,
    source_ordinal: int = 0,
):
    return projection_admission_input_block(
        block,
        retained_campaign_id=CAMPAIGN_ID,
        retained_campaign_sha256=CAMPAIGN_SHA256,
        retained_source_item_id=_digest(source_item),
        retained_range_ordinal=range_ordinal,
        resource_type="Organization",
        partition_key_hash=partition_hash,
        source_partition_ordinal=source_ordinal,
    )


def _terminal_zero(
    stream: dict[str, object],
    *,
    source_item: str,
    partition_hash: str,
    source_ordinal: int,
):
    return projection_admission_terminal_zero(
        retained_campaign_id=CAMPAIGN_ID,
        retained_campaign_sha256=CAMPAIGN_SHA256,
        retained_source_item_id=_digest(source_item),
        resource_type="Organization",
        partition_key_hash=partition_hash,
        source_partition_ordinal=source_ordinal,
        retained_stream=stream,
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
    binding = _binding(block, source_item="positive-item", range_ordinal=0)
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
