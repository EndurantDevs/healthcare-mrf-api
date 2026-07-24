# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Exact V3/V4 layout identity checks used by release audits."""

from __future__ import annotations

from copy import deepcopy

import pytest

from process.ptg_parts.ptg2_candidate_layout_identity import (
    PTG2_CANDIDATE_V3_GENERATION,
    PTG2_CANDIDATE_V4_GENERATION,
    normalize_candidate_storage_generation,
    validate_candidate_layout_identity,
)


MAP_DIGEST = b"m" * 32


def _v3_identity_parts():
    row_by_field = {
        "snapshot_key": 17,
        "layout_state": "sealed",
        "layout_generation": PTG2_CANDIDATE_V3_GENERATION,
    }
    serving_index_by_field = {
        "arch_version": "postgres_binary_v3",
        "storage_generation": PTG2_CANDIDATE_V3_GENERATION,
    }
    return (
        row_by_field,
        serving_index_by_field,
        deepcopy(serving_index_by_field),
    )


def _v4_identity_parts():
    row_by_field = {
        "snapshot_key": 17,
        "layout_state": "sealed",
        "layout_generation": PTG2_CANDIDATE_V4_GENERATION,
        "layout_mapping_digest": MAP_DIGEST,
        "v4_root_state": "complete",
        "v4_root_map_digest": memoryview(MAP_DIGEST),
    }
    serving_index_by_field = {
        "arch_version": "postgres_binary_v3",
        "type": "ptg2_shared_blocks_v4",
        "storage_generation": PTG2_CANDIDATE_V4_GENERATION,
        "provider_scope_strategy": "postgres_packed_graph_v4",
        "shared_block_layout": "packed_snapshot_maps_v4",
        "shared_snapshot_key": 17,
        "snapshot_map": {
            "contract": "ptg_v4_packed_snapshot_map_v1",
            "map_digest": MAP_DIGEST.hex(),
        },
    }
    return (
        row_by_field,
        serving_index_by_field,
        deepcopy(serving_index_by_field),
    )


def test_candidate_layout_identity_accepts_exact_v3_and_v4():
    assert (
        validate_candidate_layout_identity(*_v3_identity_parts())
        == PTG2_CANDIDATE_V3_GENERATION
    )
    assert (
        validate_candidate_layout_identity(*_v4_identity_parts())
        == PTG2_CANDIDATE_V4_GENERATION
    )


@pytest.mark.parametrize(
    ("mutator", "message"),
    (
        (
            lambda row, index, layout: row.update(v4_root_state="building"),
            "root is incomplete",
        ),
        (
            lambda row, index, layout: row.update(
                v4_root_map_digest=b"x" * 32
            ),
            "root is incomplete",
        ),
        (
            lambda row, index, layout: layout["snapshot_map"].update(
                map_digest=(b"x" * 32).hex()
            ),
            "manifest changed",
        ),
        (
            lambda row, index, layout: index.update(shared_snapshot_key=True),
            "snapshot key is invalid",
        ),
        (
            lambda row, index, layout: row.update(layout_state="building"),
            "exact strict",
        ),
        (
            lambda row, index, layout: layout.update(
                shared_snapshot_key=18
            ),
            "shared layout binding",
        ),
        (
            lambda row, index, layout: index.update(
                provider_scope_strategy="postgres_shared_graph"
            ),
            "markers are inconsistent",
        ),
    ),
)
def test_candidate_v4_layout_identity_fails_closed(mutator, message):
    row, index, layout = _v4_identity_parts()
    mutator(row, index, layout)

    with pytest.raises(ValueError, match=message):
        validate_candidate_layout_identity(row, index, layout)


def test_candidate_v4_layout_identity_accepts_canonical_string_keys():
    row_by_field, serving_index_by_field, layout_index_by_field = (
        _v4_identity_parts()
    )
    row_by_field["snapshot_key"] = "17"
    serving_index_by_field["shared_snapshot_key"] = "17"
    layout_index_by_field["shared_snapshot_key"] = "17"

    assert (
        validate_candidate_layout_identity(
            row_by_field,
            serving_index_by_field,
            layout_index_by_field,
        )
        == PTG2_CANDIDATE_V4_GENERATION
    )


@pytest.mark.parametrize("invalid_snapshot_key", (0, -1, "0", object()))
def test_candidate_v4_layout_identity_rejects_noncanonical_snapshot_keys(
    invalid_snapshot_key,
):
    row_by_field, serving_index_by_field, layout_index_by_field = (
        _v4_identity_parts()
    )
    row_by_field["snapshot_key"] = invalid_snapshot_key

    with pytest.raises(ValueError, match="snapshot key is invalid"):
        validate_candidate_layout_identity(
            row_by_field,
            serving_index_by_field,
            layout_index_by_field,
        )


@pytest.mark.parametrize("invalid_digest", ("not-hex", None, b"x"))
def test_candidate_v4_layout_identity_rejects_invalid_map_digests(
    invalid_digest,
):
    row_by_field, serving_index_by_field, layout_index_by_field = (
        _v4_identity_parts()
    )
    row_by_field["layout_mapping_digest"] = invalid_digest

    with pytest.raises(ValueError, match="layout mapping digest is invalid"):
        validate_candidate_layout_identity(
            row_by_field,
            serving_index_by_field,
            layout_index_by_field,
        )


def test_candidate_storage_generation_is_exact():
    assert (
        normalize_candidate_storage_generation(" shared_blocks_v4 ")
        == PTG2_CANDIDATE_V4_GENERATION
    )
    for unsupported_generation in ("shared_blocks_v5", "SHARED_BLOCKS_V4"):
        with pytest.raises(ValueError, match="unsupported"):
            normalize_candidate_storage_generation(unsupported_generation)
