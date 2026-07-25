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
from process.ptg_parts import ptg2_v4_taxonomy_candidates as candidates


MAP_DIGEST = b"m" * 32


def _projection_manifest() -> dict[str, object]:
    rule_digest = b"r" * 32
    member_keys = candidates.pack_inferred_taxonomy_npi_keys((0, 2))
    pattern_member_digest = (
        candidates.inferred_taxonomy_pattern_member_digest(
            rule_digest,
            representation="direct_v1",
            pattern_count=0,
            pattern_member_count=0,
            packed_pattern_payload=b"",
        )
    )
    return candidates.shape_v4_inferred_taxonomy_projection_manifest(
        (
            {
                "rule_digest": rule_digest,
                "catalog_contract": (
                    candidates.PTG2_V4_INFERRED_TAXONOMY_CATALOG_CONTRACT
                ),
                "catalog_digest": b"c" * 32,
                "vector_format": (
                    candidates.PTG2_V4_INFERRED_TAXONOMY_VECTOR_FORMAT
                ),
                "member_count": 2,
                "member_digest": candidates.inferred_taxonomy_member_digest(
                    rule_digest,
                    member_count=2,
                    payload=member_keys,
                ),
                "member_keys": member_keys,
                "representation": "direct_v1",
                "pattern_count": 0,
                "pattern_member_count": 0,
                "pattern_member_bytes": 0,
                "pattern_member_digest": pattern_member_digest,
                "pattern_member_payload": b"",
            },
        ),
        npi_count=3,
        pattern_count=0,
    )


def _install_projection(
    serving_index: dict[str, object],
    projection: dict[str, object],
) -> None:
    serving_index["provider_graph"] = {
        "inferred_taxonomy_candidates": deepcopy(projection)
    }
    serving_index["serving_binary"] = {
        "provider_graph_v4": {
            "inferred_taxonomy_candidates": deepcopy(projection)
        }
    }


def _v4_identity_parts_with_projection():
    row, serving_index, layout_serving_index = _v4_identity_parts()
    projection = _projection_manifest()
    _install_projection(serving_index, projection)
    _install_projection(layout_serving_index, projection)
    return row, serving_index, layout_serving_index


def _projection_copies(
    serving_index: dict[str, object],
    layout_serving_index: dict[str, object],
) -> tuple[dict[str, object], ...]:
    return (
        serving_index["provider_graph"]["inferred_taxonomy_candidates"],
        serving_index["serving_binary"]["provider_graph_v4"][
            "inferred_taxonomy_candidates"
        ],
        layout_serving_index["provider_graph"][
            "inferred_taxonomy_candidates"
        ],
        layout_serving_index["serving_binary"]["provider_graph_v4"][
            "inferred_taxonomy_candidates"
        ],
    )


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
    assert (
        validate_candidate_layout_identity(
            *_v4_identity_parts_with_projection()
        )
        == PTG2_CANDIDATE_V4_GENERATION
    )


def test_candidate_v4_projection_must_be_present_in_every_sealed_copy():
    row, serving_index, layout_serving_index = (
        _v4_identity_parts_with_projection()
    )
    del layout_serving_index["serving_binary"]["provider_graph_v4"][
        "inferred_taxonomy_candidates"
    ]

    with pytest.raises(ValueError, match="projection is missing"):
        validate_candidate_layout_identity(
            row,
            serving_index,
            layout_serving_index,
        )


def test_candidate_v4_projection_must_equal_sealed_layout():
    row, serving_index, layout_serving_index = (
        _v4_identity_parts_with_projection()
    )
    serving_index["serving_binary"]["provider_graph_v4"][
        "inferred_taxonomy_candidates"
    ]["projection_digest"] = "0" * 64

    with pytest.raises(ValueError, match="changed after layout sealing"):
        validate_candidate_layout_identity(
            row,
            serving_index,
            layout_serving_index,
        )


@pytest.mark.parametrize(
    ("mutator", "message"),
    (
        (
            lambda projection: projection.update(contract="changed"),
            "projection is invalid",
        ),
        (
            lambda projection: projection.update(
                projection_digest="0" * 64
            ),
            "projection is invalid",
        ),
        (
            lambda projection: projection.update(rule_count="1"),
            "projection is not canonical",
        ),
    ),
)
def test_candidate_v4_projection_requires_canonical_contract_digest_and_counts(
    mutator,
    message,
):
    row, serving_index, layout_serving_index = (
        _v4_identity_parts_with_projection()
    )
    for projection in _projection_copies(
        serving_index,
        layout_serving_index,
    ):
        mutator(projection)

    with pytest.raises(ValueError, match=message):
        validate_candidate_layout_identity(
            row,
            serving_index,
            layout_serving_index,
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
