# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Immutable shared-layout identity checks for candidate release audits."""

from __future__ import annotations

from typing import Any, Mapping


PTG2_CANDIDATE_ARCH_VERSION = "postgres_binary_v3"
PTG2_CANDIDATE_V3_GENERATION = "shared_blocks_v3"
PTG2_CANDIDATE_V4_GENERATION = "shared_blocks_v4"
PTG2_CANDIDATE_SUPPORTED_GENERATIONS = frozenset(
    {
        PTG2_CANDIDATE_V3_GENERATION,
        PTG2_CANDIDATE_V4_GENERATION,
    }
)


def normalize_candidate_storage_generation(value: Any) -> str:
    """Return one exact supported shared-layout generation."""

    generation = str(value or "").strip()
    if generation not in PTG2_CANDIDATE_SUPPORTED_GENERATIONS:
        raise ValueError("candidate storage generation is unsupported")
    return generation


def _canonical_snapshot_key(value: Any) -> int:
    if isinstance(value, bool):
        raise ValueError("candidate V4 shared snapshot key is invalid")
    if isinstance(value, int):
        snapshot_key = value
    elif (
        isinstance(value, str)
        and value.isascii()
        and value.isdecimal()
        and not value.startswith("0")
    ):
        snapshot_key = int(value)
    else:
        raise ValueError("candidate V4 shared snapshot key is invalid")
    if snapshot_key <= 0:
        raise ValueError("candidate V4 shared snapshot key is invalid")
    return snapshot_key


def _digest_bytes(value: Any, *, field: str) -> bytes:
    if isinstance(value, memoryview):
        normalized = value.tobytes()
    elif isinstance(value, (bytes, bytearray)):
        normalized = bytes(value)
    elif isinstance(value, str):
        try:
            normalized = bytes.fromhex(value)
        except ValueError as exc:
            raise ValueError(f"{field} is invalid") from exc
    else:
        normalized = b""
    if len(normalized) != 32:
        raise ValueError(f"{field} is invalid")
    return normalized


def _has_exact_v4_markers(serving_index: Mapping[str, Any]) -> bool:
    expected_by_field = {
        "arch_version": PTG2_CANDIDATE_ARCH_VERSION,
        "type": "ptg2_shared_blocks_v4",
        "storage_generation": PTG2_CANDIDATE_V4_GENERATION,
        "provider_scope_strategy": "postgres_packed_graph_v4",
        "shared_block_layout": "packed_snapshot_maps_v4",
    }
    return all(
        serving_index.get(field_name) == expected_value
        for field_name, expected_value in expected_by_field.items()
    )


def _validate_common_layout_identity(
    database_row: Mapping[str, Any],
    serving_index: Mapping[str, Any],
    layout_serving_index: Mapping[str, Any],
) -> str:
    generation = normalize_candidate_storage_generation(
        database_row.get("layout_generation")
    )
    is_exact_shared_layout = (
        str(database_row.get("layout_state") or "").strip() == "sealed"
        and serving_index.get("arch_version")
        == PTG2_CANDIDATE_ARCH_VERSION
        and layout_serving_index.get("arch_version")
        == PTG2_CANDIDATE_ARCH_VERSION
        and serving_index.get("storage_generation") == generation
        and layout_serving_index.get("storage_generation") == generation
    )
    if not is_exact_shared_layout:
        raise ValueError(
            "candidate is not an exact strict postgres_binary_v3 snapshot"
        )
    return generation


def _validate_v4_snapshot_binding(
    database_row: Mapping[str, Any],
    serving_index: Mapping[str, Any],
    layout_serving_index: Mapping[str, Any],
) -> None:
    if not (
        _has_exact_v4_markers(serving_index)
        and _has_exact_v4_markers(layout_serving_index)
    ):
        raise ValueError("candidate V4 packed-layout markers are inconsistent")
    snapshot_key = _canonical_snapshot_key(database_row.get("snapshot_key"))
    manifest_snapshot_key = _canonical_snapshot_key(
        serving_index.get("shared_snapshot_key")
    )
    layout_manifest_snapshot_key = _canonical_snapshot_key(
        layout_serving_index.get("shared_snapshot_key")
    )
    if (
        manifest_snapshot_key != snapshot_key
        or layout_manifest_snapshot_key != snapshot_key
    ):
        raise ValueError(
            "candidate V4 manifest does not match its shared layout binding"
        )


def _validate_v4_map_root(
    database_row: Mapping[str, Any],
    serving_index: Mapping[str, Any],
    layout_serving_index: Mapping[str, Any],
) -> None:
    snapshot_map = serving_index.get("snapshot_map")
    layout_snapshot_map = layout_serving_index.get("snapshot_map")
    if (
        not isinstance(snapshot_map, Mapping)
        or not snapshot_map
        or dict(snapshot_map) != dict(layout_snapshot_map or {})
    ):
        raise ValueError(
            "candidate V4 packed-map manifest changed after layout sealing"
        )
    manifest_digest = _digest_bytes(
        snapshot_map.get("map_digest"),
        field="candidate V4 manifest map digest",
    )
    layout_digest = _digest_bytes(
        database_row.get("layout_mapping_digest"),
        field="candidate V4 layout mapping digest",
    )
    root_digest = _digest_bytes(
        database_row.get("v4_root_map_digest"),
        field="candidate V4 root map digest",
    )
    has_complete_matching_root = (
        str(database_row.get("v4_root_state") or "").strip() == "complete"
        and manifest_digest == layout_digest
        and root_digest == layout_digest
    )
    if not has_complete_matching_root:
        raise ValueError(
            "candidate V4 packed-map root is incomplete or inconsistent"
        )


def validate_candidate_layout_identity(
    database_row: Mapping[str, Any],
    serving_index: Mapping[str, Any],
    layout_serving_index: Mapping[str, Any],
) -> str:
    """Bind a candidate manifest to one sealed V3 or complete V4 layout."""

    generation = _validate_common_layout_identity(
        database_row,
        serving_index,
        layout_serving_index,
    )
    if generation == PTG2_CANDIDATE_V3_GENERATION:
        return generation
    _validate_v4_snapshot_binding(
        database_row,
        serving_index,
        layout_serving_index,
    )
    _validate_v4_map_root(
        database_row,
        serving_index,
        layout_serving_index,
    )
    return generation


__all__ = [
    "PTG2_CANDIDATE_ARCH_VERSION",
    "PTG2_CANDIDATE_SUPPORTED_GENERATIONS",
    "PTG2_CANDIDATE_V3_GENERATION",
    "PTG2_CANDIDATE_V4_GENERATION",
    "normalize_candidate_storage_generation",
    "validate_candidate_layout_identity",
]
