# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Fail-closed scalar contracts for PTG result archive selection."""

from __future__ import annotations

import pytest

from process.ptg_parts import result_archive_closure as closure


def _layout_by_field() -> dict:
    serving_by_field = {
        "storage_generation": closure.PTG2_V4_SHARED_GENERATION,
        "shared_snapshot_key": 71,
    }
    return {
        "status": "validated",
        "state": "sealed",
        "generation": closure.PTG2_V4_SHARED_GENERATION,
        "map_root_state": "complete",
        "map_format": closure.PTG2_V4_MAP_FORMAT,
        "finalizer_root_state": "complete",
        "finalizer_contract": closure.PTG2_V4_FINALIZER_MAP_CONTRACT,
        "finalizer_map_format": closure.PTG2_V4_MAP_FORMAT,
        "manifest": {"serving_index": dict(serving_by_field)},
        "layout_manifest": {"serving_index": dict(serving_by_field)},
        "snapshot_key": 71,
        "mapping_digest": b"l" * 32,
        "map_digest": b"m" * 32,
        "finalizer_map_digest": b"f" * 32,
    }


@pytest.mark.parametrize(
    ("field_path", "invalid_value"),
    [
        (("snapshot_key",), None),
        (("snapshot_key",), "not-a-number"),
        (("manifest", "serving_index", "shared_snapshot_key"), None),
        (("manifest", "serving_index", "shared_snapshot_key"), "not-a-number"),
        (("layout_manifest", "serving_index", "shared_snapshot_key"), None),
        (("layout_manifest", "serving_index", "shared_snapshot_key"), "not-a-number"),
    ],
)
def test_layout_keys_reject_null_or_nonnumeric_values_as_closure_errors(field_path, invalid_value) -> None:
    """Malformed persisted keys never leak raw coercion errors."""

    layout_by_field = _layout_by_field()
    target_by_field = layout_by_field
    for field_name in field_path[:-1]:
        target_by_field = target_by_field[field_name]
    target_by_field[field_path[-1]] = invalid_value

    with pytest.raises(closure.ResultArchiveClosureError, match="snapshot key"):
        closure._validate_layout(layout_by_field)
