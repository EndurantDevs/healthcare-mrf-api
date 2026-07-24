# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Focused policy tests for targeted shared snapshot control."""

from __future__ import annotations

import json
from typing import Any

import pytest

from process.ptg_parts.ptg2_shared_blocks import (
    PTG2_V3_SHARED_GENERATION,
    PTG2_V4_SHARED_GENERATION,
)
from process.ptg_parts.snapshot_cleanup import (
    is_shared_snapshot_control_manifest,
)
from process.ptg_parts.source_snapshot_control_policy import (
    SUPPORTED_SHARED_SNAPSHOT_CONTROL_MESSAGE,
    manifest_dict,
    retirement_manifest_source_key,
    snapshot_remove_reasons,
)


def _serving_index(
    *,
    generation: str = PTG2_V4_SHARED_GENERATION,
    source_key: str | None = "source-a",
    arch_version: str = "postgres_binary_v3",
) -> dict[str, Any]:
    serving_index: dict[str, Any] = {
        "arch_version": arch_version,
        "storage_generation": generation,
    }
    if source_key is not None:
        serving_index["source_key"] = source_key
    return serving_index


def _snapshot(
    *,
    generation: str = PTG2_V4_SHARED_GENERATION,
    source_key: str | None = "source-a",
    status: str = "failed",
    manifest: Any = None,
) -> dict[str, Any]:
    resolved_manifest = (
        {"serving_index": _serving_index(
            generation=generation,
            source_key=source_key,
        )}
        if manifest is None
        else manifest
    )
    return {"manifest": resolved_manifest, "status": status}


def test_manifest_dict_returns_an_existing_dictionary_directly() -> None:
    manifest_by_field = {"serving_index": {"source_key": "source-a"}}

    assert manifest_dict(manifest_by_field) is manifest_by_field


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (
            json.dumps({"serving_index": {"source_key": "source-a"}}),
            {"serving_index": {"source_key": "source-a"}},
        ),
        ("[]", {}),
        ("not-json", {}),
        (None, {}),
        (17, {}),
    ],
    ids=[
        "json-object",
        "json-non-object",
        "malformed-json",
        "none",
        "other-type",
    ],
)
def test_manifest_dict_handles_serialized_and_invalid_values(
    value: Any,
    expected: dict[str, Any],
) -> None:
    assert manifest_dict(value) == expected


def test_snapshot_remove_reasons_is_empty_for_matching_validated_snapshot() -> None:
    assert snapshot_remove_reasons(
        source_key="source-a",
        manifest_source_key="source-a",
        snapshot_status=" VALIDATED ",
        references={},
    ) == []


def test_snapshot_remove_reasons_reports_mismatch_inflight_and_all_references() -> None:
    references_by_name = {
        "global_slots": ["current"],
        "source_keys": ["source-a"],
        "plan_source_keys": ["plan-a"],
        "previous_global_slots": ["current"],
        "previous_source_keys": ["source-a"],
        "previous_plan_source_keys": ["plan-a"],
        "plan_release_pins": ["release-a"],
        "unknown_reference": ["ignored"],
    }

    assert snapshot_remove_reasons(
        source_key="source-b",
        manifest_source_key="source-a",
        snapshot_status="BUILDING",
        references=references_by_name,
    ) == [
        "snapshot source_key does not match requested source_key",
        "snapshot is in-flight (status: BUILDING)",
        "snapshot is referenced by current global pointer",
        "snapshot is referenced by current source pointer",
        "snapshot is referenced by current plan pointer",
        "snapshot is referenced by previous global pointer",
        "snapshot is referenced by previous source pointer",
        "snapshot is referenced by previous plan pointer",
        "snapshot is referenced by plan release pin pointer",
    ]


@pytest.mark.parametrize(
    ("source_key", "manifest_source_key"),
    [(None, "source-a"), ("source-a", None)],
)
def test_snapshot_remove_reasons_ignores_partial_source_identity(
    source_key: str | None,
    manifest_source_key: str | None,
) -> None:
    assert snapshot_remove_reasons(
        source_key=source_key,
        manifest_source_key=manifest_source_key,
        snapshot_status="failed",
        references={},
    ) == []


def test_retirement_manifest_source_key_returns_none_for_missing_snapshot() -> None:
    assert retirement_manifest_source_key({}, "source-a") is None


@pytest.mark.parametrize(
    "manifest",
    [
        {},
        {"serving_index": []},
        {
            "serving_index": _serving_index(
                generation="shared_blocks_future",
            )
        },
    ],
    ids=["missing-index", "non-object-index", "unsupported-generation"],
)
def test_retirement_rejects_unsupported_manifest(manifest: Any) -> None:
    with pytest.raises(
        ValueError,
        match=SUPPORTED_SHARED_SNAPSHOT_CONTROL_MESSAGE,
    ):
        retirement_manifest_source_key(
            _snapshot(manifest=manifest),
            "source-a",
        )


def test_retirement_rejects_inflight_snapshot() -> None:
    with pytest.raises(
        ValueError,
        match=r"snapshot is in-flight \(status: building\)",
    ):
        retirement_manifest_source_key(
            _snapshot(status=" BUILDING "),
            "source-a",
        )


def test_retirement_rejects_requested_source_mismatch() -> None:
    with pytest.raises(
        ValueError,
        match="source_key does not match requested source_key",
    ):
        retirement_manifest_source_key(
            _snapshot(source_key="source-a"),
            "source-b",
        )


@pytest.mark.parametrize(
    ("generation", "manifest_source_key", "requested_source_key", "expected"),
    [
        (
            PTG2_V3_SHARED_GENERATION,
            " source-a ",
            None,
            "source-a",
        ),
        (
            PTG2_V4_SHARED_GENERATION,
            "source-a",
            "source-a",
            "source-a",
        ),
        (
            PTG2_V4_SHARED_GENERATION,
            None,
            "source-a",
            None,
        ),
    ],
    ids=["v3-no-request", "v4-matching-request", "v4-no-manifest-source"],
)
def test_retirement_accepts_supported_terminal_snapshot(
    generation: str,
    manifest_source_key: str | None,
    requested_source_key: str | None,
    expected: str | None,
) -> None:
    assert (
        retirement_manifest_source_key(
            _snapshot(
                generation=generation,
                source_key=manifest_source_key,
            ),
            requested_source_key,
        )
        == expected
    )


@pytest.mark.parametrize(
    ("serving_index", "expected"),
    [
        (None, False),
        ([], False),
        (
            _serving_index(
                generation=PTG2_V3_SHARED_GENERATION,
                arch_version="postgres_binary_v2",
            ),
            False,
        ),
        (_serving_index(generation="shared_blocks_future"), False),
        (
            _serving_index(
                generation=" SHARED_BLOCKS_V3 ",
                arch_version=" POSTGRES_BINARY_V3 ",
            ),
            True,
        ),
        (_serving_index(generation=PTG2_V4_SHARED_GENERATION), True),
    ],
    ids=[
        "none",
        "non-object",
        "wrong-architecture",
        "unsupported-generation",
        "v3",
        "v4",
    ],
)
def test_shared_snapshot_control_manifest_admits_only_v3_and_v4(
    serving_index: Any,
    expected: bool,
) -> None:
    assert is_shared_snapshot_control_manifest(serving_index) is expected
