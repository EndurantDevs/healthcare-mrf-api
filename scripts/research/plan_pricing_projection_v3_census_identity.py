# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Exact source and release identity checks for the projection-v3 census."""

from __future__ import annotations

import hashlib
import json
from typing import Any, Mapping, Sequence

from api.plan_pricing_projection_contract import projection_id
from scripts.research.plan_pricing_projection_v3_census_support import (
    HARNESS_PATHS,
    SOURCE_PATHS,
)

CENSUS_ENVELOPE_SCRIPT_PATH = (
    "scripts/research/run_plan_pricing_projection_v3_census_envelope.sh"
)
_EXPECTED_SOURCE_KEYS = frozenset(
    {
        "declared_git_head",
        "observed_git_head",
        "manifest_sha256",
        "files",
        "harness_files",
        "harness_manifest_sha256",
    }
)
_EXPECTED_TARGET_KEYS = frozenset(
    {
        "healthporta_plan_id",
        "plan_release_id",
        "serving_revision_id",
        "binding_set_digest",
        "binding_count",
        "in_network_binding_count",
        "distinct_snapshot_count",
        "distinct_plan_count",
    }
)
_EXPECTED_RELEASE_KEYS = frozenset(
    {
        "healthporta_plan_id",
        "plan_release_id",
        "serving_revision_id",
        "binding_set_digest",
        "published_at",
        "binding_count",
    }
)


def _is_sha256(value: Any) -> bool:
    return (
        isinstance(value, str)
        and len(value) == 64
        and not (set(value) - set("0123456789abcdef"))
    )


def _is_file_inventory(value: Any, expected_paths: Sequence[str]) -> bool:
    return (
        isinstance(value, list)
        and len(value) == len(expected_paths)
        and all(
            isinstance(row, list)
            and len(row) == 2
            and row[0] == expected_path
            and _is_sha256(row[1])
            for row, expected_path in zip(value, expected_paths, strict=True)
        )
    )


def _manifest_sha256(value: Any) -> str:
    serialized = json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(serialized).hexdigest()


def is_source_identity_valid(
    source_by_field: Any,
    reviewed_source_sha: str,
    expected_manifest_sha256: str,
    expected_harness_manifest_sha256: str,
) -> bool:
    """Validate and recompute one exact six-field source identity."""

    if (
        not isinstance(source_by_field, Mapping)
        or frozenset(source_by_field) != _EXPECTED_SOURCE_KEYS
        or source_by_field.get("declared_git_head") != reviewed_source_sha
        or source_by_field.get("observed_git_head") not in (None, reviewed_source_sha)
        or not _is_file_inventory(source_by_field.get("files"), SOURCE_PATHS)
        or not _is_file_inventory(source_by_field.get("harness_files"), HARNESS_PATHS)
    ):
        return False
    return source_by_field.get(
        "manifest_sha256"
    ) == expected_manifest_sha256 == _manifest_sha256(
        source_by_field["files"]
    ) and source_by_field.get(
        "harness_manifest_sha256"
    ) == expected_harness_manifest_sha256 == _manifest_sha256(
        source_by_field["harness_files"]
    )


def harness_digest(source_by_field: Mapping[str, Any]) -> str | None:
    """Return the envelope script digest from an exact harness inventory."""

    harness_files = source_by_field.get("harness_files")
    if not _is_file_inventory(harness_files, HARNESS_PATHS):
        return None
    return dict(harness_files)[CENSUS_ENVELOPE_SCRIPT_PATH]


def is_source_pair_bound(
    receipt_by_field: Mapping[str, Any],
    reviewed_source_sha: str,
    expected_manifest_sha256: str,
    expected_harness_manifest_sha256: str,
    expected_envelope_sha256: str,
) -> bool:
    """Bind both source snapshots to one reviewed source and harness."""

    source_before = receipt_by_field.get("source_before")
    source_after = receipt_by_field.get("source_after")
    return (
        isinstance(source_before, Mapping)
        and isinstance(source_after, Mapping)
        and is_source_identity_valid(
            source_before,
            reviewed_source_sha,
            expected_manifest_sha256,
            expected_harness_manifest_sha256,
        )
        and is_source_identity_valid(
            source_after,
            reviewed_source_sha,
            expected_manifest_sha256,
            expected_harness_manifest_sha256,
        )
        and harness_digest(source_before) == expected_envelope_sha256
        and harness_digest(source_after) == expected_envelope_sha256
    )


def validated_target(target_by_field: Mapping[str, Any]) -> dict[str, Any]:
    """Return one exact operator-declared release and serving shape."""

    if frozenset(target_by_field) != _EXPECTED_TARGET_KEYS:
        raise ValueError("pricing projection census target schema is invalid")
    target_by_field = dict(target_by_field)
    identity_fields = (
        "healthporta_plan_id",
        "plan_release_id",
        "serving_revision_id",
    )
    if not all(
        isinstance(target_by_field[field_name], str) and target_by_field[field_name]
        for field_name in identity_fields
    ):
        raise ValueError("pricing projection census target identity is invalid")
    binding_digest = target_by_field["binding_set_digest"]
    if not _is_sha256(binding_digest):
        raise ValueError("pricing projection census binding digest is invalid")
    count_fields = (
        "binding_count",
        "in_network_binding_count",
        "distinct_snapshot_count",
        "distinct_plan_count",
    )
    binding_count = target_by_field["binding_count"]
    if any(
        type(target_by_field[field_name]) is not int or target_by_field[field_name] <= 0
        for field_name in count_fields
    ) or any(
        target_by_field[field_name] > binding_count for field_name in count_fields[1:]
    ):
        raise ValueError("pricing projection census target shape is invalid")
    return target_by_field


def is_measurement_identity_valid(
    receipt_by_field: Mapping[str, Any],
    measured_result: Mapping[str, Any],
) -> bool:
    """Bind measured release identity to the exact reviewed target."""

    try:
        target_by_field = validated_target(receipt_by_field.get("expected_target"))
        serving_shape = validated_target(measured_result.get("serving_shape"))
    except (TypeError, ValueError):
        return False
    release_by_field = measured_result.get("release")
    provider_signature_by_field = measured_result.get("provider_signature")
    if (
        not isinstance(release_by_field, Mapping)
        or frozenset(release_by_field) != _EXPECTED_RELEASE_KEYS
        or not isinstance(release_by_field.get("published_at"), str)
        or not release_by_field["published_at"]
        or type(release_by_field.get("binding_count")) is not int
        or serving_shape != target_by_field
        or not _is_sha256(provider_signature_by_field)
    ):
        return False
    for field_name in (
        "healthporta_plan_id",
        "plan_release_id",
        "serving_revision_id",
        "binding_set_digest",
        "binding_count",
    ):
        if release_by_field.get(field_name) != target_by_field[field_name]:
            return False
    return measured_result.get("projection_id") == projection_id(
        target_by_field["binding_set_digest"],
        provider_signature_by_field,
    )
