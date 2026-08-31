# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Validation and session settings for one projection database action."""

from __future__ import annotations

from typing import Any, Mapping, TypedDict

from process.provider_directory_projection_types import (
    HASH_PATTERN,
    ProviderDirectoryProjectionError,
)


_ACTION_SETTING_FIELDS = (
    ("recipe_lease_token", "recipe_lease_token"),
    ("partition_id", "partition_id"),
    ("partition_attempt", "partition_attempt"),
    ("shard_lease_token", "shard_lease_token"),
    ("physical_projection_id", "physical_id"),
    ("reference_owner_kind", "reference_owner_kind"),
    ("reference_owner_id", "reference_owner_id"),
    ("reference_identity_hash", "reference_identity_hash"),
    ("reference_lease_token", "reference_lease_token"),
    (
        "previous_reference_lease_token",
        "reference_previous_lease_token",
    ),
    ("admission_id", "admission_id"),
    ("admission_attempt", "admission_attempt"),
    ("admission_lease_token", "admission_lease_token"),
)
PROJECTION_ACTION_IDENTITY_FIELDS = frozenset(
    field_name for field_name, _setting_name in _ACTION_SETTING_FIELDS
)
_HASH_FIELDS = PROJECTION_ACTION_IDENTITY_FIELDS - {
    "partition_attempt",
    "reference_owner_kind",
    "reference_owner_id",
    "admission_attempt",
}
_REFERENCE_FIELDS = (
    "reference_owner_kind",
    "reference_owner_id",
    "reference_identity_hash",
    "reference_lease_token",
    "previous_reference_lease_token",
)
_ADMISSION_FIELDS = (
    "admission_id",
    "admission_attempt",
    "admission_lease_token",
)
_REFERENCE_ACTIONS = {
    "reference_heartbeat",
    "reference_insert",
    "reference_reclaim",
    "reference_release",
}
_ADMISSION_ACTIONS = {
    "admission_heartbeat",
    "admission_insert",
    "admission_map",
    "admission_reclaim",
    "admission_seal",
}


class ProjectionActionIdentity(TypedDict, total=False):
    """Optional identity coordinates accepted by one projection action."""

    recipe_lease_token: str | None
    partition_id: str | None
    partition_attempt: int | None
    shard_lease_token: str | None
    physical_projection_id: str | None
    reference_owner_kind: str | None
    reference_owner_id: str | None
    reference_identity_hash: str | None
    reference_lease_token: str | None
    previous_reference_lease_token: str | None
    admission_id: str | None
    admission_attempt: int | None
    admission_lease_token: str | None


def _has_invalid_common_identity(
    recipe_id: str,
    recipe_attempt: int,
    identity_by_field: Mapping[str, Any],
) -> bool:
    partition_attempt = identity_by_field.get("partition_attempt")
    return bool(
        not isinstance(recipe_id, str)
        or HASH_PATTERN.fullmatch(recipe_id) is None
        or any(
            candidate_hash is not None
            and (
                not isinstance(candidate_hash, str)
                or HASH_PATTERN.fullmatch(candidate_hash) is None
            )
            for candidate_hash in (
                identity_by_field.get(field_name) for field_name in _HASH_FIELDS
            )
        )
        or type(recipe_attempt) is not int
        or recipe_attempt < 1
        or (
            partition_attempt is not None
            and (type(partition_attempt) is not int or partition_attempt < 0)
        )
    )


def _has_invalid_admission_identity(
    identity_by_field: Mapping[str, Any],
) -> bool:
    admission_attempt = identity_by_field.get("admission_attempt")
    return bool(
        identity_by_field.get("admission_id") is None
        or type(admission_attempt) is not int
        or admission_attempt < 1
        or identity_by_field.get("admission_lease_token") is None
        or any(
            identity_by_field.get(field_name) is not None
            for field_name in (
                "recipe_lease_token",
                "partition_id",
                "partition_attempt",
                "shard_lease_token",
                "physical_projection_id",
                *_REFERENCE_FIELDS,
            )
        )
    )


def _has_invalid_reference_identity(
    action: str,
    identity_by_field: Mapping[str, Any],
) -> bool:
    owner_id = identity_by_field.get("reference_owner_id")
    previous_token = identity_by_field.get("previous_reference_lease_token")
    return bool(
        identity_by_field.get("physical_projection_id") is None
        or identity_by_field.get("reference_owner_kind")
        not in ("dataset", "build", "artifact")
        or not isinstance(owner_id, str)
        or not owner_id
        or len(owner_id) > 128
        or identity_by_field.get("reference_identity_hash") is None
        or identity_by_field.get("reference_lease_token") is None
        or any(
            identity_by_field.get(field_name) is not None
            for field_name in (
                "recipe_lease_token",
                "partition_id",
                "partition_attempt",
                "shard_lease_token",
                *_ADMISSION_FIELDS,
            )
        )
        or (previous_token is not None) != (action == "reference_reclaim")
    )


def validate_projection_action_identity(
    action: str,
    recipe_id: str,
    recipe_attempt: int,
    identity_by_field: Mapping[str, Any],
) -> None:
    """Reject mixed or malformed action identities before setting GUCs."""

    has_invalid_identity = _has_invalid_common_identity(
        recipe_id,
        recipe_attempt,
        identity_by_field,
    )
    if action in _ADMISSION_ACTIONS:
        has_invalid_identity = (
            has_invalid_identity
            or _has_invalid_admission_identity(identity_by_field)
        )
    elif action in _REFERENCE_ACTIONS:
        has_invalid_identity = (
            has_invalid_identity
            or _has_invalid_reference_identity(action, identity_by_field)
        )
    else:
        has_invalid_identity = has_invalid_identity or any(
            identity_by_field.get(field_name) is not None
            for field_name in (*_REFERENCE_FIELDS, *_ADMISSION_FIELDS)
        )
    if has_invalid_identity:
        raise ProviderDirectoryProjectionError(
            "provider_directory_projection_action_identity_invalid"
        )


def projection_action_settings(
    action: str,
    recipe_id: str,
    recipe_attempt: int,
    identity_by_field: Mapping[str, Any],
) -> dict[str, str]:
    """Return settings in the legacy stable write order."""

    setting_by_name = {
        "action": action,
        "recipe_id": recipe_id,
        "recipe_attempt": str(recipe_attempt),
    }
    for field_name, setting_name in _ACTION_SETTING_FIELDS:
        field_value = identity_by_field.get(field_name)
        setting_by_name[setting_name] = (
            "" if field_value is None else str(field_value)
        )
    return setting_by_name


__all__ = (
    "PROJECTION_ACTION_IDENTITY_FIELDS",
    "ProjectionActionIdentity",
    "projection_action_settings",
    "validate_projection_action_identity",
)
