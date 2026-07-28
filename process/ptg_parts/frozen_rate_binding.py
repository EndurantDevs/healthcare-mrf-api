# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Immutable source-file binding for protected multipart PTG imports."""

from __future__ import annotations

import datetime as dt
import hashlib
import json
from typing import Any, Mapping

from process.ptg_parts.canonical import canonical_json_dumps
from process.ptg_parts.frozen_rate_files import (
    FROZEN_RATE_FILE_SET_CONTRACT,
    FrozenRateFileMismatchError,
    FrozenRateFileValidationError,
    normalize_frozen_rate_file_set,
)
from process.ptg_parts.snapshot_tables import _normalize_source_key


FROZEN_RATE_FILE_BINDING_CONTRACT = "ptg_frozen_source_file_binding_v1"
FROZEN_RATE_FILE_BINDING_OPTION = "frozen_rate_file_binding"
FROZEN_RATE_FILE_BINDING_TABLE = "ptg2_frozen_source_file_binding"
FROZEN_RATE_FILE_PROTECTED_FIELDS = (
    "frozen_rate_file_set_contract",
    "frozen_rate_files",
    "frozen_rate_file_set_sha256",
    "frozen_rate_file_count",
)


class FrozenRateFileBindingMismatchError(FrozenRateFileMismatchError):
    """Raised when a source-file ID is replayed with different immutable input."""


def _required_text(
    value: Any,
    *,
    field_name: str,
    max_bytes: int,
) -> str:
    normalized = str(value or "").strip()
    if (
        not normalized
        or len(normalized.encode("utf-8")) > max_bytes
        or any(ord(character) < 32 for character in normalized)
    ):
        raise FrozenRateFileValidationError(
            f"frozen rate file {field_name} is invalid"
        )
    return normalized


def source_file_import_id_from_params(
    params_by_name: Mapping[str, Any],
) -> str | None:
    """Return the validated source-file identity when one was supplied."""

    raw_value = params_by_name.get("source_file_import_id")
    if raw_value is None or not str(raw_value).strip():
        return None
    return _required_text(
        raw_value,
        field_name="source_file_import_id",
        max_bytes=64,
    )


def frozen_internal_run_id(source_file_import_id: str) -> str:
    """Return the permanent engine run identity for one source-file import."""

    normalized_id = _required_text(
        source_file_import_id,
        field_name="source_file_import_id",
        max_bytes=64,
    )
    internal_run_id = f"ptg2:{normalized_id}"
    if len(internal_run_id.encode("utf-8")) > 96:
        raise FrozenRateFileValidationError(
            "frozen rate file source_file_import_id is invalid"
        )
    return internal_run_id


def _canonical_import_month(value: Any) -> str:
    if isinstance(value, dt.datetime):
        month = value.date()
    elif isinstance(value, dt.date):
        month = value
    else:
        normalized = str(value or "").strip()
        try:
            month = dt.date.fromisoformat(
                f"{normalized}-01" if len(normalized) == 7 else normalized
            )
        except ValueError as exc:
            raise FrozenRateFileValidationError(
                "frozen rate file import_month is invalid"
            ) from exc
    return month.replace(day=1).isoformat()


def _canonical_string_set(
    value: Any,
    *,
    field_name: str,
    lowercase: bool,
) -> list[str]:
    if value is None:
        return []
    if not isinstance(value, (list, tuple)):
        raise FrozenRateFileValidationError(
            f"frozen rate file {field_name} must be an array"
        )
    normalized_values: set[str] = set()
    for raw_entry in value:
        entry = _required_text(
            raw_entry,
            field_name=field_name,
            max_bytes=256,
        )
        normalized_values.add(entry.lower() if lowercase else entry)
    return sorted(
        normalized_values,
        key=(str.casefold if lowercase else None),
    )


def _canonical_source_key(value: Any) -> str:
    normalized = _normalize_source_key(
        _required_text(
            value,
            field_name="source_key",
            max_bytes=96,
        )
    )
    if normalized is None:
        raise FrozenRateFileValidationError(
            "frozen rate file source_key is invalid"
        )
    return normalized


def protected_frozen_tuple_presence(
    params_by_name: Mapping[str, Any],
) -> tuple[str, ...]:
    """Return supplied protected marker fields in canonical declaration order."""

    return tuple(
        field_name
        for field_name in FROZEN_RATE_FILE_PROTECTED_FIELDS
        if field_name in params_by_name
    )


def normalize_protected_frozen_rate_params(
    params_by_name: Mapping[str, Any],
) -> dict[str, Any]:
    """Validate the all-or-none marker tuple and return canonical parameters."""

    normalized_params_by_name = dict(params_by_name)
    supplied_fields = protected_frozen_tuple_presence(
        normalized_params_by_name
    )
    if not supplied_fields:
        return normalized_params_by_name
    if len(supplied_fields) != len(FROZEN_RATE_FILE_PROTECTED_FIELDS):
        raise FrozenRateFileValidationError(
            "frozen_rate_file_set_contract, frozen_rate_files, "
            "frozen_rate_file_set_sha256, and frozen_rate_file_count are all "
            "required together"
        )
    if (
        normalized_params_by_name["frozen_rate_file_set_contract"]
        != FROZEN_RATE_FILE_SET_CONTRACT
    ):
        raise FrozenRateFileValidationError(
            "frozen_rate_file_set_contract is invalid"
        )
    file_count = normalized_params_by_name["frozen_rate_file_count"]
    if isinstance(file_count, bool) or not isinstance(file_count, int):
        raise FrozenRateFileValidationError(
            "frozen_rate_file_count must be an integer"
        )
    normalized_files, set_digest = normalize_frozen_rate_file_set(
        normalized_params_by_name["frozen_rate_files"],
        normalized_params_by_name["frozen_rate_file_set_sha256"],
    )
    if file_count != len(normalized_files):
        raise FrozenRateFileValidationError(
            "frozen_rate_file_count does not match frozen_rate_files"
        )
    source_file_import_id = source_file_import_id_from_params(
        normalized_params_by_name
    )
    import_id = _required_text(
        normalized_params_by_name.get("import_id"),
        field_name="import_id",
        max_bytes=64,
    )
    if source_file_import_id is None or import_id != source_file_import_id:
        raise FrozenRateFileValidationError(
            "frozen source_file_import_id and import_id must match"
        )
    normalized_params_by_name.update(
        {
            "source_file_import_id": source_file_import_id,
            "import_id": source_file_import_id,
            "frozen_rate_file_set_contract": FROZEN_RATE_FILE_SET_CONTRACT,
            "frozen_rate_files": normalized_files,
            "frozen_rate_file_set_sha256": set_digest,
            "frozen_rate_file_count": file_count,
        }
    )
    return normalized_params_by_name


def frozen_rate_binding_from_params(
    params_by_name: Mapping[str, Any],
) -> dict[str, Any] | None:
    """Build the canonical immutable binding, or return legacy absence."""

    normalized_params = normalize_protected_frozen_rate_params(params_by_name)
    if not protected_frozen_tuple_presence(normalized_params):
        return None
    source_file_import_id = str(
        normalized_params["source_file_import_id"]
    )
    return {
        "contract": FROZEN_RATE_FILE_BINDING_CONTRACT,
        "source_file_import_id": source_file_import_id,
        "frozen_rate_file_set_contract": FROZEN_RATE_FILE_SET_CONTRACT,
        "frozen_rate_file_set_sha256": normalized_params[
            "frozen_rate_file_set_sha256"
        ],
        "frozen_rate_file_count": normalized_params[
            "frozen_rate_file_count"
        ],
        "source_key": _canonical_source_key(
            normalized_params.get("source_key")
        ),
        "import_month": _canonical_import_month(
            normalized_params.get("import_month")
        ),
        "plan_ids": _canonical_string_set(
            normalized_params.get("plan_ids"),
            field_name="plan_ids",
            lowercase=False,
        ),
        "plan_market_types": _canonical_string_set(
            normalized_params.get("plan_market_types"),
            field_name="plan_market_types",
            lowercase=True,
        ),
    }


def frozen_rate_binding_sha256(binding: Mapping[str, Any]) -> str:
    """Hash one canonical binding for compact database corroboration."""

    return hashlib.sha256(
        canonical_json_dumps(dict(binding)).encode("utf-8")
    ).hexdigest()


def _stored_binding(
    stored_options: Mapping[str, Any] | None,
) -> dict[str, Any] | None:
    if not isinstance(stored_options, Mapping):
        return None
    candidate = stored_options.get(FROZEN_RATE_FILE_BINDING_OPTION)
    if isinstance(candidate, str):
        try:
            candidate = json.loads(candidate)
        except json.JSONDecodeError:
            return None
    return dict(candidate) if isinstance(candidate, Mapping) else None


def assert_existing_frozen_binding(
    stored_options: Mapping[str, Any] | None,
    expected_binding: Mapping[str, Any] | None,
    *,
    row_exists: bool,
) -> None:
    """Allow exact replay only; legacy is valid only without any binding row."""

    stored_binding = _stored_binding(stored_options)
    if not row_exists:
        return
    if expected_binding is None:
        raise FrozenRateFileBindingMismatchError(
            "an existing frozen source-file binding cannot be replayed as legacy"
        )
    if stored_binding is None or stored_binding != dict(expected_binding):
        raise FrozenRateFileBindingMismatchError(
            "immutable frozen source-file binding changed"
        )


def binding_option(
    frozen_binding_by_name: Mapping[str, Any] | None,
) -> dict[str, Any]:
    """Return the options fragment used in snapshots and manifests."""

    if frozen_binding_by_name is None:
        return {}
    return {
        FROZEN_RATE_FILE_BINDING_OPTION: dict(frozen_binding_by_name)
    }


__all__ = [
    "FROZEN_RATE_FILE_BINDING_CONTRACT",
    "FROZEN_RATE_FILE_BINDING_OPTION",
    "FROZEN_RATE_FILE_BINDING_TABLE",
    "FROZEN_RATE_FILE_PROTECTED_FIELDS",
    "FrozenRateFileBindingMismatchError",
    "assert_existing_frozen_binding",
    "binding_option",
    "frozen_internal_run_id",
    "frozen_rate_binding_from_params",
    "frozen_rate_binding_sha256",
    "normalize_protected_frozen_rate_params",
    "protected_frozen_tuple_presence",
    "source_file_import_id_from_params",
]
