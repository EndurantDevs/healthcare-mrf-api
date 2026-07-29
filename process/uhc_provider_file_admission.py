"""Fail-closed admission for UHC's official provider-file connector."""

from __future__ import annotations

import re
from types import MappingProxyType
from typing import Any, Mapping

from process.uhc_provider_file_source_identity import (
    UHC_PROVIDER_FILE_DISPLAY_NAME,
    UHC_PROVIDER_FILE_ENTRY_ID,
    UHC_PROVIDER_FILE_SOURCE_ID,
)


UHC_OFFICIAL_ACQUISITION_PROFILE: Mapping[str, object] = MappingProxyType(
    {
        "bulk_export": False,
        "open_only": True,
        "include_auth_required": False,
        "concurrency": 1,
        "linked_resource_deadline_seconds": 0,
    }
)
_LOWERCASE_SHA256 = re.compile(r"[0-9a-f]{64}")


def _requested_source_ids(params: Mapping[str, Any]) -> set[str]:
    raw_source_ids = (
        params.get("source_ids")
        or params.get("source_id")
        or params.get("provider_directory_source_ids")
        or params.get("provider_directory_source_id")
    )
    if raw_source_ids in (None, ""):
        return set()
    if isinstance(raw_source_ids, str):
        source_values = raw_source_ids.split(",")
    elif (
        isinstance(raw_source_ids, (bytes, bytearray, dict))
        or not hasattr(raw_source_ids, "__iter__")
    ):
        source_values = (raw_source_ids,)
    else:
        source_values = raw_source_ids
    return {
        source_id_text
        for source_id in source_values
        if (
            source_id_text := (
                str(source_id).strip()
                if source_id is not None
                else ""
            )
        )
    }


def should_select_uhc_official_file_source(
    *,
    requested_source_ids: set[str],
    source_query: str | None,
    test_mode: bool,
    limit: int | None,
) -> bool:
    """Return whether ordinary catalog resolution includes official UHC."""

    if requested_source_ids:
        return UHC_PROVIDER_FILE_SOURCE_ID in requested_source_ids
    if source_query:
        normalized_query = source_query.strip().casefold()
        searchable_values = (
            UHC_PROVIDER_FILE_SOURCE_ID,
            UHC_PROVIDER_FILE_DISPLAY_NAME,
            UHC_PROVIDER_FILE_ENTRY_ID,
            "uhc",
            "optum",
        )
        return any(
            normalized_query in value.casefold()
            for value in searchable_values
        )
    return not test_mode and limit is None


def _requested_limit(params: Mapping[str, Any]) -> int | None:
    return int(params.get("limit") or 0) or None


def is_uhc_official_file_acquisition_requested(
    params: Mapping[str, Any],
    *,
    test_mode: bool | None = None,
) -> bool:
    """Return whether task parameters select the official UHC connector."""

    source_query_value = params.get("source_query")
    source_query = (
        str(source_query_value).strip()
        if source_query_value is not None
        else None
    )
    effective_test_mode = (
        bool(params.get("test") or params.get("test_mode"))
        if test_mode is None
        else test_mode
    )
    return bool(params.get("import_resources")) and (
        should_select_uhc_official_file_source(
            requested_source_ids=_requested_source_ids(params),
            source_query=source_query or None,
            test_mode=effective_test_mode,
            limit=_requested_limit(params),
        )
    )


def validate_uhc_official_file_admission(
    params: Mapping[str, Any],
    *,
    required: bool = False,
    test_mode: bool | None = None,
) -> None:
    """Require the immutable catalog pin and acquisition profile."""

    if not required and not is_uhc_official_file_acquisition_requested(
        params,
        test_mode=test_mode,
    ):
        return
    catalog_hash = params.get("uhc_catalog_set_sha256")
    if (
        not isinstance(catalog_hash, str)
        or _LOWERCASE_SHA256.fullmatch(catalog_hash) is None
    ):
        raise ValueError(
            "provider_directory_uhc_catalog_set_sha256_invalid"
        )
    for field_name, expected_value in UHC_OFFICIAL_ACQUISITION_PROFILE.items():
        actual_value = params.get(field_name)
        if (
            type(actual_value) is not type(expected_value)
            or actual_value != expected_value
        ):
            raise ValueError(
                "provider_directory_uhc_acquisition_profile_invalid:"
                f"{field_name}"
            )


__all__ = [
    "UHC_OFFICIAL_ACQUISITION_PROFILE",
    "is_uhc_official_file_acquisition_requested",
    "should_select_uhc_official_file_source",
    "validate_uhc_official_file_admission",
]
