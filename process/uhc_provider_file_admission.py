"""Fail-closed admission for UHC's official provider-file connector."""

from __future__ import annotations

import re
from types import MappingProxyType
from typing import Any, Mapping
from uuid import UUID

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
_DISPATCH_ID = re.compile(r"pdd_[0-9a-f]{32}")


def _is_canonical_uuid(value: Any) -> bool:
    if not isinstance(value, str):
        return False
    try:
        return str(UUID(value)) == value
    except ValueError:
        return False


def is_uhc_official_file_repair_replay(params: Mapping[str, Any]) -> bool:
    """Return whether params carry one exact typed repair-generation identity."""

    repair_id = params.get("provider_directory_repair_id")
    if repair_id is None:
        return False
    generation = params.get("provider_directory_dispatch_generation")
    contract_version = params.get(
        "provider_directory_dispatch_contract_version"
    )
    dispatch_id = params.get("provider_directory_dispatch_id")
    request_id = params.get("provider_directory_dispatch_request_id")
    request_fingerprint = params.get(
        "provider_directory_dispatch_request_fingerprint"
    )
    catalog_digest = params.get("provider_directory_dispatch_catalog_digest")
    if not (
        _is_canonical_uuid(repair_id)
        and type(generation) is int
        and generation >= 1
        and type(contract_version) is int
        and contract_version == 2
        and isinstance(dispatch_id, str)
        and _DISPATCH_ID.fullmatch(dispatch_id) is not None
        and _is_canonical_uuid(request_id)
        and isinstance(request_fingerprint, str)
        and _LOWERCASE_SHA256.fullmatch(request_fingerprint) is not None
        and isinstance(catalog_digest, str)
        and _LOWERCASE_SHA256.fullmatch(catalog_digest) is not None
    ):
        raise ValueError("provider_directory_uhc_repair_identity_invalid")
    return True


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
    is_uhc_official_file_repair_replay(params)
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
    "is_uhc_official_file_repair_replay",
    "is_uhc_official_file_acquisition_requested",
    "should_select_uhc_official_file_source",
    "validate_uhc_official_file_admission",
]
