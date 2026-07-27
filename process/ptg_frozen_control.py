# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Worker-boundary validation for private frozen PTG rate-file sets."""

from __future__ import annotations

from typing import Any, Sequence

from process.ptg_parts.frozen_rate_files import (
    FrozenRateFileMismatchError,
    FrozenRateFileValidationError,
    assert_frozen_input_compatibility,
    normalize_frozen_rate_file_set,
)


def frozen_rate_failure_payload(
    error_leaves: Sequence[BaseException],
) -> dict[str, Any] | None:
    """Classify immutable multipart failures as terminal contract errors."""

    frozen_error = next(
        (
            error
            for error in error_leaves
            if isinstance(
                error,
                (FrozenRateFileValidationError, FrozenRateFileMismatchError),
            )
        ),
        None,
    )
    if frozen_error is None:
        return None
    return {
        "code": "ptg_frozen_rate_file_contract_failed",
        "message": str(frozen_error),
        "retryable": False,
    }


def validated_frozen_rate_params(
    params_by_name: dict[str, Any],
) -> dict[str, Any]:
    """Authenticate the bounded multipart envelope before claiming a run."""

    has_files = "frozen_rate_files" in params_by_name
    has_digest = "frozen_rate_file_set_sha256" in params_by_name
    if not has_files and not has_digest:
        return params_by_name
    if not has_files or not has_digest:
        raise FrozenRateFileValidationError(
            "frozen_rate_files and frozen_rate_file_set_sha256 are required together"
        )
    normalized_files, set_digest = normalize_frozen_rate_file_set(
        params_by_name["frozen_rate_files"],
        params_by_name["frozen_rate_file_set_sha256"],
    )
    assert_frozen_input_compatibility(
        normalized_files,
        in_network_url=params_by_name.get("in_network_url"),
        allowed_url=params_by_name.get("allowed_url"),
        toc_urls=_normalized_string_list(
            params_by_name.get("toc_urls") or params_by_name.get("toc_url")
        ),
        toc_list=params_by_name.get("toc_list"),
        file_url_contains=_normalized_string_list(
            params_by_name.get("file_url_contains")
        ),
        max_files=_normalized_optional_int(params_by_name.get("max_files")),
    )
    return {
        **params_by_name,
        "frozen_rate_files": normalized_files,
        "frozen_rate_file_set_sha256": set_digest,
    }


def frozen_rate_main_kwargs(
    params_by_name: dict[str, Any],
) -> dict[str, Any]:
    """Select only private multipart arguments for the PTG engine call."""

    if (
        "frozen_rate_files" not in params_by_name
        and "frozen_rate_file_set_sha256" not in params_by_name
    ):
        return {}
    return {
        "frozen_rate_files": params_by_name.get("frozen_rate_files"),
        "frozen_rate_file_set_sha256": params_by_name.get(
            "frozen_rate_file_set_sha256"
        ),
    }


def _normalized_string_list(raw_entries: Any) -> list[str] | None:
    if raw_entries is None:
        return None
    if isinstance(raw_entries, str):
        normalized_entry = raw_entries.strip()
        return [normalized_entry] if normalized_entry else None
    if isinstance(raw_entries, (list, tuple)):
        normalized_entries = [
            str(entry).strip()
            for entry in raw_entries
            if str(entry).strip()
        ]
        return normalized_entries or None
    return None


def _normalized_optional_int(raw_number: Any) -> int | None:
    if raw_number is None or raw_number == "":
        return None
    return int(raw_number)


__all__ = [
    "frozen_rate_failure_payload",
    "frozen_rate_main_kwargs",
    "validated_frozen_rate_params",
]
