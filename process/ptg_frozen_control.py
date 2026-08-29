# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Worker-boundary validation for private frozen PTG rate-file sets."""

from __future__ import annotations

from typing import Any, Mapping, Sequence

from process.ptg_parts.frozen_rate_binding import (
    INVALID_PRICE_EXCLUSION_POLICY_FIELD,
    normalize_protected_frozen_rate_params,
    protected_frozen_tuple_presence,
)
from process.ptg_parts.frozen_rate_binding_store import (
    recheck_frozen_binding,
)
from process.ptg_parts.frozen_rate_files import (
    FrozenRateFileMismatchError,
    FrozenRateFileValidationError,
    assert_frozen_input_compatibility,
)
from process.ptg_singleton_direct_control import (
    DIRECT_RATE_FILE_INTENT_SHA256_FIELD,
    normalize_protected_singleton_direct_params,
    protected_singleton_direct_presence,
)
from process.ptg_singleton_direct_errors import SingletonDirectValidationError


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
    """Authenticate and normalize one bounded multipart envelope."""

    normalized_params = normalize_protected_frozen_rate_params(
        params_by_name
    )
    if not protected_frozen_tuple_presence(normalized_params):
        return params_by_name
    assert_frozen_input_compatibility(
        normalized_params["frozen_rate_files"],
        in_network_url=normalized_params.get("in_network_url"),
        allowed_url=normalized_params.get("allowed_url"),
        toc_urls=_normalized_string_list(
            normalized_params.get("toc_urls")
            or normalized_params.get("toc_url")
        ),
        toc_list=normalized_params.get("toc_list"),
        file_url_contains=_normalized_string_list(
            normalized_params.get("file_url_contains")
        ),
        max_files=_normalized_optional_int(
            normalized_params.get("max_files")
        ),
    )
    return normalized_params


async def validated_worker_frozen_rate_params(
    task_payload: Mapping[str, Any],
    params_by_name: dict[str, Any],
) -> dict[str, Any]:
    """Validate outer IDs and recheck immutable admission after claim."""

    normalized_params = validated_frozen_rate_params(params_by_name)
    if protected_frozen_tuple_presence(normalized_params):
        protected_id = normalized_params["source_file_import_id"]
        outer_ids = (
            str(task_payload.get("source_file_import_id") or "").strip(),
            str(task_payload.get("import_id") or "").strip(),
        )
        if any(outer_id != protected_id for outer_id in outer_ids):
            raise FrozenRateFileValidationError(
                "protected outer and nested source_file_import_id and "
                "import_id must all match"
            )
    await recheck_frozen_binding(normalized_params)
    return normalized_params


async def validated_worker_rate_params(
    task_payload: Mapping[str, Any],
    params_by_name: dict[str, Any],
) -> dict[str, Any]:
    """Validate one protected singleton or multipart worker envelope."""

    if protected_singleton_direct_presence(params_by_name):
        normalized_params = validated_worker_singleton_direct_params(
            task_payload,
            params_by_name,
        )
        await recheck_frozen_binding(normalized_params)
        return normalized_params
    return await validated_worker_frozen_rate_params(
        task_payload,
        params_by_name,
    )


def normalize_protected_rate_params(
    params_by_name: Mapping[str, Any],
) -> dict[str, Any]:
    """Validate exactly one protected singleton or multipart envelope."""

    if protected_singleton_direct_presence(params_by_name):
        return normalize_protected_singleton_direct_params(params_by_name)
    return normalize_protected_frozen_rate_params(params_by_name)


def validated_worker_singleton_direct_params(
    task_payload: Mapping[str, Any],
    params_by_name: Mapping[str, Any],
) -> dict[str, Any]:
    """Revalidate direct identities at the worker boundary before network I/O."""

    normalized = normalize_protected_singleton_direct_params(params_by_name)
    if not protected_singleton_direct_presence(normalized):
        return normalized
    protected_id = normalized["source_file_import_id"]
    outer_ids = (
        str(task_payload.get("source_file_import_id") or "").strip(),
        str(task_payload.get("import_id") or "").strip(),
    )
    if any(outer_id != protected_id for outer_id in outer_ids):
        raise SingletonDirectValidationError(
            "singleton direct outer and nested import identities must match"
        )
    return normalized


def frozen_rate_main_kwargs(
    params_by_name: dict[str, Any],
) -> dict[str, Any]:
    """Select only private multipart arguments for the PTG engine call."""

    if not protected_frozen_tuple_presence(params_by_name):
        return {}
    main_kwargs_by_name = {
        "source_file_import_id": params_by_name.get("source_file_import_id"),
        "frozen_rate_file_set_contract": params_by_name.get("frozen_rate_file_set_contract"),
        "frozen_rate_files": params_by_name.get("frozen_rate_files"),
        "frozen_rate_file_set_sha256": params_by_name.get(
            "frozen_rate_file_set_sha256"
        ),
        "frozen_rate_file_count": params_by_name.get(
            "frozen_rate_file_count"
        ),
    }
    if INVALID_PRICE_EXCLUSION_POLICY_FIELD in params_by_name:
        main_kwargs_by_name[INVALID_PRICE_EXCLUSION_POLICY_FIELD] = params_by_name[INVALID_PRICE_EXCLUSION_POLICY_FIELD]
    return main_kwargs_by_name


def protected_rate_main_kwargs(
    params_by_name: dict[str, Any],
) -> dict[str, Any]:
    """Select engine arguments for the active protected rate envelope."""

    if protected_singleton_direct_presence(params_by_name):
        return singleton_direct_main_kwargs(params_by_name)
    return frozen_rate_main_kwargs(params_by_name)


def singleton_direct_main_kwargs(
    params_by_name: Mapping[str, Any],
) -> dict[str, Any]:
    """Return the private-progress marker consumed by the PTG runtime."""

    if not protected_singleton_direct_presence(params_by_name):
        return {}
    main_kwargs_by_name = {
        "direct_rate_file_intent_sha256": params_by_name[
            DIRECT_RATE_FILE_INTENT_SHA256_FIELD
        ]
    }
    if INVALID_PRICE_EXCLUSION_POLICY_FIELD in params_by_name:
        main_kwargs_by_name[INVALID_PRICE_EXCLUSION_POLICY_FIELD] = (
            params_by_name[INVALID_PRICE_EXCLUSION_POLICY_FIELD]
        )
    return main_kwargs_by_name


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
    "normalize_protected_rate_params",
    "protected_rate_main_kwargs",
    "singleton_direct_main_kwargs",
    "validated_frozen_rate_params",
    "validated_worker_frozen_rate_params",
    "validated_worker_rate_params",
    "validated_worker_singleton_direct_params",
]
