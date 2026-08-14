# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Admission contract for reviewed current-version FHIR acquisition.

This contract bounds the versions visible in a FHIR search by ``_lastUpdated``.
It does not recreate a historical snapshot because superseded versions are not
necessarily returned by an ordinary resource search.
"""

from __future__ import annotations

import datetime
import json
from dataclasses import dataclass
from enum import Enum
from typing import Any, Mapping, Sequence

from process.provider_directory_fhir_subset_identity import (
    CURRENT_VERSION_CENSUS_CANONICALIZATION_VERSION_FIELD,
    CURRENT_VERSION_CENSUS_COMPLETION_SCOPES_FIELD,
    CURRENT_VERSION_CENSUS_CONTINUATION_STRATEGY_FIELD,
    CURRENT_VERSION_CENSUS_CONTRACT_FIELD,
    CURRENT_VERSION_CENSUS_CONTRACT_VERSION_FIELD,
    CURRENT_VERSION_CENSUS_METADATA_STRATEGY_FIELD,
    CURRENT_VERSION_CENSUS_PAGE_COUNT_FIELD,
    CURRENT_VERSION_CENSUS_START_URLS_FIELD,
    CURRENT_VERSION_CENSUS_STRATEGY_VERSION_FIELD,
    CURRENT_VERSION_CENSUS_TRAVERSAL_VERSION_FIELD,
    SERVER_ISSUED_SUBSET_CANONICALIZATION_VERSION,
    SERVER_ISSUED_SUBSET_COMPLETION_SCOPES,
    SERVER_ISSUED_SUBSET_RESOURCE_TYPES,
    SERVER_ISSUED_SUBSET_SEMANTICS,
    SERVER_ISSUED_SUBSET_SMILE_CONTINUATION_STRATEGY,
    SERVER_ISSUED_SUBSET_STRATEGY_VERSION,
    SERVER_ISSUED_SUBSET_TRAVERSAL_VERSION,
)
CURRENT_VERSION_CENSUS_SMILE_CONTINUATION_STRATEGY = (
    "smile-opaque-logical-offset-v2"
)
CURRENT_VERSION_CENSUS_STRATEGY_VERSION = (
    "provider-directory-fhir-cutoff-bounded-current-version-census-v1"
)
_CURRENT_VERSION_CENSUS_BOOLEAN_TASK_FIELDS = frozenset(
    {
        "bulk_export",
        "canonical_backfill_only",
        "contact_backfill_only",
        "dataset_followup_only",
        "dataset_rehydrate_only",
        "defer_typed_materialization",
        "full_refresh",
        "import_resources",
        "include_auth_required",
        "include_supplemental_catalogs",
        "open_only",
        "probe",
        "publish_after_acquisition",
        "publish_artifacts",
        "publish_artifacts_only",
        "publish_corroboration",
        "restart_expired_current_census_slice",
        "seed_only",
        "stale_cleanup",
        "test",
        "test_mode",
    }
)
_CURRENT_VERSION_CENSUS_INTEGER_TASK_FIELDS = frozenset(
    {
        "bulk_export_max_pending_seconds",
        "concurrency",
        "limit",
        "linked_resource_deadline_seconds",
        "linked_resource_limit",
        "page_count",
        "page_limit",
        "resource_deadline_seconds",
        "resource_limit",
        "resource_scan_concurrency",
        "source_concurrency",
        "stream_batch_size",
        "timeout",
    }
)
_CURRENT_VERSION_CENSUS_TEXT_TASK_FIELDS = frozenset(
    {
        "amerihealth_caritas_catalog_url",
        "amerihealth_caritas_catalog_path",
        "cms_sma_endpoint_directory_url",
        "cms_sma_endpoint_directory_path",
        "contra_costa_catalog_path",
        "contra_costa_catalog_url",
        "credential_config_file",
        "provider_directory_endpoint_scope",
        "provider_directory_pagination_root_run_id",
        "refresh_preset",
        "retest_results_path",
        "retest_results_url",
        "retry_of_run_id",
        "run_id",
        "seed_db_path",
        "seed_db_url",
        "source_query",
    }
)
CURRENT_VERSION_CENSUS_SEMANTICS = "cutoff-bounded-current-version-census"
_SOURCE_ID_ALIAS_FIELDS = (
    "source_ids",
    "source_id",
    "provider_directory_source_ids",
    "provider_directory_source_id",
)


class ProviderDirectoryFHIRAcquisitionStrategy(str, Enum):
    """Supported top-level FHIR acquisition strategies."""

    CONFIGURED = "configured"
    CUTOFF_BOUNDED_CURRENT_VERSION_CENSUS = (
        "cutoff-bounded-current-version-census"
    )
    SERVER_ISSUED_TRAVERSAL_SUBSET = "server-issued-traversal-subset"


def acquisition_strategy_values() -> tuple[str, ...]:
    """Return stable values for CLI choice validation."""

    return tuple(strategy.value for strategy in ProviderDirectoryFHIRAcquisitionStrategy)


def _clean_text(value: Any) -> str | None:
    text = str(value).strip() if value is not None else ""
    return text or None


def _strategy_from_value(value: Any) -> ProviderDirectoryFHIRAcquisitionStrategy:
    raw_strategy = _clean_text(value) or ProviderDirectoryFHIRAcquisitionStrategy.CONFIGURED.value
    try:
        return ProviderDirectoryFHIRAcquisitionStrategy(raw_strategy)
    except ValueError as exc:
        allowed = ",".join(acquisition_strategy_values())
        raise ValueError(
            f"provider_directory_fhir_acquisition_strategy_unsupported:{raw_strategy}:allowed={allowed}"
        ) from exc


def _raw_vector(value: Any, *, field_name: str) -> list[Any]:
    if isinstance(value, str):
        stripped = value.strip()
        if stripped.startswith("["):
            try:
                decoded = json.loads(stripped)
            except json.JSONDecodeError as exc:
                raise ValueError(f"{field_name}_invalid_json") from exc
            return decoded
        return stripped.split(",")
    if isinstance(value, Sequence) and not isinstance(
        value, (bytes, bytearray)
    ):
        return list(value)
    raise ValueError(f"{field_name}_must_be_sequence")


def _strict_text_vector(
    value: Any,
    *,
    field_name: str,
    allowed_values: frozenset[str] | None = None,
) -> tuple[str, ...]:
    raw_values = _raw_vector(value, field_name=field_name)
    normalized_values: list[str] = []
    for raw_value in raw_values:
        if not isinstance(raw_value, str):
            raise ValueError(f"{field_name}_entries_must_be_strings")
        normalized_value = raw_value.strip()
        if not normalized_value:
            raise ValueError(f"{field_name}_entries_must_not_be_empty")
        normalized_values.append(normalized_value)
    if not normalized_values:
        raise ValueError(f"{field_name}_must_not_be_empty")
    if len(normalized_values) != len(set(normalized_values)):
        raise ValueError(f"{field_name}_must_be_unique")
    if allowed_values is not None:
        unknown_values = sorted(set(normalized_values) - allowed_values)
        if unknown_values:
            raise ValueError(
                f"{field_name}_unsupported:{','.join(unknown_values)}"
            )
    return tuple(normalized_values)


def _source_id_vector(task: Mapping[str, Any]) -> tuple[str, ...]:
    populated_aliases = [
        (field_name, task.get(field_name))
        for field_name in _SOURCE_ID_ALIAS_FIELDS
        if task.get(field_name) not in (None, "", [], ())
    ]
    if len(populated_aliases) > 1:
        raise ValueError(
            "provider_directory_current_version_census_source_id_aliases_conflict"
        )
    if not populated_aliases:
        return ()
    return _strict_text_vector(
        populated_aliases[0][1],
        field_name="provider_directory_current_version_census_source_ids",
    )


def _canonical_cutoff(value: Any, *, now: datetime.datetime | None = None) -> str:
    raw_cutoff = _clean_text(value)
    if raw_cutoff is None:
        raise ValueError("provider_directory_current_version_census_cutoff_required")
    try:
        parsed_cutoff = datetime.datetime.fromisoformat(
            raw_cutoff.replace("Z", "+00:00")
        )
    except ValueError as exc:
        raise ValueError(
            "provider_directory_current_version_census_cutoff_invalid"
        ) from exc
    if parsed_cutoff.tzinfo is None or parsed_cutoff.utcoffset() is None:
        raise ValueError(
            "provider_directory_current_version_census_cutoff_timezone_required"
        )
    normalized_cutoff = parsed_cutoff.astimezone(datetime.UTC)
    current_time = now or datetime.datetime.now(datetime.UTC)
    if current_time.tzinfo is None or current_time.utcoffset() is None:
        raise ValueError("provider_directory_current_version_census_now_timezone_required")
    if normalized_cutoff > current_time.astimezone(datetime.UTC):
        raise ValueError(
            "provider_directory_current_version_census_cutoff_cannot_be_future"
        )
    return normalized_cutoff.isoformat(timespec="microseconds").replace(
        "+00:00", "Z"
    )


@dataclass(frozen=True)
class CurrentVersionCensusRequest:
    """Strict manual request before any reviewed source is resolved."""

    source_id: str
    cutoff: str
    resources: tuple[str, ...]
    strategy: ProviderDirectoryFHIRAcquisitionStrategy = (
        ProviderDirectoryFHIRAcquisitionStrategy.CUTOFF_BOUNDED_CURRENT_VERSION_CENSUS
    )


def _validate_current_version_census_task_types(
    task: Mapping[str, Any],
) -> None:
    has_invalid_task_field = bool(
        any(
            task.get(field_name) is not None
            and type(task.get(field_name)) is not bool
            for field_name in _CURRENT_VERSION_CENSUS_BOOLEAN_TASK_FIELDS
        )
        or any(
            task.get(field_name) is not None
            and type(task.get(field_name)) is not int
            for field_name in _CURRENT_VERSION_CENSUS_INTEGER_TASK_FIELDS
        )
        or any(
            task.get(field_name) is not None
            and type(task.get(field_name)) is not str
            for field_name in _CURRENT_VERSION_CENSUS_TEXT_TASK_FIELDS
        )
    )
    if has_invalid_task_field:
        raise ValueError(
            "provider_directory_current_version_census_task_type_invalid"
        )


def current_version_census_request(
    task: Mapping[str, Any],
    *,
    allowed_resources: Sequence[str],
    now: datetime.datetime | None = None,
) -> CurrentVersionCensusRequest | None:
    """Parse manual acquisition identity and reject ambiguous fields."""

    strategy = _strategy_from_value(
        task.get("provider_directory_acquisition_strategy")
    )
    raw_cutoff = task.get("provider_directory_census_cutoff")
    if strategy is ProviderDirectoryFHIRAcquisitionStrategy.CONFIGURED:
        if raw_cutoff not in (None, ""):
            raise ValueError(
                "provider_directory_current_version_census_cutoff_without_strategy"
            )
        return None
    _validate_current_version_census_task_types(task)
    if task.get("import_resources") is not True:
        raise ValueError(
            "provider_directory_current_version_census_import_resources_required"
        )
    source_ids = _source_id_vector(task)
    if len(source_ids) != 1:
        raise ValueError(
            "provider_directory_current_version_census_exactly_one_source_required"
        )
    raw_resources = task.get("resources")
    if raw_resources in (None, ""):
        raise ValueError(
            "provider_directory_current_version_census_resources_required"
        )
    resources = _strict_text_vector(
        raw_resources,
        field_name="provider_directory_current_version_census_resources",
        allowed_values=frozenset(allowed_resources),
    )
    return CurrentVersionCensusRequest(
        source_id=source_ids[0],
        cutoff=_canonical_cutoff(raw_cutoff, now=now),
        resources=resources,
        strategy=strategy,
    )


@dataclass(frozen=True)
class CurrentVersionCensusRuntime:
    """Execution controls admitted for reviewed manual acquisition."""

    checkpointing_enabled: bool
    full_refresh: bool
    resource_limit: int
    page_limit: int
    stream_batch_size: int
    source_concurrency: int
    resource_scan_concurrency: int
    linked_resource_limit: int
    linked_resource_deadline_seconds: int
    resource_deadline_seconds: int
    probe: bool
    seed_only: bool
    test_mode: bool
    dataset_rehydrate_only: bool
    dataset_followup_only: bool
    canonical_backfill_only: bool
    contact_backfill_only: bool
    publish_artifacts_only: bool
    local_seed_catalog: bool
    local_retest_catalog: bool
    supplemental_catalogs: bool
    local_supplemental_catalog_inputs: tuple[str, ...]
    remote_catalog_inputs: tuple[str, ...]
    bulk_export: bool
    stale_cleanup: bool
    publication_requested: bool
    defer_typed_materialization: bool
    bounded_source_selection: bool
    endpoint_scope_configured: bool
    credential_configured: bool
    open_only: bool
    include_auth_required: bool


def _validate_current_version_census_runtime_types(
    runtime: CurrentVersionCensusRuntime,
) -> None:
    """Reject JSON type lookalikes before evaluating runtime values."""
    boolean_controls = (
        runtime.checkpointing_enabled, runtime.full_refresh,
        runtime.probe, runtime.seed_only,
        runtime.test_mode,
        runtime.dataset_rehydrate_only,
        runtime.dataset_followup_only,
        runtime.canonical_backfill_only,
        runtime.contact_backfill_only,
        runtime.publish_artifacts_only,
        runtime.local_seed_catalog,
        runtime.local_retest_catalog,
        runtime.supplemental_catalogs,
        runtime.bulk_export,
        runtime.stale_cleanup,
        runtime.publication_requested,
        runtime.defer_typed_materialization,
        runtime.bounded_source_selection,
        runtime.endpoint_scope_configured,
        runtime.credential_configured,
        runtime.open_only,
        runtime.include_auth_required,
    )
    integer_controls = (
        runtime.resource_limit, runtime.page_limit,
        runtime.stream_batch_size,
        runtime.source_concurrency,
        runtime.resource_scan_concurrency,
        runtime.linked_resource_limit,
        runtime.linked_resource_deadline_seconds,
        runtime.resource_deadline_seconds,
    )
    has_invalid_remote_inputs = bool(
        type(runtime.remote_catalog_inputs) is not tuple
        or any(
            type(field_name) is not str or not field_name
            for field_name in runtime.remote_catalog_inputs
        )
    )
    has_invalid_local_supplemental_inputs = bool(
        type(runtime.local_supplemental_catalog_inputs) is not tuple
        or any(
            type(field_name) is not str or not field_name
            for field_name in runtime.local_supplemental_catalog_inputs
        )
    )
    if (
        any(type(control) is not bool for control in boolean_controls)
        or any(type(control) is not int for control in integer_controls)
        or has_invalid_remote_inputs
        or has_invalid_local_supplemental_inputs
    ):
        raise ValueError(
            "provider_directory_current_version_census_runtime_type_invalid"
        )


def validate_current_version_census_runtime(
    request: CurrentVersionCensusRequest,
    runtime: CurrentVersionCensusRuntime,
) -> None:
    """Keep the dormant manual strategy unbounded, serial, and unpublished."""

    if not isinstance(request, CurrentVersionCensusRequest):
        raise TypeError("current-version census request required")
    _validate_current_version_census_runtime_types(runtime)
    invalid_control_by_name = {
        "checkpointing": not runtime.checkpointing_enabled,
        "full_refresh": not runtime.full_refresh,
        "resource_limit": runtime.resource_limit != 0,
        "page_limit": runtime.page_limit != 0,
        "stream_batch_size": runtime.stream_batch_size <= 0,
        "source_concurrency": runtime.source_concurrency != 1,
        "resource_scan_concurrency": runtime.resource_scan_concurrency != 1,
        "linked_resource_limit": runtime.linked_resource_limit != 0,
        "linked_resource_deadline_seconds": (
            runtime.linked_resource_deadline_seconds != 0
        ),
        "resource_deadline_seconds": runtime.resource_deadline_seconds != 0,
        "probe": not runtime.probe,
        "seed_only": runtime.seed_only,
        "test_mode": runtime.test_mode,
        "dataset_rehydrate_only": runtime.dataset_rehydrate_only,
        "dataset_followup_only": runtime.dataset_followup_only,
        "canonical_backfill_only": runtime.canonical_backfill_only,
        "contact_backfill_only": runtime.contact_backfill_only,
        "publish_artifacts_only": runtime.publish_artifacts_only,
        "local_seed_catalog": not runtime.local_seed_catalog,
        "local_retest_catalog": runtime.local_retest_catalog,
        "supplemental_catalogs": runtime.supplemental_catalogs,
        "local_supplemental_catalog_inputs": bool(
            runtime.local_supplemental_catalog_inputs
        ),
        "remote_catalog_inputs": bool(runtime.remote_catalog_inputs),
        "bulk_export": runtime.bulk_export,
        "stale_cleanup": runtime.stale_cleanup,
        "publication": runtime.publication_requested,
        "defer_typed_materialization": (
            not runtime.defer_typed_materialization
        ),
        "bounded_source_selection": runtime.bounded_source_selection,
        "endpoint_scope": runtime.endpoint_scope_configured,
        "credential_config": runtime.credential_configured,
        "open_only": not runtime.open_only,
        "include_auth_required": runtime.include_auth_required,
    }
    failures = sorted(
        name
        for name, is_invalid in invalid_control_by_name.items()
        if is_invalid
    )
    if failures:
        raise ValueError(
            "provider_directory_current_version_census_runtime_invalid:"
            + ",".join(failures)
        )
