# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Read-only outcome summaries for the static Provider Directory catalog."""

from __future__ import annotations

import datetime as dt
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from api.provider_directory_source_dataset_selection import (
    _source_local_dataset_statement as _current_published_dataset_statement,
)
from api.provider_directory_source_summary_outcome import source_summary_outcome_counts
from db.models import db
from process.import_status_events import isoformat_utc

_PUBLISHED_DATASET_STATUS = "published"
_SUPERSEDED_DATASET_STATUS = "superseded"
_VALIDATED_DATASET_STATUS = "validated"
_OUTCOME_RESOURCE_COUNTS_METADATA_KEY = "outcome_resource_counts_v1"
_TWIN_ROOT_VERIFICATION_METADATA_KEY = "twin_root_verification_v1"
_INTERNAL_RESOURCE_TYPE_PREFIX = "LU:"
_SUPPORTED_RESOURCE_TYPES = frozenset({
    "Endpoint", "HealthcareService", "InsurancePlan", "Location",
    "Organization", "OrganizationAffiliation", "Practitioner", "PractitionerRole",
})

_RELATION_COUNT_FIELDS = (
    ("dataset_network_plan", "InsurancePlan",
     "insurance_plan_resource_count", "network_plan_links"),
    ("dataset_affiliation_organization", "OrganizationAffiliation",
     "affiliation_resource_count", "organization_affiliation_links"),
)
@dataclass(frozen=True)
class _CurrentPublishedDataset:
    source_ids: tuple[str, ...]
    endpoint_id: str
    dataset_id: str
    acquisition_root_run_id: str | None
    dataset_hash: str | None
    status: str
    is_current: bool
    sealed_at: dt.datetime
    validated_at: dt.datetime | None
    published_at: dt.datetime | None
    resource_count: int
    publication_metadata: Mapping[str, Any]


def _clean_text(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    text = value.strip()
    return text or None


def _utc_datetime(value: Any) -> dt.datetime | None:
    if isinstance(value, str):
        raw_value = value.strip()
        if not raw_value:
            return None
        try:
            value = dt.datetime.fromisoformat(
                raw_value[:-1] + "+00:00"
                if raw_value.endswith("Z")
                else raw_value
            )
        except ValueError:
            return None
    if not isinstance(value, dt.datetime):
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=dt.UTC)
    return value.astimezone(dt.UTC)


def _valid_nonnegative_count(value: Any) -> int | None:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        return None
    return value


def _normalized_text_tuple(
    value: Any,
    *,
    reject_internal_resources: bool = False,
) -> tuple[str, ...] | None:
    if not isinstance(value, (list, tuple)) or not value:
        return None
    normalized_values: list[str] = []
    for raw_value in value:
        normalized_value = _clean_text(raw_value)
        if (
            normalized_value is None
            or normalized_value in normalized_values
            or (
                reject_internal_resources
                and normalized_value.startswith(_INTERNAL_RESOURCE_TYPE_PREFIX)
            )
        ):
            return None
        normalized_values.append(normalized_value)
    return tuple(sorted(normalized_values))


def _sealed_dataset_state(
    dataset_record: Mapping[str, Any],
) -> tuple[str, bool, dt.datetime, dt.datetime | None, dt.datetime | None] | None:
    status = _clean_text(dataset_record.get("status"))
    validated_at = _utc_datetime(dataset_record.get("validated_at"))
    published_at = _utc_datetime(dataset_record.get("published_at"))
    superseded_at = _utc_datetime(dataset_record.get("superseded_at"))
    dataset_state = {
        _VALIDATED_DATASET_STATUS: (False, validated_at)
        if published_at is None and superseded_at is None else (False, None),
        _PUBLISHED_DATASET_STATUS: (
            True,
            published_at if superseded_at is None else None,
        ),
        _SUPERSEDED_DATASET_STATUS: (
            False,
            published_at if superseded_at is not None else None,
        ),
    }.get(status or "")
    if (
        dataset_state is None
        or dataset_record.get("is_current") is not dataset_state[0]
        or dataset_state[1] is None
    ):
        return None
    return status, dataset_state[0], dataset_state[1], validated_at, published_at


def _current_dataset_from_row(
    dataset_record: Mapping[str, Any],
    expected_source_id_groups: set[tuple[str, ...]],
) -> _CurrentPublishedDataset | None:
    publication_metadata = dataset_record.get("publication_metadata")
    source_ids = (
        _normalized_text_tuple(publication_metadata.get("source_ids"))
        if isinstance(publication_metadata, Mapping)
        else None
    )
    state = _sealed_dataset_state(dataset_record)
    endpoint_id = _clean_text(dataset_record.get("endpoint_id"))
    dataset_id = _clean_text(dataset_record.get("dataset_id"))
    resource_count = _valid_nonnegative_count(
        dataset_record.get("resource_count")
    )
    if (
        source_ids not in expected_source_id_groups
        or state is None
        or endpoint_id is None
        or dataset_id is None
        or resource_count is None
        or not isinstance(publication_metadata, Mapping)
        or (
            current_source_ids := _normalized_text_tuple(
                dataset_record.get("current_source_ids")
            )
        ) is None
        or not set(source_ids).issubset(current_source_ids)
    ):
        return None
    return _CurrentPublishedDataset(
        source_ids=source_ids,
        endpoint_id=endpoint_id,
        dataset_id=dataset_id,
        acquisition_root_run_id=_clean_text(
            dataset_record.get("acquisition_root_run_id")
        ),
        dataset_hash=_clean_text(dataset_record.get("dataset_hash")),
        status=state[0],
        is_current=state[1],
        sealed_at=state[2],
        validated_at=state[3],
        published_at=state[4],
        resource_count=resource_count,
        publication_metadata=publication_metadata,
    )


def _dataset_order_key(
    dataset: _CurrentPublishedDataset,
) -> tuple[dt.datetime, str, str]:
    return dataset.sealed_at, dataset.dataset_id, dataset.endpoint_id


async def _current_published_dataset_by_source_ids(
    source_id_groups: set[tuple[str, ...]],
) -> dict[tuple[str, ...], _CurrentPublishedDataset]:
    if not source_id_groups:
        return {}
    query_result = await db.execute(
        _current_published_dataset_statement(source_id_groups)
    )
    selected_by_source_ids: dict[
        tuple[str, ...],
        _CurrentPublishedDataset,
    ] = {}
    for dataset_record in query_result.mappings().all():
        candidate = _current_dataset_from_row(
            dataset_record,
            source_id_groups,
        )
        if candidate is None:
            continue
        selected = selected_by_source_ids.get(candidate.source_ids)
        if selected is None or _dataset_order_key(candidate) > _dataset_order_key(
            selected
        ):
            selected_by_source_ids[candidate.source_ids] = candidate
    return selected_by_source_ids


def _selected_resources(
    dataset: _CurrentPublishedDataset,
) -> tuple[str, ...] | None:
    return _normalized_text_tuple(
        dataset.publication_metadata.get("selected_resources"),
        reject_internal_resources=True,
    )


def _validated_resource_counts(
    raw_counts: Any,
    selected_resources: tuple[str, ...],
    expected_total: int,
    *,
    allow_extra_resources: bool = False,
) -> dict[str, int] | None:
    if not isinstance(raw_counts, Mapping):
        return None
    resource_count_by_type: dict[str, int] = {}
    for raw_resource_type, raw_count in raw_counts.items():
        resource_type = _clean_text(raw_resource_type)
        resource_count = _valid_nonnegative_count(raw_count)
        if (
            resource_type is None
            or resource_type.startswith(_INTERNAL_RESOURCE_TYPE_PREFIX)
            or resource_type not in _SUPPORTED_RESOURCE_TYPES
            or resource_type in resource_count_by_type
            or resource_count is None
        ):
            return None
        resource_count_by_type[resource_type] = resource_count
    actual_resource_types = set(resource_count_by_type)
    selected_resource_types = set(selected_resources)
    has_expected_resource_types = (
        selected_resource_types.issubset(actual_resource_types)
        if allow_extra_resources
        else selected_resource_types == actual_resource_types
    )
    if not has_expected_resource_types or sum(
        resource_count_by_type.values()
    ) != expected_total:
        return None
    return dict(sorted(resource_count_by_type.items()))


def _is_proof_bound_to_dataset(
    proof: Mapping[str, Any],
    dataset: _CurrentPublishedDataset,
    selected_resources: tuple[str, ...],
    *,
    require_dataset_id: bool,
) -> bool:
    if require_dataset_id and proof.get("dataset_id") != dataset.dataset_id:
        return False
    return bool(
        dataset.dataset_hash is not None
        and proof.get("endpoint_id") == dataset.endpoint_id
        and _clean_text(proof.get("acquisition_root_run_id"))
        == dataset.acquisition_root_run_id
        and proof.get("dataset_hash") == dataset.dataset_hash
        and _normalized_text_tuple(proof.get("source_ids"))
        == dataset.source_ids
        and _normalized_text_tuple(
            proof.get("selected_resources"),
            reject_internal_resources=True,
        )
        == selected_resources
        and _valid_nonnegative_count(proof.get("resource_count"))
        == dataset.resource_count
    )


def _versioned_outcome_resource_counts(
    dataset: _CurrentPublishedDataset,
    selected_resources: tuple[str, ...],
) -> dict[str, int] | None:
    proof = dataset.publication_metadata.get(
        _OUTCOME_RESOURCE_COUNTS_METADATA_KEY
    )
    if (
        not isinstance(proof, Mapping)
        or proof.get("complete") is not True
        or proof.get("version") != 1
        or not _is_proof_bound_to_dataset(
            proof,
            dataset,
            selected_resources,
            require_dataset_id=True,
        )
    ):
        return None
    return _validated_resource_counts(
        proof.get("resource_counts"),
        selected_resources,
        dataset.resource_count,
        allow_extra_resources=True,
    )


def _matched_twin_resource_counts(
    dataset: _CurrentPublishedDataset,
    selected_resources: tuple[str, ...],
) -> dict[str, int] | None:
    verification = dataset.publication_metadata.get(
        _TWIN_ROOT_VERIFICATION_METADATA_KEY
    )
    proof = (
        verification.get("proof")
        if isinstance(verification, Mapping)
        else None
    )
    if (
        not isinstance(verification, Mapping)
        or verification.get("result") != "matched"
        or verification.get("mismatch_fields") != []
        or not isinstance(proof, Mapping)
        or not _is_proof_bound_to_dataset(
            proof,
            dataset,
            selected_resources,
            require_dataset_id=False,
        )
    ):
        return None
    return _validated_resource_counts(
        proof.get("resource_counts"),
        selected_resources,
        dataset.resource_count,
        allow_extra_resources=True,
    )


def _diagnostic_resource_counts(
    dataset: _CurrentPublishedDataset,
    selected_resources: tuple[str, ...],
) -> dict[str, int] | None:
    diagnostics = dataset.publication_metadata.get("resource_diagnostics")
    if not isinstance(diagnostics, Mapping):
        return None
    raw_count_by_resource: dict[str, int] = {}
    for resource_type in selected_resources:
        diagnostic = diagnostics.get(resource_type)
        if (
            not isinstance(diagnostic, Mapping)
            or diagnostic.get("complete") is not True
            or diagnostic.get("error") not in (None, "")
            or diagnostic.get("bounded") not in (None, False)
            or diagnostic.get("next_url_remaining") not in (None, False)
        ):
            return None
        rows_fetched = _valid_nonnegative_count(
            diagnostic.get("rows_fetched")
        )
        if rows_fetched is None:
            return None
        raw_count_by_resource[resource_type] = rows_fetched
    return _validated_resource_counts(
        raw_count_by_resource,
        selected_resources,
        dataset.resource_count,
    )


def _exact_resource_counts(
    dataset: _CurrentPublishedDataset,
) -> dict[str, int] | None:
    selected_resources = _selected_resources(dataset)
    if selected_resources is None:
        return None
    for resource_count_reader in (
        _versioned_outcome_resource_counts,
        _matched_twin_resource_counts,
        _diagnostic_resource_counts,
    ):
        resource_counts = resource_count_reader(dataset, selected_resources)
        if resource_counts is not None:
            return resource_counts
    return None


def _is_version_one(value: Any) -> bool:
    return not isinstance(value, bool) and value in (1, "1")


def _precomputed_relation_counts(
    dataset: _CurrentPublishedDataset,
    resource_counts: Mapping[str, int],
) -> dict[str, int]:
    summary_count_by_field: dict[str, int] = {}
    for (
        metadata_key,
        source_resource_type,
        source_count_field,
        output_field,
    ) in _RELATION_COUNT_FIELDS:
        proof = dataset.publication_metadata.get(metadata_key)
        if (
            not isinstance(proof, Mapping)
            or proof.get("complete") is not True
            or not _is_version_one(proof.get("version"))
            or proof.get("dataset_id") != dataset.dataset_id
            or _clean_text(proof.get("acquisition_root_run_id"))
            != dataset.acquisition_root_run_id
        ):
            continue
        proof_source_count = _valid_nonnegative_count(
            proof.get(source_count_field)
        )
        edge_count = _valid_nonnegative_count(proof.get("edge_count"))
        if (
            proof_source_count != resource_counts.get(source_resource_type, 0)
            or edge_count is None
        ):
            continue
        summary_count_by_field[output_field] = edge_count
    return summary_count_by_field


def _outcome_summary(dataset: _CurrentPublishedDataset) -> dict[str, Any]:
    summary_map: dict[str, Any] = {
        "dataset_id": dataset.dataset_id,
        "status": dataset.status,
        "is_current": dataset.is_current,
        "total_resources": dataset.resource_count,
    }
    if dataset.validated_at is not None:
        summary_map["validated_at"] = isoformat_utc(dataset.validated_at)
    if dataset.published_at is not None:
        summary_map["published_at"] = isoformat_utc(dataset.published_at)
    resource_counts = _exact_resource_counts(dataset)
    if resource_counts is not None:
        summary_map["resource_counts"] = resource_counts
        summary_map.update(
            source_summary_outcome_counts(
                dataset,
                _selected_resources(dataset) or (),
                resource_counts,
                _precomputed_relation_counts(dataset, resource_counts),
            )
        )
    return summary_map


def _catalog_source_id_groups(
    catalog_items: list[dict[str, Any]],
) -> set[tuple[str, ...]]:
    return {
        source_ids
        for catalog_entry in catalog_items
        if (
            source_ids := _normalized_text_tuple(
                catalog_entry.get("source_ids")
            )
        )
        is not None
    }


async def enrich_provider_directory_source_catalog(
    catalog: Mapping[str, Any],
) -> dict[str, Any]:
    """Attach latest sealed source outcomes without scanning resource rows."""
    raw_items = catalog.get("items")
    if not isinstance(raw_items, list):
        return dict(catalog)
    catalog_items = [
        dict(catalog_entry)
        for catalog_entry in raw_items
        if isinstance(catalog_entry, Mapping)
    ]
    dataset_by_source_ids = await _current_published_dataset_by_source_ids(
        _catalog_source_id_groups(catalog_items)
    )

    enriched_items: list[dict[str, Any]] = []
    for catalog_entry in catalog_items:
        enriched_entry_map = dict(catalog_entry)
        source_ids = _normalized_text_tuple(catalog_entry.get("source_ids"))
        dataset = dataset_by_source_ids.get(source_ids or ())
        if dataset is not None:
            enriched_entry_map["outcome_summary"] = _outcome_summary(dataset)
        enriched_items.append(enriched_entry_map)
    return {**dict(catalog), "items": enriched_items}
