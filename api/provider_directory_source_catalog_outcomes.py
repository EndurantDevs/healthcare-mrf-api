# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Latest and current outcome enrichment for Provider Directory sources."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from api import provider_directory_source_outcomes as outcomes
from api.provider_directory_source_dataset_selection import (
    _source_local_current_published_dataset_statement,
)
from db.models import db


def _canonical_identity_text(value: Any, *, limit: int) -> str | None:
    if not isinstance(value, str):
        return None
    text = value.strip()
    if not text or text != value or len(text) > limit:
        return None
    return text


def _profile_current_dataset_from_row(
    dataset_record: Mapping[str, Any],
    expected_source_id_groups: set[tuple[str, ...]],
) -> outcomes._CurrentPublishedDataset | None:
    """Validate the identity required by authoritative Profile selection."""

    dataset = outcomes._current_dataset_from_row(
        dataset_record,
        expected_source_id_groups,
    )
    dataset_hash = _canonical_identity_text(
        dataset_record.get("dataset_hash"),
        limit=64,
    )
    root_run_id = _canonical_identity_text(
        dataset_record.get("acquisition_root_run_id"),
        limit=64,
    )
    if (
        dataset is None
        or dataset.status != "published"
        or dataset.is_current is not True
        or _canonical_identity_text(dataset_record.get("endpoint_id"), limit=128)
        != dataset.endpoint_id
        or _canonical_identity_text(dataset_record.get("dataset_id"), limit=128)
        != dataset.dataset_id
        or root_run_id != dataset.acquisition_root_run_id
        or root_run_id is None
        or dataset_hash != dataset.dataset_hash
        or dataset_hash is None
        or len(dataset_hash) != 64
        or any(character not in "0123456789abcdef" for character in dataset_hash)
    ):
        return None
    return dataset


async def _profile_current_dataset_by_source_ids(
    source_id_groups: set[tuple[str, ...]],
) -> dict[tuple[str, ...], outcomes._CurrentPublishedDataset]:
    """Read exact current published identities using Profile state semantics."""

    if not source_id_groups:
        return {}
    query_result = await db.execute(
        _source_local_current_published_dataset_statement(source_id_groups)
    )
    selected_by_source_ids: dict[
        tuple[str, ...],
        outcomes._CurrentPublishedDataset,
    ] = {}
    for dataset_record in query_result.mappings().all():
        candidate = _profile_current_dataset_from_row(
            dataset_record,
            source_id_groups,
        )
        if candidate is None:
            continue
        selected = selected_by_source_ids.get(candidate.source_ids)
        if selected is None or outcomes._dataset_order_key(
            candidate
        ) > outcomes._dataset_order_key(selected):
            selected_by_source_ids[candidate.source_ids] = candidate
    return selected_by_source_ids


def _catalog_source_id_groups(
    catalog_items: list[dict[str, Any]],
) -> set[tuple[str, ...]]:
    return {
        source_ids
        for catalog_entry in catalog_items
        if (
            source_ids := outcomes._normalized_text_tuple(
                catalog_entry.get("source_ids")
            )
        )
        is not None
    }


def _current_outcome_summary(
    dataset: outcomes._CurrentPublishedDataset,
) -> dict[str, Any]:
    """Expose the published incumbent plus its authoritative lineage identity."""

    return {
        **outcomes._outcome_summary(dataset),
        "endpoint_id": dataset.endpoint_id,
        "acquisition_root_run_id": dataset.acquisition_root_run_id,
        "dataset_hash": dataset.dataset_hash,
    }


async def enrich_provider_directory_source_catalog(
    catalog: Mapping[str, Any],
) -> dict[str, Any]:
    """Attach latest sealed and current published source outcome summaries."""

    raw_items = catalog.get("items")
    if not isinstance(raw_items, list):
        return dict(catalog)
    catalog_items = [
        dict(catalog_entry)
        for catalog_entry in raw_items
        if isinstance(catalog_entry, Mapping)
    ]
    source_id_groups = _catalog_source_id_groups(catalog_items)
    dataset_by_source_ids = await outcomes._current_published_dataset_by_source_ids(
        source_id_groups
    )
    current_dataset_by_source_ids = await _profile_current_dataset_by_source_ids(
        source_id_groups
    )

    enriched_items: list[dict[str, Any]] = []
    for catalog_entry in catalog_items:
        enriched_entry_map = dict(catalog_entry)
        source_ids = outcomes._normalized_text_tuple(
            catalog_entry.get("source_ids")
        )
        dataset = dataset_by_source_ids.get(source_ids or ())
        if dataset is not None:
            enriched_entry_map["outcome_summary"] = outcomes._outcome_summary(
                dataset
            )
        current_dataset = current_dataset_by_source_ids.get(source_ids or ())
        if current_dataset is not None:
            enriched_entry_map["current_outcome_summary"] = (
                _current_outcome_summary(current_dataset)
            )
        enriched_items.append(enriched_entry_map)
    return {**dict(catalog), "items": enriched_items}
