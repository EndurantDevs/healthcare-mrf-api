# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Dataset-bound Provider Directory source-summary API projection."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from process.provider_directory_source_summary import (
    SOURCE_SUMMARY_METADATA_KEY,
    validated_source_summary_outcome,
)


def source_summary_outcome_counts(
    dataset: Any,
    selected_resources: Sequence[str],
    count_by_resource: Mapping[str, int],
    relation_count_by_field: Mapping[str, int],
) -> dict[str, Any]:
    """Return sealed counters plus exact legacy relation fallbacks."""
    root_run_id = dataset.acquisition_root_run_id
    raw_summary = (
        dataset.publication_metadata.get(SOURCE_SUMMARY_METADATA_KEY)
        if isinstance(root_run_id, str) and root_run_id.strip()
        else None
    )
    source_count_by_field = validated_source_summary_outcome(
        raw_summary,
        expected_by_field={
            "dataset_id": dataset.dataset_id,
            "endpoint_id": dataset.endpoint_id,
            "acquisition_root_run_id": root_run_id,
            "dataset_hash": dataset.dataset_hash,
            "source_ids": list(dataset.source_ids),
            "selected_resources": list(selected_resources),
            "total_resources": dataset.resource_count,
            "resource_counts": dict(count_by_resource),
        },
        relation_count_by_field=relation_count_by_field,
    )
    combined_count_by_field = dict(source_count_by_field or {})
    for field_name, count in relation_count_by_field.items():
        combined_count_by_field.setdefault(field_name, count)
    return combined_count_by_field
