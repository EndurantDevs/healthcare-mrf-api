# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Small value projections shared by Provider Directory catalog outcomes."""

from __future__ import annotations

from typing import Any

from api import provider_directory_source_outcomes as outcomes


def canonical_identity_text(value: Any, *, limit: int) -> str | None:
    """Return an already-canonical bounded identity or explicit null."""

    if not isinstance(value, str):
        return None
    text = value.strip()
    if not text or text != value or len(text) > limit:
        return None
    return text


def catalog_source_id_groups(
    catalog_items: list[dict[str, Any]],
) -> set[tuple[str, ...]]:
    """Return exact nonempty source groups from public catalog entries."""

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


def current_outcome_summary(
    dataset: outcomes._CurrentPublishedDataset,
) -> dict[str, Any]:
    """Expose the published incumbent plus its authoritative lineage identity."""

    return {
        **outcomes._outcome_summary(dataset),
        "endpoint_id": dataset.endpoint_id,
        "acquisition_root_run_id": dataset.acquisition_root_run_id,
        "dataset_hash": dataset.dataset_hash,
    }
