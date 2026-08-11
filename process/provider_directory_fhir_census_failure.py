# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Terminal failure classification for current-version FHIR census proofs."""

from __future__ import annotations

from typing import Any, Mapping

from process.provider_directory_fhir_subset_profiles import (
    is_advertised_pre_in_terminal_window,
)


def _exact_census_failure(
    *,
    pre_count: int,
    post_count: int,
    processed_rows: int,
    unique_candidate_rows: int,
) -> str | None:
    if post_count != pre_count:
        return "census_drift"
    if processed_rows < pre_count:
        return "cursor_loss"
    if processed_rows != unique_candidate_rows:
        return "duplicate_resource_ids"
    if processed_rows != pre_count:
        return "processed_count_mismatch"
    return None


def completion_failure(
    max_advertised_count_decrease: int | None,
    *,
    is_terminal_count_window_required: bool = False,
    terminal_page_geometry: Mapping[str, Any] | None = None,
    pre_count: int,
    post_count: int,
    processed_rows: int,
    unique_candidate_rows: int,
) -> str | None:
    """Return the exact or declared-subset terminal failure code."""

    if max_advertised_count_decrease is None:
        return _exact_census_failure(
            pre_count=pre_count,
            post_count=post_count,
            processed_rows=processed_rows,
            unique_candidate_rows=unique_candidate_rows,
        )
    advertised_count_decrease = pre_count - post_count
    if not 0 <= advertised_count_decrease <= max_advertised_count_decrease:
        return "census_drift"
    if is_terminal_count_window_required and not (
        is_advertised_pre_in_terminal_window(
            pre_count,
            terminal_page_geometry,
        )
    ):
        return "terminal_count_window_mismatch"
    if processed_rows != unique_candidate_rows:
        return "duplicate_resource_ids"
    if unique_candidate_rows > post_count:
        return "returned_count_exceeds_advertised"
    return None
