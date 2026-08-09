# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Pure projection proof for snapshot-effective NPPES lifecycle dates."""

from __future__ import annotations

import pytest

from public_evidence.nppes_registry_primitives import scan_nppes_registry_row
from public_evidence.nppes_registry_replay_contract import (
    build_nppes_manifest_from_rows,
)
from tests.public_evidence_nppes_registry_support import (
    HEADER,
    archive_identity,
    equal_day_reactivated_type_1_row,
    future_deactivation_type_1_row,
    future_last_update_type_1_row,
    orphan_reactivated_type_2_row,
)


@pytest.mark.parametrize(
    ("row_values", "expected_state", "expected_start"),
    (
        (
            equal_day_reactivated_type_1_row(),
            "active",
            "2026-06-15T00:00:00Z",
        ),
        (
            orphan_reactivated_type_2_row(),
            "active",
            "2026-06-15T00:00:00Z",
        ),
        (
            (
                "1003022534",
                "1",
                "05/23/2005",
                "07/12/2026",
                "06/20/2026",
                "06/15/2026",
            ),
            "deactivated",
            "2026-06-20T00:00:00Z",
        ),
        (
            future_deactivation_type_1_row(),
            "active",
            "2005-05-23T00:00:00Z",
        ),
        (
            future_last_update_type_1_row(),
            "active",
            "2005-05-23T00:00:00Z",
        ),
        (
            (
                "1003022534",
                "1",
                "05/23/2005",
                "07/13/2026",
                "06/20/2026",
                "07/13/2026",
            ),
            "deactivated",
            "2026-06-20T00:00:00Z",
        ),
    ),
)
def test_row_projection_uses_as_of_lifecycle_events(
    row_values: tuple[str, ...],
    expected_state: str,
    expected_start: str,
) -> None:
    observation = scan_nppes_registry_row(
        archive_identity(), HEADER, row_values, 1
    )
    assert observation.enumeration_state == expected_state
    assert observation.effective_start_at == expected_start


def test_future_source_dates_remain_in_the_hashed_observation() -> None:
    future_deactivation = scan_nppes_registry_row(
        archive_identity(), HEADER, future_deactivation_type_1_row(), 1
    )
    future_last_update = scan_nppes_registry_row(
        archive_identity(), HEADER, future_last_update_type_1_row(), 1
    )
    baseline_values = list(future_last_update_type_1_row())
    baseline_values[3] = "07/12/2026"
    baseline_last_update = scan_nppes_registry_row(
        archive_identity(), HEADER, tuple(baseline_values), 1
    )
    assert future_deactivation.npi_deactivation_date == "2026-07-13"
    assert future_last_update.last_update_date == "2026-07-13"
    assert (
        future_last_update.enumeration_state
        == baseline_last_update.enumeration_state
    )
    assert (
        future_last_update.effective_start_at
        == baseline_last_update.effective_start_at
    )
    assert (
        future_last_update.payload_sha256
        != baseline_last_update.payload_sha256
    )
    assert future_last_update.leaf_sha256 != baseline_last_update.leaf_sha256


def test_manifest_accepts_observed_lifecycle_date_shapes() -> None:
    rows = (
        equal_day_reactivated_type_1_row(),
        orphan_reactivated_type_2_row(),
        future_deactivation_type_1_row(),
        future_last_update_type_1_row(),
    )
    manifest = build_nppes_manifest_from_rows(archive_identity(), HEADER, rows)
    assert manifest.source_record_count == 4
    assert manifest.projected_record_count == 4
    assert manifest.excluded_record_count == 0
    assert manifest.exclusion_counts == ()
