# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Reviewed absolute-ceiling approval contracts for PTG V4 storage."""

from __future__ import annotations

from dataclasses import replace

import pytest

from scripts import ptg_v4_dev_canary_storage_budget as storage_policy
from scripts.ptg_v4_dev_canary_storage_budget import STORAGE_CANARY_CASES
from tests.test_ptg_v4_dev_canary_storage_budget import (
    _physical_storage_approval,
)


def test_checked_in_absolute_ceiling_rejects_unreviewed_extra_headroom() -> None:
    """A source-controlled ceiling cannot exceed measured plus two percent."""

    case = STORAGE_CANARY_CASES[1]
    inconsistent_approval = replace(
        _physical_storage_approval(case=case),
        approved_graph_physical_storage_bytes=1_021,
    )

    with pytest.raises(RuntimeError, match="incomplete or inconsistent"):
        storage_policy._validate_storage_approval(
            case,
            inconsistent_approval,
        )
    with pytest.raises(RuntimeError, match="incomplete or inconsistent"):
        storage_policy._validate_storage_approval(
            case,
            replace(inconsistent_approval, measurement_image_identity=""),
        )


def test_storage_approval_requires_exact_two_percent() -> None:
    """Tolerance is fixed at two percent rather than operator-selected."""

    case = STORAGE_CANARY_CASES[1]
    approval = _physical_storage_approval(case=case)

    for tolerance_basis_points in (199, 201):
        with pytest.raises(RuntimeError, match="incomplete or inconsistent"):
            storage_policy._validate_storage_approval(
                case,
                replace(
                    approval,
                    tolerance_basis_points=tolerance_basis_points,
                ),
            )


def test_storage_approval_rejects_cross_case_or_mistyped_evidence() -> None:
    """Approval identity is bound to one case, measurement, and snapshot."""

    case = STORAGE_CANARY_CASES[1]
    approval = _physical_storage_approval(case=case)

    with pytest.raises(RuntimeError, match="incomplete or inconsistent"):
        storage_policy._validate_storage_approval(
            STORAGE_CANARY_CASES[2],
            approval,
        )
    with pytest.raises(RuntimeError, match="incomplete or inconsistent"):
        storage_policy._validate_storage_approval(
            case,
            replace(approval, measurement_evidence_sha256="0" * 64),
        )
    with pytest.raises(RuntimeError, match="incomplete or inconsistent"):
        storage_policy._validate_storage_approval(
            case,
            replace(approval, measurement_snapshot_id="ptg2:v4:mistyped"),
        )
