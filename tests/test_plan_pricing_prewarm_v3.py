# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""V3 ranked-shape selection for plan-pricing prewarm."""

from __future__ import annotations

import pytest

from api import plan_pricing_prewarm as prewarm

from .test_plan_pricing_prewarm import PROJECTION_ID, _Session


@pytest.mark.asyncio
async def test_v3_shape_selection_reads_only_sealed_ranked_shortlist() -> None:
    session = _Session(
        [
            {
                "projection_id": PROJECTION_ID,
                "shape_rank": 1,
                "code_system": "CPT",
                "code": "27447",
                "geo_cell": "10001",
                "provider_count": 30,
            },
            {
                "projection_id": PROJECTION_ID,
                "shape_rank": 2,
                "code_system": "HCPCS",
                "code": "G0439",
                "geo_cell": "10002",
                "provider_count": 20,
            },
        ]
    )

    selected_shapes = await prewarm._select_shapes(
        session, PROJECTION_ID, prewarm.PROJECTION_CONTRACT
    )

    assert selected_shapes == (
        prewarm.PrewarmShape("CPT", "27447", "10001", 30),
        prewarm.PrewarmShape("HCPCS", "G0439", "10002", 20),
    )
    statement = session.statements[0][0]
    assert "plan_pricing_prewarm_shape" in statement
    assert "ORDER BY shape_rank" in statement
    assert "plan_pricing_cell_aggregate" not in statement


@pytest.mark.asyncio
async def test_v3_shape_selection_rejects_rank_gaps_and_unknown_contracts() -> None:
    shape_row_by_field = {
        "projection_id": PROJECTION_ID,
        "shape_rank": 2,
        "code_system": "HCPCS",
        "code": "G0439",
        "geo_cell": "10002",
        "provider_count": 20,
    }
    with pytest.raises(ValueError, match="aggregate row"):
        await prewarm._select_shapes(
            _Session([shape_row_by_field]),
            PROJECTION_ID,
            prewarm.PROJECTION_CONTRACT,
        )
    with pytest.raises(ValueError, match="contract is unsupported"):
        await prewarm._select_shapes(
            _Session([]), PROJECTION_ID, "future-contract"
        )
