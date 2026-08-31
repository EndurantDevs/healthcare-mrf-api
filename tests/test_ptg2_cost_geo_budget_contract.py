# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Keep deterministic cost-and-ZIP budget refusals on the retryable contract."""

from unittest.mock import AsyncMock

import pytest

from api import ptg2_serving as serving
from tests.test_ptg2_geo_rate_prefix import _production_tables
from tests.test_ptg2_manifest_search_transitions import (
    _CODE_ROW,
    _install_base_dependencies,
    _query_args,
    _search,
)


@pytest.mark.asyncio
async def test_manifest_cost_geo_budget_refusal_offers_distance_retry(monkeypatch):
    _install_base_dependencies(monkeypatch)
    monkeypatch.setattr(
        serving,
        "_strict_cost_provider_expansion_selection",
        AsyncMock(
            side_effect=serving.PTG2OnlineWorkBudgetExceeded("candidate_members")
        ),
    )

    with pytest.raises(serving.PTG2LocationScopeError) as refusal:
        await _search(
            args=_query_args(
                code="74177",
                include_providers=True,
                zip5="38103",
                zip_radius_miles=25,
                order_by="total_allowed_amount",
            ),
            code_rows=[{**_CODE_ROW, "reported_code": "74177", "rate_count": 1}],
            serving_tables=_production_tables(),
        )

    assert refusal.value.allows_distance_retry is True
