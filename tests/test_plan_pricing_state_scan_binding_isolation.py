# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Release-binding isolation for plan-pricing state-scan hydration."""

from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from api import plan_pricing_state_scan_hydration as hydration
from api import ptg2_serving as serving


def _binding(ordinal: int) -> SimpleNamespace:
    return SimpleNamespace(
        binding_ordinal=ordinal,
        snapshot_id=f"snapshot-{ordinal}",
        source_key=f"source-{ordinal}",
        plan_id=f"plan-{ordinal}",
        plan_market_type="group",
        role="in_network",
        required=True,
    )


@pytest.mark.asyncio
async def test_selected_group_hydration_merges_within_each_release_binding(
    monkeypatch,
) -> None:
    """Never pass snapshot-local source keys through one cross-binding merge."""

    release_bindings = (_binding(1), _binding(2))
    selection = SimpleNamespace(
        in_network_bindings=release_bindings,
        serving_tables_for_snapshot=lambda _snapshot_id: SimpleNamespace(),
    )
    occurrence_rows = [
        {"npi": 1000000001, "binding_ordinal": binding.binding_ordinal}
        for binding in release_bindings
    ]
    monkeypatch.setattr(
        hydration,
        "_validated_providers_by_npi",
        lambda *_args: {1000000001: {}},
    )
    monkeypatch.setattr(
        hydration,
        "_hydrate_binding",
        AsyncMock(
            side_effect=[
                ([{"npi": 1000000001, "plan_id": binding.plan_id}], 1)
                for binding in release_bindings
            ]
        ),
    )
    merge_rates = Mock(side_effect=lambda provider_rates, _billing: provider_rates)
    monkeypatch.setattr(serving, "_merge_provider_rates_for_request", merge_rates)

    hydrated_items = await hydration.hydrate_selected_groups(
        object(),
        selection,
        {"plan_release_id": "hprelease_01J00000000000000000000000"},
        occurrence_rows,
        {1000000001: b"ignored-by-test"},
    )

    assert [
        [provider_rate["plan_id"] for provider_rate in call.args[0]]
        for call in merge_rates.call_args_list
    ] == [["plan-1"], ["plan-2"]]
    assert [provider_rate["network"] for provider_rate in hydrated_items] == [
        "source-1",
        "source-2",
    ]
