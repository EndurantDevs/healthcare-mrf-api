# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import pytest

from process.provider_directory_rooted_graph_acquisition import (
    ProviderDirectoryRootedGraphAcquisitionConfig,
    acquire_provider_directory_rooted_graph_twins,
)
from tests.provider_directory_rooted_graph_acquisition_test_support import identity
from tests.provider_directory_rooted_graph_runtime_test_support import (
    RuntimeHarness,
    enabled_config,
)


@pytest.mark.asyncio
async def test_pair_runs_fixed_points_in_order_with_distinct_sessions() -> None:
    harness = RuntimeHarness()
    receipt = await acquire_provider_directory_rooted_graph_twins(
        identity("baseline"),
        identity("candidate"),
        config=enabled_config(),
        database=object(),
        dependencies=harness.dependencies(),
    )

    assert receipt.rooted_graphs_match is True
    assert harness.ledger.opened == [1, 2]
    assert harness.ledger.closed == [1, 2]
    fetch_sessions = [event[1] for event in harness.events if event[0] == "fetch"]
    assert fetch_sessions[:4] == [1, 1, 1, 1]
    assert fetch_sessions[4:] == [2, 2, 2, 2]
    for role in ("baseline", "candidate"):
        role_events = [event for event in harness.events if role in event]
        assert any(event[0] == "claim_census" for event in role_events)
        assert any(event == ("seal", role) for event in role_events)
    assert [event[:2] for event in harness.events if event[0] == "revalidate"] == [
        ("revalidate", "baseline"),
        ("revalidate", "baseline"),
        ("revalidate", "candidate"),
        ("revalidate", "candidate"),
        ("revalidate", "baseline"),
        ("revalidate", "candidate"),
    ]


def test_default_disabled_and_root_timeout_is_hard_bounded() -> None:
    assert ProviderDirectoryRootedGraphAcquisitionConfig().enabled is False
    with pytest.raises(ValueError, match="config_invalid"):
        ProviderDirectoryRootedGraphAcquisitionConfig(
            root_timeout_seconds=30 * 24 * 60 * 60 + 1
        )
