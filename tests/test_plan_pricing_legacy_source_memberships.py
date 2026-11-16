# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Legacy aggregate reverse membership stays inside existing request bounds."""

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from api import plan_pricing_legacy_source_memberships as memberships
from api import ptg2_serving as serving
from api.plan_pricing_projection_contract import PlanPricingProjectionUnsupported
from process.ptg_parts.ptg2_manifest_artifacts import ManifestReadLimitError


@pytest.mark.asyncio
async def test_legacy_graph_legs_admit_members_before_final_expansion(monkeypatch):
    reader = AsyncMock(side_effect=[{"member": ("group",)}, {"group": ("set",)}])
    monkeypatch.setattr(serving, "_ptg2_npi_member_id", lambda _npi: "member")
    monkeypatch.setattr(serving, "_shared_graph_members_by_id", reader)
    session, tables = object(), SimpleNamespace(uses_v4_graph=False)
    assert await memberships.bounded_source_provider_sets(session, tables, (101,), 7) == {101: ("set",)}
    assert [call.args[2] for call in reader.await_args_list] == ["provider_npi_group", "provider_inverted"]
    assert all(call.kwargs == {"max_members": 7, "max_projection_members": 7} for call in reader.await_args_list)
    reader.side_effect = ManifestReadLimitError("graph memberships exceed their bounded limit")
    with pytest.raises(PlanPricingProjectionUnsupported, match="membership bound"):
        await memberships.bounded_source_provider_sets(session, tables, (101,), 7)


@pytest.mark.asyncio
async def test_v4_reverse_expansion_is_bounded_before_dictionary_hydration(monkeypatch):
    reader = AsyncMock(return_value={101: (1,)})
    dictionary = AsyncMock(return_value={1: "set"})
    monkeypatch.setattr(serving, "_v4_sets_by_npi", reader)
    monkeypatch.setattr(serving, "_provider_set_ids_for_keys", dictionary)
    session, tables = object(), SimpleNamespace(uses_v4_graph=True)
    assert await memberships.bounded_source_provider_sets(session, tables, (101,), 7) == {101: ("set",)}
    reader.assert_awaited_once_with(session, tables, (101,), max_members=7, max_projection_members=7)
    dictionary.reset_mock()
    reader.side_effect = serving.PTG2SharedBlockError("PTG V4 graph selection exceeds max_members")
    with pytest.raises(PlanPricingProjectionUnsupported, match="membership bound"):
        await memberships.bounded_source_provider_sets(session, tables, (101,), 7)
    dictionary.assert_not_awaited()


def test_legacy_shared_groups_cannot_multiply_beyond_final_membership_budget():
    with pytest.raises(PlanPricingProjectionUnsupported, match="membership bound"):
        memberships._bounded_sets_by_npi(
            (101, 102), {101: ("group",), 102: ("group",)}, {"group": ("set-a", "set-b")}, 3
        )
