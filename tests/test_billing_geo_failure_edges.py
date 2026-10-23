# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Unit admission failures around the indexed billing geography caller."""

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from api import ptg2_billing_geo_reader as geo
from api import ptg2_serving as serving
from api import ptg2_v4_graph as graph
from api import ptg_membership_reader as reader
from tests.test_ptg2_indexed_geo_scope import _production_tables, _ready, _request


@pytest.fixture
def geo_case(monkeypatch):
    _ready(monkeypatch)
    tables = _production_tables()
    locations = AsyncMock(return_value=[{"npi": 1000000001, "_ptg_source_exhausted": True}])
    monkeypatch.setattr(serving, "_indexed_geo_location_rows", locations)
    return SimpleNamespace(
        session=object(),
        tables=tables,
        request=_request(),
        budget=serving._geo_rate_selection_budget(tables),
        limits=serving._v4_geo_rate_forward_limits(tables),
        locations=locations,
    )


async def test_missing_location_context_never_executes_sql(monkeypatch):
    lookup = AsyncMock(return_value=None)
    execute = AsyncMock()
    finalize = AsyncMock()
    monkeypatch.setattr(serving, "_membership_location_query", lookup)
    monkeypatch.setattr(serving, "_execute_membership_location_sql", execute)
    monkeypatch.setattr(serving, "_finalize_location_rows", finalize)
    session, tables, args_by_name = object(), object(), {"zip5": "00001"}
    assert await geo.query_indexed_membership_locations(session, tables, args_by_name, object(), (7,), limit=4) is None
    lookup.assert_awaited_once_with(session, tables, args_by_name, candidate_npis=None, limit=4, offset=0)
    execute.assert_not_awaited()
    finalize.assert_not_awaited()


async def test_exhausted_candidate_budget_precedes_membership_and_address_io(monkeypatch, geo_case):
    case = geo_case
    case.budget.candidate_members = case.budget.maximum_candidate_members
    membership = AsyncMock()
    monkeypatch.setattr(reader, "indexed_memberships", membership)
    with pytest.raises(serving.PTG2OnlineWorkBudgetExceeded):
        await geo.indexed_geo_provider_sets(case.session, case.tables, case.request, case.budget, case.limits)
    membership.assert_not_awaited()
    case.locations.assert_not_awaited()
    assert case.budget.reverse_geo_scope is None


async def test_unavailable_locations_restore_projection_context_without_admission(geo_case):
    case = geo_case
    case.locations.return_value = None
    previous = graph._ACTIVE_V4_TAXONOMY_WORK.get()
    assert (
        await geo.indexed_geo_provider_sets(case.session, case.tables, case.request, case.budget, case.limits) is None
    )
    assert graph._ACTIVE_V4_TAXONOMY_WORK.get() is previous
    assert case.budget.candidate_members == 0 and case.budget.reverse_geo_scope is None
    assert case.budget.reverse_provider_set_keys_by_npi == {}


async def test_taxonomy_filter_precedes_final_membership_and_retains_only_matches(monkeypatch, geo_case):
    case = geo_case
    case.request.args["provider_sex_code"] = "F"
    case.locations.return_value += [{"npi": 1000000002, "_ptg_source_exhausted": True}]
    taxonomy = AsyncMock(return_value=(1000000002,))
    monkeypatch.setattr(serving, "_filter_npis_by_taxonomy", taxonomy)
    previous = graph._ACTIVE_V4_TAXONOMY_WORK.get()
    result = await geo.indexed_geo_provider_sets(case.session, case.tables, case.request, case.budget, case.limits)
    taxonomy.assert_awaited_once_with(case.session, case.request.args, (1000000001, 1000000002), limit=2)
    assert result == (11,) and case.budget.candidate_members == 2
    assert case.budget.reverse_provider_set_keys_by_npi == {1000000002: (11,)}
    assert case.budget.reverse_geo_scope == ((1000000002,), True, case.budget.maximum_candidate_members)
    assert graph._ACTIVE_V4_TAXONOMY_WORK.get() is previous


@pytest.mark.parametrize("membership_by_npi", [None, {}, {1000000002: (11,)}])
async def test_lost_membership_attachment_cannot_publish_scope(monkeypatch, geo_case, membership_by_npi):
    case = geo_case
    original = reader.indexed_memberships

    async def changed_attachment(session, tables, npis, request, state):
        if not npis:
            return await original(session, tables, npis, request, state)
        assert npis == (1000000001,)
        return membership_by_npi

    monkeypatch.setattr(reader, "indexed_memberships", changed_attachment)
    previous = graph._ACTIVE_V4_TAXONOMY_WORK.get()
    with pytest.raises(serving.PTG2ManifestArtifactError, match="lost its complete attachment"):
        await geo.indexed_geo_provider_sets(case.session, case.tables, case.request, case.budget, case.limits)
    assert graph._ACTIVE_V4_TAXONOMY_WORK.get() is previous
    assert case.budget.reverse_geo_scope is None and case.budget.reverse_provider_set_keys_by_npi == {}


async def test_provider_set_overflow_cannot_publish_complete_scope(monkeypatch, geo_case):
    case = geo_case
    case.budget.caps = serving.replace(
        case.budget.caps, maximum_rate_rows=1, rate_page_rows=1, maximum_provider_sets=1, maximum_graph_batches=1
    )

    async def pairs(_session, _tables, npis, _request, _state, _ownership, _code_keys):
        return (
            {
                "first_count": len(npis),
                "priced_count": len(npis),
                "pairs": [[npi, key] for npi in npis for key in (11, 12)],
            },
            100,
            100,
            len(npis),
        )

    monkeypatch.setattr(reader, "_membership_pairs", pairs)
    previous = graph._ACTIVE_V4_TAXONOMY_WORK.get()
    with pytest.raises(serving.PTG2OnlineWorkBudgetExceeded):
        await geo.indexed_geo_provider_sets(case.session, case.tables, case.request, case.budget, case.limits)
    assert case.budget.reverse_geo_scope is None and case.budget.candidate_members == 1
    assert graph._ACTIVE_V4_TAXONOMY_WORK.get() is previous
