# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from api import ptg2_serving as serving
from tests.ptg2_serving_coverage_paydown_support import strict_v3_tables


_PROVIDER_ID = "00000000000000000000000000000007"


def _tables():
    return strict_v3_tables(
        price_dictionary_item_count=64,
        price_dictionary_block_bytes=4096,
        coverage_scope_id="coverage-scope",
        plan_id="plan-a",
        plan_market_type="group",
    )


async def _raises_manifest_error(match, awaitable):
    with pytest.raises(serving.PTG2ManifestArtifactError, match=match):
        await awaitable


def _raises_sync_manifest_error(match, callback):
    with pytest.raises(serving.PTG2ManifestArtifactError, match=match):
        callback()


def test_v4_component_groups_and_projection_budget_boundaries():
    sources = serving._V4SetGroupSources({}, {7: (17,)})
    assert serving._merge_v4_source_groups(7, sources, {}, {17: (3, 2)}) == (2, 3)
    _raises_sync_manifest_error(
        "exceeds max_members",
        lambda: serving._projected_members_by_owner(
            (7, 8), {7: (17,), 8: (18,)}, {17: (1,), 18: (2,)}, 1
        ),
    )


@pytest.mark.asyncio
async def test_v4_group_source_empty_and_incomplete_boundaries(monkeypatch):
    assert (
        await serving._v4_groups_via_sources(
            object(),
            snapshot_key=41,
            provider_set_keys=(),
            max_members=5,
            maximum_pattern_degree=5,
            maximum_component_degree=5,
        )
        == {}
    )
    group_sources = serving._V4SetGroupSources({7: (17,)}, {})
    monkeypatch.setattr(
        serving,
        "_load_v4_pattern_set_group_sources",
        AsyncMock(return_value=group_sources),
    )
    monkeypatch.setattr(
        serving, "_load_v4_source_groups", AsyncMock(return_value=({}, {}))
    )
    await _raises_manifest_error(
        "relation is incomplete",
        serving._v4_groups_via_sources(
            object(),
            snapshot_key=41,
            provider_set_keys=(7,),
            max_members=5,
            maximum_pattern_degree=5,
            maximum_component_degree=5,
        ),
    )


@pytest.mark.asyncio
async def test_v4_group_source_projection_enforces_aggregate_budget(monkeypatch):
    sources = serving._V4SetGroupSources({7: (17,), 8: (18,)}, {})
    monkeypatch.setattr(
        serving,
        "_load_v4_pattern_set_group_sources",
        AsyncMock(return_value=sources),
    )
    monkeypatch.setattr(
        serving,
        "_load_v4_source_groups",
        AsyncMock(return_value=({17: (1,), 18: (2,)}, {})),
    )
    await _raises_manifest_error(
        "exceeds max_members",
        serving._v4_groups_via_sources(
            object(),
            snapshot_key=41,
            provider_set_keys=(7, 8),
            max_members=1,
            maximum_pattern_degree=5,
            maximum_component_degree=5,
        ),
    )


@pytest.mark.asyncio
async def test_legacy_graph_owner_rejects_malformed_npi():
    await _raises_manifest_error(
        "NPI graph owner is malformed",
        serving._legacy_graph_owner_keys(
            object(),
            _tables(),
            serving.PTG2_V3_GRAPH_NPI_TO_GROUP,
            ["not-a-member-id"],
        ),
    )


@pytest.mark.asyncio
async def test_forward_code_rows_skip_missing_dictionary_key(monkeypatch):
    lookup = AsyncMock()
    monkeypatch.setattr(
        serving,
        "_version_three_provider_counts_for_keys",
        AsyncMock(return_value=None),
    )
    monkeypatch.setattr(serving, "_lookup_shared_forward_rows", lookup)
    assert (
        await serving._shared_forward_entries_for_code_rows(object(), _tables(), ({},))
        == []
    )
    lookup.assert_not_awaited()


@pytest.mark.asyncio
async def test_rate_provider_set_scope_empty_boundaries(monkeypatch):
    assert (
        await serving._shared_rate_provider_set_keys(
            object(),
            _tables(),
            plan_id="",
            reported_code="70553",
            code_system="CPT",
        )
        == ()
    )
    monkeypatch.setattr(serving, "_shared_rate_code_rows", AsyncMock(return_value=[]))
    assert (
        await serving._shared_rate_provider_set_keys(
            object(),
            _tables(),
            plan_id="plan-a",
            reported_code="70553",
            code_system="CPT",
        )
        == ()
    )


@pytest.mark.asyncio
async def test_v4_npi_membership_records_cold_exact_fallback(monkeypatch):
    record = Mock()
    cold_lookup = AsyncMock(return_value={_PROVIDER_ID: ()})
    monkeypatch.setattr(serving, "_require_strict_shared_v3", Mock())
    monkeypatch.setattr(
        serving, "_v4_hot_prefix_limits", Mock(return_value=SimpleNamespace(target=1))
    )
    monkeypatch.setattr(serving, "record_v4_cold_exact_request", record)
    monkeypatch.setattr(serving, "_cold_provider_npi_member_ids_by_set", cold_lookup)
    tables = SimpleNamespace(uses_v4_graph=True)
    assert await serving._provider_npi_member_ids_by_set(
        object(), tables, (_PROVIDER_ID,), limit_per_set=2
    ) == {_PROVIDER_ID: ()}
    record.assert_called_once_with()
    cold_lookup.assert_awaited_once()
