# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Reverse-membership and projection contracts for provider graph serving."""

from unittest.mock import AsyncMock

import pytest

from api import ptg2_serving as serving
from tests.ptg2_serving_coverage_paydown_support import strict_v3_tables


_PROVIDER_SET_ID = "01" * 16


def _rate_row() -> dict[str, object]:
    return {
        "provider_set_global_id_128": _PROVIDER_SET_ID,
        "serving_content_hash_128": "02" * 16,
        "reported_code_system": "CPT",
        "reported_code": "00001",
        "negotiation_arrangement": "FFS",
        "source_key": 7,
    }


@pytest.mark.asyncio
async def test_v4_reverse_membership_requires_complete_set_dictionary(monkeypatch):
    """Translate every V4 graph key or reject the reverse membership proof."""

    tables = strict_v3_tables(
        storage_generation="shared_blocks_v4",
        shared_block_layout="packed_snapshot_maps_v4",
    )
    monkeypatch.setattr(
        serving,
        "_v4_sets_by_npi",
        AsyncMock(return_value={1234567890: (7,)}),
    )
    set_ids = AsyncMock(side_effect=({}, {7: _PROVIDER_SET_ID}))
    monkeypatch.setattr(serving, "_provider_set_ids_for_keys", set_ids)

    with pytest.raises(serving.PTG2ManifestArtifactError, match="missing provider-set"):
        await serving._provider_sets_from_membership_graph(
            object(), tables, 1234567890
        )
    resolved = await serving._provider_sets_from_membership_graph(
        object(), tables, 1234567890
    )

    assert resolved == (_PROVIDER_SET_ID,)


@pytest.mark.asyncio
async def test_v3_reverse_membership_accepts_proven_no_groups(monkeypatch):
    """Return exact emptiness when the V3 NPI graph has no group owners."""

    monkeypatch.setattr(
        serving,
        "_shared_graph_members_for_id",
        AsyncMock(return_value=()),
    )

    assert await serving._provider_sets_from_membership_graph(
        object(), strict_v3_tables(), 1234567890
    ) == ()


@pytest.mark.asyncio
async def test_filtered_prefix_reuses_memberships_and_deduplicates_rank_keys(
    monkeypatch,
):
    """Read each set once and retain first-seen provider rank order."""

    filtered_npis = AsyncMock(return_value=(1234567890,))
    monkeypatch.setattr(
        serving,
        "_filtered_provider_npis_for_expansion_set",
        filtered_npis,
    )
    rate_row = _rate_row()
    rank_by_key, selected_npis, selected_sets = (
        await serving._rank_filtered_provider_expansion_prefix(
            object(),
            strict_v3_tables(),
            serving._FilteredProviderExpansionRequest(
                row_data=[rate_row, dict(rate_row)],
                args={},
                target_count=2,
                npis_by_set={},
            ),
        )
    )

    assert len(rank_by_key) == 1
    assert selected_npis == (1234567890,)
    assert selected_sets == (_PROVIDER_SET_ID,)
    filtered_npis.assert_awaited_once()


@pytest.mark.asyncio
async def test_filtered_prefix_rejects_missing_set_and_stops_at_target(
    monkeypatch,
):
    """Require set identity and stop after the requested distinct NPI prefix."""

    with pytest.raises(serving.PTG2ManifestArtifactError, match="missing its provider-set"):
        await serving._rank_filtered_provider_expansion_prefix(
            object(),
            strict_v3_tables(),
            serving._FilteredProviderExpansionRequest(
                row_data=[{}], args={}, target_count=1, npis_by_set={}
            ),
        )
    fail_lookup = AsyncMock(side_effect=AssertionError("cached set must not reload"))
    monkeypatch.setattr(
        serving,
        "_filtered_provider_npis_for_expansion_set",
        fail_lookup,
    )
    rank_by_key, selected_npis, _selected_sets = (
        await serving._rank_filtered_provider_expansion_prefix(
            object(),
            strict_v3_tables(),
            serving._FilteredProviderExpansionRequest(
                row_data=[_rate_row()],
                args={},
                target_count=2,
                npis_by_set={_PROVIDER_SET_ID: (1234567890, 1234567891)},
            ),
        )
    )

    assert len(rank_by_key) == 2
    assert selected_npis == (1234567890, 1234567891)
    fail_lookup.assert_not_awaited()


@pytest.mark.parametrize(
    ("address_payload", "expected"),
    [
        ("not-json", False),
        (["not", "an", "object"], False),
        ({"first_line": "1 Test Way"}, True),
    ],
)
def test_street_address_payload_requires_valid_object(address_payload, expected):
    """Accept only parseable object-shaped addresses with a street line."""

    assert serving._has_street_address_payload(address_payload) is expected
