from unittest.mock import AsyncMock

import pytest

from api import ptg2_serving as serving
from tests.test_ptg2_v4_serving_exact_paths import _tables


@pytest.mark.asyncio
async def test_v4_selected_npi_memberships_are_cached_and_fail_closed(
    monkeypatch,
) -> None:
    serving._PTG2_PROVIDER_SET_IDS_BY_NPI_CACHE.clear()
    graph_lookup = AsyncMock(
        return_value={1234567890: (3, 5), 2234567890: ()}
    )
    dictionary = AsyncMock(return_value={3: "31" * 16, 5: "51" * 16})
    monkeypatch.setattr(serving, "_v4_sets_by_npi", graph_lookup)
    monkeypatch.setattr(serving, "_provider_set_ids_for_keys", dictionary)

    expected_ids_by_npi = {
        1234567890: ("31" * 16, "51" * 16),
        2234567890: (),
    }
    assert await serving._provider_set_ids_for_selected_npis(
        object(), _tables(), (1234567890, 2234567890)
    ) == expected_ids_by_npi
    assert await serving._provider_set_ids_for_selected_npis(
        object(), _tables(), (1234567890, 2234567890)
    ) == expected_ids_by_npi
    graph_lookup.assert_awaited_once()

    serving._PTG2_PROVIDER_SET_IDS_BY_NPI_CACHE.clear()
    dictionary.return_value = {3: "31" * 16}
    with pytest.raises(serving.PTG2ManifestArtifactError, match="missing provider-set"):
        await serving._provider_set_ids_for_selected_npis(
            object(), _tables(), (1234567890,)
        )


@pytest.mark.asyncio
async def test_v4_code_scoped_npi_membership_bypasses_unscoped_cache(
    monkeypatch,
) -> None:
    """A CPT scope must reach only allowed sets and never poison global cache."""

    serving._PTG2_PROVIDER_SET_IDS_BY_NPI_CACHE.clear()
    graph_lookup = AsyncMock(return_value={1234567890: (3,)})
    dictionary = AsyncMock(return_value={3: "31" * 16})
    monkeypatch.setattr(serving, "_v4_sets_by_npi", graph_lookup)
    monkeypatch.setattr(serving, "_provider_set_ids_for_keys", dictionary)
    allowed_keys = frozenset({3, 5})

    for _attempt in range(2):
        assert await serving._provider_set_ids_for_selected_npis(
            object(),
            _tables(),
            (1234567890,),
            allowed_provider_set_keys=allowed_keys,
        ) == {1234567890: ("31" * 16,)}

    assert graph_lookup.await_count == 2
    assert all(
        call.kwargs["allowed_provider_set_keys"] == allowed_keys
        for call in graph_lookup.await_args_list
    )
    assert serving._PTG2_PROVIDER_SET_IDS_BY_NPI_CACHE == {}
