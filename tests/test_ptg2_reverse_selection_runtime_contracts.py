# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Exact-state contracts for bounded provider-to-procedure reverse reads."""

from unittest.mock import AsyncMock, Mock

import pytest

from api import ptg2_serving as serving
from tests.ptg2_serving_coverage_paydown_support import strict_v3_tables


_PROVIDER_SET_ID = "01" * 16
_PRICE_SET_IDS = ("02" * 16, "03" * 16, "04" * 16)


def _reverse_query(*, limit=1, code_value=""):
    return serving._VersionThreeReverseQuery(
        provider_set_ids=(_PROVIDER_SET_ID,),
        requested_plan="synthetic-plan",
        code_value=code_value,
        code_system="CPT" if code_value else None,
        q_text="",
        code_context=None,
        source_trace_set_hash=None,
        network_names=[],
        limit=limit,
        offset=0,
        apply_window=True,
    )


def _reverse_scope(*, exact=False):
    return serving._VersionThreeReverseScope(
        provider_set_id_by_key={7: _PROVIDER_SET_ID},
        candidate_code_keys=(11,),
        exact_code_metadata_rows=({"code_key": 11},) if exact else None,
    )


def _candidate_row(price_index=0):
    return {
        "provider_set_global_id_128": _PROVIDER_SET_ID,
        "price_set_global_id_128": _PRICE_SET_IDS[price_index],
        "price_key": price_index + 1,
    }


@pytest.mark.asyncio
async def test_reverse_selection_handles_zero_window_and_projected_page(
    monkeypatch,
):
    """Avoid reads for a zero window and reuse an exact projected page."""

    empty = await serving._version_three_reverse_selection(
        object(), strict_v3_tables(), _reverse_query(limit=0)
    )
    projected = serving._VersionThreeReverseSelection(
        rows=(_candidate_row(),),
        exhausted=False,
    )
    monkeypatch.setattr(
        serving,
        "_version_three_reverse_page_selection",
        AsyncMock(return_value=projected),
    )
    selected = await serving._version_three_reverse_selection(
        object(), strict_v3_tables(), _reverse_query()
    )

    assert empty == serving._VersionThreeReverseSelection((), False)
    assert selected is projected


@pytest.mark.asyncio
async def test_reverse_selection_preserves_empty_unavailable_and_full_batches(
    monkeypatch,
):
    """Distinguish empty scope, unavailable metadata, exhaustion, and a full page."""

    monkeypatch.setattr(
        serving,
        "_version_three_reverse_page_selection",
        AsyncMock(return_value=None),
    )
    reverse_scope = AsyncMock(side_effect=(None, _reverse_scope(), _reverse_scope(), _reverse_scope()))
    monkeypatch.setattr(serving, "_version_three_reverse_scope", reverse_scope)
    candidate_batch = AsyncMock(
        side_effect=(
            None,
            ([], 0),
            (([_candidate_row()],), 1),
        )
    )
    monkeypatch.setattr(serving, "_version_three_candidate_batch", candidate_batch)

    empty = await serving._version_three_reverse_selection(
        object(), strict_v3_tables(), _reverse_query()
    )
    with pytest.raises(serving.PTG2ManifestArtifactError, match="metadata is unavailable"):
        await serving._version_three_reverse_selection(
            object(), strict_v3_tables(), _reverse_query()
        )
    exhausted = await serving._version_three_reverse_selection(
        object(), strict_v3_tables(), _reverse_query()
    )
    full = await serving._version_three_reverse_selection(
        object(), strict_v3_tables(), _reverse_query()
    )

    assert empty == serving._VersionThreeReverseSelection((), True, 0)
    assert exhausted == serving._VersionThreeReverseSelection((), True, 0)
    assert full.rows == (_candidate_row(),)
    assert full.exhausted is False


@pytest.mark.asyncio
async def test_filtered_reverse_selection_handles_empty_and_unavailable_scope(
    monkeypatch,
):
    """Return exact empty for no scope and reject unavailable metadata."""

    scope = AsyncMock(side_effect=(None, _reverse_scope()))
    monkeypatch.setattr(serving, "_version_three_reverse_scope", scope)
    monkeypatch.setattr(
        serving,
        "_version_three_candidate_batch",
        AsyncMock(return_value=None),
    )

    empty = await serving._version_three_filtered_reverse_selection(
        object(), strict_v3_tables(), _reverse_query(), {}, offset=0, limit=1
    )
    with pytest.raises(serving.PTG2ManifestArtifactError, match="metadata is unavailable"):
        await serving._version_three_filtered_reverse_selection(
            object(), strict_v3_tables(), _reverse_query(), {}, offset=0, limit=1
        )

    assert empty == serving._VersionThreeFilteredReverseSelection(
        rows=(),
        prices_by_price_set={},
        exhausted=True,
        matched_rows_seen=0,
        total_row_count=0,
    )


@pytest.mark.asyncio
async def test_filtered_reverse_selection_handles_empty_batch_and_page_window(
    monkeypatch,
):
    """Prove empty metadata and preserve filtered offset plus sentinel order."""

    monkeypatch.setattr(
        serving,
        "_version_three_reverse_scope",
        AsyncMock(return_value=_reverse_scope(exact=True)),
    )
    candidate_rows = [_candidate_row(index) for index in range(3)]
    candidate_batch = AsyncMock(
        side_effect=(([], 0), ((candidate_rows,), 3))
    )
    monkeypatch.setattr(serving, "_version_three_candidate_batch", candidate_batch)
    monkeypatch.setattr(
        serving,
        "_prices_for_price_sets",
        AsyncMock(
            return_value={
                price_set_id: [{"negotiated_rate": index + 1}]
                for index, price_set_id in enumerate(_PRICE_SET_IDS)
            }
        ),
    )
    price_filter = Mock(
        side_effect=([], [{"negotiated_rate": 2}], [{"negotiated_rate": 3}])
    )
    monkeypatch.setattr(serving, "_ptg2_manifest_filter_prices", price_filter)

    empty = await serving._version_three_filtered_reverse_selection(
        object(), strict_v3_tables(), _reverse_query(code_value="00001"), {},
        offset=0,
        limit=1,
    )
    selected = await serving._version_three_filtered_reverse_selection(
        object(), strict_v3_tables(), _reverse_query(code_value="00001"), {},
        offset=1,
        limit=1,
    )

    assert empty.total_row_count == 0
    assert selected.rows == (_candidate_row(2),)
    assert selected.matched_rows_seen == 2
    assert selected.exhausted is False


@pytest.mark.asyncio
async def test_filtered_reverse_zero_limit_stops_without_retaining_row(
    monkeypatch,
):
    """Honor a zero result limit after proving the first matching occurrence."""

    monkeypatch.setattr(
        serving,
        "_version_three_reverse_scope",
        AsyncMock(return_value=_reverse_scope(exact=True)),
    )
    monkeypatch.setattr(
        serving,
        "_version_three_candidate_batch",
        AsyncMock(return_value=(([_candidate_row()],), 1)),
    )
    monkeypatch.setattr(
        serving,
        "_prices_for_price_sets",
        AsyncMock(return_value={_PRICE_SET_IDS[0]: [{"negotiated_rate": 1}]}),
    )
    monkeypatch.setattr(
        serving,
        "_ptg2_manifest_filter_prices",
        Mock(return_value=[{"negotiated_rate": 1}]),
    )

    selected = await serving._version_three_filtered_reverse_selection(
        object(), strict_v3_tables(), _reverse_query(code_value="00001"), {},
        offset=0,
        limit=0,
    )

    assert selected.rows == ()
    assert selected.matched_rows_seen == 1
    assert selected.exhausted is False


@pytest.mark.asyncio
async def test_reverse_scope_rejects_missing_set_and_code_coordinates(monkeypatch):
    """Distinguish no sets, an incomplete dictionary, and no code membership."""

    provider_keys = AsyncMock(
        side_effect=(
            {},
            {_PROVIDER_SET_ID: 7},
            {_PROVIDER_SET_ID: 7},
        )
    )
    monkeypatch.setattr(serving, "_provider_set_keys_for_ids", provider_keys)
    monkeypatch.setattr(
        serving,
        "_version_three_scope_code_keys",
        AsyncMock(return_value=((), None)),
    )

    assert await serving._version_three_reverse_scope(
        object(), strict_v3_tables(), _reverse_query()
    ) is None
    missing_query = serving._VersionThreeReverseQuery(
        **{
            **_reverse_query().__dict__,
            "provider_set_ids": (_PROVIDER_SET_ID, "05" * 16),
        }
    )
    with pytest.raises(serving.PTG2ManifestArtifactError, match="missing a referenced provider set"):
        await serving._version_three_reverse_scope(
            object(), strict_v3_tables(), missing_query
        )
    assert await serving._version_three_reverse_scope(
        object(), strict_v3_tables(), _reverse_query()
    ) is None
